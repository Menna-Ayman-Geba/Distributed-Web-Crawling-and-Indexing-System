from mpi4py import MPI
import boto3
import requests
from bs4 import BeautifulSoup
import tempfile
import os
import logging
import sys
import time  # Added for crawl delay

# --- Safe logging setup ---
class MPIRankFilter(logging.Filter):
    def __init__(self, rank):
        self.rank = rank
        super().__init__()

    def filter(self, record):
        record.rank = self.rank
        return True

def setup_logger(rank):
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [Crawler %(rank)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logger = logging.getLogger(f"crawler_{rank}")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.addFilter(MPIRankFilter(rank))
    logger.propagate = False

    return logger

# --- MPI Setup ---
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# --- Logger Initialization ---
logger = setup_logger(rank)

# --- S3 Setup ---
s3_client = boto3.client('s3')
BUCKET_NAME = 'cse35400-bucket'

# --- Crawl Delay ---
CRAWL_DELAY = 2  # seconds

def crawler_node():
    logger.info("Starting crawler")

    # Receive URLs from Master
    urls = comm.recv(source=0, tag=0)
    logger.info(f"Received {len(urls)} URLs")

    for i, url in enumerate(urls):
        html_tmp_path = txt_tmp_path = None
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                html_content = response.text

                # Save HTML
                with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as html_tmp:
                    html_tmp.write(html_content.encode('utf-8'))
                    html_tmp_path = html_tmp.name

                html_s3_key = f"crawl_data/page_{rank}_{i}.html"
                if os.path.exists(html_tmp_path):
                    s3_client.upload_file(html_tmp_path, BUCKET_NAME, html_s3_key)
                    logger.info(f"Uploaded HTML: {html_s3_key}")
                else:
                    logger.warning(f"Missing HTML file: {html_tmp_path}")

                # Extract and save text
                soup = BeautifulSoup(html_content, "html.parser")
                text_content = soup.get_text(separator='\n')

                with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as txt_tmp:
                    txt_tmp.write(text_content.encode('utf-8'))
                    txt_tmp_path = txt_tmp.name

                txt_s3_key = f"crawl_data/page_{rank}_{i}.txt"
                if os.path.exists(txt_tmp_path):
                    s3_client.upload_file(txt_tmp_path, BUCKET_NAME, txt_s3_key)
                    logger.info(f"Uploaded TXT: {txt_s3_key}")
                else:
                    logger.warning(f"Missing TXT file: {txt_tmp_path}")

            else:
                logger.error(f"Failed to fetch {url}: Status {response.status_code}")
        except Exception as e:
            logger.exception(f"Error fetching {url}: {e}")
        finally:
            if html_tmp_path and os.path.exists(html_tmp_path):
                os.remove(html_tmp_path)
            if txt_tmp_path and os.path.exists(txt_tmp_path):
                os.remove(txt_tmp_path)

        # --- Politeness: Crawl Delay ---
        logger.info(f"Waiting for {CRAWL_DELAY} seconds before next request")
        time.sleep(CRAWL_DELAY)

    comm.send("done", dest=0, tag=1)
    logger.info("Finished crawling")

if __name__ == "__main__":
    crawler_node()
