from mpi4py import MPI
import boto3
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT
import os
import shutil
import logging
import sys

# --- MPI Setup ---
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# --- Logging Setup ---
class MPIRankFilter(logging.Filter):
    def __init__(self, rank):
        self.rank = rank
        super().__init__()

    def filter(self, record):
        record.rank = self.rank
        return True

def setup_logger(rank):
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [Indexer %(rank)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger = logging.getLogger(f"indexer_{rank}")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.addFilter(MPIRankFilter(rank))
    logger.propagate = False
    return logger

logger = setup_logger(rank)

# --- S3 Setup ---
s3_client = boto3.client('s3')
BUCKET_NAME = 'cse35400-bucket'

def indexer_node():
    logger.info("Indexer node starting...")

    # Wait for signal from Master
    signal = comm.recv(source=0, tag=0)
    logger.info(f"Received signal from Master: {signal}")

    # Create Whoosh schema and index
    schema = Schema(title=TEXT(stored=True), content=TEXT(stored=True))  # Make content stored
    if os.path.exists("indexdir"):
        shutil.rmtree("indexdir")
    os.makedirs("indexdir", exist_ok=True)
    ix = create_in("indexdir", schema)

    # List and index crawled files from S3
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="crawl_data/")
    if 'Contents' not in response:
        logger.warning("No crawled files found in S3.")
        return

    writer = ix.writer()
    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.txt'):  # Only index text files (ignore mapping file)
            local_file = 'temp_crawled.txt'
            try:
                s3_client.download_file(BUCKET_NAME, key, local_file)
                with open(local_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                writer.add_document(title=key, content=content)
                logger.info(f"Indexed file: {key}")
            except Exception as e:
                logger.error(f"Failed to index {key}: {e}")
            finally:
                if os.path.exists(local_file):
                    os.remove(local_file)

    writer.commit()
    logger.info("Committed all documents to index")

    # Upload index files to S3
    for root, dirs, files in os.walk("indexdir"):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = f"index_data/{file}"
            try:
                s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
                logger.info(f"Uploaded index file to S3: {s3_key}")
            except Exception as e:
                logger.error(f"Failed to upload {s3_key}: {e}")

    logger.info("Indexer finished and cleaned up")

if __name__ == "__main__":
    indexer_node()