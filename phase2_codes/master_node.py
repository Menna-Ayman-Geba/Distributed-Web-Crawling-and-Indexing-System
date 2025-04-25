#!/usr/bin/env python3
from mpi4py import MPI
import sys
import logging
import boto3
import json

# Setup logging
def setup_logging():
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(processName)s] - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logging.basicConfig(level=logging.INFO, handlers=[console_handler])

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# S3 setup
s3_client = boto3.client('s3')
BUCKET_NAME = 'cse35400-bucket'

def master_node():
    logging.info(f"Master Node (Rank {rank}) starting...")

    # Download seed URLs from S3
    urls_s3_path = 'seed_url/seed_urls.txt'
    local_urls_file = 'seed_urls.txt'
    try:
        s3_client.download_file(BUCKET_NAME, urls_s3_path, local_urls_file)
    except Exception as e:
        logging.error(f"Error downloading seed URL file: {e}")
        sys.exit(1)

    # Read and clean URLs
    with open(local_urls_file, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]
    
    if not urls:
        logging.error("Error: No URLs found in seed file.")
        sys.exit(1)

    # Distribute URLs to crawler nodes
    crawler_ranks = [1, 2]
    url_per_crawler = len(urls) // len(crawler_ranks)

    for i, crawler_rank in enumerate(crawler_ranks):
        start_idx = i * url_per_crawler
        end_idx = (i + 1) * url_per_crawler if i < len(crawler_ranks) - 1 else len(urls)
        crawler_urls = urls[start_idx:end_idx]
        comm.send(crawler_urls, dest=crawler_rank, tag=0)
        logging.info(f"Master sent {len(crawler_urls)} URLs to Crawler {crawler_rank}")

    # Collect combined mapping of .html and .txt
    url_mapping = {}
    for crawler_rank in crawler_ranks:
        mappings = comm.recv(source=crawler_rank, tag=1)  # Expect a dictionary
        url_mapping.update(mappings)
        logging.info(f"Received {len(mappings)} mappings from Crawler {crawler_rank}")

    # Save combined mapping to S3
    mapping_file = 'url_mapping.json'
    with open(mapping_file, 'w') as f:
        json.dump(url_mapping, f)
    s3_client.upload_file(mapping_file, BUCKET_NAME, 'crawl_data/url_mapping.json')
    logging.info("Uploaded combined URL mapping to S3")

    # Signal indexer to start
    comm.send("start", dest=3, tag=0)
    logging.info("Master signaled Indexer to start.")

# Main execution
if __name__ == "__main__":
    setup_logging()
    if size != 4:
        if rank == 0:
            logging.error("Error: Please run with exactly 4 processes (e.g., mpiexec -n 4 python3 master_node.py)")
        sys.exit(1)
    
    if rank == 0:
        master_node()
    elif rank in [1, 2]:
        from crawler_node import crawler_node
        crawler_node()
    elif rank == 3:
        from indexer_node import indexer_node
        indexer_node()
    else:
        logging.error(f"Rank {rank} has no assigned role.")
