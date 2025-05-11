#!/usr/bin/env python3
import boto3
import json
import os
import tempfile
import re
from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser
from whoosh.fields import Schema, TEXT
from collections import defaultdict
import subprocess
import sys
from urllib.parse import urlparse
from mpi4py import MPI
import logging

# AWS Setup
s3_client = boto3.client('s3', region_name='eu-north-1')
sts_client = boto3.client('sts')
BUCKET_NAME = 'cse354000-bucket'
INDEX_PREFIX = 'index_data/'
MAPPING_FILE = 'url_mapping.json'
SEED_URLS_PATH = 'seed_urls/seed_urls.txt'

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Client] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def check_aws_identity():
    """Verify AWS credentials and print identity"""
    try:
        identity = sts_client.get_caller_identity()
        logger.info(f"AWS Identity: UserId={identity['UserId']}, Account={identity['Account']}, Arn={identity['Arn']}")
        return True
    except Exception as e:
        logger.error(f"Failed to verify AWS identity: {e}")
        return False

def validate_url(url):
    """Validate URL format"""
    try:
        parsed = urlparse(url.strip())
        return parsed.scheme in ['http', 'https'] and parsed.netloc
    except:
        return False

def submit_urls():
    """Allow client to submit seed URLs and upload to S3"""
    logger.info("Enter seed URLs (one per line, empty line to finish):")
    urls = []
    while True:
        url = input("URL: ").strip()
        if not url:
            break
        if validate_url(url):
            urls.append(url)
        else:
            logger.warning(f"Invalid URL: {url}. Skipping.")
    
    if not urls:
        logger.error("No valid URLs provided.")
        return False
    
    # Save URLs to a temporary file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as temp_file:
        for url in urls:
            temp_file.write(url + '\n')
        temp_file_path = temp_file.name
    
    try:
        # Upload to S3
        s3_client.upload_file(temp_file_path, BUCKET_NAME, SEED_URLS_PATH)
        logger.info(f"Uploaded {len(urls)} seed URLs to S3: {SEED_URLS_PATH}")
        os.remove(temp_file_path)
        return True
    except Exception as e:
        logger.error(f"Failed to upload seed URLs to S3: {e}")
        os.remove(temp_file_path)
        return False

def run_master_node(num_crawlers=2, max_depth=2):
    """Run Master.py using MPI"""
    try:
        logger.info(f"Starting master node with num_crawlers={num_crawlers}, max_depth={max_depth}")
        # Run Master.py with MPI
        cmd = [
            'mpirun', '-np', '1',
            'python3', 'master_node.py',
            '--num-crawlers', str(num_crawlers),
            '--max-depth', str(max_depth)
        ]
        process = subprocess.run(cmd, capture_output=True, text=True)
        if process.returncode == 0:
            logger.info("Master node completed successfully")
            return True
        else:
            logger.error(f"Master node failed: {process.stderr}")
            return False
    except Exception as e:
        logger.error(f"Error running master node: {e}")
        return False

def download_index_from_s3(temp_dir):
    """Download index files from S3 into separate subdirectories per indexer"""
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INDEX_PREFIX)
        if 'Contents' not in response:
            logger.error(f"No objects found in bucket '{BUCKET_NAME}' with prefix '{INDEX_PREFIX}'. Run the indexer first.")
            return []

        indexer_files = defaultdict(list)
        for obj in response['Contents']:
            key = obj['Key']
            filename = os.path.basename(key)
            match = re.match(r'^indexer(\d+)_(.*)$', filename)
            if match:
                indexer_id, base_filename = match.groups()
                indexer_files[indexer_id].append((key, base_filename))

        if not indexer_files:
            logger.error("No valid index files found in S3.")
            return []

        index_dirs = []
        for indexer_id, files in indexer_files.items():
            indexer_dir = os.path.join(temp_dir, f"indexer{indexer_id}")
            os.makedirs(indexer_dir, exist_ok=True)
            for key, base_filename in files:
                local_path = os.path.join(indexer_dir, base_filename)
                logger.info(f"Downloading {key} to {local_path}")
                s3_client.download_file(BUCKET_NAME, key, local_path)
            index_dirs.append(indexer_dir)
        
        logger.info("Successfully downloaded index files from S3")
        return index_dirs
    except s3_client.exceptions.NoSuchBucket:
        logger.error(f"Bucket '{BUCKET_NAME}' does not exist")
        return []
    except s3_client.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        logger.error(f"S3 Error: {error_code} - {error_msg}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error downloading index from S3: {e}")
        return []

def download_url_mapping():
    """Download URL mapping from S3"""
    try:
        s3_client.download_file(BUCKET_NAME, "crawl_data/url_mapping.json", MAPPING_FILE)
        with open(MAPPING_FILE, 'r') as f:
            logger.info("Successfully downloaded URL mapping")
            return json.load(f)
    except s3_client.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        logger.error(f"S3 Error downloading URL mapping: {error_code} - {error_msg}")
        return {}
    except Exception as e:
        logger.error(f"Error downloading URL mapping: {e}")
        return {}

def search():
    """Interactive search interface with advanced query support"""
    if not check_aws_identity():
        logger.error("Cannot proceed without valid AWS credentials")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        index_dirs = download_index_from_s3(temp_dir)
        if not index_dirs:
            return

        url_mapping = download_url_mapping()
        if not url_mapping:
            logger.error("URL mapping not found. Cannot display URLs.")

        indexes = []
        for index_dir in index_dirs:
            try:
                ix = open_dir(index_dir)
                indexes.append(ix)
                logger.info(f"Opened index in {index_dir}")
            except Exception as e:
                logger.error(f"Error opening index in {index_dir}: {e}")
                continue

        if not indexes:
            logger.error("No valid indexes could be opened. Cannot proceed with search.")
            return

        schema = Schema(title=TEXT(stored=True), content=TEXT(stored=True))
        parser = MultifieldParser(["title", "content"], schema=schema)

        print("\nSearch Tips:")
        print("- Exact match: 'python'")
        print("- Phrase search: '\"python programming\"'")
        print("- Boolean operators: 'python AND programming', 'python OR java', 'python NOT java'")
        print("- Case-insensitive, use 'exit' to quit")

        while True:
            try:
                query_str = input("\nEnter search query: ").strip()
                if not query_str:
                    print("Please enter a query or 'exit'")
                    continue
                if query_str.lower() == "exit":
                    break

                query = parser.parse(query_str)
                all_results = []

                for ix in indexes:
                    with ix.searcher() as searcher:
                        results = searcher.search(query, limit=20)
                        for hit in results:
                            all_results.append((hit['title'], hit.score))

                all_results = sorted(all_results, key=lambda x: x[1], reverse=True)
                seen_titles = set()
                unique_results = []
                for title, score in all_results:
                    if title not in seen_titles:
                        unique_results.append(title)
                        seen_titles.add(title)

                if unique_results:
                    print(f"\nFound {len(unique_results)} unique result(s) for query '{query_str}':")
                    for i, s3_key in enumerate(unique_results[:1000], 1):
                        url = url_mapping.get(s3_key, f"URL not found for {s3_key}")
                        print(f"{i}. {url}")
                else:
                    print(f"No results found for query '{query_str}'")
                    
            except Exception as e:
                logger.error(f"Search error: {e}. Please check your query syntax.")
                continue

    if os.path.exists(MAPPING_FILE):
        os.remove(MAPPING_FILE)
    logger.info("Cleaned up local files")

def main():
    """Main client interface"""
    if not check_aws_identity():
        logger.error("Exiting due to invalid AWS credentials")
        return

    while True:
        print("\nClient Menu:")
        print("1. Submit seed URLs")
        print("2. Run master node")
        print("3. Search")
        print("4. Exit")
        choice = input("Enter choice (1-4): ").strip()

        if choice == '1':
            submit_urls()
        elif choice == '2':
            num_crawlers = input("Enter number of crawlers (default 2): ").strip() or '2'
            max_depth = input("Enter max crawl depth (default 2): ").strip() or '2'
            try:
                num_crawlers = int(num_crawlers)
                max_depth = int(max_depth)
                if num_crawlers < 1 or max_depth < 0:
                    logger.error("Number of crawlers must be at least 1, and max depth must be non-negative.")
                    continue
                run_master_node(num_crawlers, max_depth)
            except ValueError:
                logger.error("Invalid input. Please enter numeric values.")
        elif choice == '3':
            search()
        elif choice == '4':
            logger.info("Exiting client")
            break
        else:
            logger.warning("Invalid choice. Please enter 1, 2, 3, or 4.")

if __name__ == "__main__":
    main() 
