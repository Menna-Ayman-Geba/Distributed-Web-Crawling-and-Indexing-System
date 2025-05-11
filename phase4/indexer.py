#!/usr/bin/env python3
import boto3
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT
import os
import shutil
import logging
import sys
import json
import argparse
from multiprocessing import Process, Queue, Value
from logging.handlers import QueueHandler
import multiprocessing as mp

# --- AWS Setup ---
sqs_client = boto3.client('sqs', region_name='eu-north-1')
s3_client = boto3.client('s3')
TASK_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/ces354_Queue'
RESULTS_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Results_Queue'
BUCKET_NAME = 'cse354000-bucket'

# --- Shared counter for unique IDs ---
indexer_counter = Value('i', 0)

class IndexerIdFilter(logging.Filter):
    """Add indexer_id to log records"""
    def __init__(self, indexer_id):
        super().__init__()
        self.indexer_id = indexer_id

    def filter(self, record):
        record.indexer_id = self.indexer_id
        return True

def setup_logger(indexer_id, log_queue):
    """Set up logging for multiprocessing"""
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [Indexer %(indexer_id)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler = QueueHandler(log_queue)
    handler.setFormatter(formatter)
    handler.addFilter(IndexerIdFilter(indexer_id))
    logger = logging.getLogger(f"indexer_{indexer_id}")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

def log_listener(log_queue):
    """Listen to log messages from all processes"""
    while True:
        try:
            record = log_queue.get()
            if record is None:  # Sentinel to stop listener
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception as e:
            print(f"Log listener error: {e}", file=sys.stderr)

def index_files(file_queue, indexer_id):
    """Index a batch of files and upload to S3"""
    schema = Schema(title=TEXT(stored=True), content=TEXT(stored=True))
    index_dir = f"indexdir_{indexer_id}"
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    os.makedirs(index_dir, exist_ok=True)
    ix = create_in(index_dir, schema)
    writer = ix.writer()

    while True:
        try:
            key = file_queue.get(timeout=1)  # Timeout to check for empty queue
            if key is None:  # Sentinel to stop indexing
                break
            local_file = f"temp_crawled_{indexer_id}.txt"
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
        except Queue.Empty:
            break  # No more files to process

    writer.commit()
    logger.info(f"Committed documents for indexer {indexer_id}")

    # Upload index files to S3 in a single folder
    for root, dirs, files in os.walk(index_dir):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = f"index_data/{indexer_id}_{file}"
            try:
                s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
                logger.info(f"Uploaded index file to S3: {s3_key}")
            except Exception as e:
                logger.error(f"Failed to upload {s3_key}: {e}")

    shutil.rmtree(index_dir)  # Clean up

def list_s3_objects(bucket, prefix):
    """List S3 objects with pagination"""
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.txt'):
                yield obj['Key']

def indexer_worker(indexer_id, log_queue, file_queue):
    """Worker function for each indexer process"""
    global logger
    logger = setup_logger(indexer_id, log_queue)
    logger.info("Starting indexer worker")
    
    index_files(file_queue, indexer_id)
    logger.info("Finished indexing")

def run_indexers(num_indexers, logger):
    """Run multiple indexer processes"""
    # Set up logging queue and listener
    log_queue = mp.Queue()
    listener = Process(target=log_listener, args=(log_queue,))
    listener.start()

    # Fetch list of crawled files from S3 with pagination
    file_queue = mp.Queue()
    txt_files = list_s3_objects(BUCKET_NAME, "crawl_data/")
    file_count = 0
    for key in txt_files:
        file_queue.put(key)
        file_count += 1

    if file_count == 0:
        logger.warning("No crawled files found in S3.")
        return

    logger.info(f"Found {file_count} text files to index")

    # Add sentinels to stop workers
    for _ in range(num_indexers):
        file_queue.put(None)

    # Start indexer processes
    processes = []
    for i in range(num_indexers):
        with indexer_counter.get_lock():
            indexer_id = f"indexer{indexer_counter.value}"
            indexer_counter.value += 1
        p = Process(target=indexer_worker, args=(indexer_id, log_queue, file_queue))
        p.start()
        processes.append(p)

    # Wait for all processes to finish
    for p in processes:
        p.join()

    # Stop the log listener
    log_queue.put(None)
    listener.join()

def indexer_node(num_indexers):
    """Main indexer node function"""
    # Configure root logger for initial messages
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logger = logging.getLogger("indexer_node")
    logger.info("Indexer node starting...")

    while True:
        response = sqs_client.receive_message(
            QueueUrl=TASK_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )
        
        if 'Messages' not in response:
            logger.info("No messages received, continuing to poll...")
            continue
        
        message = response['Messages'][0]
        body = json.loads(message['Body'])
        
        if 'start_indexer' in body:
            logger.info("Received start signal from Master")
            run_indexers(num_indexers, logger)
            sqs_client.delete_message(
                QueueUrl=TASK_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
            sqs_client.send_message(
                QueueUrl=RESULTS_QUEUE_URL,
                MessageBody=json.dumps({'indexer_complete': True})
            )
            logger.info("Indexer finished and signaled completion")
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-Process Indexer Node")
    parser.add_argument('--num-indexers', type=int, default=2, help="Number of indexer processes")
    args = parser.parse_args()
    
    indexer_node(args.num_indexers)
