#!/usr/bin/env python3
import boto3
import json
import logging
import os
import sys
import time
import shutil
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [Indexer] - %(message)s')

# AWS setup
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs', region_name='eu-north-1')
BUCKET_NAME = 'cse354000-bucket'
QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Queue'

def indexer_node():
    logging.info("Indexer starting...")

    # Wait for signal from Master via SQS
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            messages = response.get('Messages', [])
            if not messages:
                logging.info("No messages received, waiting...")
                continue

            message = messages[0]
            receipt_handle = message['ReceiptHandle']
            body = json.loads(message['Body'])

            if body.get("type") == "index" and body.get("action") == "start":
                logging.info("Received signal to start indexing from Master")
                sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
                break

            sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
            logging.info("Deleted message from SQS")

        except Exception as e:
            logging.error(f"Error receiving message from SQS: {e}")
            time.sleep(5)

    # Download url_mapping.json from S3
    mapping_s3_key = 'crawl_data/url_mapping.json'
    local_mapping_file = 'url_mapping.json'
    try:
        s3_client.download_file(BUCKET_NAME, mapping_s3_key, local_mapping_file)
        with open(local_mapping_file, 'r') as f:
            url_mapping = json.load(f)
        logging.info(f"Downloaded url_mapping.json with {len(url_mapping)} entries")
    except Exception as e:
        logging.error(f"Error downloading url_mapping.json: {e}")
        sys.exit(1)

    # Create Whoosh schema and index
    schema = Schema(title=TEXT(stored=True), content=TEXT(stored=True))
    if os.path.exists("indexdir"):
        shutil.rmtree("indexdir")
    os.makedirs("indexdir", exist_ok=True)
    ix = create_in("indexdir", schema)

    # Index text files based on url_mapping.json
    writer = ix.writer()
    for url, mapping in url_mapping.items():
        text_s3_key = mapping['text']['s3_key']
        local_file = 'temp_crawled.txt'
        try:
            s3_client.download_file(BUCKET_NAME, text_s3_key, local_file)
            with open(local_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            writer.add_document(title=url, content=content)
            logging.info(f"Indexed text file for URL: {url} (S3 key: {text_s3_key})")
        except Exception as e:
            logging.error(f"Failed to index {text_s3_key} for URL {url}: {e}")
        finally:
            if os.path.exists(local_file):
                os.remove(local_file)

    writer.commit()
    logging.info("Committed all documents to index")

    # Upload index files to S3
    for root, dirs, files in os.walk("indexdir"):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = f"index_data/{file}"
            try:
                s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
                logging.info(f"Uploaded index file to S3: {s3_key}")
            except Exception as e:
                logging.error(f"Failed to upload {s3_key}: {e}")

    # Clean up
    if os.path.exists("indexdir"):
        shutil.rmtree("indexdir")
    if os.path.exists(local_mapping_file):
        os.remove(local_mapping_file)

    logging.info("Indexer finished and cleaned up")

if __name__ == "__main__":
    indexer_node()
