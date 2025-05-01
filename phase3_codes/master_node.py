#!/usr/bin/env python3
import boto3
import json
import logging
import sys
import time
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [Master] - %(message)s')

# AWS setup
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs', region_name='eu-north-1')
BUCKET_NAME = 'cse354000-bucket'
QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Queue'

def master_node():
    logging.info("Master starting...")

    # Download seed URLs from S3
    urls_s3_path = 'seed_urls/seed_urls.txt'
    local_urls_file = 'seed_urls.txt'
    try:
        s3_client.download_file(BUCKET_NAME, urls_s3_path, local_urls_file)
        logging.info(f"Downloaded seed URL file from s3://{BUCKET_NAME}/{urls_s3_path}")
    except Exception as e:
        logging.error(f"Error downloading seed URL file: {e}")
        sys.exit(1)

    # Read and clean URLs
    try:
        with open(local_urls_file, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        if not urls:
            logging.error("Error: No URLs found in seed file.")
            sys.exit(1)
        logging.info(f"Total {len(urls)} URLs read from seed file:")
        for url in urls:
            logging.info(f"URL: {url}")
    except Exception as e:
        logging.error(f"Error reading seed file: {e}")
        sys.exit(1)

    # Fault tolerance: Track task and crawler status
    task_id = "single_batch"
    crawler_active = True
    status_timeout = 60  # Seconds to wait for crawler status
    task_timeout = 300   # Seconds to wait for task completion

    # Send all URLs to SQS queue for single crawler
    task = {"type": "crawl", "urls": urls, "task_id": task_id}
    try:
        response = sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(task)
        )
        logging.info(f"Sent {len(urls)} URLs to SQS for single crawler (Message ID: {response['MessageId']})")
        for url in urls:
            logging.info(f"Sent URL: {url}")
    except Exception as e:
        logging.error(f"Failed to send URLs to SQS: {e}")
        sys.exit(1)

    # Monitor crawler status, crawl progress, and task completion
    start_time = time.time()
    task_completed = False
    urls_processed = set()  # Track processed URLs
    while not task_completed and crawler_active:
        # Check for crawler status, crawl progress, or task completion
        try:
            response = sqs_client.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
            messages = response.get('Messages', [])
            if messages:
                message = messages[0]
                receipt_handle = message['ReceiptHandle']
                body = json.loads(message['Body'])
                if body.get("type") == "status" and body.get("status") == "active":
                    logging.info("Received crawler status: active")
                    start_time = time.time()  # Reset timeout on status
                elif body.get("type") == "crawl_status" and body.get("status") == "finished":
                    url = body.get("url")
                    urls_processed.add(url)
                    logging.info(f"Crawler finished crawling URL: {url} ({len(urls_processed)}/{len(urls)} URLs processed)")
                sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
            elif time.time() - start_time > status_timeout:
                logging.warning("Crawler status timeout. Assuming crawler failure.")
                crawler_active = False
                break
        except Exception as e:
            logging.error(f"Error checking status: {e}")

        # Check for task completion (url_mapping.json)
        mapping_s3_key = 'crawl_data/url_mapping.json'
        local_mapping_file = 'url_mapping.json'
        try:
            s3_client.download_file(BUCKET_NAME, mapping_s3_key, local_mapping_file)
            with open(local_mapping_file, 'r') as f:
                url_mapping = json.load(f)
            logging.info(f"Downloaded url_mapping.json with {len(url_mapping)} entries")
            task_completed = True
            break
        except Exception:
            logging.info("Waiting for url_mapping.json...")

        # Check for task timeout
        if time.time() - start_time > task_timeout:
            logging.warning("Task timeout. Re-queueing task.")
            if not task_completed:
                task = {"type": "crawl", "urls": urls, "task_id": f"requeue_{task_id}"}
                try:
                    sqs_client.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(task))
                    logging.info(f"Re-queued task {task_id} with {len(urls)} URLs")
                except Exception as e:
                    logging.error(f"Failed to re-queue task: {e}")
                break

        time.sleep(5)

    if not crawler_active:
        logging.error("Crawler failed. Aborting.")
        sys.exit(1)

    # Signal indexer via SQS
    try:
        sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps({"type": "index", "action": "start"})
        )
        logging.info("Signaled Indexer to start via SQS")
    except Exception as e:
        logging.error(f"Failed to signal indexer: {e}")
        sys.exit(1)

if __name__ == "__main__":
    master_node()
