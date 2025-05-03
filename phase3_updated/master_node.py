#!/usr/bin/env python3
import sys
import logging
import boto3
import json
import time
import argparse

# Setup logging
def setup_logging():
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [Master] - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logging.basicConfig(level=logging.INFO, handlers=[console_handler])

# AWS setup
sqs_client = boto3.client('sqs', region_name='eu-north-1')
s3_client = boto3.client('s3')
TASK_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Queue'
RESULTS_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Results_Queue'
BUCKET_NAME = 'cse354000-bucket'

def master_node(num_crawlers):
    logging.info("Master Node starting...")

    # Download seed URLs from S3
    urls_s3_path = 'seed_urls/seed_urls.txt'
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

    # Distribute URLs via SQS
    for url in urls:
        sqs_client.send_message(
            QueueUrl=TASK_QUEUE_URL,
            MessageBody=json.dumps({'url': url})
        )
    logging.info(f"Master added {len(urls)} URLs to SQS task queue")

    # Add termination signals for specified number of crawlers
    for _ in range(num_crawlers):
        sqs_client.send_message(
            QueueUrl=TASK_QUEUE_URL,
            MessageBody=json.dumps({'terminate': True})
        )
    logging.info(f"Sent {num_crawlers} termination signals for crawlers")

    # Collect results from SQS
    url_mapping = {}
    completed_crawlers = 0

    while completed_crawlers < num_crawlers:
        response = sqs_client.receive_message(
            QueueUrl=RESULTS_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
        
        if 'Messages' in response:
            for message in response['Messages']:
                body = json.loads(message['Body'])
                if 'terminate' in body:
                    completed_crawlers += 1
                    logging.info(f"Crawler completed. Total completed: {completed_crawlers}/{num_crawlers}")
                else:
                    mappings = body['mappings']
                    url_mapping.update(mappings)
                    logging.info(f"Received {len(mappings)} mappings from a crawler")
                
                sqs_client.delete_message(
                    QueueUrl=RESULTS_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )
        else:
            time.sleep(5)

    # Save combined mapping to S3
    mapping_file = 'url_mapping.json'
    with open(mapping_file, 'w') as f:
        json.dump(url_mapping, f)
    s3_client.upload_file(mapping_file, BUCKET_NAME, 'crawl_data/url_mapping.json')
    logging.info("Uploaded combined URL mapping to S3")

    # Signal indexer and monitor completion
    sqs_client.send_message(
        QueueUrl=TASK_QUEUE_URL,
        MessageBody=json.dumps({'start_indexer': True})
    )
    logging.info("Master signaled Indexer to start.")

    indexer_completed = False
    while not indexer_completed:
        response = sqs_client.receive_message(
            QueueUrl=RESULTS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )
        
        if 'Messages' in response:
            for message in response['Messages']:
                body = json.loads(message['Body'])
                if 'indexer_complete' in body:
                    indexer_completed = True
                    logging.info("Indexer reported completion")
                    sqs_client.delete_message(
                        QueueUrl=RESULTS_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
        else:
            time.sleep(5)

    logging.info("Master process completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Node")
    parser.add_argument('--num-crawlers', type=int, default=2, help="Number of crawler processes")
    args = parser.parse_args()
    
    setup_logging()
    master_node(args.num_crawlers)