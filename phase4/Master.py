#!/usr/bin/env python3
import sys
import logging
import boto3
import json
import time
import argparse
from datetime import datetime

# Setup logging
def setup_logging():
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [Master] - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logging.basicConfig(level=logging.INFO, handlers=[console_handler])

# AWS setup
sqs_client = boto3.client('sqs', region_name='eu-north-1')
s3_client = boto3.client('s3')
TASK_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/ces354_Queue'
RESULTS_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Results_Queue'
HEARTBEAT_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Heartbeat_Queue'
BUCKET_NAME = 'cse354000-bucket'

def get_queue_attributes(queue_url):
    """Get approximate number of messages in the SQS queue"""
    try:
        response = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(response['Attributes']['ApproximateNumberOfMessages'])
    except Exception as e:
        logging.error(f"Error getting queue attributes: {e}")
        return -1

def master_node(num_crawlers, max_depth):
    logging.info("Master Node starting...")

    # Calculate dynamic timeouts based on max_depth
    base_wait_time = 20
    base_sleep_time = 3  # Fast task processing
    wait_time = min(20, base_wait_time + 5 * max_depth)
    sleep_time = base_sleep_time + 2 * max_depth
    MAX_EMPTY_POLLS = 7
    logging.info(f"Using WaitTimeSeconds={wait_time}s, sleep_time={sleep_time}s, MAX_EMPTY_POLLS={MAX_EMPTY_POLLS} for max_depth={max_depth}")

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

    # Initialize crawler tracking
    active_crawlers = {f"crawler{i}": {'last_heartbeat': datetime.now(), 'assigned_urls': {}} for i in range(num_crawlers)}
    failed_crawlers = {}  # Track crawlers that missed heartbeats but not yet terminated
    completed_crawlers = 0
    url_mapping = {}

    # Distribute URLs via SQS with depth, pre-assign https://example.com to crawler0
    for url in urls:
        if url == 'https://example.com' and 'crawler0' in active_crawlers:
            # Pre-assign to crawler0
            active_crawlers['crawler0']['assigned_urls'][url] = {'depth': 0}
            sqs_client.send_message(
                QueueUrl=TASK_QUEUE_URL,
                MessageBody=json.dumps({'url': url, 'depth': 0, 'crawler_id': 'crawler0'})
            )
            logging.info(f"Pre-assigned URL {url} at depth 0 to crawler crawler0")
        else:
            sqs_client.send_message(
                QueueUrl=TASK_QUEUE_URL,
                MessageBody=json.dumps({'url': url, 'depth': 0})
            )
            logging.info(f"Added seed URL to queue: {url} at depth 0")
    logging.info(f"Master added {len(urls)} URLs to SQS task queue with depth 0")

    # Monitor task queue, heartbeats, and results
    empty_polls = 0
    REASSIGN_TIMEOUT = 60  # Reassign URLs after 60 seconds
    TERMINATE_TIMEOUT = 120  # Terminate crawler after 120 seconds

    while completed_crawlers < num_crawlers:
        # Check heartbeats
        current_time = datetime.now()

        # Check active crawlers for reassignment
        for crawler_id, info in list(active_crawlers.items()):
            time_since_heartbeat = (current_time - info['last_heartbeat']).total_seconds()
            if time_since_heartbeat > REASSIGN_TIMEOUT:
                logging.warning(f"Crawler {crawler_id} missed heartbeat for {time_since_heartbeat:.2f}s, reassigning URLs")
                # Reassign unprocessed URLs
                for url, task in info['assigned_urls'].items():
                    if url not in url_mapping.values():
                        logging.info(f"Reassigning unprocessed URL {url} from crawler {crawler_id}")
                        sqs_client.send_message(
                            QueueUrl=TASK_QUEUE_URL,
                            MessageBody=json.dumps({'url': url, 'depth': task['depth']})
                        )
                # Move to failed_crawlers
                failed_crawlers[crawler_id] = {
                    'last_heartbeat': info['last_heartbeat'],
                    'assigned_urls': info['assigned_urls'].copy()
                }
                del active_crawlers[crawler_id]
                logging.info(f"Moved {crawler_id} to failed crawlers, awaiting termination timeout")

        # Check failed crawlers for termination
        for crawler_id, info in list(failed_crawlers.items()):
            time_since_heartbeat = (current_time - info['last_heartbeat']).total_seconds()
            if time_since_heartbeat > TERMINATE_TIMEOUT:
                logging.warning(f"Crawler {crawler_id} failed (no heartbeat for {time_since_heartbeat:.2f}s), terminating")
                sqs_client.send_message(
                    QueueUrl=TASK_QUEUE_URL,
                    MessageBody=json.dumps({'terminate': True})
                )
                logging.info(f"Sent termination signal for failed crawler {crawler_id}")
                del failed_crawlers[crawler_id]
                completed_crawlers += 1
                logging.info(f"Crawler {crawler_id} terminated. Total completed: {completed_crawlers}/{num_crawlers}")

        # Receive heartbeats
        heartbeat_response = sqs_client.receive_message(
            QueueUrl=HEARTBEAT_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
        if 'Messages' in heartbeat_response:
            logging.info(f"Received {len(heartbeat_response['Messages'])} heartbeats")
            for message in heartbeat_response['Messages']:
                body = json.loads(message['Body'])
                crawler_id = body['crawler_id']
                if crawler_id in active_crawlers:
                    active_crawlers[crawler_id]['last_heartbeat'] = datetime.now()
                    logging.info(f"Received heartbeat from active crawler {crawler_id}")
                elif crawler_id in failed_crawlers:
                    # Move back to active_crawlers if heartbeat received before termination
                    logging.info(f"Crawler {crawler_id} recovered with heartbeat, moving back to active")
                    active_crawlers[crawler_id] = {
                        'last_heartbeat': datetime.now(),
                        'assigned_urls': failed_crawlers[crawler_id]['assigned_urls']
                    }
                    del failed_crawlers[crawler_id]
                sqs_client.delete_message(
                    QueueUrl=HEARTBEAT_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )

        # Check task queue
        num_messages = get_queue_attributes(TASK_QUEUE_URL)
        if num_messages == -1:
            logging.warning("Failed to check queue size, continuing to poll...")
        elif num_messages == 0:
            if active_crawlers or failed_crawlers:
                empty_polls += 1
                logging.info(f"Task queue empty, empty poll count: {empty_polls}/{MAX_EMPTY_POLLS}")
                if empty_polls >= MAX_EMPTY_POLLS:
                    # Send termination signals to active crawlers only
                    num_signals = len(active_crawlers)
                    for crawler_id in list(active_crawlers.keys()):
                        sqs_client.send_message(
                            QueueUrl=TASK_QUEUE_URL,
                            MessageBody=json.dumps({'terminate': True})
                        )
                    logging.info(f"Sent {num_signals} termination signals to active crawlers")
                    # Wait for all crawlers to confirm termination
                    while active_crawlers or failed_crawlers:
                        result_response = sqs_client.receive_message(
                            QueueUrl=RESULTS_QUEUE_URL,
                            MaxNumberOfMessages=10,
                            WaitTimeSeconds=20
                        )
                        if 'Messages' in result_response:
                            for message in result_response['Messages']:
                                body = json.loads(message['Body'])
                                if 'terminate' in body:
                                    crawler_id = body.get('crawler_id', 'unknown')
                                    if crawler_id in active_crawlers:
                                        completed_crawlers += 1
                                        del active_crawlers[crawler_id]
                                        logging.info(f"Crawler {crawler_id} completed. Total completed: {completed_crawlers}/{num_crawlers}")
                                    elif crawler_id in failed_crawlers:
                                        completed_crawlers += 1
                                        del failed_crawlers[crawler_id]
                                        logging.info(f"Crawler {crawler_id} completed. Total completed: {completed_crawlers}/{num_crawlers}")
                                else:
                                    crawler_id = body.get('crawler_id', 'unknown')
                                    mappings = body.get('mappings', {})
                                    if mappings:
                                        url_mapping.update(mappings)
                                        logging.info(f"Received {len(mappings)} mappings from crawler {crawler_id}: {mappings}")
                                    if crawler_id in active_crawlers:
                                        for url in mappings.values():
                                            if url in active_crawlers[crawler_id]['assigned_urls']:
                                                logging.info(f"Removing processed URL {url} from assigned_urls[{crawler_id}]")
                                                del active_crawlers[crawler_id]['assigned_urls'][url]
                                    elif crawler_id in failed_crawlers:
                                        for url in mappings.values():
                                            if url in failed_crawlers[crawler_id]['assigned_urls']:
                                                logging.info(f"Removing processed URL {url} from assigned_urls[{crawler_id}]")
                                                del failed_crawlers[crawler_id]['assigned_urls'][url]
                                sqs_client.delete_message(
                                    QueueUrl=RESULTS_QUEUE_URL,
                                    ReceiptHandle=message['ReceiptHandle']
                                )
                        time.sleep(1)
                    break  # Exit the main loop after all crawlers confirm termination
        else:
            empty_polls = 0
            logging.info(f"Task queue has ~{num_messages} messages, continuing to monitor...")

        # Receive results
        result_response = sqs_client.receive_message(
            QueueUrl=RESULTS_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=wait_time
        )
        if 'Messages' in result_response:
            for message in result_response['Messages']:
                body = json.loads(message['Body'])
                if 'terminate' in body:
                    crawler_id = body.get('crawler_id', 'unknown')
                    if crawler_id in active_crawlers:
                        completed_crawlers += 1
                        del active_crawlers[crawler_id]
                        logging.info(f"Crawler {crawler_id} completed. Total completed: {completed_crawlers}/{num_crawlers}")
                    elif crawler_id in failed_crawlers:
                        completed_crawlers += 1
                        del failed_crawlers[crawler_id]
                        logging.info(f"Crawler {crawler_id} completed. Total completed: {completed_crawlers}/{num_crawlers}")
                else:
                    crawler_id = body.get('crawler_id', 'unknown')
                    mappings = body.get('mappings', {})
                    if mappings:
                        url_mapping.update(mappings)
                        logging.info(f"Received {len(mappings)} mappings from crawler {crawler_id}: {mappings}")
                    if crawler_id in active_crawlers:
                        for url in mappings.values():
                            if url in active_crawlers[crawler_id]['assigned_urls']:
                                logging.info(f"Removing processed URL {url} from assigned_urls[{crawler_id}]")
                                del active_crawlers[crawler_id]['assigned_urls'][url]
                    elif crawler_id in failed_crawlers:
                        for url in mappings.values():
                            if url in failed_crawlers[crawler_id]['assigned_urls']:
                                logging.info(f"Removing processed URL {url} from assigned_urls[{crawler_id}]")
                                del failed_crawlers[crawler_id]['assigned_urls'][url]
                sqs_client.delete_message(
                    QueueUrl=RESULTS_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )

        # Assign tasks
        task_response = sqs_client.receive_message(
            QueueUrl=TASK_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1  # Fixed to integer
        )
        if 'Messages' in task_response:
            for message in task_response['Messages']:
                body = json.loads(message['Body'])
                if 'url' in body and 'depth' in body:
                    url = body['url']
                    depth = body['depth']
                    target_crawler = body.get('crawler_id')
                    if target_crawler and target_crawler in active_crawlers:
                        # Respect pre-assigned crawler
                        crawler_id = target_crawler
                    elif active_crawlers:
                        # Choose crawler with fewest assigned URLs
                        crawler_id = min(
                            active_crawlers,
                            key=lambda cid: len(active_crawlers[cid]['assigned_urls'])
                        )
                    else:
                        # No active crawlers, re-queue
                        sqs_client.send_message(
                            QueueUrl=TASK_QUEUE_URL,
                            MessageBody=json.dumps({'url': url, 'depth': depth})
                        )
                        sqs_client.delete_message(
                            QueueUrl=TASK_QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        continue
                    active_crawlers[crawler_id]['assigned_urls'][url] = {'depth': depth}
                    logging.info(f"Assigned URL {url} at depth {depth} to crawler {crawler_id}")
                    # Re-queue for the crawler
                    sqs_client.send_message(
                        QueueUrl=TASK_QUEUE_URL,
                        MessageBody=json.dumps({'url': url, 'depth': depth, 'crawler_id': crawler_id})
                    )
                sqs_client.delete_message(
                    QueueUrl=TASK_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )

        time.sleep(sleep_time)

    # Save combined mapping to S3
    mapping_file = 'url_mapping.json'
    with open(mapping_file, 'w') as f:
        json.dump(url_mapping, f)
    s3_client.upload_file(mapping_file, BUCKET_NAME, 'crawl_data/url_mapping.json')
    logging.info(f"Uploaded combined URL mapping to S3 with {len(url_mapping)} entries")

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
            WaitTimeSeconds=wait_time
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
            logging.info(f"No messages received, sleeping for {sleep_time}s")
            time.sleep(sleep_time)

    logging.info("Master process completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Node")
    parser.add_argument('--num-crawlers', type=int, default=2, help="Number of crawler processes")
    parser.add_argument('--max-depth', type=int, default=2, help="Maximum crawl depth")
    args = parser.parse_args()
    
    setup_logging()
    master_node(args.num_crawlers, args.max_depth)
