#!/usr/bin/env python3
import boto3
import requests
from bs4 import BeautifulSoup
import tempfile
import os
import logging
import sys
import time
import json
import re
import argparse
from multiprocessing import Process, Value
from logging.handlers import QueueHandler
import multiprocessing as mp
from urllib.parse import urlparse
import urllib.robotparser

# --- AWS Setup ---
sqs_client = boto3.client('sqs', region_name='eu-north-1')
s3_client = boto3.client('s3')
TASK_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Queue'
RESULTS_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Results_Queue'
BUCKET_NAME = 'cse354000-bucket'

# --- Crawl Delay ---
CRAWL_DELAY = 2  # seconds

# --- Shared counter for unique IDs ---
crawler_counter = Value('i', 0)

class CrawlerIdFilter(logging.Filter):
    """Add crawler_id to log records"""
    def __init__(self, crawler_id):
        super().__init__()
        self.crawler_id = crawler_id

    def filter(self, record):
        record.crawler_id = self.crawler_id
        return True

def setup_logger(crawler_id, log_queue):
    """Set up logging for multiprocessing"""
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [Crawler %(crawler_id)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler = QueueHandler(log_queue)
    handler.setFormatter(formatter)
    handler.addFilter(CrawlerIdFilter(crawler_id))  # Add filter to inject crawler_id
    logger = logging.getLogger(f"crawler_{crawler_id}")
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

def sanitize_filename(url):
    """Convert URL to a safe S3 key name"""
    name = re.sub(r'https?://', '', url)
    name = re.sub(r'[^\w\-_\.]', '_', name)
    return name[:200]

def crawl_urls(url, crawler_id):
    """Crawl a single URL and return mappings, respecting robots.txt"""
    mappings = {}
    html_tmp_path = txt_tmp_path = None
    
    try:
        # Parse the URL to get the base domain
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        robots_url = f"{base_url}/robots.txt"

        # Check robots.txt
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()  # Fetch and parse robots.txt
        except Exception as e:
            logger.warning(f"Could not fetch robots.txt from {robots_url}: {e}. Proceeding with crawl.")

        # Define your crawler's User-Agent
        user_agent = "MyCrawlerBot"
        if not rp.can_fetch(user_agent, url):
            logger.info(f"Crawling disallowed by robots.txt for {url}")
            return mappings  # Skip crawling this URL

        # Proceed with crawling if allowed
        response = requests.get(url, timeout=5, headers={"User-Agent": user_agent})
        if response.status_code == 200:
            html_content = response.text
            base_name = sanitize_filename(url)
            html_s3_key = f"crawl_data/{base_name}_{crawler_id}.html"
            txt_s3_key = f"crawl_data/{base_name}_{crawler_id}.txt"

            # Save HTML
            with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as html_tmp:
                html_tmp.write(html_content.encode('utf-8'))
                html_tmp_path = html_tmp.name

            if os.path.exists(html_tmp_path):
                s3_client.upload_file(html_tmp_path, BUCKET_NAME, html_s3_key)
                logger.info(f"Uploaded HTML: {html_s3_key}")
                mappings[html_s3_key] = url
            else:
                logger.warning(f"Missing HTML file: {html_tmp_path}")

            # Extract and save text
            soup = BeautifulSoup(html_content, "html.parser")
            text_content = soup.get_text(separator='\n')

            with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as txt_tmp:
                txt_tmp.write(text_content.encode('utf-8'))
                txt_tmp_path = txt_tmp.name

            if os.path.exists(txt_tmp_path):
                s3_client.upload_file(txt_tmp_path, BUCKET_NAME, txt_s3_key)
                logger.info(f"Uploaded TXT: {txt_s3_key}")
                mappings[txt_s3_key] = url
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
    
    return mappings

def crawler_worker(crawler_id, log_queue):
    """Worker function for each crawler process"""
    global logger
    logger = setup_logger(crawler_id, log_queue)
    logger.info("Starting crawler")
    
    local_mappings = {}
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
        
        if 'terminate' in body:
            sqs_client.send_message(
                QueueUrl=RESULTS_QUEUE_URL,
                MessageBody=json.dumps({'mappings': local_mappings})
            )
            sqs_client.send_message(
                QueueUrl=RESULTS_QUEUE_URL,
                MessageBody=json.dumps({'terminate': True})
            )
            sqs_client.delete_message(
                QueueUrl=TASK_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
            logger.info("Finished crawling")
            break
        
        if 'url' in body:  # Check if 'url' exists before accessing
            url = body['url']
            mappings = crawl_urls(url, crawler_id)
            local_mappings.update(mappings)
            logger.info(f"Processed URL: {url}")
        
        sqs_client.delete_message(
            QueueUrl=TASK_QUEUE_URL,
            ReceiptHandle=message['ReceiptHandle']
        )
        
        logger.info(f"Waiting for {CRAWL_DELAY} seconds before next request")
        time.sleep(CRAWL_DELAY)

def run_crawlers(num_crawlers):
    """Run multiple crawler processes"""
    # Set up logging queue and listener
    log_queue = mp.Queue()
    listener = Process(target=log_listener, args=(log_queue,))
    listener.start()

    # Start crawler processes
    processes = []
    for i in range(num_crawlers):
        with crawler_counter.get_lock():
            crawler_id = f"crawler{crawler_counter.value}"
            crawler_counter.value += 1
        p = Process(target=crawler_worker, args=(crawler_id, log_queue))
        p.start()
        processes.append(p)

    # Wait for all processes to finish
    for p in processes:
        p.join()

    # Stop the log listener
    log_queue.put(None)
    listener.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-Process Crawler Node")
    parser.add_argument('--num-crawlers', type=int, default=2, help="Number of crawler processes")
    args = parser.parse_args()

    # Configure root logger for the listener
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    run_crawlers(args.num_crawlers)