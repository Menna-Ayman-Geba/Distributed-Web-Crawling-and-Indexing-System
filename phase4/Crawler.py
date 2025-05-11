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
from multiprocessing import Process, Value, Manager
from logging.handlers import QueueHandler
import multiprocessing as mp
from urllib.parse import urlparse
import urllib.robotparser
import threading

# AWS Setup
sqs_client = boto3.client('sqs', region_name='eu-north-1')
s3_client = boto3.client('s3')
TASK_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/ces354_Queue'
RESULTS_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Results_Queue'
HEARTBEAT_QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Heartbeat_Queue'
BUCKET_NAME = 'cse354000-bucket'

# Crawl Delay
DEFAULT_CRAWL_DELAY = 2  # seconds
HEARTBEAT_INTERVAL = 5  # seconds

# Shared counter for unique IDs and visited URLs
crawler_counter = Value('i', 0)
manager = Manager()
visited_urls = manager.dict()  # Shared dictionary to track visited URLs

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
    handler.addFilter(CrawlerIdFilter(crawler_id))
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

def send_heartbeat(crawler_id, stop_event):
    """Send heartbeat messages every 5 seconds"""
    while not stop_event.is_set():
        try:
            sqs_client.send_message(
                QueueUrl=HEARTBEAT_QUEUE_URL,
                MessageBody=json.dumps({'crawler_id': crawler_id})
            )
            logger.info(f"Sent heartbeat for {crawler_id}")
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")
        time.sleep(HEARTBEAT_INTERVAL)

def sanitize_filename(url):
    """Convert URL to a safe S3 key name"""
    name = re.sub(r'https?://', '', url)
    name = re.sub(r'[^\w\-\.]', '', name)
    return name[:200]

def crawl_urls(url, depth, crawler_id, max_depth):
    """Crawl a single URL, return mappings, new links, and crawl delay, respecting robots.txt and depth"""
    mappings = {}
    new_links = []
    html_tmp_path = None
    txt_tmp_path = None
    
    try:
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            logger.warning(f"Invalid URL format: {url}")
            return mappings, new_links, DEFAULT_CRAWL_DELAY
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        robots_url = f"{base_url}/robots.txt"

        logger.info(f"Checking robots.txt at {robots_url} for {url}")
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
        except Exception as e:
            logger.warning(f"Failed to fetch robots.txt from {robots_url}: {e}. Proceeding with crawl.")

        user_agent = "MyCrawlerBot"
        if not rp.can_fetch(user_agent, url):
            logger.info(f"Crawling disallowed by robots.txt for {url}")
            return mappings, new_links, DEFAULT_CRAWL_DELAY

        crawl_delay = rp.crawl_delay(user_agent) or DEFAULT_CRAWL_DELAY
        logger.info(f"Fetching URL: {url} with crawl delay {crawl_delay}s")
        response = requests.get(url, timeout=5, headers={"User-Agent": user_agent})
        if response.status_code == 200:
            html_content = response.text
            base_name = sanitize_filename(url)
            html_s3_key = f"crawl_data/{base_name}_{crawler_id}.html"
            txt_s3_key = f"crawl_data/{base_name}_{crawler_id}.txt"

            with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as html_tmp:
                html_tmp.write(html_content.encode('utf-8'))
                html_tmp_path = html_tmp.name

            if os.path.exists(html_tmp_path):
                s3_client.upload_file(html_tmp_path, BUCKET_NAME, html_s3_key)
                logger.info(f"Uploaded HTML: {html_s3_key}")
                mappings[html_s3_key] = url
            else:
                logger.warning(f"Missing HTML file: {html_tmp_path}")

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

            if depth < max_depth:
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    full_url = urllib.parse.urljoin(url, href)
                    parsed_full_url = urlparse(full_url)
                    if parsed_full_url.scheme in ['http', 'https'] and not parsed_full_url.fragment:
                        new_links.append(full_url)
                        logger.info(f"Found link: {full_url} at depth {depth + 1}")

        else:
            logger.error(f"Failed to fetch {url}: Status code {response.status_code}")
        return mappings, new_links, crawl_delay
    except Exception as e:
        logger.exception(f"Error crawling {url}: {e}")
        return mappings, new_links, DEFAULT_CRAWL_DELAY
    finally:
        if html_tmp_path and os.path.exists(html_tmp_path):
            os.remove(html_tmp_path)
        if txt_tmp_path and os.path.exists(txt_tmp_path):
            os.remove(txt_tmp_path)

def crawler_worker(crawler_id, log_queue, max_depth):
    """Worker function for each crawler process"""
    global logger
    logger = setup_logger(crawler_id, log_queue)
    logger.info(f"Starting crawler with max_depth={max_depth}")
    
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(target=send_heartbeat, args=(crawler_id, stop_event))
    heartbeat_thread.start()

    processed_urls = set()  # Track URLs processed by this crawler to avoid duplicate mappings
    has_assigned_url = False  # Flag for crawler0 failure simulation

    try:
        while True:
            response = sqs_client.receive_message(
                QueueUrl=TASK_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            
            if 'Messages' not in response:
                logger.info("No messages received, continuing to poll...")
                time.sleep(1)
                continue
            
            message = response['Messages'][0]
            body = json.loads(message['Body'])
            
            if 'terminate' in body:
                logger.info("Received termination signal, sending mappings and stopping")
                sqs_client.send_message(
                    QueueUrl=RESULTS_QUEUE_URL,
                    MessageBody=json.dumps({'terminate': True, 'crawler_id': crawler_id})
                )
                sqs_client.delete_message(
                    QueueUrl=TASK_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )
                break
            
            if 'url' in body and 'depth' in body:
                url = body['url']
                depth = body['depth']
                target_crawler = body.get('crawler_id')
                if target_crawler and target_crawler != crawler_id:
                    logger.info(f"Skipping URL {url} targeted for crawler {target_crawler}")
                    sqs_client.delete_message(
                        QueueUrl=TASK_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    continue
                logger.info(f"Received URL: {url} at depth {depth}")
                
                # Simulate failure for crawler0 after receiving a URL
                if crawler_id == "crawler0" and not has_assigned_url:
                    has_assigned_url = True
                    logger.info(f"Simulating failure: stopping heartbeats and exiting")
                    stop_event.set()
                    heartbeat_thread.join()
                    sqs_client.delete_message(
                        QueueUrl=TASK_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    sys.exit(1)  # Exit the process
                
                if depth > max_depth:
                    logger.warning(f"Skipping URL {url} at depth {depth} (exceeds max_depth {max_depth})")
                    sqs_client.delete_message(
                        QueueUrl=TASK_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    continue

                if url in visited_urls:
                    logger.info(f"Skipping already visited URL: {url} at depth {depth}")
                    sqs_client.delete_message(
                        QueueUrl=TASK_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    continue
                
                logger.info(f"Processing URL: {url} at depth {depth}/{max_depth}")
                mappings, new_links, crawl_delay = crawl_urls(url, depth, crawler_id, max_depth)
                
                # Mark URL as visited only after successful crawl
                if mappings:
                    visited_urls[url] = True
                
                # Send mappings only if not already processed
                if mappings and url not in processed_urls:
                    sqs_client.send_message(
                        QueueUrl=RESULTS_QUEUE_URL,
                        MessageBody=json.dumps({'mappings': mappings, 'crawler_id': crawler_id})
                    )
                    logger.info(f"Sent {len(mappings)} mappings to results queue")
                    processed_urls.add(url)
                
                if depth < max_depth:
                    for link in new_links:
                        if link not in visited_urls:
                            sqs_client.send_message(
                                QueueUrl=TASK_QUEUE_URL,
                                MessageBody=json.dumps({'url': link, 'depth': depth + 1})
                            )
                            logger.info(f"Added new URL to queue: {link} at depth {depth + 1}")
            
            sqs_client.delete_message(
                QueueUrl=TASK_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
            
            logger.info(f"Waiting for {crawl_delay} seconds before next request")
            time.sleep(crawl_delay)
    finally:
        stop_event.set()
        heartbeat_thread.join()

def run_crawlers(num_crawlers, max_depth):
    """Run multiple crawler processes"""
    log_queue = mp.Queue()
    listener = Process(target=log_listener, args=(log_queue,))
    listener.start()

    processes = []
    for i in range(num_crawlers):
        with crawler_counter.get_lock():
            crawler_id = f"crawler{crawler_counter.value}"
            crawler_counter.value += 1
        p = Process(target=crawler_worker, args=(crawler_id, log_queue, max_depth))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    log_queue.put(None)
    listener.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-Process Crawler Node")
    parser.add_argument('--num-crawlers', type=int, default=2, help="Number of crawler processes")
    parser.add_argument('--max-depth', type=int, default=2, help="Maximum crawl depth")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    run_crawlers(args.num_crawlers, args.max_depth)
