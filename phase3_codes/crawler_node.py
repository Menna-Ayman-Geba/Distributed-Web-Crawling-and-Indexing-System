#!/usr/bin/env python3
import boto3
import json
import logging
import os
import sys
import time
import requests
import re
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [Crawler] - %(message)s')

# AWS setup
sqs_client = boto3.client('sqs', region_name='eu-north-1')
s3_client = boto3.client('s3')
QUEUE_URL = 'https://sqs.eu-north-1.amazonaws.com/965766185618/cse354_Queue'
BUCKET_NAME = 'cse354000-bucket'

def sanitize_url_for_filename(url):
    """Convert URL to a valid filename by removing scheme, replacing invalid characters, and adding extension."""
    # Remove scheme (e.g., https://) and trailing slashes
    parsed = urlparse(url)
    domain_path = f"{parsed.netloc}{parsed.path}".rstrip('/')
    # Replace invalid characters (e.g., /, ?, :, etc.) with underscores
    safe_name = re.sub(r'[^a-zA-Z0-9\-\.]', '_', domain_path)
    # Ensure the filename isn't too long (truncate if necessary)
    max_length = 100  # S3 supports longer, but keeping it reasonable
    if len(safe_name) > max_length:
        safe_name = safe_name[:max_length]
    return safe_name

def can_crawl(url):
    """Check if crawling is allowed by robots.txt."""
    rp = RobotFileParser()
    robots_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}/robots.txt"
    try:
        response = requests.get(robots_url, timeout=5)
        if response.status_code == 200:
            rp.parse(response.text.splitlines())
            return rp.can_fetch("*", url)
        return True  # Allow if robots.txt is inaccessible
    except Exception:
        return True  # Allow if error occurs

def extract_text(html_content):
    """Extract text content from HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    for script in soup(["script", "style"]):
        script.decompose()
    text = soup.get_text(separator=' ', strip=True)
    return text

def crawler_node():
    logging.info("Crawler starting...")

    # Status update loop
    def send_status():
        while True:
            try:
                sqs_client.send_message(
                    QueueUrl=QUEUE_URL,
                    MessageBody=json.dumps({"type": "status", "status": "active"})
                )
                logging.info("Sent status: active")
            except Exception as e:
                logging.error(f"Error sending status: {e}")
            time.sleep(30)  # Send every 30 seconds

    import threading
    status_thread = threading.Thread(target=send_status, daemon=True)
    status_thread.start()

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

            if body.get("type") == "crawl":
                urls = body.get("urls", [])
                logging.info(f"Received {len(urls)} URLs to crawl")

                url_mapping = {}
                for url in urls:
                    if not can_crawl(url):
                        logging.warning(f"Crawling disallowed by robots.txt for {url}")
                        continue

                    try:
                        # Fetch webpage
                        headers = {'User-Agent': 'CSE354Bot/1.0'}
                        resp = requests.get(url, headers=headers, timeout=10)
                        resp.raise_for_status()
                        html_content = resp.text

                        # Generate filename based on URL
                        base_filename = sanitize_url_for_filename(url)
                        html_filename = f"crawl_data/html/{base_filename}.html"
                        text_filename = f"crawl_data/text/{base_filename}.txt"
                        local_html_file = f"{base_filename}.html"
                        local_text_file = f"{base_filename}.txt"

                        # Save HTML to local file and upload to S3
                        with open(local_html_file, 'w', encoding='utf-8') as f:
                            f.write(html_content)
                        s3_client.upload_file(local_html_file, BUCKET_NAME, html_filename)
                        html_size = os.path.getsize(local_html_file)
                        os.remove(local_html_file)

                        # Extract and save text
                        text_content = extract_text(html_content)
                        with open(local_text_file, 'w', encoding='utf-8') as f:
                            f.write(text_content)
                        s3_client.upload_file(local_text_file, BUCKET_NAME, text_filename)
                        text_size = os.path.getsize(local_text_file)
                        os.remove(local_text_file)

                        # Update mapping
                        url_mapping[url] = {
                            "html": {
                                "s3_key": html_filename,
                                "url": url,
                                "size": html_size
                            },
                            "text": {
                                "s3_key": text_filename,
                                "url": url,
                                "size": text_size
                            }
                        }
                        logging.info(f"Crawled {url}, saved HTML to {html_filename}, text to {text_filename}")

                        # Notify master that this URL is finished
                        try:
                            sqs_client.send_message(
                                QueueUrl=QUEUE_URL,
                                MessageBody=json.dumps({
                                    "type": "crawl_status",
                                    "url": url,
                                    "status": "finished"
                                })
                            )
                            logging.info(f"Notified master: Finished crawling URL {url}")
                        except Exception as e:
                            logging.error(f"Failed to notify master for URL {url}: {e}")

                        time.sleep(1)  # Crawl delay for politeness
                    except Exception as e:
                        logging.warning(f"Failed to crawl {url}: {e}")

                # Upload mapping file to S3
                mapping_file = 'url_mapping.json'
                with open(mapping_file, 'w') as f:
                    json.dump(url_mapping, f)
                s3_client.upload_file(mapping_file, BUCKET_NAME, 'crawl_data/url_mapping.json')
                logging.info("Uploaded url_mapping.json to S3")

            elif body.get("type") == "index":
                logging.info("Received signal to start indexing.")
                break  # Exit crawler to allow indexer to proceed

            # Delete message after processing
            sqs_client.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            logging.info("Deleted message from SQS")

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            time.sleep(5)

if __name__ == "__main__":
    crawler_node()
