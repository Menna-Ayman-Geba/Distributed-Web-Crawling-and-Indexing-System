#!/usr/bin/env python3
from whoosh.index import open_dir
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.query import And, Or, Not
import os
import boto3
import json
import shutil

# S3 setup
s3_client = boto3.client('s3')
sts_client = boto3.client('sts')
BUCKET_NAME = 'cse35400-bucket'  # Verify this matches your bucket

# Local directories
INDEX_DIR = "indexdir"
MAPPING_FILE = "url_mapping.json"
INDEX_PREFIX = "index_data/"

def check_aws_identity():
    """Verify AWS credentials and print identity"""
    try:
        identity = sts_client.get_caller_identity()
        print(f"AWS Identity: UserId={identity['UserId']}, Account={identity['Account']}, Arn={identity['Arn']}")
        return True
    except Exception as e:
        print(f"Failed to verify AWS identity: {e}")
        return False

def download_index_from_s3():
    """Download index files from S3"""
    try:
        if os.path.exists(INDEX_DIR):
            shutil.rmtree(INDEX_DIR)
        os.makedirs(INDEX_DIR)

        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INDEX_PREFIX)
        if 'Contents' not in response:
            print(f"No objects found in bucket '{BUCKET_NAME}' with prefix '{INDEX_PREFIX}'. Run the indexer first.")
            return False

        for obj in response['Contents']:
            key = obj['Key']
            filename = os.path.basename(key)
            if filename:
                local_path = os.path.join(INDEX_DIR, filename)
                print(f"Downloading {key} to {local_path}")
                s3_client.download_file(BUCKET_NAME, key, local_path)
        print("Successfully downloaded index files from S3")
        return True
    except s3_client.exceptions.NoSuchBucket:
        print(f"Bucket '{BUCKET_NAME}' does not exist")
        return False
    except s3_client.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        print(f"S3 Error: {error_code} - {error_msg}")
        return False
    except Exception as e:
        print(f"Unexpected error downloading index from S3: {e}")
        return False

def download_url_mapping():
    """Download URL mapping from S3"""
    try:
        s3_client.download_file(BUCKET_NAME, "crawl_data/url_mapping.json", MAPPING_FILE)
        with open(MAPPING_FILE, 'r') as f:
            print("Successfully downloaded URL mapping")
            return json.load(f)
    except s3_client.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        print(f"S3 Error downloading URL mapping: {error_code} - {error_msg}")
        return {}
    except Exception as e:
        print(f"Error downloading URL mapping: {e}")
        return {}

def simple_search():
    """Interactive search interface with advanced query support"""
    if not check_aws_identity():
        print("Cannot proceed without valid AWS credentials")
        return

    if not download_index_from_s3():
        return

    url_mapping = download_url_mapping()
    if not url_mapping:
        print("URL mapping not found. Cannot display URLs.")
        return

    try:
        ix = open_dir(INDEX_DIR)
    except Exception as e:
        print(f"Error opening index: {e}")
        return

    # Use MultifieldParser to search both title and content fields
    parser = MultifieldParser(["title", "content"], schema=ix.schema)

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

            # Parse the query with boolean operators and phrases
            query = parser.parse(query_str)
            
            with ix.searcher() as searcher:
                results = searcher.search(query, limit=20)
                if results:
                    print(f"\nFound {len(results)} result(s) for query '{query_str}':")
                    for i, hit in enumerate(results, 1):
                        s3_key = hit['title']
                        url = None
                        if s3_key in url_mapping:
                            url = s3_key
                        else:
                            for mapped_url, data in url_mapping.items():
                                if 'text' in data and data['text']['s3_key'] == s3_key:
                                    url = mapped_url
                                    break
                        url_display = url if url else "URL not found in mapping"
                        print(f"{i}. {url_display}")
                else:
                    print(f"No results found for query '{query_str}'")
                    
        except Exception as e:
            print(f"Search error: {e}. Please check your query syntax.")
            continue

    # Cleanup
    if os.path.exists(INDEX_DIR):
        shutil.rmtree(INDEX_DIR)
    if os.path.exists(MAPPING_FILE):
        os.remove(MAPPING_FILE)
    print("Cleaned up local files")

if __name__ == "__main__":
    simple_search()
