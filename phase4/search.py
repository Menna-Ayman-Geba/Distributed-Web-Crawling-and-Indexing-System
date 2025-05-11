#!/usr/bin/env python3
from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser
from whoosh.fields import Schema, TEXT
import os
import boto3
import json
import shutil
import re
import tempfile
from collections import defaultdict

# S3 setup
s3_client = boto3.client('s3')
sts_client = boto3.client('sts')
BUCKET_NAME = 'cse354000-bucket'
INDEX_BASE_DIR = "indexdir"
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

def download_index_from_s3(temp_dir):
    """Download index files from S3 into separate subdirectories per indexer"""
    try:
        # Group files by indexer ID
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INDEX_PREFIX)
        if 'Contents' not in response:
            print(f"No objects found in bucket '{BUCKET_NAME}' with prefix '{INDEX_PREFIX}'. Run the indexer first.")
            return []

        # Organize files by indexer (e.g., indexer0, indexer1)
        indexer_files = defaultdict(list)
        for obj in response['Contents']:
            key = obj['Key']
            filename = os.path.basename(key)
            match = re.match(r'^indexer(\d+)_(.*)$', filename)
            if match:
                indexer_id, base_filename = match.groups()
                indexer_files[indexer_id].append((key, base_filename))

        if not indexer_files:
            print("No valid index files found in S3.")
            return []

        # Download files into separate subdirectories
        index_dirs = []
        for indexer_id, files in indexer_files.items():
            indexer_dir = os.path.join(temp_dir, f"indexer{indexer_id}")
            os.makedirs(indexer_dir, exist_ok=True)
            for key, base_filename in files:
                local_path = os.path.join(indexer_dir, base_filename)
                print(f"Downloading {key} to {local_path}")
                s3_client.download_file(BUCKET_NAME, key, local_path)
            index_dirs.append(indexer_dir)
        
        print("Successfully downloaded index files from S3")
        return index_dirs
    except s3_client.exceptions.NoSuchBucket:
        print(f"Bucket '{BUCKET_NAME}' does not exist")
        return []
    except s3_client.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        print(f"S3 Error: {error_code} - {error_msg}")
        return []
    except Exception as e:
        print(f"Unexpected error downloading index from S3: {e}")
        return []

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

    with tempfile.TemporaryDirectory() as temp_dir:
        index_dirs = download_index_from_s3(temp_dir)
        if not index_dirs:
            return

        url_mapping = download_url_mapping()
        if not url_mapping:
            print("URL mapping not found. Cannot display URLs.")

        # Open all indexes
        indexes = []
        for index_dir in index_dirs:
            try:
                ix = open_dir(index_dir)
                indexes.append(ix)
                print(f"Opened index in {index_dir}")
            except Exception as e:
                print(f"Error opening index in {index_dir}: {e}")
                continue

        if not indexes:
            print("No valid indexes could be opened. Cannot proceed with search.")
            return

        # Schema for parsing (must match indexer's schema)
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

                # Parse the query
                query = parser.parse(query_str)
                all_results = []

                # Search each index and collect results
                for ix in indexes:
                    with ix.searcher() as searcher:
                        results = searcher.search(query, limit=20)
                        for hit in results:
                            all_results.append((hit['title'], hit.score))

                # Sort results by score (descending) and deduplicate by title
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
                print(f"Search error: {e}. Please check your query syntax.")
                continue

    # Cleanup URL mapping file
    if os.path.exists(MAPPING_FILE):
        os.remove(MAPPING_FILE)
    print("Cleaned up local files")

if __name__ == "__main__":
    simple_search()
