from whoosh.index import open_dir
from whoosh.qparser import QueryParser
import os
import boto3
import json
import shutil

# S3 setup
s3_client = boto3.client('s3')
BUCKET_NAME = 'cse35400-bucket'

# Local directories
INDEX_DIR = "indexdir"
MAPPING_FILE = "url_mapping.json"
INDEX_PREFIX = "index_data/"

def download_index_from_s3():
    if os.path.exists(INDEX_DIR):
        shutil.rmtree(INDEX_DIR)
    os.makedirs(INDEX_DIR)

    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INDEX_PREFIX)
    if 'Contents' not in response:
        print("No index files found in S3. Run the indexer first.")
        return False

    for obj in response['Contents']:
        key = obj['Key']
        filename = os.path.basename(key)
        if filename:
            local_path = os.path.join(INDEX_DIR, filename)
            s3_client.download_file(BUCKET_NAME, key, local_path)

    return True

def download_url_mapping():
    try:
        s3_client.download_file(BUCKET_NAME, "crawl_data/url_mapping.json", MAPPING_FILE)
        with open(MAPPING_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error downloading URL mapping: {e}")
        return {}

def simple_search():
    if not download_index_from_s3():
        return

    url_mapping = download_url_mapping()
    if not url_mapping:
        print("URL mapping not found. Cannot display URLs.")
        return

    ix = open_dir(INDEX_DIR)
    parser = QueryParser("content", ix.schema)

    while True:
        query_str = input("\nEnter a keyword to search (exact match), or 'exit' to quit: ").strip()
        if query_str.lower() == "exit":
            break

        query = parser.parse(f'"{query_str}"')

        with ix.searcher() as searcher:
            results = searcher.search(query)
            if results:
                print(f"\nFound {len(results)} result(s):")
                for hit in results:
                    s3_key = hit['title']
                    url = url_mapping.get(s3_key, "URL not found")
                    print(f"- {url}")
            else:
                print("No results found.")

if __name__ == "__main__":
    simple_search()
