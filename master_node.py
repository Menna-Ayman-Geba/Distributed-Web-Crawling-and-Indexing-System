from mpi4py import MPI  # Enables message passing between nodes
import time
import logging  #Gives timestamped logs

import redis  # Task queue 
import json   #for encoding/decoding data sent via Redis or MPI.
import uuid   # Unique IDs for tasks


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master -%(levelname)s - %(message)s')

def master_process():

    comm = MPI.COMM_WORLD   #Connects to all MPI nodes.
    rank = comm.Get_rank()  #Gets current node's rank (0 = master).
    size = comm.Get_size()  #how many total nodes are in the system.
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    # Initialize redis connection
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    queue_name = 'url_queue'

    # Initialize task queue with seed URLs
    seed_urls = ["http://example.com", "http://example.org"]
    for url in seed_urls:
        redis_client.rpush(queue_name, json.dumps({'url': url, 'task_id': str(uuid.uuid4())}))

    crawler_nodes = size - 2 # Assuming master and at least one indexer node
    indexer_nodes = 1 # At least one indexer node

    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Not enough nodes to run crawler and indexer. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
        return


    active_crawler_nodes = list(range(1, 1 + crawler_nodes)) # Ranks for crawler nodes (assuming rank 0 is master)
    active_indexer_nodes = list(range(1 + crawler_nodes, size)) # Ranks for indexer nodes
    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")
    crawler_tasks_assigned = 0
    task_count = 0
    max_tasks_per_crawler = 5  # Limit tasks per crawler for load balancing
    
    #Keeps running until all URLs are crawled and no crawler is busy.
    while redis_client.llen(queue_name) > 0 or crawler_tasks_assigned > 0:  
            # Check for completed crawler tasks and results
            if crawler_tasks_assigned > 0:
                if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
                    message_source = status.Get_source()
                    message_tag = status.Get_tag()
                    message_data = comm.recv(source=message_source, tag=message_tag)
                    
                    if message_tag == 1:  # Crawler completed task and sent back extracted URLs and content
                        crawler_tasks_assigned -= 1
                        extracted_urls, content = message_data
                        if extracted_urls:
                            for url in extracted_urls:
                                redis_client.rpush(queue_name, json.dumps({'url': url, 'task_id': str(uuid.uuid4())}))
                        if content:
                            # Send content to indexer (round-robin)
                            indexer_rank = active_indexer_nodes[task_count % len(active_indexer_nodes)]
                            comm.send(content, dest=indexer_rank, tag=2)
                        logging.info(f"Master received URLs from Crawler {message_source}, URLs in queue: {redis_client.llen(queue_name)}, Tasks assigned: {crawler_tasks_assigned}")
                    
                    elif message_tag == 99:  # Crawler node status/heartbeat
                        logging.info(f"Crawler {message_source} status: {message_data}")
                    
                    elif message_tag == 999:  # Crawler node reports error
                        logging.error(f"Crawler {message_source} reported error: {message_data}")
                        crawler_tasks_assigned -= 1
                        # Re-queue the failed task
                        if 'url' in message_data:
                            redis_client.rpush(queue_name, json.dumps({'url': message_data['url'], 'task_id': str(uuid.uuid4())}))

            # Assign new crawling tasks
            while redis_client.llen(queue_name) > 0 and crawler_tasks_assigned < len(active_crawler_nodes) * max_tasks_per_crawler:
                task_data = redis_client.lpop(queue_name)
                if task_data:
                    task = json.loads(task_data)
                    url_to_crawl = task['url']
                    task_id = task['task_id']
                    available_crawler_rank = active_crawler_nodes[crawler_tasks_assigned % len(active_crawler_nodes)]
                    comm.send({'url': url_to_crawl, 'task_id': task_id}, dest=available_crawler_rank, tag=0)
                    crawler_tasks_assigned += 1
                    task_count += 1
                    logging.info(f"Master assigned task {task_id} (crawl {url_to_crawl}) to Crawler {available_crawler_rank}, Tasks assigned: {crawler_tasks_assigned}")

            time.sleep(0.1)  # Prevent overwhelming master

        # Send shutdown signal to crawlers and indexers
    for crawler_rank in active_crawler_nodes:
        comm.send(None, dest=crawler_rank, tag=0)
    for indexer_rank in active_indexer_nodes:
        comm.send(None, dest=indexer_rank, tag=2)

    logging.info("Master node finished URL distribution.")
    print("Master Node Finished.")

if __name__ == '__main__':
    master_process()
