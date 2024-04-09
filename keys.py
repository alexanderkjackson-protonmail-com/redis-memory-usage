import redis
import heapq
import argparse

# Collect argument
parser = argparse.ArgumentParser(description='Retrieves top memory-consuming\
                                 keys from Redis database.')
parser.add_argument('--heap-size', type=int, default=10, help='Specify number\
                    of keys to retrieve (top heap_size keys by size).')
args = parser.parse_args()

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Batch size for pipelining
BATCH_SIZE = 10
heap_size = args.heap_size

# Function to process a batch of keys with MEMORY USAGE using pipelining
def process_keys(keys):
    if not keys:
        return
    pipeline = r.pipeline()
    for key in keys:
        pipeline.memory_usage(key)
    results = pipeline.execute()
    for key, memory in zip(keys, results):
        heapq.heappush(key_memory_heap, (memory, key))
        if len(key_memory_heap) > heap_size:
            heapq.heappop(key_memory_heap)

# Scan all keys and batch process them
cursor = '0'
batch_keys = []
key_memory_heap = []
while cursor != 0:
    cursor, keys = r.scan(cursor=cursor, match='*', count=BATCH_SIZE)
    for key in keys:
        batch_keys.append(key)
        if len(batch_keys) == BATCH_SIZE:
            process_keys(batch_keys)
            batch_keys = []

# Process any remaining keys in the last batch
process_keys(batch_keys)

key_memory_heap_sorted = sorted(key_memory_heap, key=lambda x: x[0],
                                reverse=True)

for key, memory in key_memory_heap_sorted:
    print(f"{key}: {memory}")
