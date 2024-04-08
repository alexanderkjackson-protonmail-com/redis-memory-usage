import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Batch size for pipelining
BATCH_SIZE = 10

# Function to process a batch of keys with MEMORY USAGE using pipelining
def process_keys(keys):
    if not keys:
        return
    pipeline = r.pipeline()
    for key in keys:
        pipeline.memory_usage(key)
    results = pipeline.execute()
    for key, memory in zip(keys, results):
        key_memory_pairs.append((key, memory))

# Scan all keys and batch process them
cursor = '0'
batch_keys = []
key_memory_pairs = []
while cursor != 0:
    cursor, keys = r.scan(cursor=cursor, match='*', count=BATCH_SIZE)
    for key in keys:
        batch_keys.append(key)
        if len(batch_keys) == BATCH_SIZE:
            process_keys(batch_keys)
            batch_keys = []

# Process any remaining keys in the last batch
process_keys(batch_keys)

key_memory_pairs_sorted = sorted(key_memory_pairs, key=lambda x: x[1], reverse=True)

for key, memory in key_memory_pairs_sorted:
    print(f"{key}: {memory}")
