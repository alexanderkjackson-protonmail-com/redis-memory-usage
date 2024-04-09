import redis
import heapq
import argparse
import socket

# Verify IP address or hostname supplied to arg parser
def valid_port(port):
    try:
        port = int(port)
        if 1 <= port <= 65535:
            return port
        else:
            raise argparse.ArgumentTypeError(f"Invalid port number: {port}."
                                             " Must be integer between"
                                             " 1-65535 inclusive.")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid port number: {port}. Port"
                                          "number must be an integer.")

def ip_or_hostname(string):
    try:
        # Confirm valid IP address
        socket.inet_aton(string)
        return string
    except socket.error:
        # Not IP. Try and validate hostname
        try:
            socket.gethostbyname(string)
            return string
        except socket.error:
            # Neither valid hostname nor IP supplied
            raise argparse.ArgumentTypeError(f"{string} is not a valid IP"
                                             " address nor hostname.")

# Collect arguments
parser = argparse.ArgumentParser(description="Retrieves top memory-consuming"
                                 " keys from Redis database.")
parser.add_argument('--heap-size', type=int, default=10, help="Specify number"
                    " of keys to retrieve (top heap_size keys by size).")
parser.add_argument('--host', type=ip_or_hostname, help="IP address or"
                    " hostname of Redis host.")
parser.add_argument('--port', type=valid_port, help="Port number to connect"
                    " to")
parser.add_argument('--database', type=int, help="Database to connect to")

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
