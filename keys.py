import redis
import heapq
import argparse
import socket
import csv
import string
import random

def populate_csv(connection, csv_file):
    """
    Populates a Redis database with key-value pairs from a CSV file.

    This function attempts to read a CSV file specified by `csv_file` and for 
    each row in the file, sets a key-value pair in the Redis database 
    represented by `connection`. The function first checks if the connection 
    to Redis is active. It handles file and Redis-specific errors gracefully, 
    providing informative error messages.

    Parameters:
    connection (redis.StrictRedis): A Redis connection object.
    csv_file (str): The path to the CSV file to be processed.

    Returns:
    None: This function does not return any value but prints status messages.

    Raises:
    redis.ConnectionError: If the Redis connection fails.
    FileNotFoundError: If the CSV file does not exist.
    PermissionError: If there is no permission to read the CSV file.
    csv.Error: If there is an error processing the CSV file.
    Exception: Catches other miscellaneous errors during the operation.
    """
    # Ensure the Redis connection is alive
    try:
        if not connection.ping():
            print('Failed to connect to Redis. Exiting.')
            return
    except redis.ConnectionError:
        print('Redis connection error occurred. Exiting.')
        return
    except Exception as e:
        print(f'An unexpected error occurred with Redis: {e}. Exiting.')
        return

    # Attempt to load data from the CSV file
    print(f'Loading data from {csv_file}')
    try:
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            for key, value in reader:
                try:
                    connection.set(key, value)
                except redis.RedisError as e:
                    print(f'Failed to set Redis key-value pair {key}:{value}'
                          f' due to: {e}')
                except Exception as e:
                    print(f'An unexpected error occurred while setting'
                          f'key-value in Redis: {e}')
    except FileNotFoundError:
        print(f'The file {csv_file} does not exist.'
              f' Please check the file path.')
    except PermissionError:
        print(f'Permission denied to read the file {csv_file}.')
    except csv.Error as e:
        print(f'Error reading CSV file {csv_file}: {e}')
    except Exception as e:
        print(f'An unexpected error occurred while reading {csv_file}: {e}')

def populate_synthetic(connection, numKeys, keyLength):
    """
    Populate a database with synthetic keys.

    This function generates a specified number of synthetic keys, each with a
    specified length, and stores them in the database using the provided
    connection.

    :param connection: The database connection object.
    :type connection: Redis or similar database connection type
    :param numKeys: The number of synthetic keys to generate.
    :type numKeys: int
    :param keyLength: The length of each key.
    :type keyLength: int
    :raises TypeError: If numKeys or keyLength is not an integer.
    :raises ValueError: If numKeys or keyLength is negative.
    """
    print('Populating database with synthetic data')
    for index in range(numKeys):
        keyValue = random_string(keyLength)
        keyName = 'key' + f'{index}'
        try:
            connection.set(keyName, keyValue)
        except Exception as e:
            print(f'Error while populating database with key {keyName}:'
                  f' {str(e)}')
    print(f'Successfully populated database with {numKeys} keys of size:'
          f' {keyLength}')

# Function to process a batch of keys with MEMORY USAGE using pipelining
def largest_keys(connection, heap_size, batch_size):
    """
    Identify and return the largest Redis keys based on memory usage.

    This function scans all keys in the Redis database in batches, 
    calculates their memory usage, and keeps track of the largest ones using a 
    min-heap. The function returns the largest keys sorted by their memory 
    usage in descending order.
    :param connection: The Redis connection object.
    :type connection: redis.StrictRedis or redis.Redis
    :param heap_size: The number of top keys to retain in terms of memory 
                      usage.
    :type heap_size: int
    :param batch_size: The number of keys to scan in each batch.
    :type batch_size: int
    :return: A list of tuples, each containing memory usage and the
             corresponding key, sorted by memory usage descending.
    :rtype: list of (int, str)
    :raises RedisError: If an error occurs in communication with the Redis 
                        server.
    :raises ValueError: If the provided batch size or heap size is not valid.
    """
    if not isinstance(heap_size, int) or heap_size <= 0:
        raise ValueError('heap_size must be a positive integer.')
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError('batch_size must be a positive integer.')

    cursor = 0
    key_memory_heap = []

    while True:
        try:
            cursor, keys = connection.scan(cursor=cursor, match='*',
                                           count=batch_size)
        except redis.RedisError as e:
            raise redis.RedisError(f'Failed to scan keys: {str(e)}')

        try:
            pipeline = connection.pipeline()
            for key in keys:
                pipeline.memory_usage(key)
            results = pipeline.execute()
        except redis.RedisError as e:
            raise redis.RedisError(f'Failed to fetch memory usage for keys:'
                                   f'{str(e)}')

        try:
            for key, memory in zip(keys, results):
                heapq.heappush(key_memory_heap, (memory, key))
                if len(key_memory_heap) > heap_size:
                    heapq.heappop(key_memory_heap)
        except Exception as e:
            raise Exception(f'Failed during heap operations: {str(e)}')
        if cursor == 0:
            break
    return sorted(key_memory_heap, key=lambda x: x[0], reverse=True)

# Generate random string
def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits,
                                  k=length))

# Verify port number is valid. 
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
                                          " number must be an integer.")

# Verify IP or hostname provided is valid.
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
def redis_connect(connection_params):
# Connect to Redis
    try:
        connection = redis.Redis(**connection_params, decode_responses=True)
        connection.ping() # Check if connection is alive - Add test or remove
        return connection
    except redis.exceptions.ConnectionError as error:
        print(f"Connection error: {error}")
        print(f"Failed to connect to Redis at {connection_params['host']}"
            f":{connection_params['port']}")
        exit(-1)

def main():
# Collect arguments
    parser = argparse.ArgumentParser(description="Retrieves top memory-"
                                    "consuming keys from Redis database.")
    parser.add_argument('--heap-size', type=int, default=10, help='Specify'
                        ' number of keys to retrieve (top heap_size keys by'
                        ' size).')
    parser.add_argument('--host', type=ip_or_hostname, default='localhost',
                        help="IP address or hostname of Redis host.")
    parser.add_argument('-p', '--port', type=valid_port, default=6379,
                        help="Port number to connect to")
    parser.add_argument('-d', '--database', type=int, help="Database to use")
    parser.add_argument('-a', '--password', type=str,
                        help="Password for authentication")
    parser.add_argument('--populate_csv', metavar='CSV-FILE', type=str,
                        help='Populate database from CSV file')
    parser.add_argument('--synpopulate', nargs=2,
                        metavar=('NUM_KEYS', 'KEY_LENGTH'),
                        type=int, help='Populate database with NUMBER keys'
                        ' with keys of KEY_LENGTH length.')

    args = parser.parse_args()

# Batch size for pipelining
    batch_size = 10
    heap_size = args.heap_size

# Pack connection parameters dictionary
    connection_params = {
        'host': args.host,
        'port': args.port,
    }

# Set password, if requested to be specified 
    if args.password is not None:
        connection_params['password'] = args.password
# Set database, if requested to be specified
    if args.database is not None:
        connection_params['db'] = args.database
# Connect to Redis
    connection = redis_connect(connection_params)
# Populate database routines. 
    if args.populate_csv:
        populate_csv(connection, args.populate_csv)
        exit(0)
# Populate database with synthetic data of fixed length
    if args.synpopulate:
        num_keys = args.synpopulate[0]
        key_length = args.synpopulate[1]
        populate_synthetic(connection, num_keys, key_length)
        exit(0)
# Process keys to find largest heap_size keys
    key_memory_heap_sorted = largest_keys(connection, heap_size, batch_size)
    if not key_memory_heap_sorted:
        print("No keys in database.")
        exit(0)
    for key, memory in key_memory_heap_sorted:
          print(f"{key}: {memory}")

if __name__ == "__main__":
    main()
