from client import redis_client
import redis
import csv
import subprocess
import pytest

key_size = 20
num_keys = 10
csv_file = '../simpleTest.csv'

@pytest.fixture
def insert_data():
    # Use the program to populate synthetic data.
    print(f'Inserting data from {csv_file}')
    subprocess.run(['python',
                    '../keys.py',
                    '--populate_csv',
                    csv_file],
                    check=True)

def test_validate_data(redis_client, insert_data):
    try:
        keys = redis_client.keys('*')
    except redis.RedisError as error:
        raise redis.RedisError(f'Failed to scan keys: {str(error)}')
    keys = sorted(keys)
    # Validate key names and size
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for key, value in reader:
            try:
                redis_value = redis_client.get(key)
                assert redis_value is not None, f'{key} not in database.'
                redis_value = redis_value.decode('utf-8')
                assert value == redis_value, (f'Expected {value}, got '
                                              f'{redis_value}'
                                             )
            except redis.RedisError as e:
                print('Redis error: {e}')
