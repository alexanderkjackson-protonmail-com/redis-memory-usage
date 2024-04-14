from client import redis_client
import redis
import subprocess
import pytest

key_size = 20
num_keys = 10

@pytest.fixture
def insert_data():
    # Use the program to populate synthetic data.
    print(f'Inserting {num_keys} keys of size {key_size} from synthetic data.')
    subprocess.run(['python',
                    '../keys.py',
                    '--synpopulate',
                    str(num_keys),
                    str(key_size)],
                    check=True)

def test_validate_data(redis_client, insert_data):
    try:
        keys = redis_client.keys('*')
    except redis.RedisError as error:
        raise redis.RedisError(f'Failed to scan keys: {str(error)}')
    keys = sorted(keys)
    # Validate key names and size
    for index, key in enumerate(keys):
        expected_key = f'key{index}'.encode()
        assert key == expected_key, f'Expected {expected_key}, got {key}'
        key_value = redis_client.get(key)
        assert len(key_value) == key_size, (f'Expected {key} to have key size '
                                            f'of {key_size}. Found '
                                            f'{len(key_value)} instead.'
                                           )
    assert len(keys) == num_keys, (f'Expected {num_keys} keys, found '
                                   f'{len(keys)}'
                                  )
