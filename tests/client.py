import redis
import pytest

@pytest.fixture
def redis_client():
    client = redis.Redis(host='localhost', port=6379, db=0)
    client.flushdb()  # Clear the database before each test
    yield client
    client.close()
