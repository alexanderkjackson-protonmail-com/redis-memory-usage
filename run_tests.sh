#!/bin/bash

# Start Redis container
docker-compose up -d redis

# Navigate to the tests directory
cd tests

# Run tests (using pytest as an example)
echo "Executing tests..."
pytest

echo "Tests complete."
docker stop redis-memory-usage-redis-1
