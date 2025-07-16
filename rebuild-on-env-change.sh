#!/bin/bash

# Generate hash of .env file
if [ -f ".env" ]; then
    ENV_HASH=$(md5sum .env | cut -d' ' -f1)
    export ENV_HASH
    echo "ENV hash: $ENV_HASH"
else
    echo "No .env file found"
    ENV_HASH=""
    export ENV_HASH
fi

# Rebuild and start services
echo "Rebuilding with ENV_HASH=$ENV_HASH..."
docker-compose build --build-arg ENV_HASH="$ENV_HASH"
docker-compose up -d 