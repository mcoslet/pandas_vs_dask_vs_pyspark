#!/bin/bash
# Display an error message in red text
error() {
    echo -e "\033[0;31m$1\033[0m"
}

if [ $# -eq 0 ]; then
    error "[ERROR] No Python script specified. You must pass the path to py script for execution"
    exit 1
fi

IMAGE_NAME="grid_talk"
CONTAINER_NAME="benchmark_test"

# Check if the Docker image already exists
image_exists=$(docker images -q $IMAGE_NAME)

if [[ -z "$image_exists" ]]; then
    echo "Image does not exist, building..."
    docker build -t $IMAGE_NAME .
else
    echo "Image already exists, skipping build."
fi

SCRIPT_NAME=$1

echo "Running $SCRIPT_NAME in the Docker container $CONTAINER_NAME."
docker run --rm --name $CONTAINER_NAME --cpus="2" --memory="10g" $IMAGE_NAME $SCRIPT_NAME

