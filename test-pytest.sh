#!/bin/bash

# Remove all containers
docker rm -f $(docker ps -aq)

RANGER_DOCKER_PATH="dev-support/ranger-docker"
TESTS_PATH="$HOME/Desktop/PyTest-KMS-EP"
cd "$RANGER_DOCKER_PATH"

# Download archives
./download-archives.sh hadoop hbase 

docker system prune --all --force --volumes

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
export RANGER_DB_TYPE=postgres

# Build base image
docker-compose -f docker-compose.ranger-base.yml build --no-cache

# Build Apache Ranger
docker-compose -f docker-compose.ranger-base.yml -f docker-compose.ranger-build.yml up --build

# Bring up all services
docker compose -f docker-compose.ranger-${RANGER_DB_TYPE}.yml \
  -f docker-compose.ranger.yml \
  -f docker-compose.ranger-usersync.yml \
  -f docker-compose.ranger-tagsync.yml \
  -f docker-compose.ranger-kms.yml \
  -f docker-compose.ranger-hadoop.yml \
  -f docker-compose.ranger-hbase.yml up -d --build

echo "🕐 Waiting for containers to start..."
sleep 60

echo "✅ Checking container status..."
containers=(
  ranger ranger-zk ranger-solr ranger-postgres ranger-usersync
  ranger-tagsync ranger-kms ranger-hadoop ranger-hbase
)
flag=true
for container in "${containers[@]}"; do
  if [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null) == "true" ]]; then
    echo "✔️ Container $container is running!"
  else
    echo "❌ Container $container is NOT running!"
    flag=false
  fi
done

if [[ $flag == true ]]; then
  echo "🚀 All required containers are up. Running test cases..."
  cd "$TESTS_PATH"  # Switch to the tests directory
source ./myenv/bin/activate
  pytest -vs test_kms/   # Runs all tests in the tests directory
  pytest -vs test_hdfs/
else
  echo "⚠️ Some containers failed to start. Exiting..."
  docker stop $(docker ps -q) && docker rm $(docker ps -aq)
  exit 1
fi

echo "🧹 Cleaning up containers..."
docker stop $(docker ps -q) && docker rm $(docker ps -aq)

echo "✅ Test execution complete and environment cleaned up!"
exit  0

