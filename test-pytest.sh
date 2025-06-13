# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This workflow will build a Java project with Maven, and cache/restore any dependencies to improve the workflow execution time
# For more information see: https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-java-with-maven

# This workflow uses actions that are not certified by GitHub.
# They are provided by a third-party and are governed by
# separate terms of service, privacy policy, and support
# documentation.



#!/bin/bash

#handle input
if [ $# -lt 1 ]; then
  echo "no arguments passed , using default DB: postgres"
  DB_TYPE=postgres
  EXTRA_SERVICES=()

else
  DB_TYPE=$1
  shift
  EXTRA_SERVICES=("$@")
fi

# Remove all containers and clean up docker space
docker rm -f $(docker ps -aq --filter "name=ranger")
docker system prune --all --force --volumes

#path setup
RANGER_DOCKER_PATH="dev-support/ranger-docker"
TESTS_PATH="../../PyTest-KMS-HDFS"
cd "$RANGER_DOCKER_PATH"|| exit 1

# Download archives
if [${#EXTRA_SERVICES[@]} -gt 0]; then
  ./download-archives.sh "${EXTRA_SERVICES[@]}"
fi

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1
export RANGER_DB_TYPE="$DB_TYPE"

# Build base image
docker-compose -f docker-compose.ranger-base.yml build

# Build Apache Ranger
docker-compose -f docker-compose.ranger-base.yml -f docker-compose.ranger-build.yml up --build

# Bring up basic services
DOCKER_FILES=(
  "-f" "docker-compose.ranger-${RANGER_DB_TYPE}.yml"
  "-f" "docker-compose.ranger.yml"
  "-f" "docker-compose.ranger-usersync.yml"
  "-f" "docker-compose.ranger-tagsync.yml"
  "-f" "docker-compose.ranger-kms.yml"
  )

# add extra service from input
for service in "${EXTRA_SERVICES[@]}" ; do
  DOCKER_FILES+=("-f" "docker-compose.ranger-${service}.yml")
done

#start containers
docker compose "${DOCKER_FILES[@]}" up -d --build

echo "🕐 Waiting for containers to start..."
sleep 60

echo "✅ Checking container status..."
BASE_SERVICES=(ranger ranger-${RANGER_DB_TYPE} ranger-zk ranger-solr ranger-usersync ranger-tagsync ranger-kms)
ALL_SERVICES=("${BASE_SERVICES[@]}")

for service in "${EXTRA_SERVICES[@]}"; do
  ALL_SERVICES+=("ranger-${service}")
done


flag=true
for container in "${ALL_SERVICES[@]}"; do
  if [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null) == "true" ]]; then
    echo "✔️ Container $container is running!"
  else
    echo "❌ Container $container is NOT running!"
    flag=false
  fi
done

#RUN TESTS--------
if [[ $flag == true ]]; then
  echo "🚀 All required containers are up. Running test cases..."
  cd "$TESTS_PATH" || exit 1         # Switch to the tests directory

  python3 -m venv myenv || { echo "Failed to create venv"; exit 1; }         # Create a new environment
  source myenv/bin/activate || { echo "Failed to activate venv"; exit 1; }   # Activate it
  pip install --upgrade pip
  pip install -r requirements.txt || { echo "❌ Failed to install requirements"; exit 1; }  # Install dependencies

  pytest -vs test_kms/ --html=report_kms.html  # Runs all tests in the tests directory
  pytest -vs test_hdfs/ --html=report_hdfs.html
else
  echo "⚠️ Some containers failed to start. Exiting..."
  docker stop $(docker ps -q --filter "name=ranger") && docker rm $(docker ps -aq --filter "name=ranger")
  exit 1
fi

echo "🧹 Cleaning up containers..."
docker stop $(docker ps -q --filter "name=ranger") && docker rm $(docker ps -aq --filter "name=ranger")

# Open the generated reports
echo " Opening test reports..."
if command -v xdg-open &> /dev/null; then
  xdg-open report_kms.html  # Opens the test_kms report
  xdg-open report_hdfs.html # Opens the test_hdfs report
elif command -v open &> /dev/null; then
  open report_kms.html  # macOS command
  open report_hdfs.html # macOS command
fi

echo "✅ Test execution complete and environment cleaned up!"
exit  0

