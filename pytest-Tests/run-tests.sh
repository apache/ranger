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
DB_TYPE="${1:-postgres}"
shift || true
EXTRA_SERVICES=("$@")

# Cleanup is optional if you need Fresh containers install(export CLEAN_CONTAINERS=1 to force fresh start)
CLEAN_CONTAINERS="${CLEAN_CONTAINERS:-0}"

echo "Using DB type: ${DB_TYPE}"
if [ "${#EXTRA_SERVICES[@]}" -gt 0 ]; then
  echo "Extra services: ${EXTRA_SERVICES[*]}"
fi

echo "CLEAN_CONTAINERS=${CLEAN_CONTAINERS}"

# Remove all containers and clean up docker space
if [[ "${CLEAN_CONTAINERS}" == "1" ]]; then
  docker rm -f $(docker ps -aq --filter "name=ranger") 2>/dev/null || true
  docker system prune --all --force --volumes
fi

#path setup
RANGER_DOCKER_PATH="../dev-support/ranger-docker"
TESTS_PATH="../../pytest-Tests"

cd "$RANGER_DOCKER_PATH"|| exit 1

# Ensure scripts are executable
chmod +x scripts/**/*.sh || true
chmod +x download-archives.sh || true

# Download archives
if [ "${#EXTRA_SERVICES[@]}" -gt 0 ]; then
  ./download-archives.sh "${EXTRA_SERVICES[@]}"
fi

export RANGER_DB_TYPE="${DB_TYPE}"

# Build Apache ranger (admin) only if missing (or CLEAN_CONTAINERS=1)
ADMIN_SERVICE="ranger"
admin_missing=false

if [[ "${CLEAN_CONTAINERS}" == "1" ]]; then
  admin_missing=true
elif [[ -z "$(docker compose -f docker-compose.ranger-build.yml ps -q "${ADMIN_SERVICE}" 2>/dev/null)" ]]; then
  admin_missing=true
fi

if [[ "${admin_missing}" == "true" ]]; then
  echo "Admin service (${ADMIN_SERVICE}) missing. Building and starting"
  docker compose -f docker-compose.ranger-build.yml build
  docker compose -f docker-compose.ranger-build.yml up -d
else
  echo "Admin service (${ADMIN_SERVICE}) already exists. Skipping build/up."
  docker compose -f docker-compose.ranger-build.yml up -d
fi

# Bring up basic services
DOCKER_FILES=(
  "-f" "docker-compose.ranger.yml"
  "-f" "docker-compose.ranger-usersync.yml"
  "-f" "docker-compose.ranger-tagsync.yml"
  "-f" "docker-compose.ranger-kms.yml"
  )

# add extra service from input
for service in "${EXTRA_SERVICES[@]}" ; do
  DOCKER_FILES+=("-f" "docker-compose.ranger-${service}.yml")
done

BASE_SERVICES=(ranger ranger-${RANGER_DB_TYPE} ranger-zk ranger-solr ranger-usersync ranger-tagsync ranger-kms)
ALL_SERVICES=("${BASE_SERVICES[@]}")
for service in "${EXTRA_SERVICES[@]}"; do
  ALL_SERVICES+=("ranger-${service}")
done

# only create/build if containers do not exist
missing=false
for container in "${ALL_SERVICES[@]}"; do
  if ! docker container inspect "$container" >/dev/null 2>&1; then
    missing=true
    break
  fi
done

if [[ "${missing}" == "true" ]]; then
  echo "Some containers are missing. Creating services..."
  docker compose "${DOCKER_FILES[@]}" up -d --build
else
  echo "All containers already exist. Starting without rebuild..."
  docker compose "${DOCKER_FILES[@]}" up -d
fi

echo "Waiting for containers to start..."
if [[ "${missing}" == "true" || "${admin_missing}" == "true" ]]; then
  sleep 60
else
  echo "No rebuild/start needed. Skipping wait."
fi

echo "Checking container status..."
flag=true
for container in "${ALL_SERVICES[@]}"; do
  if [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null) == "true" ]]; then
    echo "Container $container is running!"
  else
    echo "Container $container is NOT running!"
    flag=false
  fi
done

#RUN TESTS--------
#Use export RUN_TESTS=0 to only bring up infra and allow all services to initialise properly (NOTE: It'll skip tests to avoid early startup failures).
RUN_TESTS="${RUN_TESTS:-1}"

if [[ $flag == true ]]; then
  if [[ "${RUN_TESTS}" == "1" ]]; then
    echo "All required containers are up. Running test cases..."
    cd "$TESTS_PATH" || exit 1         # Switch to the tests directory

    python3 -m venv myenv || { echo "Failed to create venv"; exit 1; }         # Create a new virtual environment
    source myenv/bin/activate || { echo "Failed to activate venv"; exit 1; }   # Activate it
    pip install --upgrade pip
    pip install -r requirements.txt || { echo "Failed to install requirements"; exit 1; }  # Install dependencies

    pytest -vs hdfs/ --html=report_hdfs.html # Runs all tests in the hdfs directory
    pytest -vs kms/ --html=report_kms.html   # Runs all tests in the kms directory
  else
      echo "All required containers are up. Skipping tests (TESTS_RUN=${TESTS_RUN})."
  fi
else
  echo "Some containers failed to start. Exiting..."
  if [[ "${CLEAN_CONTAINERS}" == "1" ]]; then
      docker stop $(docker ps -q --filter "name=ranger") 2>/dev/null || true
      docker rm $(docker ps -aq --filter "name=ranger") 2>/dev/null || true
    fi
    exit 1
  fi

if [[ "${CLEAN_CONTAINERS}" == "1" ]]; then
  echo "Cleaning up containers..."
  docker stop $(docker ps -q --filter "name=ranger") 2>/dev/null || true
  docker rm $(docker ps -aq --filter "name=ranger") 2>/dev/null || true
else
  echo "Skipping cleanup (CLEAN_CONTAINERS!=1)."
fi

echo "Test execution complete!"
exit  0
