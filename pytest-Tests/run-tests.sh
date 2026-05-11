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

# All available test suites (pytest folders)
ALL_TEST_SUITES=(xuserrest servicerest hdfs kms)

# Suites that have actual docker-compose services
DOCKER_SERVICES=(hdfs kms)

# Test-only suites (no docker-compose file needed)
TEST_ONLY_SUITES=(xuserrest servicerest)

#handle input
DB_TYPE="${1:-}"
shift || true
EXTRA_SERVICES=("$@")

# Prompt for DB_TYPE if not provided
if [[ -z "${DB_TYPE}" ]]; then
  echo ""
  echo "Available DB types: postgres, mysql, oracle"
  read -rp "Enter DB type (press Enter to default to postgres): " input_db
  DB_TYPE="${input_db:-postgres}"
fi

# Prompt for EXTRA_SERVICES / TEST SUITES if not provided
if [[ "${#EXTRA_SERVICES[@]}" -eq 0 ]]; then
  echo ""
  echo "Available test suites: ${ALL_TEST_SUITES[*]}"
  read -rp "Enter test suites space-separated (press Enter to run ALL): " -a input_services
  if [[ "${#input_services[@]}" -gt 0 && -n "${input_services[0]}" ]]; then
    EXTRA_SERVICES=("${input_services[@]}")
  else
    echo "No input given. Running ALL test suites..."
    EXTRA_SERVICES=("${ALL_TEST_SUITES[@]}")
  fi
fi

echo ""
echo "DB Type     : ${DB_TYPE}"
echo "Test Suites : ${EXTRA_SERVICES[*]}"
echo ""
echo "CLEAN_CONTAINERS=${CLEAN_CONTAINERS}"

# Remove all containers and clean up docker space
if [[ "${CLEAN_CONTAINERS}" == "1" ]]; then
  docker rm -f $(docker ps -aq --filter "name=ranger") 2>/dev/null || true
  docker system prune --all --force --volumes
fi

#path setup
RANGER_DOCKER_PATH="../dev-support/ranger-docker"
TESTS_PATH="../../pytest-Tests"

cd "$RANGER_DOCKER_PATH" || exit 1

# Ensure scripts are executable
chmod +x scripts/**/*.sh || true
chmod +x download-archives.sh || true

# Download archives — only for docker-backed services
DOCKER_BACKED=()
for service in "${EXTRA_SERVICES[@]}"; do
  for ds in "${DOCKER_SERVICES[@]}"; do
    if [[ "$service" == "$ds" ]]; then
      case "$service" in
        hdfs)
          # 'hive' arg downloads hadoop + tez (both needed by Dockerfile.ranger-hadoop)
          # 'hadoop' arg alone does NOT download tez
          DOCKER_BACKED+=("hive")
          ;;
        *)
          DOCKER_BACKED+=("$service")
          ;;
      esac
    fi
  done
done

# Deduplicatec
DOCKER_BACKED+=("kms")
DOCKER_BACKED=($(printf '%s\n' "${DOCKER_BACKED[@]}" | sort -u))

if [[ "${#DOCKER_BACKED[@]}" -gt 0 ]]; then
  echo "Downloading archives for: ${DOCKER_BACKED[*]}"
  ./download-archives.sh "${DOCKER_BACKED[@]}"
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
  echo "Admin service (${ADMIN_SERVICE}) missing."
  # Remove leftover 'version' directory from previous build to prevent mv error
  if [[ "${CLEAN_CONTAINERS}" == "1" ]]; then
    rm -rf dist/version
  fi

  docker compose -f docker-compose.ranger-build.yml build
  if [[ $? -ne 0 ]]; then
    echo "ERROR: Ranger build failed. Exiting..."
    exit 1
  fi

  if [[ "${CLEAN_CONTAINERS}" == "1" ]]; then
    echo "Starting containers because CLEAN_CONTAINERS=1"
    docker compose -f docker-compose.ranger-build.yml up

    if [[ $? -ne 0 ]]; then
      echo "ERROR: Ranger startup failed. Exiting..."
      exit 1
    fi
  else
    echo "Skipping 'docker compose up' because CLEAN_CONTAINERS is not 1"
  fi
else
  echo "Admin service (${ADMIN_SERVICE}) already exists. Skipping build/up."
fi

# Bring up basic services
DOCKER_FILES=(
  "-f" "docker-compose.ranger.yml"
  "-f" "docker-compose.ranger-usersync.yml"
  "-f" "docker-compose.ranger-tagsync.yml"
  "-f" "docker-compose.ranger-kms.yml"
)

# Add compose files based on requested suites
for service in "${EXTRA_SERVICES[@]}"; do
  case "$service" in
    hdfs)
      DOCKER_FILES+=("-f" "docker-compose.ranger-hadoop.yml")
      ;;
    kms)
      # already included in base
      ;;
  esac
done

# Build ALL_SERVICES list — only docker-backed services get container checks
BASE_SERVICES=(ranger ranger-${RANGER_DB_TYPE} ranger-zk ranger-solr ranger-kms)
ALL_SERVICES=("${BASE_SERVICES[@]}")
for service in "${EXTRA_SERVICES[@]}"; do
  case "$service" in
    hdfs)
      ALL_SERVICES+=("ranger-hadoop")
      ;;
    kms)
      : # already in BASE_SERVICES
      ;;
  esac
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
  sleep 20
else
  echo "No rebuild/start needed. Skipping wait."
fi
echo "Checking container status..."
flag=true

for container in "${ALL_SERVICES[@]}"; do
  if [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null) == "true" ]]; then
    echo "Container $container is running!"
  else
    echo "Container $container is NOT running! Attempting restart..."

    docker restart "$container" >/dev/null 2>&1

    echo "Waiting 5 seconds before re-check..."
    sleep 5

    if [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null) == "true" ]]; then
      echo "Container $container successfully restarted!"
      # DO NOTHING → keep flag=true
    else
      echo "Container $container FAILED to restart!"
      flag=false
      break
    fi
  fi
done

if [ "$flag" = false ]; then
  echo "Some containers failed to start. Exiting..."
  exit 1
else
  echo "All containers are running fine!"
fi

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

    echo ""
    echo "Running tests for suites: ${EXTRA_SERVICES[*]}"
    echo ""

    for suite in "${EXTRA_SERVICES[@]}"; do
      if [[ -d "${suite}" ]]; then
        echo "-------------------------------------------"
        echo "Running tests for: ${suite}"
        echo "-------------------------------------------"
        pytest -vs "${suite}/" --html="report_${suite}.html"
      else
        echo "WARNING: No test folder found for '${suite}', skipping..."
      fi
    done

  else
    echo "All required containers are up. Skipping tests (RUN_TESTS=${RUN_TESTS})."
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