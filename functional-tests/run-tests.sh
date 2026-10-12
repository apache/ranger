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

#!/bin/bash

# All available test suites (pytest folders)

ALL_TEST_SUITES=(rolerest xuserrest servicerest tagrest hdfs kms api hive)

# Test-only suites (no docker-compose file needed)
TEST_ONLY_SUITES=(rolerest xuserrest servicerest tagrest api)

# All optional containers (on top of base services) — shown in the containers prompt
# Each maps to a docker-compose file and a set of archives to download.
ALL_OPTIONAL_CONTAINERS=(hadoop hive hbase knox trino ozone kafka)

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

# Prompt for AUDIT_INDEX_STORE if not provided
AUDIT_INDEX_STORE="${AUDIT_INDEX_STORE:-}"
if [[ -z "${AUDIT_INDEX_STORE}" ]]; then
  echo ""
  echo "Available audit stores: opensearch, solr, none"
  read -rp "Enter audit store (press Enter to default to opensearch): " input_audit
  AUDIT_INDEX_STORE="${input_audit:-opensearch}"
fi

# Set AUDIT_DESTINATIONS based on AUDIT_INDEX_STORE
if [[ "${AUDIT_INDEX_STORE}" == "none" ]]; then
  AUDIT_DESTINATIONS=""
else
  AUDIT_DESTINATIONS="audit-store-${AUDIT_INDEX_STORE}"
fi

# Export environment variables
export RANGER_DB_TYPE="${DB_TYPE}"
export AUDIT_INDEX_STORE="${AUDIT_INDEX_STORE}"
export AUDIT_DESTINATIONS="${AUDIT_DESTINATIONS}"

# Default to 0 — containers persist after each run.
# Pass CLEAN_CONTAINERS=1 explicitly to wipe and rebuild everything.
CLEAN_CONTAINERS="${CLEAN_CONTAINERS:-0}"

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

# Prompt for optional containers to bring up (independent of test suites)
# Base services always start: ranger, kdc, db, zk, kms, pdp, usersync, tagsync + audit pipeline.
# Pre-set via env var (space-separated): export SELECTED_CONTAINERS="hadoop hive"  OR  "all"
SELECTED_CONTAINERS_ENV="${SELECTED_CONTAINERS:-}"
SELECTED_CONTAINERS=()
if [[ -n "${SELECTED_CONTAINERS_ENV}" ]]; then
  if [[ "${SELECTED_CONTAINERS_ENV}" == "all" ]]; then
    SELECTED_CONTAINERS=("${ALL_OPTIONAL_CONTAINERS[@]}")
  else
    read -ra SELECTED_CONTAINERS <<< "${SELECTED_CONTAINERS_ENV}"
  fi
else
  echo ""
  echo "Available containers : ${ALL_OPTIONAL_CONTAINERS[*]}"
  echo "  (base always starts: ranger, kdc, db, zk, kms, pdp, usersync, tagsync + audit pipeline)"
  read -rp "Enter containers space-separated (press Enter or 'all' for ALL): " -a input_containers
  if [[ "${#input_containers[@]}" -eq 0 || "${input_containers[0]}" == "all" ]]; then
    echo "Bringing up ALL optional containers..."
    SELECTED_CONTAINERS=("${ALL_OPTIONAL_CONTAINERS[@]}")
  else
    SELECTED_CONTAINERS=("${input_containers[@]}")
  fi
fi

echo ""
echo "DB Type           : ${DB_TYPE}"
echo "Audit Store       : ${AUDIT_INDEX_STORE}"
echo "Audit Destinations: ${AUDIT_DESTINATIONS}"
echo "Test Suites       : ${EXTRA_SERVICES[*]}"
echo "Containers        : ${SELECTED_CONTAINERS[*]:-none (base only)}"
echo ""
echo "CLEAN_CONTAINERS=${CLEAN_CONTAINERS}"

# Remove all containers and clean up docker space
if [[ "${CLEAN_CONTAINERS}" == "1" ]]; then
  docker rm -f $(docker ps -aq --filter "name=ranger") 2>/dev/null || true
  docker system prune --all --force --volumes
fi

#path setup
RANGER_DOCKER_PATH="../dev-support/ranger-docker"
TESTS_PATH="../../functional-tests"

cd "$RANGER_DOCKER_PATH" || exit 1

# Ensure scripts are executable
chmod +x scripts/**/*.sh || true
chmod +x download-archives.sh || true

# Download archives for selected optional containers
# 'hive' arg downloads hadoop + tez + hive (all needed by Dockerfile.ranger-hadoop and ranger-hive)
DOCKER_BACKED=()
for container in "${SELECTED_CONTAINERS[@]}"; do
  case "$container" in
    hadoop) DOCKER_BACKED+=("hive") ;;          # 'hive' arg downloads hadoop + tez (needed by Dockerfile.ranger-hadoop)
    hive)   DOCKER_BACKED+=("hive") ;;
    hbase)  DOCKER_BACKED+=("hive" "hbase") ;;  # hadoop + tez (compose dep) + hbase image
    knox)   DOCKER_BACKED+=("knox") ;;
    trino)  ;;                                   # no archive — pulled from Docker image directly
    ozone)  DOCKER_BACKED+=("ozone") ;;
    kafka)  DOCKER_BACKED+=("kafka") ;;
  esac
done

# Kafka archive is needed whenever the audit pipeline is up (ranger-kafka build)
if [[ "${AUDIT_INDEX_STORE}" != "none" ]]; then
  DOCKER_BACKED+=("kafka")
fi

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

# Bring up base services — matches README ranger-core command:
# ranger + usersync + tagsync + pdp + kms (+ audit-service when audit store != none)
DOCKER_FILES=(
  "-f" "docker-compose.ranger.yml"
  "-f" "docker-compose.ranger-usersync.yml"
  "-f" "docker-compose.ranger-tagsync.yml"
  "-f" "docker-compose.ranger-pdp.yml"
  "-f" "docker-compose.ranger-kms.yml"
)

# Add audit service if audit store is not "none"
if [[ "${AUDIT_INDEX_STORE}" != "none" ]]; then
  DOCKER_FILES+=("-f" "docker-compose.ranger-audit-service.yml")
fi

# Helper: add a compose file only if not already in the list (prevents duplicates
# when e.g. both 'hadoop' and 'hive' are selected — both need ranger-hadoop.yml)
_add_compose() {
  local file="$1"
  local already=false
  for ac in "${ADDED_COMPOSE[@]}"; do
    [[ "$ac" == "$file" ]] && already=true && break
  done
  if ! $already; then
    DOCKER_FILES+=("-f" "$file")
    ADDED_COMPOSE+=("$file")
  fi
}

# Add compose files for selected optional containers
ADDED_COMPOSE=()
for container in "${SELECTED_CONTAINERS[@]}"; do
  case "$container" in
    hadoop) _add_compose "docker-compose.ranger-hadoop.yml" ;;
    hive)   _add_compose "docker-compose.ranger-hadoop.yml"  # hive depends_on: ranger-hadoop: service_healthy
            _add_compose "docker-compose.ranger-hive.yml" ;;
    hbase)  _add_compose "docker-compose.ranger-hadoop.yml"  # hbase depends_on: ranger-hadoop: service_healthy
            _add_compose "docker-compose.ranger-hbase.yml" ;;
    knox)   _add_compose "docker-compose.ranger-knox.yml" ;;
    trino)  _add_compose "docker-compose.ranger-trino.yml" ;;
    ozone)  _add_compose "docker-compose.ranger-ozone.yml" ;;
    kafka)  _add_compose "docker-compose.ranger-kafka.yml" ;;
  esac
done

# Build ALL_SERVICES list — only docker-backed services get container health checks
BASE_SERVICES=(ranger ranger-kdc ranger-${RANGER_DB_TYPE} ranger-zk ranger-kms ranger-pdp)
ALL_SERVICES=("${BASE_SERVICES[@]}")

# Audit pipeline containers brought up by docker-compose.ranger-audit-service.yml
if [[ "${AUDIT_INDEX_STORE}" != "none" ]]; then
  ALL_SERVICES+=(
    ranger-kafka
    ranger-audit-ingestor
    "ranger-${AUDIT_INDEX_STORE}"
    "ranger-audit-dispatcher-${AUDIT_INDEX_STORE}"
  )
fi

# Add containers for selected optional services
for container in "${SELECTED_CONTAINERS[@]}"; do
  case "$container" in
    hadoop) ALL_SERVICES+=("ranger-hadoop") ;;
    hive)   ALL_SERVICES+=("ranger-hadoop" "ranger-hive") ;;
    hbase)  ALL_SERVICES+=("ranger-hadoop" "ranger-hbase") ;;
    knox)   ALL_SERVICES+=("ranger-knox") ;;
    trino)  ALL_SERVICES+=("ranger-trino") ;;
    ozone)  ALL_SERVICES+=("ozone-om" "ozone-scm" "ozone-datanode") ;;
    kafka)  ALL_SERVICES+=("ranger-kafka") ;;
  esac
done

# Deduplicate — e.g. 'hadoop' + 'hive' both selected adds ranger-hadoop twice
ALL_SERVICES=($(printf '%s\n' "${ALL_SERVICES[@]}" | sort -u))

# Run ozone pre-setup script if ozone is among selected containers
for container in "${SELECTED_CONTAINERS[@]}"; do
  if [[ "$container" == "ozone" ]]; then
    echo "Running ozone pre-setup..."
    bash scripts/ozone/ozone-plugin-docker-setup.sh
    break
  fi
done


# Determine container state: missing, stopped, or all already running
missing=false
all_running=true

for container in "${ALL_SERVICES[@]}"; do
  if ! docker container inspect "$container" >/dev/null 2>&1; then
    missing=true
    all_running=false
    break
  elif [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null) != "true" ]]; then
    all_running=false
  fi
done

if [[ "${missing}" == "true" ]]; then
  # First run or containers were wiped — build and start everything
  echo "Some containers are missing. Creating services..."
  if [[ -n "${AUDIT_DESTINATIONS}" ]]; then
    docker compose --profile "${AUDIT_DESTINATIONS}" "${DOCKER_FILES[@]}" up -d --build
  else
    docker compose "${DOCKER_FILES[@]}" up -d --build
  fi
  echo "Waiting for containers to start..."
  sleep 20
elif [[ "${all_running}" == "false" ]]; then
  # Containers exist but some are stopped — start them without rebuild
  echo "Some containers are stopped. Starting services..."
  if [[ -n "${AUDIT_DESTINATIONS}" ]]; then
    docker compose --profile "${AUDIT_DESTINATIONS}" "${DOCKER_FILES[@]}" up -d
  else
    docker compose "${DOCKER_FILES[@]}" up -d
  fi
else
  # All containers already running — skip compose and sleeps entirely
  echo "All containers already running. Skipping compose and proceeding directly to tests..."
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

if [[ "${missing}" == "true" || "${admin_missing}" == "true" || "${all_running}" == "false" ]]; then
  echo -e "\n\n\n\n\n\n\n\n\n\n-------------------------------------------"
  echo "Please wait while all services are fully initialized. This may take a few minutes..."
  echo "-------------------------------------------"
  sleep 60  # Wait for services to initialize after a fresh start or restart
fi


#RUN TESTS--------
#Use export RUN_TESTS=0 to only bring up infra and allow all services to initialise properly (NOTE: It'll skip tests to avoid early startup failures).
RUN_TESTS="${RUN_TESTS:-1}"

if [[ $flag == true ]]; then
  if [[ "${RUN_TESTS}" == "1" ]]; then
    echo "All required containers are up. Running test cases..."
    cd "$TESTS_PATH" || exit 1         # Switch to the tests directory

    # Only create the venv (and upgrade pip) on first run — skip on re-runs.
    if [[ ! -d "myenv" ]]; then
      python3 -m venv myenv || { echo "Failed to create venv"; exit 1; }
      source myenv/bin/activate || { echo "Failed to activate venv"; exit 1; }
      pip install --upgrade pip        # upgrade pip once, when the venv is fresh
    else
      source myenv/bin/activate || { echo "Failed to activate venv"; exit 1; }
    fi
    pip install -q -r requirements.txt || { echo "Failed to install requirements"; exit 1; }  # Install/sync dependencies

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