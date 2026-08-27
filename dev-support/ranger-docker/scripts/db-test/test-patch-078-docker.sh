#!/bin/bash
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

# End-to-end patch 078 test: build admin (if needed), start Ranger Admin per DB, verify.
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/db-test/test-patch-078-docker.sh              # all docker DBs
#   ./scripts/db-test/test-patch-078-docker.sh postgres     # one DB
#
# Prerequisites:
#   - Docker with >= 8 GB memory
#   - JDBC jars in downloads/ (run ./download-archives.sh)
#   - Ranger admin tarball in dist/ OR repo built via mvn

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RANGER_DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${RANGER_DOCKER_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
source "${RANGER_DOCKER_DIR}/.env"

DB_TYPES=("$@")
if [ "${#DB_TYPES[@]}" -eq 0 ]; then
  DB_TYPES=(postgres mysql oracle sqlserver)
fi

ensure_admin_tarball() {
  local tarball="${RANGER_DOCKER_DIR}/dist/ranger-${RANGER_VERSION}-admin.tar.gz"
  if [ -f "${tarball}" ]; then
    echo "[ok] Admin tarball present: ${tarball}"
    return
  fi
  echo "[build] Packaging admin tarball from ${REPO_ROOT} (distro module)..."
  (cd "${REPO_ROOT}" && mvn -pl distro -am package -DskipTests -q)
  cp "${REPO_ROOT}"/target/ranger-"${RANGER_VERSION}"-admin.tar.gz "${tarball}"
  echo "${RANGER_VERSION}" > "${RANGER_DOCKER_DIR}/dist/version"
}

teardown_stack() {
  local db_type="$1"
  echo "[teardown] ${db_type} stack..."
  export RANGER_DB_TYPE="${db_type}"
  docker compose -f "${RANGER_DOCKER_DIR}/docker-compose.ranger.yml" down -v --remove-orphans 2>/dev/null || true
  docker rm -f ranger ranger-postgres ranger-mysql ranger-oracle ranger-sqlserver \
    ranger-kdc ranger-zk ranger-solr 2>/dev/null || true
  docker network rm rangernw 2>/dev/null || true
  local i
  for i in $(seq 1 12); do
    if ! docker ps -a --format '{{.Names}}' | grep -qE '^ranger(-|$)'; then
      return 0
    fi
    sleep 2
  done
  local leftover
  leftover="$(docker ps -a --filter name=ranger --format '{{.Names}}' 2>/dev/null || true)"
  if [ -n "${leftover}" ]; then
    echo "${leftover}" | xargs docker rm -f 2>/dev/null || true
  fi
}

wait_for_db_setup() {
  local db_type="$1"
  local max_wait=90
  case "${db_type}" in
    oracle|sqlserver) max_wait=120 ;;
  esac
  local i
  echo "[3/4] Waiting for DB setup / patch 078 (up to $((max_wait * 10))s)..."
  for i in $(seq 1 "${max_wait}"); do
    if docker logs ranger 2>&1 | grep -qE "DB_PATCHES have already been applied|Ranger Admin Setup completed|Patch 078|DEFAULT_ALL_ADMIN"; then
      sleep 5
      if "${SCRIPT_DIR}/verify-patch-078.sh" >/dev/null 2>&1; then
        "${SCRIPT_DIR}/verify-patch-078.sh"
        echo "  DB verified after ~$((i * 10))s"
        return 0
      fi
    fi
    if "${SCRIPT_DIR}/verify-patch-078.sh" >/dev/null 2>&1; then
      "${SCRIPT_DIR}/verify-patch-078.sh"
      echo "  DB verified after ~$((i * 10))s"
      return 0
    fi
    if [ "${i}" -eq "${max_wait}" ]; then
      echo "[error] Patch 078 verification timed out; ranger logs:"
      docker logs ranger 2>&1 | tail -40
      return 1
    fi
    sleep 10
  done
}

test_db() {
  local db_type="$1"
  echo ""
  echo "=========================================="
  echo "Testing patch 078 with RANGER_DB_TYPE=${db_type}"
  echo "=========================================="

  export RANGER_DB_TYPE="${db_type}"
  export KERBEROS_ENABLED=false

  teardown_stack "${db_type}"

  echo "[1/4] Building DB + Admin images..."
  docker compose -f "${RANGER_DOCKER_DIR}/docker-compose.ranger.yml" \
    build ranger-db ranger ranger-kdc ranger-zk

  echo "[2/4] Starting stack (DB + Admin + ZK; Kerberos off, no Solr)..."
  if ! docker compose -f "${RANGER_DOCKER_DIR}/docker-compose.ranger.yml" \
    up -d 2>&1 | tee /tmp/ranger-compose-up-${db_type}.log; then
    echo "[error] docker compose up failed:"
    tail -20 /tmp/ranger-compose-up-${db_type}.log
    return 1
  fi

  if ! wait_for_db_setup "${db_type}"; then
    return 1
  fi

  teardown_stack "${db_type}"
}

ensure_admin_tarball

FAILED=()
for db in "${DB_TYPES[@]}"; do
  if ! test_db "${db}"; then
    FAILED+=("${db}")
  fi
done

echo ""
echo "=========================================="
if [ "${#FAILED[@]}" -eq 0 ]; then
  echo "All tested DB types passed patch 078 verification."
else
  echo "Failed DB types: ${FAILED[*]}"
  exit 1
fi
