#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# End-to-end OpenSearch audit dispatcher test.
#
# Starts the full Docker stack, validates the pipeline end-to-end, and tears
# down on exit.  Pipeline: Plugin -> Ingestor -> Kafka -> Dispatcher -> OpenSearch
#
# Usage (from dev-support/ranger-docker/):
#   ./scripts/audit/e2e-audit-opensearch.sh                  # full lifecycle
#   ./scripts/audit/e2e-audit-opensearch.sh --build          # force rebuild tarball, then test
#   ./scripts/audit/e2e-audit-opensearch.sh --no-teardown    # keep stack for debugging
#   ./scripts/audit/e2e-audit-opensearch.sh --skip-start     # test against running stack
#
# Prerequisites:
#   - Docker running
#   - dist/ranger-*-audit-dispatcher.tar.gz present (or use --build)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RD_HOME="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─── Flags ───────────────────────────────────────────────────────────────────

SKIP_START=false
NO_TEARDOWN=false
DO_BUILD=false

for arg in "$@"; do
  case "${arg}" in
    --build)       DO_BUILD=true ;;
    --skip-start)  SKIP_START=true ;;
    --no-teardown) NO_TEARDOWN=true ;;
    --help|-h)
      sed -n '/^# Usage/,/^#$/p' "$0" | sed 's/^# \?//'
      exit 0 ;;
    *) echo "Unknown flag: ${arg}" >&2; exit 1 ;;
  esac
done

# ─── Configuration ───────────────────────────────────────────────────────────

export RANGER_DB_TYPE="${RANGER_DB_TYPE:-postgres}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-120}"

OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
OPENSEARCH_INDEX="${OPENSEARCH_INDEX:-ranger_audits}"
INGESTOR_URL="${INGESTOR_URL:-http://localhost:7081}"
DISPATCHER_URL="${DISPATCHER_URL:-http://localhost:7093}"
RANGER_URL="${RANGER_URL:-http://localhost:6080}"
RANGER_USER="${RANGER_USER:-admin}"
RANGER_PASSWORD="${RANGER_PASSWORD:-rangerR0cks!}"
SERVICE_NAME="${SERVICE_NAME:-dev_hdfs}"
WAIT_SECONDS="${WAIT_SECONDS:-90}"

COMPOSE_RANGER="docker-compose.ranger.yml"
COMPOSE_KAFKA="docker-compose.ranger-kafka.yml"
COMPOSE_HADOOP="docker-compose.ranger-hadoop.yml"
COMPOSE_AUDIT_SERVER="docker-compose.ranger-audit-server.yml"
COMPOSE_OPENSEARCH="docker-compose.ranger-opensearch.yml"
COMPOSE_DISPATCHER="docker-compose.ranger-audit-dispatcher-opensearch.yml"

COMPOSE_BASE="-f ${COMPOSE_RANGER} -f ${COMPOSE_KAFKA} -f ${COMPOSE_HADOOP}"
COMPOSE_ALL="${COMPOSE_BASE} -f ${COMPOSE_AUDIT_SERVER} -f ${COMPOSE_OPENSEARCH} -f ${COMPOSE_DISPATCHER}"

# ─── Helpers ─────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }
info() { echo -e "${YELLOW}[INFO]${NC} $*"; }

wait_for_url() {
  local name="$1" url="$2" max_wait="${3:-${WAIT_TIMEOUT}}"
  local elapsed=0
  info "Waiting for ${name} at ${url} (up to ${max_wait}s)..."
  while ! curl -sf -m 2 "${url}" >/dev/null 2>&1; do
    elapsed=$((elapsed + 3))
    if [ "${elapsed}" -ge "${max_wait}" ]; then
      fail "${name} not ready after ${max_wait}s"
    fi
    sleep 3
  done
  pass "${name} is ready"
}

wait_for_container_health() {
  local name="$1" container="$2" max_wait="${3:-${WAIT_TIMEOUT}}"
  local elapsed=0
  info "Waiting for ${name} container health (up to ${max_wait}s)..."
  while true; do
    local status
    status="$(docker inspect --format='{{.State.Health.Status}}' "${container}" 2>/dev/null || echo "not_found")"
    if [ "${status}" = "healthy" ]; then
      pass "${name} is healthy"
      return 0
    fi
    elapsed=$((elapsed + 3))
    if [ "${elapsed}" -ge "${max_wait}" ]; then
      fail "${name} not healthy after ${max_wait}s (status: ${status})"
    fi
    sleep 3
  done
}

get_os_count() {
  curl -sf "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_count" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))"
}

teardown() {
  if [ "${NO_TEARDOWN}" = true ]; then
    info "Skipping teardown (--no-teardown). Stack is still running."
    return
  fi
  echo ""
  info "Tearing down OpenSearch audit stack..."
  cd "${RD_HOME}"
  docker compose ${COMPOSE_ALL} down -v 2>/dev/null || true
  pass "All containers stopped and volumes removed."
}

# ─── Teardown trap ───────────────────────────────────────────────────────────

if [ "${SKIP_START}" = false ]; then
  trap teardown EXIT
fi

# ══════════════════════════════════════════════════════════════════════════════
#  Phase 1: Start Stack
# ══════════════════════════════════════════════════════════════════════════════

if [ "${SKIP_START}" = false ]; then
  echo "========================================"
  echo " OpenSearch Audit Stack — Startup"
  echo "========================================"
  echo ""

  cd "${RD_HOME}"

  # Prerequisites
  info "Checking prerequisites..."
  if ! docker info >/dev/null 2>&1; then
    fail "Docker is not running"
  fi

  # Populate dependency archives exactly as dev-support expects; this is
  # idempotent and skips files already present in downloads/.
  info "Ensuring required Docker build archives are present..."
  mkdir -p "${RD_HOME}/downloads"
  chmod +x "${RD_HOME}/download-archives.sh"
  "${RD_HOME}/download-archives.sh" kafka opensearch hadoop
  pass "Required archives are available"

  TARBALL="$(find dist/ -name 'ranger-*-audit-dispatcher.tar.gz' 2>/dev/null | head -1)"

  # Build tarball if requested or absent (skips kylin plugin which has a corrupt dependency)
  if [ "${DO_BUILD}" = true ] || [ -z "${TARBALL}" ]; then
    info "Building audit-dispatcher tarball (excluding kylin plugin)..."
    RANGER_ROOT="$(cd "${RD_HOME}/../.." && pwd)"
    mkdir -p "${RD_HOME}/dist"
    (cd "${RANGER_ROOT}" && mvn clean package -DskipTests \
      -pl 'distro,!plugin-kylin,!ranger-kylin-plugin-shim' -am \
      -Dcheckstyle.skip=true -q)
    cp "${RANGER_ROOT}/target/ranger-"*"-audit-dispatcher.tar.gz" "${RD_HOME}/dist/"
    pass "Tarball built and copied to dist/"
  fi

  TARBALL="$(find dist/ -name 'ranger-*-audit-dispatcher.tar.gz' 2>/dev/null | head -1)"
  if [ -z "${TARBALL}" ]; then
    fail "No audit-dispatcher tarball found in dist/. Manual build command: mvn clean package -DskipTests -pl 'distro,!plugin-kylin,!ranger-kylin-plugin-shim' -am"
  fi
  pass "Found tarball: ${TARBALL}"

  # Build all images first (avoids container recreation during phased startup)
  info "Building images..."
  docker compose ${COMPOSE_ALL} build

  # Start OpenSearch early so it's ready before Ranger Admin's bootstrapper connects
  info "Starting OpenSearch..."
  docker compose ${COMPOSE_ALL} up -d ranger-opensearch

  # Core stack (Ranger Admin needs OpenSearch to be reachable for index bootstrap)
  info "Starting core stack (Ranger Admin, KDC, ZK, Kafka, Hadoop)..."
  docker compose ${COMPOSE_ALL} up -d ranger ranger-kafka ranger-hadoop

  wait_for_url "OpenSearch" "http://localhost:9200/_cluster/health" 90

  info "Waiting for Kafka to accept connections (up to 120s)..."
  kafka_elapsed=0
  while ! docker exec ranger-kafka bash -c "echo > /dev/tcp/localhost/9092" 2>/dev/null; do
    kafka_elapsed=$((kafka_elapsed + 3))
    if [ "${kafka_elapsed}" -ge 120 ]; then
      fail "Kafka not ready after 120s"
    fi
    sleep 3
  done
  pass "Kafka is ready"

  wait_for_url "Ranger Admin" "http://localhost:6080/login.jsp" 180

  # Pre-create Kafka topic (after Ranger Admin is up so Kafka plugin has policies)
  info "Pre-creating Kafka audit topic..."
  chmod +x "${RD_HOME}/scripts/kafka/create-ranger-audit-topic.sh"
  "${RD_HOME}/scripts/kafka/create-ranger-audit-topic.sh"
  pass "Kafka topic ready"

  # Audit ingestor
  info "Starting audit ingestor..."
  docker compose ${COMPOSE_ALL} up -d ranger-audit-ingestor

  wait_for_url "Audit Ingestor" "http://localhost:7081/api/audit/health" 90

  # OpenSearch index is auto-created by Ranger Admin's OpenSearchIndexBootStrapper on startup

  # Wait for Kafka Ranger plugin to download policies granting rangerauditserver access
  info "Waiting for Kafka Ranger plugin policy refresh (30s)..."
  sleep 30

  # Dispatcher
  info "Starting OpenSearch dispatcher..."
  docker compose ${COMPOSE_ALL} up -d ranger-audit-dispatcher-opensearch

  wait_for_url "OpenSearch Dispatcher" "http://localhost:7093/api/health/ping" 120

  echo ""
  pass "OpenSearch audit stack is ready!"
  echo ""
fi

# ══════════════════════════════════════════════════════════════════════════════
#  Phase 2: E2E Test
# ══════════════════════════════════════════════════════════════════════════════

echo "========================================"
echo " OpenSearch Audit Dispatcher — E2E Test"
echo "========================================"
echo ""

cd "${RD_HOME}"

# Health checks
info "Checking OpenSearch..."
curl -sf "${OPENSEARCH_URL}/_cluster/health" | grep -q '"status"' || fail "OpenSearch unreachable"
pass "OpenSearch is up"

info "Waiting for audit index (created by Ranger Admin OpenSearchIndexBootStrapper)..."
index_elapsed=0
while ! curl -sf "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}" >/dev/null 2>&1; do
  index_elapsed=$((index_elapsed + 5))
  if [ "${index_elapsed}" -ge 120 ]; then
    fail "Index ${OPENSEARCH_INDEX} not created after 120s. Check Ranger Admin logs for OpenSearchIndexBootStrapper errors."
  fi
  sleep 5
done
pass "Index ${OPENSEARCH_INDEX} ready"

info "Checking audit ingestor..."
curl -sf "${INGESTOR_URL}/api/audit/health" | grep -q '"status":"UP"' || fail "Audit ingestor down"
pass "Audit ingestor healthy"

dispatcher_running="$(docker ps --format '{{.Names}}' | grep -c '^ranger-audit-dispatcher-opensearch$' || true)"
if [ "${dispatcher_running}" -eq 0 ]; then
  fail "ranger-audit-dispatcher-opensearch is not running"
else
  pass "OpenSearch dispatcher container is running"
fi

info "Checking OpenSearch dispatcher health..."
curl -sf "${DISPATCHER_URL}/api/health/ping" >/dev/null || fail "OpenSearch dispatcher health check failed at ${DISPATCHER_URL}"
pass "OpenSearch dispatcher is healthy"

# Post test audit via ingestor
before_count="$(get_os_count)"
marker="e2e-opensearch-$(date +%s)"
info "OpenSearch doc count before: ${before_count}"

info "Posting test audit to ingestor..."
http_code="$(docker exec ranger-hadoop bash -c "
  kinit -kt /etc/keytabs/hdfs.keytab hdfs/ranger-hadoop.rangernw@EXAMPLE.COM 2>/dev/null
  curl -s -o /tmp/os_audit_resp.json -w '%{http_code}' \
    --negotiate -u : \
    -H 'Content-Type: application/json' \
    -X POST 'http://ranger-audit-ingestor.rangernw:7081/api/audit/access?serviceName=${SERVICE_NAME}' \
    -d '[{
      \"id\": \"${marker}\",
      \"access\": \"read\",
      \"enforcer\": \"ranger-acl\",
      \"agent\": \"hdfs\",
      \"repo\": \"${SERVICE_NAME}\",
      \"repoType\": 1,
      \"reqUser\": \"testuser1\",
      \"reqData\": \"${marker}\",
      \"resource\": \"/tmp/opensearch_audit_test\",
      \"resType\": \"path\",
      \"result\": 0,
      \"policy\": -1,
      \"reason\": \"OpenSearch audit dispatcher test\",
      \"action\": \"read\",
      \"cliIP\": \"127.0.0.1\",
      \"agentHost\": \"ranger-hadoop.rangernw\"
    }]'
")"

if [[ "${http_code}" != "200" && "${http_code}" != "202" ]]; then
  docker exec ranger-hadoop cat /tmp/os_audit_resp.json 2>/dev/null || true
  fail "Ingestor returned HTTP ${http_code}"
fi
pass "Audit accepted by ingestor (HTTP ${http_code})"

# Wait for OpenSearch indexing via dispatcher
info "Waiting up to ${WAIT_SECONDS}s for document in OpenSearch..."
elapsed=0
found=0
while [ "${elapsed}" -lt "${WAIT_SECONDS}" ]; do
  result="$(curl -sf "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_search" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":{\"term\":{\"id\":\"${marker}\"}},\"size\":1}" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['hits']['total']['value'])" 2>/dev/null || echo 0)"
  if [ "${result}" -gt 0 ]; then
    found=1
    break
  fi
  sleep 3
  elapsed=$((elapsed + 3))
done

[ "${found}" -eq 1 ] || fail "Timed out waiting for marker '${marker}' in OpenSearch"
after_count="$(get_os_count)"
pass "Document indexed in OpenSearch (${before_count} -> ${after_count} docs)"

# Query sample document
curl -sf "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_search?q=id:${marker}&pretty" | head -30
echo ""

# Ranger UI (only if configured for elasticsearch store)
audit_source="$(curl -sf -u "${RANGER_USER}:${RANGER_PASSWORD}" \
  "${RANGER_URL}/service/assets/accessAudit?pageSize=1" 2>/dev/null \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('totalCount','?'))" 2>/dev/null || echo "skip")"

if [ "${audit_source}" != "skip" ]; then
  info "Ranger UI audit API returned totalCount=${audit_source}"
  ui_match="$(curl -sf -u "${RANGER_USER}:${RANGER_PASSWORD}" \
    "${RANGER_URL}/service/assets/accessAudit?pageSize=10&sortBy=eventTime&sortType=desc" 2>/dev/null \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
marker = '${marker}'
hits = [a for a in d.get('vXAccessAudits', []) if marker in (a.get('requestData') or '')]
print(len(hits))
" 2>/dev/null || echo 0)"
  if [ "${ui_match}" -gt 0 ]; then
    pass "Audit visible in Ranger UI (elasticsearch store active)"
  else
    info "Audit not in Ranger UI yet — switch audit_store to elasticsearch and restart Ranger:"
    info "  Enable the OpenSearch block in scripts/admin/ranger-admin-install-postgres.properties"
  fi
fi

# ══════════════════════════════════════════════════════════════════════════════
#  Done
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "========================================"
pass "OpenSearch audit dispatcher E2E test completed successfully"
echo "  OpenSearch: ${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_search"
echo "  Dispatcher: ${DISPATCHER_URL}/api/health/ping"
echo "  Ranger UI:  ${RANGER_URL} -> Audit -> Access (requires elasticsearch audit_store)"
echo "========================================"
