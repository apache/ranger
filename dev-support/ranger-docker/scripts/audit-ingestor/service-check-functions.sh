#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Reusable service availability check functions
#

# Check if a TCP port is available
# Usage: check_tcp_port "ServiceName" "host:port" [max_wait_seconds]
check_tcp_port() {
  local SERVICE_NAME=$1
  local HOST_PORT=$2
  local MAX_WAIT=${3:-60}

  # Parse host and port
  local HOST="${HOST_PORT%%:*}"
  local PORT="${HOST_PORT##*:}"

  echo "[INFO] Waiting for ${SERVICE_NAME}..."
  local WAIT_COUNT=0

  while ! timeout 1 bash -c "echo > /dev/tcp/${HOST}/${PORT}" 2>/dev/null; do
    WAIT_COUNT=$((WAIT_COUNT+1))
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
      echo "[WARN] ${SERVICE_NAME} not available after ${MAX_WAIT} seconds, continuing anyway..."
      return 1
    fi
    echo "[INFO] Waiting for ${SERVICE_NAME}... ($WAIT_COUNT/$MAX_WAIT)"
    sleep 2
  done

  echo "[INFO] ✓ ${SERVICE_NAME} is available at ${HOST}:${PORT}"
  return 0
}

# Check if an HTTP service is available
# Usage: check_http_service "ServiceName" "http://host:port/path" [max_wait_seconds]
check_http_service() {
  local SERVICE_NAME=$1
  local URL=$2
  local MAX_WAIT=${3:-60}

  echo "[INFO] Waiting for ${SERVICE_NAME}..."
  local WAIT_COUNT=0

  while ! curl -f -s -m 2 "${URL}" > /dev/null 2>&1; do
    WAIT_COUNT=$((WAIT_COUNT+1))
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
      echo "[WARN] ${SERVICE_NAME} not available after ${MAX_WAIT} seconds, continuing anyway..."
      return 1
    fi
    echo "[INFO] Waiting for ${SERVICE_NAME}... ($WAIT_COUNT/$MAX_WAIT)"
    sleep 2
  done

  echo "[INFO] ✓ ${SERVICE_NAME} is available at ${URL}"
  return 0
}

# Copy a Ranger audit site XML template to its runtime location, setting
# <prefix>.kafka.sasl.mechanism from KAFKA_SASL_MECHANISM (empty = OAUTHBEARER, the
# code default). Templates are mounted read-only, so the rendered copy lives in the conf dir.
render_kafka_sasl_site_xml() {
  local SRC=$1
  local DST=$2
  local MECHANISM="${KAFKA_SASL_MECHANISM:-}"

  if [ "${MECHANISM}" = "OAUTHBEARER" ]; then
    MECHANISM=""
  fi

  awk -v mech="${MECHANISM}" '
    /<name>[^<]*kafka\.sasl\.mechanism<\/name>/ { in_mech = 1 }
    in_mech && /<value>/ { sub(/<value>[^<]*<\/value>/, "<value>" mech "</value>"); in_mech = 0 }
    { print }
  ' "${SRC}" > "${DST}"

  echo "[INFO] Rendered ${DST} from ${SRC} (kafka.sasl.mechanism='${MECHANISM:-OAUTHBEARER (default)}')"
}

# Wait for the dev SASL/OAUTHBEARER token minted by the ranger-kafka container.
wait_for_kafka_oauth_token() {
  local TOKEN_FILE=$1
  local MAX_WAIT=${2:-60}
  local WAIT_COUNT=0

  if [ "${KERBEROS_ENABLED}" != "true" ] || [ "${KAFKA_SASL_MECHANISM:-OAUTHBEARER}" != "OAUTHBEARER" ]; then
    return 0
  fi

  echo "[INFO] Waiting for Kafka OAUTHBEARER token ${TOKEN_FILE}..."

  while [ ! -s "${TOKEN_FILE}" ]; do
    WAIT_COUNT=$((WAIT_COUNT+1))
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
      echo "[WARN] ${TOKEN_FILE} not available after ${MAX_WAIT} seconds, continuing anyway..."
      return 1
    fi
    sleep 2
  done

  echo "[INFO] ✓ Kafka OAUTHBEARER token available at ${TOKEN_FILE}"
  return 0
}
