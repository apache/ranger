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

# Waits for Kerberos keytab + KDC before exec'ing Ozone SCM/datanode. CI smoke tests
# hit PortUnreachableException when the datanode starts before KDC UDP/TCP is ready.

set -euo pipefail

KEYTAB_NAME="${OZONE_KEYTAB_NAME:-}"
KDC_HOST="${KDC_HOST:-ranger-kdc.rangernw}"
KDC_PORT="${KDC_PORT:-88}"
MAX_WAIT_SEC="${OZONE_KERBEROS_WAIT_SEC:-120}"

wait_for_keytab() {
  if [[ -z "${KEYTAB_NAME}" ]] || [[ "${RANGER_KERBEROS_ENABLED:-}" != "true" ]]; then
    return 0
  fi

  local keytab_path="/etc/keytabs/${KEYTAB_NAME}"
  local deadline=$((SECONDS + MAX_WAIT_SEC))

  echo "Waiting for keytab ${keytab_path} (up to ${MAX_WAIT_SEC}s)..."
  while (( SECONDS < deadline )); do
    if [[ -f "${keytab_path}" ]]; then
      echo "Found keytab ${keytab_path}"
      return 0
    fi
    sleep 2
  done

  echo "ERROR: timed out waiting for keytab ${keytab_path}" >&2
  exit 1
}

wait_for_kdc() {
  if [[ "${RANGER_KERBEROS_ENABLED:-}" != "true" ]]; then
    return 0
  fi

  local deadline=$((SECONDS + MAX_WAIT_SEC))

  echo "Waiting for KDC at ${KDC_HOST}:${KDC_PORT} (up to ${MAX_WAIT_SEC}s)..."
  while (( SECONDS < deadline )); do
    if (echo > "/dev/tcp/${KDC_HOST}/${KDC_PORT}") 2>/dev/null; then
      # KDC healthcheck can pass slightly before clients succeed; brief settle.
      sleep 3
      echo "KDC reachable at ${KDC_HOST}:${KDC_PORT}"
      return 0
    fi
    sleep 2
  done

  echo "ERROR: timed out waiting for KDC at ${KDC_HOST}:${KDC_PORT}" >&2
  exit 1
}

wait_for_keytab
wait_for_kdc
exec "$@"
