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

# ========================================
# Ranger Audit Server Service Stop Script
# ========================================

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$(dirname "$SCRIPT_DIR")"

# Default log directory
AUDIT_SERVER_LOG_DIR="${AUDIT_SERVER_LOG_DIR:-${SERVICE_DIR}/logs}"
PID_FILE="${AUDIT_SERVER_LOG_DIR}/ranger-audit-server.pid"

echo "=========================================="
echo "Stopping Ranger Audit Server Service"
echo "=========================================="

if [ ! -f "${PID_FILE}" ]; then
  echo "[WARNING] PID file not found: ${PID_FILE}"
  echo "[INFO] Service may not be running"
  exit 0
fi

PID=$(cat "${PID_FILE}")

if ! kill -0 "$PID" 2>/dev/null; then
  echo "[WARNING] Process ${PID} is not running"
  echo "[INFO] Removing stale PID file"
  rm -f "${PID_FILE}"
  exit 0
fi

echo "[INFO] Stopping process ${PID}..."
kill "$PID"

# Wait for process to stop (max 30 seconds)
TIMEOUT=30
COUNT=0
while kill -0 "$PID" 2>/dev/null && [ $COUNT -lt $TIMEOUT ]; do
  sleep 1
  COUNT=$((COUNT + 1))
  echo -n "."
done
echo ""

if kill -0 "$PID" 2>/dev/null; then
  echo "[WARNING] Process did not stop gracefully, forcing shutdown..."
  kill -9 "$PID"
  sleep 2
fi

rm -f "${PID_FILE}"
echo "[INFO] ✓ Ranger Audit Server stopped successfully"
