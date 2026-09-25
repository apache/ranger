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
# Start All Ranger Audit Services
# ========================================
# This script starts all three audit services in the correct order:
# 1. Audit Ingestor (receives audits and produces to Kafka)
# 2. Solr Dispatcher (consumes from Kafka and indexes to Solr)
# 3. HDFS Dispatcher (consumes from Kafka and writes to HDFS)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Starting All Ranger Audit Services"
echo "=========================================="
echo ""

# Start Audit Ingestor
echo "[1/3] Starting Ranger Audit Ingestor Service..."
if [ -f "${PARENT_DIR}/audit-ingestor/scripts/start-audit-ingestor.sh" ]; then
  bash "${PARENT_DIR}/audit-ingestor/scripts/start-audit-ingestor.sh"
  echo ""
  echo "Waiting 10 seconds for Audit Ingestor to initialize..."
  sleep 10
else
  echo "[ERROR] Audit Ingestor start script not found"
  exit 1
fi

# Start Solr Dispatcher
echo "[2/3] Starting Ranger Audit Dispatcher - Solr..."
if [ -f "${PARENT_DIR}/audit-dispatcher/scripts/start-audit-dispatcher.sh" ]; then
  nohup bash "${PARENT_DIR}/audit-dispatcher/scripts/start-audit-dispatcher.sh" solr > "${PARENT_DIR}/audit-dispatcher/logs/start-solr.log" 2>&1 &
  PID=$!
  echo $PID > "${PARENT_DIR}/audit-dispatcher/logs/ranger-audit-dispatcher-solr.pid"
  echo "Started Solr Dispatcher with PID: $PID"
  echo ""
  echo "Waiting 5 seconds for Solr Dispatcher to initialize..."
  sleep 5
else
  echo "[WARNING] Solr Dispatcher start script not found, skipping..."
fi

# Start HDFS Dispatcher
echo "[3/3] Starting Ranger Audit Dispatcher - HDFS..."
if [ -f "${PARENT_DIR}/audit-dispatcher/scripts/start-audit-dispatcher.sh" ]; then
  nohup bash "${PARENT_DIR}/audit-dispatcher/scripts/start-audit-dispatcher.sh" hdfs > "${PARENT_DIR}/audit-dispatcher/logs/start-hdfs.log" 2>&1 &
  PID=$!
  echo $PID > "${PARENT_DIR}/audit-dispatcher/logs/ranger-audit-dispatcher-hdfs.pid"
  echo "Started HDFS Dispatcher with PID: $PID"
  echo ""
else
  echo "[WARNING] HDFS Dispatcher start script not found, skipping..."
fi

echo "=========================================="
echo "All Ranger Audit Services Started"
echo "=========================================="
echo ""
echo "Service Endpoints:"
echo "  - Audit Ingestor:     http://localhost:7081/api/audit/health"
echo "  - Solr Dispatcher:    http://localhost:7091/api/health"
echo "  - HDFS Dispatcher:    http://localhost:7092/api/health"
echo ""
echo "To stop all services: ${SCRIPT_DIR}/stop-all-services.sh"
