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
# 1. Audit Server (receives audits and produces to Kafka)
# 2. Solr Consumer (consumes from Kafka and indexes to Solr)
# 3. HDFS Consumer (consumes from Kafka and writes to HDFS)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Starting All Ranger Audit Services"
echo "=========================================="
echo ""

# Start Audit Server
echo "[1/3] Starting Ranger Audit Server Service..."
if [ -f "${PARENT_DIR}/ranger-audit-server-service/scripts/start-audit-server.sh" ]; then
  bash "${PARENT_DIR}/ranger-audit-server-service/scripts/start-audit-server.sh"
  echo ""
  echo "Waiting 10 seconds for Audit Server to initialize..."
  sleep 10
else
  echo "[ERROR] Audit Server start script not found"
  exit 1
fi

# Start Solr Consumer
echo "[2/3] Starting Ranger Audit Consumer - Solr..."
if [ -f "${PARENT_DIR}/ranger-audit-consumer-solr/scripts/start-consumer-solr.sh" ]; then
  bash "${PARENT_DIR}/ranger-audit-consumer-solr/scripts/start-consumer-solr.sh"
  echo ""
  echo "Waiting 5 seconds for Solr Consumer to initialize..."
  sleep 5
else
  echo "[WARNING] Solr Consumer start script not found, skipping..."
fi

# Start HDFS Consumer
echo "[3/3] Starting Ranger Audit Consumer - HDFS..."
if [ -f "${PARENT_DIR}/ranger-audit-consumer-hdfs/scripts/start-consumer-hdfs.sh" ]; then
  bash "${PARENT_DIR}/ranger-audit-consumer-hdfs/scripts/start-consumer-hdfs.sh"
  echo ""
else
  echo "[WARNING] HDFS Consumer start script not found, skipping..."
fi

echo "=========================================="
echo "✓ All Ranger Audit Services Started"
echo "=========================================="
echo ""
echo "Service Endpoints:"
echo "  - Audit Server:     http://localhost:7081/api/audit/health"
echo "  - Solr Consumer:    http://localhost:7091/api/health"
echo "  - HDFS Consumer:    http://localhost:7092/api/health"
echo ""
echo "To stop all services: ${SCRIPT_DIR}/stop-all-services.sh"
