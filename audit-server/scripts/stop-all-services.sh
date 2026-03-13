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
# Stop All Ranger Audit Services
# ========================================
# This script stops all three audit services in the reverse order:
# 1. HDFS Consumer
# 2. Solr Consumer
# 3. Audit Server

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Stopping All Ranger Audit Services"
echo "=========================================="
echo ""

# Stop HDFS Consumer
echo "[1/3] Stopping Ranger Audit Consumer - HDFS..."
if [ -f "${PARENT_DIR}/ranger-audit-consumer-hdfs/scripts/stop-consumer-hdfs.sh" ]; then
  bash "${PARENT_DIR}/ranger-audit-consumer-hdfs/scripts/stop-consumer-hdfs.sh" || true
  echo ""
else
  echo "[WARNING] HDFS Consumer stop script not found, skipping..."
fi

# Stop Solr Consumer
echo "[2/3] Stopping Ranger Audit Consumer - Solr..."
if [ -f "${PARENT_DIR}/ranger-audit-consumer-solr/scripts/stop-consumer-solr.sh" ]; then
  bash "${PARENT_DIR}/ranger-audit-consumer-solr/scripts/stop-consumer-solr.sh" || true
  echo ""
else
  echo "[WARNING] Solr Consumer stop script not found, skipping..."
fi

# Stop Audit Server
echo "[3/3] Stopping Ranger Audit Server Service..."
if [ -f "${PARENT_DIR}/ranger-audit-server-service/scripts/stop-audit-server.sh" ]; then
  bash "${PARENT_DIR}/ranger-audit-server-service/scripts/stop-audit-server.sh" || true
  echo ""
else
  echo "[WARNING] Audit Server stop script not found, skipping..."
fi

echo "=========================================="
echo "✓ All Ranger Audit Services Stopped"
echo "=========================================="
