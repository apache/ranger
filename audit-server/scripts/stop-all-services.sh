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
# 1. HDFS Dispatcher
# 2. Solr Dispatcher
# 3. Audit Ingestor

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Stopping All Ranger Audit Services"
echo "=========================================="
echo ""

# Stop HDFS Dispatcher
echo "[1/3] Stopping Ranger Audit Dispatcher - HDFS..."
if [ -f "${PARENT_DIR}/audit-dispatcher/scripts/stop-audit-dispatcher.sh" ]; then
  bash "${PARENT_DIR}/audit-dispatcher/scripts/stop-audit-dispatcher.sh" hdfs || true
  echo ""
else
  echo "[WARNING] HDFS Dispatcher stop script not found, skipping..."
fi

# Stop Solr Dispatcher
echo "[2/3] Stopping Ranger Audit Dispatcher - Solr..."
if [ -f "${PARENT_DIR}/audit-dispatcher/scripts/stop-audit-dispatcher.sh" ]; then
  bash "${PARENT_DIR}/audit-dispatcher/scripts/stop-audit-dispatcher.sh" solr || true
  echo ""
else
  echo "[WARNING] Solr Dispatcher stop script not found, skipping..."
fi

# Stop Audit Ingestor
echo "[3/3] Stopping Ranger Audit Ingestor Service..."
if [ -f "${PARENT_DIR}/audit-ingestor/scripts/stop-audit-ingestor.sh" ]; then
  bash "${PARENT_DIR}/audit-ingestor/scripts/stop-audit-ingestor.sh" || true
  echo ""
else
  echo "[WARNING] Audit Ingestor stop script not found, skipping..."
fi

echo "=========================================="
echo "All Ranger Audit Services Stopped"
echo "=========================================="
