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

# Comprehensive HA Load Test Runner
# This script orchestrates a complete HA load test scenario

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Test configuration
TEST_DURATION_MINUTES=${1:-5}
HDFS_OPERATIONS=${2:-1000}
HIVE_OPERATIONS=${3:-100}
CONCURRENCY=${4:-10}

echo "============================================"
echo "Ranger Audit HA Load Test"
echo "============================================"
echo "Test Duration: $TEST_DURATION_MINUTES minutes"
echo "HDFS Operations: $HDFS_OPERATIONS"
echo "Hive Operations: $HIVE_OPERATIONS"
echo "Concurrency: $CONCURRENCY"
echo "============================================"
echo ""

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check if HAProxy is running
if ! docker ps | grep -q "ranger-audit$"; then
  echo "✗ HAProxy is not running"
  echo ""
  echo "Starting HA setup with 3 audit servers..."
  "$SCRIPT_DIR/start_ha_audit_servers.sh" 3
  echo ""
  sleep 10
fi

# Check if HDFS container is running
if ! docker ps | grep -q "ranger-hadoop"; then
  echo "✗ HDFS container (ranger-hadoop) is not running"
  echo "Please start it first: docker compose -f docker-compose.ranger-hadoop.yml up -d"
  exit 1
fi

# Check if Hive container is running
if ! docker ps | grep -q "ranger-hive"; then
  echo "⚠️  Hive container (ranger-hive) is not running"
  echo "Hive load tests will be skipped"
  SKIP_HIVE=true
else
  SKIP_HIVE=false
fi

echo "✓ Prerequisites check complete"
echo ""

# Copy load generator scripts to containers
echo "Copying load generator scripts to containers..."
docker cp "$SCRIPT_DIR/hdfs_load_generator.sh" ranger-hadoop:/tmp/
docker exec ranger-hadoop chmod +x /tmp/hdfs_load_generator.sh

if [ "$SKIP_HIVE" = false ]; then
  docker cp "$SCRIPT_DIR/hive_load_generator.sh" ranger-hive:/tmp/
  docker exec ranger-hive chmod +x /tmp/hive_load_generator.sh
fi

echo "✓ Scripts copied"
echo ""

# Start monitoring in background
echo "Starting monitoring..."
"$SCRIPT_DIR/monitor_ha_load.sh" 10 > /tmp/ha_load_monitor.log 2>&1 &
MONITOR_PID=$!

echo "✓ Monitor started (PID: $MONITOR_PID)"
echo ""

# Record start time
TEST_START=$(date +%s)

echo "============================================"
echo "Phase 1: Baseline Load (3 servers)"
echo "============================================"
echo ""

# Get initial consumer group state
echo "Initial Kafka Consumer State:"
docker exec ranger-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group ranger_audit_solr_consumer_group \
  --describe 2>/dev/null | head -5

echo ""
echo "Starting baseline load generation..."

# Generate HDFS load
docker exec ranger-hadoop /tmp/hdfs_load_generator.sh $HDFS_OPERATIONS $CONCURRENCY &
HDFS_PID=$!

# Generate Hive load
if [ "$SKIP_HIVE" = false ]; then
  docker exec ranger-hive /tmp/hive_load_generator.sh $HIVE_OPERATIONS &
  HIVE_PID=$!
fi

# Wait for initial load
echo "Waiting for baseline load to complete..."
wait $HDFS_PID
if [ "$SKIP_HIVE" = false ]; then
  wait $HIVE_PID
fi

echo "✓ Baseline load complete"
echo ""

# Check distribution
echo "Checking load distribution across servers..."
echo ""
docker stats --no-stream | grep -E "NAME|ranger-audit"
echo ""

# Sleep before next phase
sleep 5

echo "============================================"
echo "Phase 2: Scale Up Test (3 -> 5 servers)"
echo "============================================"
echo ""

cd "$(dirname "$SCRIPT_DIR")/.."

echo "Scaling up to 5 audit servers..."
docker compose -f docker-compose.ranger-audit-haproxy.yml up -d --scale ranger-audit-svc=5

echo "Waiting for new servers to join..."
sleep 15

echo "Generating load during scale-up..."
docker exec ranger-hadoop /tmp/hdfs_load_generator.sh $((HDFS_OPERATIONS / 2)) $CONCURRENCY &
HDFS_PID=$!

if [ "$SKIP_HIVE" = false ]; then
  docker exec ranger-hive /tmp/hive_load_generator.sh $((HIVE_OPERATIONS / 2)) &
  HIVE_PID=$!
fi

wait $HDFS_PID
if [ "$SKIP_HIVE" = false ]; then
  wait $HIVE_PID
fi

echo "✓ Scale-up load test complete"
echo ""

# Check rebalancing
echo "Checking partition rebalancing..."
docker exec ranger-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group ranger_audit_solr_consumer_group \
  --describe 2>/dev/null | head -10

echo ""
sleep 5

echo "============================================"
echo "Phase 3: Failure Simulation"
echo "============================================"
echo ""

# Get a random audit server to kill
TARGET_SERVER=$(docker ps --filter "name=ranger-audit-svc" --format "{{.Names}}" | head -1)

echo "Stopping one audit server: $TARGET_SERVER"
docker stop $TARGET_SERVER

echo "Waiting 10 seconds for rebalancing..."
sleep 10

echo "Generating load with reduced capacity..."
docker exec ranger-hadoop /tmp/hdfs_load_generator.sh $((HDFS_OPERATIONS / 2)) $CONCURRENCY &
HDFS_PID=$!

if [ "$SKIP_HIVE" = false ]; then
  docker exec ranger-hive /tmp/hive_load_generator.sh $((HIVE_OPERATIONS / 2)) &
  HIVE_PID=$!
fi

wait $HDFS_PID
if [ "$SKIP_HIVE" = false ]; then
  wait $HIVE_PID
fi

echo "✓ Failure scenario complete"
echo ""

echo "Restarting stopped server: $TARGET_SERVER"
docker start $TARGET_SERVER

echo "Waiting for recovery..."
sleep 15

echo ""

echo "============================================"
echo "Phase 4: Scale Down Test (4 -> 2 servers)"
echo "============================================"
echo ""

echo "Scaling down to 2 audit servers..."
docker compose -f docker-compose.ranger-audit-haproxy.yml up -d --scale ranger-audit-svc=2

echo "Waiting for graceful shutdown..."
sleep 15

echo "Generating load with reduced servers..."
docker exec ranger-hadoop /tmp/hdfs_load_generator.sh $((HDFS_OPERATIONS / 2)) $CONCURRENCY &
HDFS_PID=$!

if [ "$SKIP_HIVE" = false ]; then
  docker exec ranger-hive /tmp/hive_load_generator.sh $((HIVE_OPERATIONS / 2)) &
  HIVE_PID=$!
fi

wait $HDFS_PID
if [ "$SKIP_HIVE" = false ]; then
  wait $HIVE_PID
fi

echo "✓ Scale-down test complete"
echo ""

# Stop monitoring
kill $MONITOR_PID 2>/dev/null

# Calculate test duration
TEST_END=$(date +%s)
TEST_DURATION=$((TEST_END - TEST_START))

echo "============================================"
echo "Test Complete"
echo "============================================"
echo "Total Duration: $TEST_DURATION seconds"
echo ""

echo "--- Final System State ---"
echo ""
echo "Audit Servers:"
docker ps | grep -E "NAME|ranger-audit"
echo ""

echo "Consumer Group State:"
docker exec ranger-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group ranger_audit_solr_consumer_group \
  --describe 2>/dev/null | head -10

echo ""
echo "Kafka Topic Messages:"
docker exec ranger-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic ranger_audits \
  --time -1 2>/dev/null | awk '{sum += $3} END {print "Total messages: " sum}'

echo ""
echo "============================================"
echo "Summary"
echo "============================================"
echo "✓ Baseline load test (3 servers)"
echo "✓ Scale-up test (3 -> 5 servers)"
echo "✓ Failure simulation (1 server down)"
echo "✓ Scale-down test (4 -> 2 servers)"
echo ""
echo "Monitor log saved to: /tmp/ha_load_monitor.log"
echo ""
echo "To restore to 3 servers:"
echo "  docker compose -f docker-compose.ranger-audit-haproxy.yml up -d --scale ranger-audit-svc=3"
echo ""
echo "============================================"

