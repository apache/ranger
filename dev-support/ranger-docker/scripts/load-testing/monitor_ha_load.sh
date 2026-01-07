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

# Monitor Ranger Audit HA Load Distribution

REFRESH_INTERVAL=${1:-5}

echo "============================================"
echo "Ranger Audit HA Load Monitor"
echo "============================================"
echo "Refresh Interval: ${REFRESH_INTERVAL}s"
echo "Press Ctrl+C to exit"
echo "============================================"
echo ""

# Function to get container stats
get_stats() {
  clear
  echo "============================================"
  echo "Ranger Audit HA Status - $(date)"
  echo "============================================"
  echo ""
  
  # HAProxy status
  echo "--- HAProxy Status ---"
  if docker ps | grep -q "ranger-audit$"; then
    echo "✓ HAProxy: Running"
    echo ""
    
    # Try to get HAProxy stats (if stats endpoint is enabled)
    echo "HAProxy Backend Servers:"
    docker exec ranger-audit cat /usr/local/etc/haproxy/haproxy.cfg | \
      grep -A 20 "backend be_audit" | grep "server" || echo "  (check HAProxy logs for details)"
  else
    echo "✗ HAProxy: Not Running"
  fi
  
  echo ""
  echo "--- Audit Server Instances ---"
  AUDIT_SERVERS=$(docker ps --filter "name=ranger-audit-svc" --format "{{.Names}}")
  SERVER_COUNT=$(echo "$AUDIT_SERVERS" | grep -v "^$" | wc -l)
  
  if [ $SERVER_COUNT -eq 0 ]; then
    echo "✗ No audit servers running"
  else
    echo "✓ Running Servers: $SERVER_COUNT"
    echo ""
    
    # Show resource usage for each server
    echo "Resource Usage:"
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" | \
      grep -E "NAME|ranger-audit"
  fi
  
  echo ""
  echo "--- Kafka Consumer Groups ---"
  if docker ps | grep -q "ranger-kafka"; then
    echo "Solr Consumer Group:"
    docker exec ranger-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      --group ranger_audit_solr_consumer_group \
      --describe 2>/dev/null | head -10 || echo "  (No active consumers)"
    
    echo ""
    echo "HDFS Consumer Group:"
    docker exec ranger-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      --group ranger_audit_hdfs_consumer_group \
      --describe 2>/dev/null | head -10 || echo "  (No active consumers)"
  else
    echo "✗ Kafka container not running"
  fi
  
  echo ""
  echo "--- Recent HAProxy Logs (last 5 lines) ---"
  docker logs --tail 5 ranger-audit 2>/dev/null || echo "  (No logs available)"
  
  echo ""
  echo "--- Kafka Topic Info ---"
  if docker ps | grep -q "ranger-kafka"; then
    docker exec ranger-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --describe \
      --topic ranger_audits 2>/dev/null | grep -E "Topic:|PartitionCount:|ReplicationFactor:" || \
      echo "  (Topic not found or Kafka not ready)"
  fi
  
  echo ""
  echo "============================================"
  echo "Tip: Run load generators to see distribution"
  echo "============================================"
}

# Main monitoring loop
while true; do
  get_stats
  sleep $REFRESH_INTERVAL
done

