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

# Start Ranger Audit Servers in HA mode with HAProxy

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

NUM_SERVERS=${1:-3}

cd "$DOCKER_DIR"

echo "============================================"
echo "Starting Ranger Audit HA Setup"
echo "============================================"
echo "Number of Audit Servers: $NUM_SERVERS"
echo "Load Balancer: HAProxy"
echo "Port: 7081"
echo "============================================"
echo ""

# Check if single audit server is running
if docker ps | grep -q "ranger-audit$"; then
  echo "⚠️  Single audit server is running. Stopping it first..."
  docker compose -f docker-compose.ranger-audit.yml down
  echo ""
fi

# Start HA setup with HAProxy
echo "Starting HAProxy and $NUM_SERVERS audit server instances..."
docker compose -f docker-compose.ranger-audit-haproxy.yml up -d --scale ranger-audit-svc=$NUM_SERVERS

echo ""
echo "Waiting for services to start..."
sleep 10

echo ""
echo "============================================"
echo "Status Check"
echo "============================================"

# Check HAProxy status
if docker ps | grep -q "ranger-audit$"; then
  echo "✓ HAProxy is running (ranger-audit)"
else
  echo "✗ HAProxy is not running"
  exit 1
fi

# Count running audit servers
RUNNING_SERVERS=$(docker ps | grep "ranger-audit-svc" | wc -l)
echo "✓ Audit servers running: $RUNNING_SERVERS"

if [ $RUNNING_SERVERS -lt $NUM_SERVERS ]; then
  echo "⚠️  Warning: Expected $NUM_SERVERS servers, but only $RUNNING_SERVERS are running"
fi

echo ""
echo "============================================"
echo "Container Details"
echo "============================================"
docker ps | grep -E "CONTAINER|ranger-audit"

echo ""
echo "============================================"
echo "HAProxy Configuration"
echo "============================================"
echo "Load Balancer URL: http://localhost:7081"
echo "Backend Servers: $RUNNING_SERVERS"
echo ""

# Test connectivity
echo "Testing HAProxy connectivity..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:7081/ | grep -q "200"; then
  echo "✓ HAProxy is responding on port 7081"
else
  echo "⚠️  HAProxy health check - verify with: curl http://localhost:7081/"
fi

echo ""
echo "============================================"
echo "Next Steps"
echo "============================================"
echo "1. Check HAProxy logs:"
echo "   docker logs -f ranger-audit"
echo ""
echo "2. Check individual audit server logs:"
echo "   docker logs -f \$(docker ps | grep ranger-audit-svc | head -1 | awk '{print \$1}')"
echo ""
echo "3. Generate load from HDFS:"
echo "   docker exec -it ranger-hadoop bash"
echo "   chmod +x /scripts/load-testing/hdfs_load_generator.sh"
echo "   /scripts/load-testing/hdfs_load_generator.sh 1000 10"
echo ""
echo "4. Generate load from Hive:"
echo "   docker exec -it ranger-hive bash"
echo "   chmod +x /scripts/load-testing/hive_load_generator.sh"
echo "   /scripts/load-testing/hive_load_generator.sh 100"
echo ""
echo "5. Monitor distribution:"
echo "   watch 'docker stats --no-stream | grep audit'"
echo ""
echo "6. Scale up/down:"
echo "   docker compose -f docker-compose.ranger-audit-haproxy.yml up -d --scale ranger-audit-svc=5"
echo ""
echo "============================================"

