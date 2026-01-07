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

# HDFS Load Generator for Ranger Audit Testing
# This script generates HDFS operations to create audit events

# Configuration
NUM_OPERATIONS=${1:-1000}
CONCURRENCY=${2:-10}
BASE_PATH="/tmp/ranger-audit-load-test-$(date +%s)"

echo "============================================"
echo "HDFS Load Generator for Ranger Audit"
echo "============================================"
echo "Total Operations: $NUM_OPERATIONS"
echo "Concurrent Threads: $CONCURRENCY"
echo "Base Path: $BASE_PATH"
echo "Estimated Audit Events: ~$((NUM_OPERATIONS * 5))"
echo "============================================"

# Function to generate random HDFS operations
generate_hdfs_ops() {
  local thread_id=$1
  local ops_per_thread=$((NUM_OPERATIONS / CONCURRENCY))
  local thread_path="$BASE_PATH/thread-$thread_id"
  
  echo "[Thread $thread_id] Starting with $ops_per_thread operations..."
  
  for i in $(seq 1 $ops_per_thread); do
    local file_path="$thread_path/dir-$i/file-$i.txt"
    local dir_path="$thread_path/dir-$i"
    
    # Operation 1: Create directory (generates audit)
    hdfs dfs -mkdir -p "$dir_path" 2>/dev/null
    
    # Operation 2: Write file (generates audit)
    echo "Test data from thread $thread_id, iteration $i, timestamp $(date +%s)" | \
      hdfs dfs -put -f - "$file_path" 2>/dev/null
    
    # Operation 3: Read file (generates audit)
    hdfs dfs -cat "$file_path" > /dev/null 2>&1
    
    # Operation 4: Get file status (generates audit)
    hdfs dfs -stat "%n %b %y" "$file_path" > /dev/null 2>&1
    
    # Operation 5: List directory (generates audit)
    hdfs dfs -ls "$dir_path" > /dev/null 2>&1
    
    # Every 10 operations, do additional operations
    if [ $((i % 10)) -eq 0 ]; then
      # Change permissions (generates audit)
      hdfs dfs -chmod 755 "$dir_path" 2>/dev/null
      
      # Change ownership (generates audit - may fail if not superuser)
      hdfs dfs -chown hdfs:hadoop "$dir_path" 2>/dev/null || true
    fi
    
    # Progress indicator
    if [ $((i % 100)) -eq 0 ]; then
      echo "[Thread $thread_id] Completed $i/$ops_per_thread operations"
    fi
  done
  
  echo "[Thread $thread_id] Completed all $ops_per_thread operations"
}

# Record start time
start_time=$(date +%s)
echo ""
echo "Starting load generation at $(date)"
echo ""

# Launch concurrent threads
pids=()
for thread in $(seq 1 $CONCURRENCY); do
  generate_hdfs_ops $thread &
  pids+=($!)
done

# Wait for all background jobs
echo "Waiting for all threads to complete..."
for pid in "${pids[@]}"; do
  wait $pid
done

# Calculate duration
end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
echo "============================================"
echo "Load Generation Complete"
echo "============================================"
echo "Duration: $duration seconds"
echo "Operations: $NUM_OPERATIONS"
echo "Throughput: $((NUM_OPERATIONS / duration)) ops/sec"
echo "Estimated Audit Events: ~$((NUM_OPERATIONS * 5))"
echo "Test Data Location: $BASE_PATH"
echo "============================================"
echo ""
echo "To view generated data:"
echo "  hdfs dfs -ls -R $BASE_PATH"
echo ""
echo "To cleanup test data:"
echo "  hdfs dfs -rm -r $BASE_PATH"
echo ""
echo "To check audit events in Kafka:"
echo "  kafka-console-consumer.sh --bootstrap-server localhost:9092 \\"
echo "    --topic ranger_audits --max-messages 10"
echo ""

