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

set -e 

echo "Checking Ranger plugin status via Ranger Admin API"

# Load environment variables from .env file
SCRIPT_DIR=$(dirname "$0")
source "$SCRIPT_DIR/../.env"

RANGER_HOST="http://localhost:6080"
ENDPOINT="$RANGER_HOST/service/public/v2/api/plugins/info"

# Trigger activity for KNOX to make plugin active
echo
echo "Triggering Knox activity to ensure plugin status is updated..."
KNOX_ENDPOINT="https://localhost:8443/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS"

curl -k -u "$KNOX_USER:$KNOX_PASS" "$KNOX_ENDPOINT" > /dev/null 2>&1
echo "Knox activity triggered."

sleep 30

echo "Fetching plugin info from $ENDPOINT ..."
response=$(curl -s -u "$RANGER_ADMIN_USER:$RANGER_ADMIN_PASS" "$ENDPOINT")

if [[ -z "$response" || "$response" == "[]" ]]; then
  echo "No plugin info returned from API."
  exit 1
fi

expected_services=("hdfs" "hbase" "kms" "yarn" "kafka" "ozone" "knox" "hive")
failed=false

echo
echo "<---------  Plugin Status  ----------> "
for svc in "${expected_services[@]}"; do
  echo
  echo "Checking service type: $svc"
  
  entries=$(echo "$response" | jq --arg svc "$svc" '[.[] | select(.serviceType == $svc)]')
  count=$(echo "$entries" | jq 'length')

  if (( count == 0 )); then
    echo "MISSING: No plugins found for service type '$svc'."
    failed=true
    continue
  fi

  active_count=$(echo "$entries" | jq '[.[] | select(.info.policyActiveVersion != null and .info.policyActiveVersion != "")] | length')

  echo "🟢 Active plugins: $active_count / $count total plugins found."

  if (( active_count == 0 )); then
    echo "WARNING: Plugins present but NONE are active for '$svc'."
    failed=true
  fi

  # List hostnames and plugin statuses for this service type
  echo "Details:"
  echo "$entries" | jq -r '.[] | "- Host: \(.hostName), AppType: \(.appType), PolicyActiveVersion: \(.info.policyActiveVersion // "null")"'
done

echo
if $failed; then
  echo "❌ One or more plugins are missing or inactive."
  exit 1
else
  echo "✅ All expected plugins are present and active."
fi
