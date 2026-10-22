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

set -e

ES_URL="http://127.0.0.1:9200"
ES_USER="elastic"
ES_PASS="${ELASTICSEARCH_BOOTSTRAP_PASSWORD:-rangerR0cks!}"
RANGER_URL="http://ranger.rangernw:6080"
RANGER_USER="${RANGER_ADMIN_USER:-admin}"
RANGER_PASS="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
SERVICE_NAME="dev_elasticsearch"
CACHE_DIR="${ELASTICSEARCH_HOME}/data/ranger-policycache"
CACHE_FILE="${CACHE_DIR}/elasticsearch_${SERVICE_NAME}.json"
MAX_ATTEMPTS=60
MODE="${1:-all}"

wait_for_ranger_admin() {
  local attempt=0
  until curl -s --max-time 5 -u "${RANGER_USER}:${RANGER_PASS}" \
    "${RANGER_URL}/service/public/v2/api/version" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "${attempt}" -ge "${MAX_ATTEMPTS}" ]; then
      echo "ERROR: Ranger Admin did not become reachable in time." >&2
      exit 1
    fi
    sleep 5
  done
}

wait_for_authorized_search() {
  local attempt=0
  local http_code=""
  until http_code="$(curl -s --max-time 10 -o /dev/null -w '%{http_code}' -u "${ES_USER}:${ES_PASS}" \
    "${ES_URL}/test-index/_search")" && echo "${http_code}" | grep -qE '^(200|404)$'; do
    attempt=$((attempt + 1))
    if [ "${attempt}" -ge "${MAX_ATTEMPTS}" ]; then
      echo "ERROR: Elasticsearch did not authorize ${ES_USER} in time." >&2
      exit 1
    fi
    sleep 5
  done
}

seed_policy_cache() {
  echo "Seeding Ranger policy cache under ${CACHE_DIR}..."
  mkdir -p "${CACHE_DIR}"
  python3 - "${RANGER_URL}" "${RANGER_USER}" "${RANGER_PASS}" "${SERVICE_NAME}" "${CACHE_FILE}" <<'PY'
import json
import sys
import urllib.request
import base64

ranger_url, user, password, service_name, cache_file = sys.argv[1:6]
auth = base64.b64encode(f"{user}:{password}".encode()).decode()

def fetch(path):
    req = urllib.request.Request(f"{ranger_url}{path}")
    req.add_header("Authorization", f"Basic {auth}")
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)

service = fetch(f"/service/public/v2/api/service/name/{service_name}")
policies = fetch(f"/service/public/v2/api/service/{service_name}/policy")
if not isinstance(policies, list):
    raise SystemExit(f"unexpected policy payload type: {type(policies)}")

for policy in policies:
    resources = policy.get("resources") or {}
    index_resource = resources.get("index") or {}
    index_values = index_resource.get("values") or []
    if "test-index" in index_values:
        for item in policy.get("policyItems") or []:
            item["users"] = ["elastic", "testuser_2"]
        policy_id = policy.get("id")
        if policy_id is not None:
            update_body = {
                "id": policy_id,
                "name": policy.get("name"),
                "service": policy.get("service", service_name),
                "serviceType": policy.get("serviceType", "elasticsearch"),
                "resources": policy.get("resources"),
                "policyItems": policy.get("policyItems"),
                "isEnabled": policy.get("isEnabled", True),
                "policyType": policy.get("policyType", 0),
                "isAuditEnabled": policy.get("isAuditEnabled", True),
            }
            try:
                update_req = urllib.request.Request(
                    f"{ranger_url}/service/public/v2/api/policy/{policy_id}",
                    data=json.dumps(update_body).encode(),
                    method="PUT",
                )
                update_req.add_header("Authorization", f"Basic {auth}")
                update_req.add_header("Content-Type", "application/json")
                with urllib.request.urlopen(update_req) as resp:
                    if resp.status != 200:
                        print(f"WARN: Ranger Admin policy update returned HTTP {resp.status}", file=sys.stderr)
            except Exception as exc:
                print(f"WARN: Could not update test-index policy in Ranger Admin: {exc}", file=sys.stderr)

policy_version = service.get("policyVersion")
if policy_version is None:
    policy_version = max((p.get("version") or 1) for p in policies) if policies else 1

payload = {
    "serviceName": service_name,
    "serviceId": service.get("id", 1),
    "policyVersion": policy_version,
    "policies": policies,
}

with open(cache_file, "w", encoding="utf-8") as handle:
    json.dump(payload, handle)
PY

  chown elasticsearch:hadoop "${CACHE_FILE}"
  chmod 640 "${CACHE_FILE}"
}

disable_admin_policy_download() {
  SECURITY_XML="${ELASTICSEARCH_HOME}/config/ranger-elasticsearch-plugin/ranger-elasticsearch-security.xml"
  if [ -f "${SECURITY_XML}" ]; then
    python3 "${RANGER_SCRIPTS}/patch-ranger-security-xml.py" "${SECURITY_XML}" --clear-admin-creds
  fi
}

create_test_fixtures() {
  echo "Creating test index test-index..."
  curl -s --max-time 30 -u "${ES_USER}:${ES_PASS}" -X PUT "${ES_URL}/test-index" \
    -H 'Content-Type: application/json' \
    -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}' \
    | grep -q '"acknowledged":true' || echo "test-index may already exist"
}

case "${MODE}" in
  seed)
    wait_for_ranger_admin
    seed_policy_cache
    ;;
  fixtures)
    sleep 30
    wait_for_authorized_search
    create_test_fixtures
    ;;
  all)
    wait_for_ranger_admin
    seed_policy_cache
    sleep 30
    wait_for_authorized_search
    create_test_fixtures
    ;;
  *)
    echo "Usage: $0 [seed|fixtures|all]" >&2
    exit 1
    ;;
esac

echo "Elasticsearch post-setup (${MODE}) completed successfully"
