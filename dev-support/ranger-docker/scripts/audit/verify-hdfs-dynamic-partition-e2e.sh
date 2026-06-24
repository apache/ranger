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

# HDFS-focused dynamic partition E2E:
#   1. Enable dynamic mode (greenfield buffer-only plan)
#   2. POST /partition-plan/plugins — dev_hdfs + hdfs plugin + mandatory services allowlist
#   3. Verify POST /access + Kafka partition routing
#   4. PATCH /partition-plan/plugins/hdfs — expand partition count
#   5. Verify routing again after scale
#
# Usage:
#   cd dev-support/ranger-docker
#   ./scripts/audit/verify-hdfs-dynamic-partition-e2e.sh
#   ./scripts/audit/verify-hdfs-dynamic-partition-e2e.sh --with-hdfs-trigger

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=partition-plan-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/partition-plan-e2e-lib.sh"
# shellcheck source=dynamic-auth-to-local-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/dynamic-auth-to-local-e2e-lib.sh"
# shellcheck source=dynamic-partition-plugin-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/dynamic-partition-plugin-e2e-lib.sh"

readonly HDFS_SPEC="dev_hdfs|hdfs|hdfs|2|ranger-hadoop|/etc/keytabs/hdfs.keytab|hdfs/ranger-hadoop.rangernw@EXAMPLE.COM"
SCALE_ADDITIONAL="${SCALE_ADDITIONAL:-2}"
TIMEOUT=300
FRESH_PLAN=false
WITH_HDFS_TRIGGER=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fresh-plan) FRESH_PLAN=true; shift ;;
    --with-hdfs-trigger) WITH_HDFS_TRIGGER=true; shift ;;
    --scale-additional) SCALE_ADDITIONAL="${2:?}"; shift 2 ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    -h|--help)
      sed -n '26,29p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

hdfs_scale_plugin() {
  local plugin_id="$1"
  local additional="$2"
  local version="$3"
  local url body before after attempt

  pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
  before="$(dpp_plan_plugin_partitions "${plugin_id}" "${HTTP_BODY}")"

  url="${PLAN_URL}/plugins/${plugin_id}"
  body="{\"additionalPartitions\":${additional},\"expectedVersion\":${version}}"
  for attempt in 1 2 3; do
    echo "  PATCH ${url} (+${additional} partitions, expectedVersion=${version}, attempt ${attempt})..."
    pp_ingestor_request "${CONTAINER}" PATCH "${url}" "${body}"
    if [[ "${HTTP_CODE}" == "200" ]]; then
      after="$(dpp_plan_plugin_partitions "${plugin_id}" "${HTTP_BODY}")"
      pp_record_pass "scale ${plugin_id} partitions [${before}] -> [${after}] plan v$(pp_json_field "${HTTP_BODY}" version)"
      if [[ "$(echo "${before}" | tr ',' '\n' | wc -l | tr -d ' ')" -ge "$(echo "${after}" | tr ',' '\n' | wc -l | tr -d ' ')" ]]; then
        pp_record_fail "scale ${plugin_id} did not increase partition count"
        return 1
      fi
      return 0
    fi
    if [[ "${HTTP_CODE}" == "503" && "${attempt}" -lt 3 ]]; then
      echo "  scale returned 503 (Kafka grow timeout?) — retrying in 15s..."
      sleep 15
      version="$(dpp_plan_version)"
      body="{\"additionalPartitions\":${additional},\"expectedVersion\":${version}}"
      continue
    fi
    pp_record_fail "scale ${plugin_id} expected 200, got ${HTTP_CODE}: ${HTTP_BODY}"
    return 1
  done
  return 1
}

echo "=== HDFS dynamic partition REST + routing E2E ==="
if [[ -x "${SCRIPT_DIR}/scripts/audit/ensure-audit-ingestor-plugin-users.sh" ]]; then
  "${SCRIPT_DIR}/scripts/audit/ensure-audit-ingestor-plugin-users.sh" || true
fi
for site in "${SITE_XMLS[@]}"; do
  if docker exec "${CONTAINER}" test -f "${site}" 2>/dev/null; then
    pp_patch_site_prop "${CONTAINER}" "${site}" "ranger.audit.ingestor.service.dev_hdfs.allowed.users" "hdfs,nn"
  fi
done
pp_preflight_tier3 "${TIMEOUT}"

if ! dael_container_running "ranger-hadoop"; then
  echo "ERROR: ranger-hadoop is not running. Start HDFS stack first, e.g.:" >&2
  echo "  ./setup-audit-e2e.sh up --hdfs-only --no-verify" >&2
  exit 1
fi

PLAN_URL="$(pp_ingestor_plan_url "${CONTAINER}")"

echo ""
echo "=== Step 1: Enable dynamic partition plan ==="
if [[ "${FRESH_PLAN}" == "true" ]]; then
  echo "  Fresh plan requested — resetting plan topic and greenfield audit topic layout..."
  pp_set_dynamic_enabled "${CONTAINER}" "false" "false" || true
  pp_prepare_audit_topic_for_greenfield "${CONTAINER}"
  pp_delete_plan_topic
fi
if ! pp_dynamic_enabled "${CONTAINER}" "${SITE_XMLS[0]}"; then
  pp_prepare_audit_topic_for_greenfield "${CONTAINER}"
  if [[ "${FRESH_PLAN}" == "true" ]]; then
    pp_delete_plan_topic
  fi
  pp_set_dynamic_enabled "${CONTAINER}" "true" || { pp_record_fail "enable dynamic"; pp_print_results; exit 1; }
  pp_wait_health 7081 "Ingestor after enable" "${TIMEOUT}" || { pp_record_fail "health"; pp_print_results; exit 1; }
else
  pp_record_pass "dynamic mode already enabled"
fi

pp_wait_watcher "${CONTAINER}" "${TIMEOUT}" || { pp_record_fail "watcher"; pp_print_results; exit 1; }
pp_record_pass "PartitionPlanWatcher ready"

echo ""
echo "=== Step 2: POST /partition-plan/plugins (dev_hdfs + allowlist) ==="
dpp_ensure_plugin_onboarded "${HDFS_SPEC}" || { pp_print_results; exit 1; }

echo ""
echo "=== Step 3: E2E after onboard — POST /access + Kafka partition routing ==="
dpp_verify_plugin_partition_routing "${HDFS_SPEC}" || true

if [[ "${WITH_HDFS_TRIGGER}" == "true" && -x "${SCRIPT_DIR}/setup-audit-e2e.sh" ]]; then
  echo ""
  echo "=== Step 3b: Full HDFS audit pipeline (dfs smoke) ==="
  dpp_ensure_solr_dispatcher_ready || true
  if "${SCRIPT_DIR}/setup-audit-e2e.sh" trigger-hdfs-audit --no-verify 2>/dev/null; then
    pp_record_pass "trigger-hdfs-audit pipeline"
  else
    pp_record_fail "trigger-hdfs-audit pipeline"
  fi
fi

echo ""
echo "=== Step 4: PATCH /partition-plan/plugins/hdfs (expand partitions) ==="
version="$(dpp_plan_version)"
if [[ -z "${version}" ]]; then
  pp_record_fail "could not read plan version before scale"
  pp_print_results
  exit 1
fi
hdfs_scale_plugin "hdfs" "${SCALE_ADDITIONAL}" "${version}" || true
dael_wait_auth_to_local_applied "${CONTAINER}" 45 && pp_record_pass "auth_to_local after scale" || pp_record_fail "auth_to_local after scale"
echo "  waiting 10s for ingestor partitioner metadata after scale..."
sleep 10

echo ""
echo "=== Step 5: E2E after scale — POST /access + Kafka partition routing ==="
dpp_verify_plugin_partition_routing "${HDFS_SPEC}" || true

if [[ "${WITH_HDFS_TRIGGER}" == "true" && -x "${SCRIPT_DIR}/setup-audit-e2e.sh" ]]; then
  echo ""
  echo "=== Step 5b: Full HDFS audit pipeline after scale ==="
  dpp_ensure_solr_dispatcher_ready || true
  if "${SCRIPT_DIR}/setup-audit-e2e.sh" trigger-hdfs-audit --no-verify 2>/dev/null; then
    pp_record_pass "trigger-hdfs-audit after scale"
  else
    pp_record_fail "trigger-hdfs-audit after scale"
  fi
fi

pp_print_results
[[ "${PP_FAIL}" -eq 0 ]]
