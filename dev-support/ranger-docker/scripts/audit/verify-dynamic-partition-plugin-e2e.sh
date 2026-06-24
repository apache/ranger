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

# E2E: dynamic partition allocation — POST /partition-plan/plugins per plugin + Kafka routing.
#
# Flow (mirrors ranger-audit-e2e-harness plugin matrix):
#   1. Enable dynamic mode (empty configured.plugins → buffer-only bootstrap)
#   2. For each running plugin container: POST /partition-plan/plugins
#      (pluginId, partitionCount, expectedVersion, mandatory services map)
#   3. POST /api/audit/access from plugin keytab → verify Kafka record partition ∈ plan
#   4. Optional: run harness trigger-* scripts when present (KMS, Kafka, Knox, HBase)
#
# Prerequisites: Tier 3 Docker (see audit-server/README-DYNAMIC-PARTITION-PLUGIN-E2E.md).
# Reference harness branch: ranger-audit-e2e-harness (plugin verify/trigger scripts).
#
# Usage:
#   cd dev-support/ranger-docker
#   ./scripts/audit/verify-dynamic-partition-plugin-e2e.sh
#   ./scripts/audit/verify-dynamic-partition-plugin-e2e.sh --no-enable --plugins hdfs,kms
#   ./scripts/audit/verify-dynamic-partition-plugin-e2e.sh --with-harness-triggers

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=partition-plan-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/partition-plan-e2e-lib.sh"
# shellcheck source=dynamic-auth-to-local-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/dynamic-auth-to-local-e2e-lib.sh"
# shellcheck source=dynamic-partition-plugin-e2e-lib.sh
source "${SCRIPT_DIR}/scripts/audit/dynamic-partition-plugin-e2e-lib.sh"

DO_ENABLE=true
PLUGIN_FILTER=""
WITH_HARNESS=false
TIMEOUT=300
FRESH_PLAN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-enable) DO_ENABLE=false; shift ;;
    --plugins) PLUGIN_FILTER="${2:?}"; shift 2 ;;
    --with-harness-triggers) WITH_HARNESS=true; shift ;;
    --fresh-plan) FRESH_PLAN=true; shift ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    -h|--help)
      sed -n '31,36p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

echo "=== Dynamic partition plugin onboard + routing E2E ==="
pp_preflight_tier3 "${TIMEOUT}"

if [[ "${DO_ENABLE}" == "true" ]]; then
  if ! pp_dynamic_enabled "${CONTAINER}" "${SITE_XMLS[0]}"; then
    echo "Preparing audit topic for greenfield layout..."
    pp_prepare_audit_topic_for_greenfield "${CONTAINER}"
    if [[ "${FRESH_PLAN}" == "true" ]]; then
      pp_delete_plan_topic
    fi
    echo "Enabling dynamic partition plan..."
    pp_set_dynamic_enabled "${CONTAINER}" "true" || { pp_record_fail "enable dynamic"; pp_print_results; exit 1; }
    pp_wait_health 7081 "Ingestor after enable" "${TIMEOUT}" || { pp_record_fail "health"; pp_print_results; exit 1; }
  fi
fi

if ! pp_dynamic_enabled "${CONTAINER}" "${SITE_XMLS[0]}"; then
  pp_record_fail "dynamic.enabled must be true"
  pp_print_results
  exit 1
fi

if ! pp_wait_watcher "${CONTAINER}" "${TIMEOUT}"; then
  pp_record_fail "PartitionPlanWatcher not ready"
  pp_print_results
  exit 1
fi
pp_record_pass "PartitionPlanWatcher ready"

PLAN_URL="$(pp_ingestor_plan_url "${CONTAINER}")"
pp_ingestor_request "${CONTAINER}" GET "${PLAN_URL}"
if [[ "${HTTP_CODE}" == "200" ]]; then
  pp_record_pass "GET partition-plan returns 200 (v$(pp_json_field "${HTTP_BODY}" version))"
else
  pp_record_fail "GET partition-plan expected 200, got ${HTTP_CODE}"
  pp_print_results
  exit 1
fi

echo ""
echo "=== Phase 1: POST /plugins per running plugin (REST, mandatory services) ==="
spec=""
while IFS= read -r spec; do
  [[ -n "${spec}" ]] || continue
  dpp_ensure_plugin_onboarded "${spec}" || true
done < <(dpp_filter_specs "${PLUGIN_FILTER}")

echo ""
echo "=== Phase 2: POST /access + Kafka partition ∈ plan ==="
while IFS= read -r spec; do
  [[ -n "${spec}" ]] || continue
  IFS='|' read -r repo plugin_id _ _ container _ _ <<< "${spec}"
  echo ""
  echo "--- ${plugin_id} (${repo}) ---"
  dpp_verify_plugin_partition_routing "${spec}" || true
done < <(dpp_filter_specs "${PLUGIN_FILTER}")

if [[ "${WITH_HARNESS}" == "true" ]]; then
  echo ""
  echo "=== Phase 3: optional ranger-audit-e2e-harness plugin triggers ==="
  dpp_optional_harness_triggers "${SCRIPT_DIR}"
fi

echo ""
echo "See: audit-server/README-DYNAMIC-PARTITION-PLUGIN-E2E.md"
echo "Curl cookbook: dist/audit-e2e/access-ingestor-curl-cookbook.sh (run verify-dynamic-auth-to-local-e2e.sh --generate-curl-only)"

pp_print_results
