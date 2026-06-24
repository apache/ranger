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

# E2E helpers: dynamic partition-plan POST /plugins (mandatory services) + Kafka partition verification.
# Requires partition-plan-e2e-lib.sh and dynamic-auth-to-local-e2e-lib.sh.

# repo|pluginId|shortName|partitionCount|source_container|keytab|principal
# partitionCount is the default for POST /plugins when not overridden.
readonly -a DPP_PLUGIN_ONBOARD_SPECS=(
  "dev_hdfs|hdfs|hdfs|2|ranger-hadoop|/etc/keytabs/hdfs.keytab|hdfs/ranger-hadoop.rangernw@EXAMPLE.COM"
  "dev_hive|hiveServer2|hive|2|ranger-hive|/etc/keytabs/hive.keytab|hive/ranger-hive.rangernw@EXAMPLE.COM"
  "dev_hbase|hbaseMaster|hbase|2|ranger-hbase|/etc/keytabs/hbase.keytab|hbase/ranger-hbase.rangernw@EXAMPLE.COM"
  "dev_kafka|kafka|kafka|2|ranger-kafka|/etc/keytabs/kafka.keytab|kafka/ranger-kafka.rangernw@EXAMPLE.COM"
  "dev_knox|knox|knox|2|ranger-knox|/etc/keytabs/knox.keytab|knox/ranger-knox.rangernw@EXAMPLE.COM"
  "dev_kms|kms|rangerkms|2|ranger-kms|/etc/keytabs/rangerkms.keytab|rangerkms/ranger-kms.rangernw@EXAMPLE.COM"
  "dev_ozone|ozone|om|2|ozone-om|/etc/keytabs/om.keytab|om/ranger-ozone.rangernw@EXAMPLE.COM"
)

dpp_filter_specs() {
  local want="$1"
  local spec repo plugin_id
  for spec in "${DPP_PLUGIN_ONBOARD_SPECS[@]}"; do
    IFS='|' read -r repo plugin_id _ _ _ _ _ <<< "${spec}"
    if [[ -z "${want}" || ",${want}," == *",${plugin_id},"* || ",${want}," == *",${repo},"* ]]; then
      echo "${spec}"
    fi
  done
}

dpp_plan_version() {
  local plan_url
  plan_url="$(pp_ingestor_plan_url "${CONTAINER}")"
  pp_ingestor_request "${CONTAINER}" GET "${plan_url}"
  if [[ "${HTTP_CODE}" != "200" ]]; then
    echo ""
    return 1
  fi
  pp_json_field "${HTTP_BODY}" version
}

dpp_plan_has_plugin() {
  local plugin_id="$1"
  local plan_json="$2"
  printf '%s' "${plan_json}" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); sys.exit(0 if sys.argv[1] in (d.get('plugins') or {}) else 1)" \
    "${plugin_id}" 2>/dev/null
}

dpp_plan_plugin_partitions() {
  local plugin_id="$1"
  local plan_json="$2"
  printf '%s' "${plan_json}" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); p=(d.get('plugins') or {}).get(sys.argv[1]); \
print(','.join(str(x) for x in (p or {}).get('partitions') or []))" "${plugin_id}" 2>/dev/null || echo ""
}

dpp_allowed_users_for_repo() {
  local repo="$1"
  local short_name="$2"
  if [[ "${repo}" == "dev_ozone" ]]; then
    printf '%s' "om,ozone"
  elif [[ "${repo}" == "dev_hdfs" ]]; then
    printf '%s' "hdfs,nn"
  else
    printf '%s' "${short_name}"
  fi
}

dpp_plan_service_has_users() {
  local repo="$1"
  local users_csv="$2"
  local plan_json="$3"
  printf '%s' "${plan_json}" | python3 -c \
    'import sys,json; d=json.load(sys.stdin); repo=sys.argv[1]; want=set(u.strip() for u in sys.argv[2].split(",") if u.strip()); \
entry=(d.get("services") or {}).get(repo) or {}; have=set(entry.get("allowedUsers") or []); sys.exit(0 if want.issubset(have) else 1)' \
    "${repo}" "${users_csv}" 2>/dev/null
}

dpp_onboard_repo_multi() {
  local repo="$1"
  local plugin_id="$2"
  local partition_count="$3"
  local users_csv="$4"
  local version="$5"
  local url body

  url="$(pp_ingestor_plan_url "${CONTAINER}")/plugins"
  body="$(pp_build_onboard_plugin_json "${plugin_id}" "${partition_count}" "${version}" "${repo}:${users_csv}")"
  pp_ingestor_request "${CONTAINER}" POST "${url}" "${body}"
  [[ "${HTTP_CODE}" == "200" ]]
}

dpp_ensure_plugin_onboarded() {
  local spec="$1"
  local repo plugin_id short_name part_count container _keytab _principal
  local version plan_url users_csv

  IFS='|' read -r repo plugin_id short_name part_count container _keytab _principal <<< "${spec}"

  if ! dael_container_running "${container}"; then
    echo "  SKIP onboard ${plugin_id}: ${container} not running"
    return 0
  fi

  plan_url="$(pp_ingestor_plan_url "${CONTAINER}")"
  pp_ingestor_request "${CONTAINER}" GET "${plan_url}"
  if [[ "${HTTP_CODE}" != "200" ]]; then
    pp_record_fail "GET plan before onboard ${plugin_id}: ${HTTP_CODE}"
    return 1
  fi

  users_csv="$(dpp_allowed_users_for_repo "${repo}" "${short_name}")"

  if dpp_plan_has_plugin "${plugin_id}" "${HTTP_BODY}"; then
    if dpp_plan_service_has_users "${repo}" "${users_csv}" "${HTTP_BODY}"; then
      pp_record_pass "${plugin_id} already in partition plan (allowlist OK)"
      return 0
    fi
    pp_record_fail "${plugin_id} in plan but allowlist missing [${users_csv}] — rerun with --fresh-plan"
    return 1
  fi

  version="$(pp_json_field "${HTTP_BODY}" version)"

  echo "  Onboard ${repo} pluginId=${plugin_id} partitions=${part_count} allowedUsers=[${users_csv}]..."
  if ! dpp_onboard_repo_multi "${repo}" "${plugin_id}" "${part_count}" "${users_csv}" "${version}"; then
    pp_record_fail "POST /plugins ${repo} failed: HTTP ${HTTP_CODE} ${HTTP_BODY}"
    return 1
  fi

  pp_record_pass "POST /plugins ${plugin_id} -> plan v$(pp_json_field "${HTTP_BODY}" version)"
  if dael_wait_auth_to_local_applied "${CONTAINER}" 45; then
    pp_record_pass "auth_to_local recomposed after onboard ${plugin_id}"
  else
    pp_record_fail "no auth_to_local recompose log after onboard ${plugin_id}"
  fi
  return 0
}

dpp_audit_event_with_marker() {
  local repo="$1"
  local app_id="$2"
  local marker="$3"
  local evt_ms
  evt_ms="$(python3 -c 'import time; print(int(time.time()*1000))')"
  printf '[{"repo":"%s","reqUser":"e2e-partition","evtTime":%s,"access":"read","resource":"/e2e/%s","result":1,"agent":"%s"}]' \
    "${repo}" "${evt_ms}" "${marker}" "${app_id}"
}

dpp_post_access_with_marker() {
  local src_container="$1"
  local keytab="$2"
  local principal="$3"
  local repo="$4"
  local app_id="$5"
  local marker="$6"
  local body url outfile krb_cc body_file

  body="$(dpp_audit_event_with_marker "${repo}" "${app_id}" "${marker}")"
  url="$(dael_ingestor_access_url)?serviceName=${repo}&appId=${app_id}"
  outfile="/tmp/dpp-access-body-${src_container}-$$"
  krb_cc="/tmp/dpp-access-krb-${src_container}-$$"
  body_file="/tmp/dpp-access-req-${src_container}-$$"

  printf '%s' "${body}" | docker exec -i "${src_container}" tee "${body_file}" >/dev/null
  DAEL_ACCESS_CODE="$(docker exec "${src_container}" bash -c \
    "export KRB5CCNAME=${krb_cc}; kinit -kt '${keytab}' '${principal}' >/dev/null 2>&1 && \
curl -s -o ${outfile} -w '%{http_code}' --negotiate -u : -X POST \
  -H 'Content-Type: application/json' -d @${body_file} '${url}'")"
  DAEL_ACCESS_BODY="$(docker exec "${src_container}" cat "${outfile}" 2>/dev/null || true)"
  docker exec "${src_container}" rm -f "${outfile}" "${body_file}" "${krb_cc}" 2>/dev/null || true
}

dpp_kafka_find_partition_for_marker() {
  local record_key="$1"
  local marker="$2"
  local partitions_csv="${3:-}"
  local max_messages="${4:-800}"
  local out found partition_id end_offset start_offset

  if [[ -n "${partitions_csv}" ]]; then
    for partition_id in $(printf '%s' "${partitions_csv}" | tr ',' ' '); do
      [[ -n "${partition_id}" ]] || continue
      end_offset="$(pp_kafka_run "/opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server ranger-kafka.rangernw:9092 --topic '${AUDIT_TOPIC}' --partitions ${partition_id} --time -1" \
        | awk -F: '{print $NF}' | tail -1)"
      [[ -n "${end_offset}" && "${end_offset}" =~ ^[0-9]+$ ]] || continue
      [[ "${end_offset}" -gt 0 ]] || continue
      start_offset=$((end_offset > 120 ? end_offset - 120 : 0))
      out="$(pp_kafka_consumer_run "timeout 25 /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server ranger-kafka.rangernw:9092 \
        --topic '${AUDIT_TOPIC}' \
        --partition ${partition_id} \
        --offset ${start_offset} \
        --max-messages 120 \
        --property print.key=true \
        --timeout-ms 15000")"
      found="$(printf '%s' "${out}" | python3 -c "
import sys
key, marker, pid = sys.argv[1:4]
for line in sys.stdin:
    if key in line and marker in line:
        print(pid)
        break
" "${record_key}" "${marker}" "${partition_id}" 2>/dev/null || true)"
      if [[ -n "${found}" ]]; then
        echo "${found}"
        return 0
      fi
    done
    return 1
  fi

  out="$(pp_kafka_consumer_run "timeout 25 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server ranger-kafka.rangernw:9092 \
    --topic '${AUDIT_TOPIC}' \
    --from-beginning \
    --max-messages ${max_messages} \
    --property print.partition=true \
    --property print.key=true \
    --timeout-ms 15000")"

  printf '%s' "${out}" | python3 -c "
import re
import sys
key, marker = sys.argv[1:3]
for line in sys.stdin:
    if key not in line or marker not in line:
        continue
    match = re.search(r'Partition[:\\s]+(\\d+)', line, re.I)
    if match:
        print(match.group(1))
        break
" "${record_key}" "${marker}" 2>/dev/null || true
}

dpp_verify_plugin_partition_routing() {
  local spec="$1"
  local repo plugin_id short_name _part_count container keytab principal
  local marker assigned part found

  IFS='|' read -r repo plugin_id short_name _part_count container keytab principal <<< "${spec}"

  if ! dael_container_running "${container}"; then
    return 0
  fi

  pp_ingestor_request "${CONTAINER}" GET "$(pp_ingestor_plan_url "${CONTAINER}")"
  if [[ "${HTTP_CODE}" != "200" ]]; then
    pp_record_fail "GET plan for ${plugin_id} routing check"
    return 1
  fi

  assigned="$(dpp_plan_plugin_partitions "${plugin_id}" "${HTTP_BODY}")"
  if [[ -z "${assigned}" ]]; then
    pp_record_fail "${plugin_id} not in plan — run onboard first"
    return 1
  fi
  pp_record_pass "${plugin_id} assigned partitions [${assigned}]"

  marker="partition-e2e-${plugin_id}-$(date +%s)"
  dpp_post_access_with_marker "${container}" "${keytab}" "${principal}" "${repo}" "${plugin_id}" "${marker}"
  if [[ "${DAEL_ACCESS_CODE}" != "200" && "${DAEL_ACCESS_CODE}" != "202" ]]; then
    pp_record_fail "${plugin_id} POST /access for partition test: HTTP ${DAEL_ACCESS_CODE} ${DAEL_ACCESS_BODY}"
    return 1
  fi
  pp_record_pass "${plugin_id} POST /access HTTP ${DAEL_ACCESS_CODE}"

  found=""
  for _attempt in 1 2 3 4 5; do
    sleep 4
    found="$(dpp_kafka_find_partition_for_marker "${plugin_id}" "${marker}" "${assigned}")"
    [[ -n "${found}" ]] && break
  done
  if [[ -z "${found}" ]]; then
    pp_record_fail "${plugin_id} audit not found on Kafka (key=${plugin_id}, marker=${marker}) — check ACLs or dispatcher lag"
    return 1
  fi

  if printf '%s' "${assigned}" | python3 -c "import sys; allowed=set(sys.argv[1].split(',')); sys.exit(0 if sys.argv[2] in allowed else 1)" "${assigned}" "${found}"; then
    pp_record_pass "${plugin_id} Kafka partition ${found} in assigned [${assigned}]"
    return 0
  fi
  pp_record_fail "${plugin_id} Kafka partition ${found} NOT in assigned [${assigned}]"
  return 1
}

dpp_run_harness_plugin_smoke() {
  local script="$1"
  local label="$2"
  if [[ -x "${script}" ]]; then
    echo ""
    echo "  Harness smoke: ${label}..."
    if "${script}" 2>/dev/null; then
      pp_record_pass "harness ${label} smoke"
    else
      pp_record_fail "harness ${label} smoke"
    fi
  fi
}

dpp_optional_harness_triggers() {
  local docker_dir="$1"
  dpp_run_harness_plugin_smoke "${docker_dir}/scripts/kms/trigger-kms-plugin-audit-e2e.sh" "KMS"
  dpp_run_harness_plugin_smoke "${docker_dir}/scripts/kafka/trigger-kafka-plugin-audit-e2e.sh" "Kafka"
  dpp_run_harness_plugin_smoke "${docker_dir}/scripts/knox/trigger-knox-plugin-audit-e2e.sh" "Knox"
  dpp_run_harness_plugin_smoke "${docker_dir}/scripts/hbase/trigger-hbase-plugin-audit-e2e.sh" "HBase"
}

# Solr dispatcher Kerberos can break after ingestor/topic restarts; restart before HDFS pipeline smoke.
dpp_ensure_solr_dispatcher_ready() {
  local container="ranger-audit-dispatcher-solr"
  local site="/opt/ranger/audit-dispatcher/conf/ranger-audit-dispatcher-solr-site.xml"
  local keytab="/etc/keytabs/rangerauditserver.keytab"
  local principal="rangerauditserver/ranger-audit-dispatcher-solr.rangernw@EXAMPLE.COM"

  if ! docker ps --filter "name=^${container}$" --filter status=running -q | grep -q .; then
    pp_record_fail "Solr dispatcher not running"
    return 1
  fi
  if docker exec "${container}" test -f "${site}" 2>/dev/null; then
    pp_patch_site_prop "${container}" "${site}" "xasecure.audit.jaas.Client.option.useTicketCache" "false" || true
  fi
  echo "  restarting ${container} (Solr Kerberos + consumer reset)..."
  docker restart "${container}" >/dev/null
  sleep 30
  if docker exec "${container}" bash -c "kinit -kt '${keytab}' '${principal}'" >/dev/null 2>&1 \
    && curl -sf "http://localhost:7091/api/health/ping" >/dev/null 2>&1; then
    return 0
  fi
  pp_record_fail "Solr dispatcher not healthy after restart"
  return 1
}
