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

# E2E helpers: dynamic allowlist + auth_to_local + POST /api/audit/access via curl/SPNEGO.
# Requires partition-plan-e2e-lib.sh (pp_*) to be sourced first.

DAEL_INGESTOR_HOST="${CONTAINER}.rangernw"
DAEL_INGESTOR_HTTP_PORT="${INGESTOR_HTTP_PORT:-7081}"
DAEL_ACCESS_PATH="/api/audit/access"
DAEL_ONBOARD_PATH="/api/audit/partition-plan/plugins"
DAEL_ACCESS_CODE=""
DAEL_ACCESS_BODY=""

# repo|appId|shortName|source_container|keytab|principal
# shortName = auth_to_local output checked against services[].allowedUsers
# appId     = Kafka plugin id (agentId on audit events)
readonly -a DAEL_PLUGIN_PROFILES=(
  "dev_hdfs|hdfs|hdfs|ranger-hadoop|/etc/keytabs/hdfs.keytab|hdfs/ranger-hadoop.rangernw@EXAMPLE.COM"
  "dev_yarn|yarn|yarn|ranger-hadoop|/etc/keytabs/yarn.keytab|yarn/ranger-hadoop.rangernw@EXAMPLE.COM"
  "dev_hive|hiveServer2|hive|ranger-hive|/etc/keytabs/hive.keytab|hive/ranger-hive.rangernw@EXAMPLE.COM"
  "dev_hbase|hbaseMaster|hbase|ranger-hbase|/etc/keytabs/hbase.keytab|hbase/ranger-hbase.rangernw@EXAMPLE.COM"
  "dev_kafka|kafka|kafka|ranger-kafka|/etc/keytabs/kafka.keytab|kafka/ranger-kafka.rangernw@EXAMPLE.COM"
  "dev_knox|knox|knox|ranger-knox|/etc/keytabs/knox.keytab|knox/ranger-knox.rangernw@EXAMPLE.COM"
  "dev_kms|kms|rangerkms|ranger-kms|/etc/keytabs/rangerkms.keytab|rangerkms/ranger-kms.rangernw@EXAMPLE.COM"
  "dev_trino|trino|trino|ranger-trino|/etc/keytabs/trino.keytab|trino/ranger-trino.rangernw@EXAMPLE.COM"
  "dev_solr|solr|solr|ranger-solr|/etc/keytabs/solr.keytab|solr/ranger-solr.rangernw@EXAMPLE.COM"
  "dev_ozone|ozone|om|ozone-om|/etc/keytabs/om.keytab|om/ranger-ozone.rangernw@EXAMPLE.COM"
)

dael_ingestor_access_url() {
  echo "http://${DAEL_INGESTOR_HOST}:${DAEL_INGESTOR_HTTP_PORT}${DAEL_ACCESS_PATH}"
}

dael_audit_event_json() {
  local repo="$1"
  local app_id="$2"
  local evt_ms
  evt_ms="$(python3 -c 'import time; print(int(time.time()*1000))')"
  printf '[{"repo":"%s","reqUser":"e2e-audit-user","evtTime":%s,"access":"read","resource":"/e2e/path","result":1,"agent":"%s"}]' \
    "${repo}" "${evt_ms}" "${app_id}"
}

dael_container_running() {
  local name="$1"
  docker ps --filter "name=^${name}$" --filter status=running --format '{{.Names}}' | grep -qx "${name}"
}

dael_access_post_from_container() {
  local src_container="$1"
  local keytab="$2"
  local principal="$3"
  local service_name="$4"
  local app_id="$5"
  local body
  local url
  local outfile="/tmp/dael-access-body-${src_container}-$$"
  local krb_cc="/tmp/dael-access-krb-${src_container}-$$"
  local body_file="/tmp/dael-access-req-${src_container}-$$"

  body="$(dael_audit_event_json "${service_name}" "${app_id}")"
  url="$(dael_ingestor_access_url)?serviceName=${service_name}&appId=${app_id}"

  if ! dael_container_running "${src_container}"; then
    DAEL_ACCESS_CODE="000"
    DAEL_ACCESS_BODY="container ${src_container} not running"
    return 1
  fi

  if ! docker exec "${src_container}" test -f "${keytab}" 2>/dev/null; then
    DAEL_ACCESS_CODE="000"
    DAEL_ACCESS_BODY="keytab missing: ${keytab}"
    return 1
  fi

  printf '%s' "${body}" | docker exec -i "${src_container}" tee "${body_file}" >/dev/null
  DAEL_ACCESS_CODE="$(docker exec "${src_container}" bash -c \
    "export KRB5CCNAME=${krb_cc}; kinit -kt '${keytab}' '${principal}' >/dev/null 2>&1 && \
curl -s -o ${outfile} -w '%{http_code}' --negotiate -u : -X POST \
  -H 'Content-Type: application/json' -d @${body_file} '${url}'")"
  DAEL_ACCESS_BODY="$(docker exec "${src_container}" cat "${outfile}" 2>/dev/null || true)"
  docker exec "${src_container}" rm -f "${outfile}" "${body_file}" "${krb_cc}" 2>/dev/null || true
}

dael_expect_access() {
  local label="$1"
  local expect_code="$2"
  if [[ "${DAEL_ACCESS_CODE}" == "${expect_code}" ]]; then
    pp_record_pass "${label} (HTTP ${expect_code})"
    return 0
  fi
  pp_record_fail "${label} expected HTTP ${expect_code}, got ${DAEL_ACCESS_CODE}: ${DAEL_ACCESS_BODY}"
  return 1
}

dael_json_authenticated_user() {
  printf '%s' "${DAEL_ACCESS_BODY}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('authenticatedUser',''))" 2>/dev/null || echo ""
}

dael_plan_update_services_body() {
  local plan_json="$1"
  local plugin_id="$2"
  local repo="$3"
  local users_csv="$4"
  printf '%s' "${plan_json}" | python3 -c \
    "import sys,json; plan=json.load(sys.stdin); plugin_id,repo,users=sys.argv[1:4]; \
users=[u.strip() for u in users.split(',') if u.strip()]; \
body={'expectedVersion': plan['version'], 'updateServices': {repo: {'allowedUsers': users}}}; \
print(json.dumps(body))" "${plugin_id}" "${repo}" "${users_csv}"
}

dael_patch_allowed_users() {
  local repo="$1"
  local plugin_id="$2"
  local users_csv="$3"
  local patch_url
  local patch_body

  patch_url="$(pp_ingestor_plan_url "${CONTAINER}")"
  pp_ingestor_request "${CONTAINER}" GET "${patch_url}"
  if [[ "${HTTP_CODE}" != "200" ]]; then
    pp_record_fail "GET plan before PATCH allowedUsers for ${repo}: ${HTTP_CODE}"
    return 1
  fi
  patch_body="$(dael_plan_update_services_body "${HTTP_BODY}" "${plugin_id}" "${repo}" "${users_csv}")"
  pp_ingestor_request "${CONTAINER}" PATCH "${patch_url}/plugins/${plugin_id}" "${patch_body}"
  if [[ "${HTTP_CODE}" == "200" ]]; then
    return 0
  fi
  pp_record_fail "PATCH updateServices for ${repo} (${plugin_id}) failed: HTTP ${HTTP_CODE} ${HTTP_BODY}"
  return 1
}

dael_onboard_repo() {
  local repo="$1"
  local plugin_id="$2"
  local short_name="$3"
  local partition_count="${4:-2}"
  local version="$5"
  local url body

  url="$(pp_ingestor_plan_url "${CONTAINER}")/plugins"
  body="$(pp_build_onboard_plugin_json "${plugin_id}" "${partition_count}" "${version}" "${repo}:${short_name}")"
  pp_ingestor_request "${CONTAINER}" POST "${url}" "${body}"
  [[ "${HTTP_CODE}" == "200" ]]
}

dael_wait_auth_to_local_applied() {
  local container="$1"
  local timeout="${2:-60}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if docker logs "${container}" 2>&1 | tail -200 | grep -q "Applied composed auth_to_local rules"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

dael_write_curl_cookbook() {
  local out_file="$1"
  local access_url onboard_url
  access_url="$(dael_ingestor_access_url)"
  onboard_url="http://${DAEL_INGESTOR_HOST}:${DAEL_INGESTOR_HTTP_PORT}${DAEL_ONBOARD_PATH}"

  {
    echo "# Ranger audit ingestor access E2E — curl cookbook"
    echo "# Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# Run inside a container on rangernw with the plugin keytab (SPNEGO Host must be FQDN)."
    echo ""
    echo "# Health (no auth):"
    echo "curl -s http://localhost:7081/api/audit/health"
    echo ""
    echo "# Partition plan (HTTP SPNEGO as ingestor HTTP service):"
    echo "kinit -kt /etc/keytabs/HTTP.keytab HTTP/ranger-audit-ingestor.rangernw@EXAMPLE.COM"
    echo "curl -s --negotiate -u : http://ranger-audit-ingestor.rangernw:7081/api/audit/partition-plan"
    echo ""
    echo "# Onboard plugin + allowlist (dynamic mode, services required):"
    echo "curl -s --negotiate -u : -X POST -H 'Content-Type: application/json' \\"
    echo "  '${onboard_url}' \\"
    echo "  -d '{\"pluginId\":\"trino\",\"partitionCount\":3,\"expectedVersion\":1,\"services\":{\"dev_trino\":{\"allowedUsers\":[\"trino\"]}}}'"
    echo ""

    local profile repo app_id short_name src_container keytab principal body
    for profile in "${DAEL_PLUGIN_PROFILES[@]}"; do
      IFS='|' read -r repo app_id short_name src_container keytab principal <<< "${profile}"
      body="$(dael_audit_event_json "${repo}" "${app_id}")"
      echo "# POST audit — ${repo} (${short_name} via auth_to_local, appId=${app_id})"
      echo "# Run on: ${src_container}"
      echo "kinit -kt ${keytab} ${principal}"
      echo "curl -s -w '\\nHTTP %{http_code}\\n' --negotiate -u : -X POST \\"
      echo "  -H 'Content-Type: application/json' \\"
      echo "  '${access_url}?serviceName=${repo}&appId=${app_id}' \\"
      echo "  -d '${body}'"
      echo ""
    done
  } > "${out_file}"
  echo "  Wrote curl cookbook: ${out_file}"
}

dael_test_plugin_access() {
  local profile="$1"
  local repo app_id short_name src_container keytab principal auth_user

  IFS='|' read -r repo app_id short_name src_container keytab principal <<< "${profile}"

  if ! dael_container_running "${src_container}"; then
    echo "  SKIP ${repo}: ${src_container} not running"
    return 0
  fi

  echo ""
  echo "  Plugin ${app_id} / repo ${repo} (shortName=${short_name})..."
  dael_access_post_from_container "${src_container}" "${keytab}" "${principal}" "${repo}" "${app_id}"
  if ! dael_expect_access "${repo} POST /access as ${short_name}" "200"; then
    return 1
  fi
  auth_user="$(dael_json_authenticated_user)"
  if [[ "${auth_user}" == "${short_name}" ]]; then
    pp_record_pass "${repo} authenticatedUser=${auth_user}"
  else
    pp_record_fail "${repo} authenticatedUser expected ${short_name}, got ${auth_user}"
    return 1
  fi
  return 0
}

dael_test_dynamic_user_toggle() {
  local repo="$1"
  local short_name="$2"
  local src_container="$3"
  local keytab="$4"
  local principal="$5"
  local app_id="$6"

  echo ""
  echo "  Dynamic allowlist toggle for ${repo}..."

  if ! dael_patch_allowed_users "${repo}" "${app_id}" "wronguser-not-allowed"; then
    return 1
  fi
  if dael_wait_auth_to_local_applied "${CONTAINER}" 30; then
    pp_record_pass "auth_to_local recomposed after allowlist changed"
  else
    pp_record_fail "no auth_to_local recompose log after allowlist changed"
  fi

  dael_access_post_from_container "${src_container}" "${keytab}" "${principal}" "${repo}" "${app_id}"
  dael_expect_access "${repo} denied when user not in allowlist" "403" || return 1

  if ! dael_patch_allowed_users "${repo}" "${app_id}" "${short_name}"; then
    return 1
  fi
  if dael_wait_auth_to_local_applied "${CONTAINER}" 30; then
    pp_record_pass "auth_to_local recomposed after allowlist restored"
  else
    pp_record_fail "no auth_to_local recompose log after allowlist restored"
  fi

  dael_access_post_from_container "${src_container}" "${keytab}" "${principal}" "${repo}" "${app_id}"
  dael_expect_access "${repo} allowed after allowlist restored" "200" || return 1
  return 0
}

dael_test_cross_repo_denied() {
  local src_container="$1"
  local keytab="$2"
  local principal="$3"
  local wrong_repo="$4"
  local app_id="$5"
  local label="$6"

  dael_access_post_from_container "${src_container}" "${keytab}" "${principal}" "${wrong_repo}" "${app_id}"
  dael_expect_access "${label}" "403"
}

dael_filter_profiles() {
  local want="$1"
  local profile
  for profile in "${DAEL_PLUGIN_PROFILES[@]}"; do
    IFS='|' read -r _ app_id _ _ _ _ <<< "${profile}"
    if [[ -z "${want}" || ",${want}," == *",${app_id},"* || ",${want}," == *",$(echo "${profile}" | cut -d'|' -f1),"* ]]; then
      echo "${profile}"
    fi
  done
}
