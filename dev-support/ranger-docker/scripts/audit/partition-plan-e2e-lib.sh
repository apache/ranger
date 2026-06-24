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

# Shared helpers for partition-plan E2E scripts.

PP_E2E_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=audit-stack-lib.sh
source "${PP_E2E_LIB_DIR}/scripts/audit/audit-stack-lib.sh"

CONTAINER="${AUDIT_INGESTOR_CONTAINER:-ranger-audit-ingestor}"
REPLICA_CONTAINER="${AUDIT_INGESTOR_REPLICA_CONTAINER:-ranger-audit-ingestor-2}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-ranger-kafka}"
PROP_DYNAMIC="ranger.audit.ingestor.kafka.partition.plan.dynamic.enabled"
PLAN_TOPIC="ranger_audit_partition_plan"
AUDIT_TOPIC="ranger_audits"
INGESTOR_PLAN_PATH="/api/audit/partition-plan"
KEYTAB="/etc/keytabs/HTTP.keytab"
HTTP_PRINCIPAL="HTTP/ranger-audit-ingestor.rangernw@EXAMPLE.COM"
INGESTOR_HTTP_PORT="7081"
KAFKA_KEYTAB="/etc/keytabs/kafka.keytab"
KAFKA_PRINCIPAL="kafka/ranger-kafka.rangernw@EXAMPLE.COM"
SITE_XMLS=(
  "/opt/ranger/audit-ingestor/conf/ranger-audit-ingestor-site.xml"
  "/opt/ranger/audit-ingestor/webapp/audit-ingestor/WEB-INF/classes/conf/ranger-audit-ingestor-site.xml"
)

# Example plugin list for E2E promote tests when site XML leaves configured.plugins empty.
PP_CONFIGURED_PLUGINS="hdfs,yarn,knox,hiveServer2,hiveMetastore,kafka,hbaseRegional,hbaseMaster,solr,trino,ozone,kudu,nifi"
PP_BUFFER_PROMOTE_CANDIDATES=(storm ambari atlas impala sqoop kylin presto elasticsearch)
PP_DEFAULT_PROMOTE_PLUGIN="storm"

PP_PASS=0
PP_FAIL=0
HTTP_CODE=""
HTTP_BODY=""

pp_record_pass() {
  PP_PASS=$((PP_PASS + 1))
  echo "  PASS: $*"
}

pp_record_fail() {
  PP_FAIL=$((PP_FAIL + 1))
  echo "  FAIL: $*" >&2
}

pp_require_container() {
  local name="$1"
  if ! docker ps --filter "name=^${name}$" --filter status=running --format '{{.Names}}' | grep -qx "${name}"; then
    echo "ERROR: container ${name} is not running" >&2
    exit 1
  fi
}

pp_json_field() {
  local json="$1"
  local field="$2"
  printf '%s' "${json}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('${field}',''))" 2>/dev/null || echo ""
}

# Build POST /partition-plan/plugins JSON. services_spec: "repo1:user1,user2|repo2:user3"
pp_build_onboard_plugin_json() {
  local plugin_id="$1"
  local partition_count="$2"
  local expected_version="$3"
  local services_spec="$4"
  python3 - "${plugin_id}" "${partition_count}" "${expected_version}" "${services_spec}" <<'PY'
import json, sys
plugin_id, part, ver, spec = sys.argv[1:5]
services = {}
for entry in spec.split("|"):
    entry = entry.strip()
    if not entry:
        continue
    repo, users = entry.split(":", 1)
    services[repo.strip()] = {"allowedUsers": [u.strip() for u in users.split(",") if u.strip()]}
print(json.dumps({
    "pluginId": plugin_id,
    "partitionCount": int(part),
    "expectedVersion": int(ver),
    "services": services,
}))
PY
}

pp_read_site_prop() {
  local container="$1"
  local prop="$2"
  local site path
  for site in "${SITE_XMLS[@]}"; do
    if docker exec "${container}" test -f "${site}" 2>/dev/null; then
      path="${site}"
      break
    fi
  done
  [[ -n "${path:-}" ]] || return 1
  docker exec "${container}" python3 -c \
    "import xml.etree.ElementTree as ET; root=ET.parse('${path}').getroot(); want='${prop}'; \
[print((v.text or '').strip()) for p in root.findall('property') for n in [p.find('name')] if n is not None and (n.text or '').strip()==want for v in [p.find('value')] if v is not None]" \
    2>/dev/null || true
}

pp_expected_topic_partitions() {
  local container="$1"
  local plugins per buffer count
  plugins="$(pp_read_site_prop "${container}" "ranger.audit.ingestor.kafka.configured.plugins")"
  per="$(pp_read_site_prop "${container}" "ranger.audit.ingestor.kafka.topic.partitions.per.configured.plugin")"
  buffer="$(pp_read_site_prop "${container}" "ranger.audit.ingestor.kafka.topic.partitions.buffer")"
  per="${per:-3}"
  buffer="${buffer:-9}"
  if [[ -z "${plugins}" ]]; then
    per="$(pp_read_site_prop "${container}" "ranger.audit.ingestor.kafka.topic.partitions")"
    echo "${per:-10}"
    return 0
  fi
  count="$(printf '%s' "${plugins}" | python3 -c "import sys; print(len([p for p in sys.stdin.read().split(',') if p.strip()]))")"
  echo $(( count * per + buffer ))
}

pp_pick_buffer_promote_plugin() {
  local container="$1"
  local plan_url="$2"
  local configured="${3:-}"
  local candidate

  if [[ -z "${configured}" ]]; then
    configured="$(pp_read_site_prop "${container}" "ranger.audit.ingestor.kafka.configured.plugins")"
  fi
  if [[ -z "${configured}" ]]; then
    configured="${PP_CONFIGURED_PLUGINS}"
  fi

  for candidate in "${PP_BUFFER_PROMOTE_CANDIDATES[@]}"; do
    if [[ ",${configured}," == *",${candidate},"* ]]; then
      continue
    fi
    pp_ingestor_request "${container}" GET "${plan_url}"
    if [[ "${HTTP_CODE}" != "200" ]]; then
      continue
    fi
    if ! printf '%s' "${HTTP_BODY}" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if sys.argv[1] in (d.get('plugins') or {}) else 1)" "${candidate}" 2>/dev/null; then
      echo "${candidate}"
      return 0
    fi
  done
  # Prior test runs may have promoted all named candidates; use a unique buffer plugin id.
  local synthetic="e2eBuffer$(date +%s)"
  echo "${synthetic}"
}

pp_ingestor_host() {
  local container="$1"
  if [[ "${container}" == "${REPLICA_CONTAINER}" ]]; then
    echo "${REPLICA_CONTAINER}.rangernw"
  else
    echo "${CONTAINER}.rangernw"
  fi
}

# SPNEGO requires the HTTP Host to match the keytab principal (FQDN, not localhost).
pp_ingestor_plan_url() {
  local container="$1"
  echo "http://$(pp_ingestor_host "${container}"):${INGESTOR_HTTP_PORT}${INGESTOR_PLAN_PATH}"
}

pp_http_principal_for() {
  local container="$1"
  if [[ "${container}" == "${REPLICA_CONTAINER}" ]]; then
    echo "HTTP/${REPLICA_CONTAINER}.rangernw@EXAMPLE.COM"
  else
    echo "${HTTP_PRINCIPAL}"
  fi
}

pp_ingestor_request() {
  local container="${1:?}"
  local method="${2:?}"
  local url="${3:?}"
  local body="${4:-}"
  local outfile="/tmp/pp-e2e-body-${container}-$$"
  local krb_cc="/tmp/pp-e2e-krb-${container}-$$"
  local body_file=""
  local curl_cmd
  local principal
  principal="$(pp_http_principal_for "${container}")"

  curl_cmd="export KRB5CCNAME=${krb_cc}; kinit -kt ${KEYTAB} ${principal} >/dev/null 2>&1"
  if [[ -n "${body}" ]]; then
    body_file="/tmp/pp-e2e-req-${container}-$$"
    printf '%s' "${body}" | docker exec -i "${container}" tee "${body_file}" >/dev/null
    curl_cmd+="; curl -s -o ${outfile} -w '%{http_code}' --negotiate -u : -X ${method}"
    curl_cmd+=" -H 'Content-Type: application/json' -d @${body_file} '${url}'"
  else
    curl_cmd+="; curl -s -o ${outfile} -w '%{http_code}' --negotiate -u : -X ${method} '${url}'"
  fi

  HTTP_CODE="$(docker exec "${container}" bash -c "${curl_cmd}")"
  HTTP_BODY="$(docker exec "${container}" cat "${outfile}" 2>/dev/null || true)"
  docker exec "${container}" rm -f "${outfile}" "${body_file}" "${krb_cc}" 2>/dev/null || true
}

pp_dynamic_enabled() {
  local container="$1"
  local path="$2"
  docker exec "${container}" grep -A1 "${PROP_DYNAMIC}" "${path}" 2>/dev/null | grep -qi '<value>true</value>'
}

pp_patch_site_prop() {
  local container="$1"
  local site="$2"
  local prop="$3"
  local value="$4"
  docker exec -i -u root "${container}" python3 - "${site}" "${prop}" "${value}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path, prop, value = sys.argv[1:4]
root = ET.parse(path).getroot()
found = False
for p in root.findall("property"):
    name = p.find("name")
    if name is None or (name.text or "").strip() != prop:
        continue
    val = p.find("value")
    if val is None:
        val = ET.SubElement(p, "value")
    val.text = value
    found = True
    break
if not found:
    prop_el = ET.SubElement(root, "property")
    ET.SubElement(prop_el, "name").text = prop
    ET.SubElement(prop_el, "value").text = value
ET.indent(root, space="    ")
tree = ET.ElementTree(root)
tree.write(path, encoding="unicode", xml_declaration=False)
PY
}

pp_ensure_audit_partitioner_for_dynamic() {
  local container="${1:-${CONTAINER}}"
  local seed_plugin="${2:-${PP_DYNAMIC_PRODUCER_PLUGIN:-hdfs}}"
  local plugins
  plugins="$(pp_read_site_prop "${container}" "ranger.audit.ingestor.kafka.configured.plugins")"
  if [[ -n "${plugins}" ]]; then
    return 0
  fi
  for site in "${SITE_XMLS[@]}"; do
    if docker exec "${container}" test -f "${site}" 2>/dev/null; then
      pp_patch_site_prop "${container}" "${site}" "ranger.audit.ingestor.kafka.configured.plugins" "${seed_plugin}"
    fi
  done
}

pp_set_dynamic_enabled() {
  local container="$1"
  local value="$2"
  local restart="${3:-true}"
  local changed=false
  for site in "${SITE_XMLS[@]}"; do
    if ! docker exec "${container}" test -f "${site}" 2>/dev/null; then
      continue
    fi
    pp_patch_site_prop "${container}" "${site}" "${PROP_DYNAMIC}" "${value}"
    changed=true
  done
  if [[ "${value}" == "true" ]]; then
    pp_ensure_audit_partitioner_for_dynamic "${container}"
    pp_prepare_audit_topic_for_greenfield "${container}"
  fi
  if [[ "${changed}" == "true" && "${restart}" == "true" ]]; then
    echo "  Restarting ${container} (dynamic=${value})..."
    PP_RESTART_SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker restart "${container}" >/dev/null
    sleep 45
  fi
}

pp_wait_health() {
  local port="${1:-7081}"
  local label="${2:-Ingestor}"
  local timeout="${3:-120}"
  audit_stack_wait_url "http://localhost:${port}/api/audit/health" "${label}" "${timeout}"
}

pp_wait_watcher() {
  local container="$1"
  local timeout="${2:-300}"
  local deadline=$((SECONDS + timeout))
  local url log_args=()

  url="$(pp_ingestor_plan_url "${container}")"
  if [[ -n "${PP_RESTART_SINCE:-}" ]]; then
    log_args=(--since "${PP_RESTART_SINCE}")
  fi

  while (( SECONDS < deadline )); do
    pp_ingestor_request "${container}" GET "${url}"
    if [[ "${HTTP_CODE}" == "200" ]]; then
      return 0
    fi
    if docker logs ${log_args+"${log_args[@]}"} "${container}" 2>&1 | grep -q "Partition plan watcher ready"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

pp_kafka_run() {
  local script="$1"
  docker exec "${KAFKA_CONTAINER}" bash -c \
    "cat > /tmp/pp-kafka-client.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
EOF
cat > /tmp/pp-kafka-client-jaas.conf <<EOF
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  keyTab=\"${KAFKA_KEYTAB}\"
  principal=\"${KAFKA_PRINCIPAL}\";
};
EOF
export KAFKA_OPTS=\"-Djava.security.auth.login.config=/tmp/pp-kafka-client-jaas.conf\"
${script} --command-config /tmp/pp-kafka-client.properties" \
    2>/dev/null || true
}

# kafka-console-consumer uses --consumer.config (not --command-config).
pp_kafka_consumer_run() {
  local script="$1"
  docker exec "${KAFKA_CONTAINER}" bash -c \
    "cat > /tmp/pp-kafka-client.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
EOF
cat > /tmp/pp-kafka-client-jaas.conf <<EOF
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  keyTab=\"${KAFKA_KEYTAB}\"
  principal=\"${KAFKA_PRINCIPAL}\";
};
EOF
export KAFKA_OPTS=\"-Djava.security.auth.login.config=/tmp/pp-kafka-client-jaas.conf\"
${script} --consumer.config /tmp/pp-kafka-client.properties" \
    2>/dev/null || true
}


pp_stop_audit_stack_consumers() {
  docker stop ranger-audit-ingestor ranger-audit-dispatcher-solr ranger-audit-dispatcher-hdfs 2>/dev/null || true
}

pp_start_audit_stack_consumers() {
  docker start ranger-audit-ingestor ranger-audit-dispatcher-solr ranger-audit-dispatcher-hdfs 2>/dev/null || true
}

pp_kafka_wait_topic_absent() {
  local topic="$1"
  local timeout="${2:-180}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if ! pp_kafka_run "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ranger-kafka.rangernw:9092 --list" | grep -qx "${topic}"; then
      return 0
    fi
    sleep 5
  done
  echo "ERROR: Kafka topic '${topic}' still present after ${timeout}s" >&2
  return 1
}

pp_kafka_topic_partition_count() {
  local topic="$1"
  local out
  out="$(pp_kafka_run "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ranger-kafka.rangernw:9092 --describe --topic '${topic}'")"
  printf '%s' "${out}" | python3 -c "import sys,re; t=sys.stdin.read(); m=re.search(r'PartitionCount:\\s*(\\d+)', t); print(m.group(1) if m else '')" 2>/dev/null || echo ""
}

pp_delete_plan_topic() {
  pp_stop_audit_stack_consumers
  pp_kafka_run "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ranger-kafka.rangernw:9092 --delete --topic '${PLAN_TOPIC}'"
  pp_kafka_wait_topic_absent "${PLAN_TOPIC}" 180 || true
  pp_start_audit_stack_consumers
  sleep 10
}

# Greenfield bootstrap requires plan.topicPartitionCount == Kafka ranger_audits partitions.
pp_prepare_audit_topic_for_greenfield() {
  local container="${1:-${CONTAINER}}"
  local expected actual

  expected="$(pp_expected_topic_partitions "${container}")"
  actual="$(pp_kafka_topic_partition_count "${AUDIT_TOPIC}")"
  if [[ -z "${actual}" || "${actual}" == "${expected}" ]]; then
    return 0
  fi
  echo "  Recreating ${AUDIT_TOPIC} (${actual} partitions -> ${expected} for site XML layout)..."
  pp_stop_audit_stack_consumers
  pp_kafka_run "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ranger-kafka.rangernw:9092 --delete --topic '${AUDIT_TOPIC}'"
  pp_kafka_run "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ranger-kafka.rangernw:9092 --delete --topic '${PLAN_TOPIC}'" || true
  pp_kafka_wait_topic_absent "${AUDIT_TOPIC}" 180
  pp_kafka_wait_topic_absent "${PLAN_TOPIC}" 180 || true
  pp_kafka_run "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ranger-kafka.rangernw:9092 --create --topic '${AUDIT_TOPIC}' --partitions ${expected} --replication-factor 1"
  sleep 3
  pp_start_audit_stack_consumers
  sleep 10
}

pp_seed_plan_json() {
  local json="$1"
  local tmp
  tmp="$(mktemp)"
  printf '%s' "${json}" > "${tmp}"
  pp_seed_plan_file "${tmp}"
  rm -f "${tmp}"
}

pp_seed_plan_file() {
  local file="$1"
  pp_ensure_plan_topic
  { printf '%s:' "${AUDIT_TOPIC}"; cat "${file}"; } | docker exec -i "${KAFKA_CONTAINER}" bash -c \
    "cat > /tmp/pp-kafka-client.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
EOF
cat > /tmp/pp-kafka-client-jaas.conf <<EOF
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  keyTab=\"${KAFKA_KEYTAB}\"
  principal=\"${KAFKA_PRINCIPAL}\";
};
EOF
export KAFKA_OPTS=\"-Djava.security.auth.login.config=/tmp/pp-kafka-client-jaas.conf\"
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ranger-kafka.rangernw:9092 --producer.config /tmp/pp-kafka-client.properties --topic '${PLAN_TOPIC}' --property parse.key=true --property key.separator=:" \
    >/dev/null 2>&1 || true
  sleep 2
}

pp_ensure_plan_topic() {
  if ! pp_kafka_run "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ranger-kafka.rangernw:9092 --list" | grep -qx "${PLAN_TOPIC}"; then
    pp_kafka_run "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ranger-kafka.rangernw:9092 --create --topic '${PLAN_TOPIC}' --partitions 1 --replication-factor 1 --config cleanup.policy=compact"
  fi
}

pp_ensure_replica_keytab() {
  local rep_fqdn="${REPLICA_CONTAINER}.rangernw"
  local host_kt="${PP_E2E_LIB_DIR}/dist/keytabs/${REPLICA_CONTAINER}"
  docker exec ranger-kdc bash -c "
    set -euo pipefail
    keytab_dir=/etc/keytabs/${REPLICA_CONTAINER}
    mkdir -p \"\${keytab_dir}\"
    kadmin.local -q 'addprinc -randkey HTTP/${rep_fqdn}' >/dev/null 2>&1 || true
    kadmin.local -q 'addprinc -randkey rangerauditserver/${rep_fqdn}' >/dev/null 2>&1 || true
    rm -f \"\${keytab_dir}/HTTP.keytab\" \"\${keytab_dir}/rangerauditserver.keytab\"
    kadmin.local -q 'ktadd -k '\${keytab_dir}'/HTTP.keytab HTTP/${rep_fqdn}'
    kadmin.local -q 'ktadd -k '\${keytab_dir}'/rangerauditserver.keytab rangerauditserver/${rep_fqdn}'
    chmod 444 \"\${keytab_dir}/\"*.keytab
  "
  mkdir -p "${host_kt}"
  docker cp "ranger-kdc:/etc/keytabs/${REPLICA_CONTAINER}/." "${host_kt}/" >/dev/null
}

pp_patch_replica_site_file() {
  local file="$1"
  local rep_fqdn="${REPLICA_CONTAINER}.rangernw"
  local primary_fqdn="${CONTAINER}.rangernw"
  python3 - "${file}" "${primary_fqdn}" "${rep_fqdn}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path, primary_fqdn, rep_fqdn = sys.argv[1:4]
props = {
    "ranger.audit.ingestor.host": rep_fqdn,
    "ranger.audit.ingestor.bind.address": rep_fqdn,
}
root = ET.parse(path).getroot()
for prop in root.findall("property"):
    name = prop.find("name")
    if name is None:
        continue
    key = (name.text or "").strip()
    if key in props:
        value = prop.find("value")
        if value is None:
            value = ET.SubElement(prop, "value")
        value.text = props[key]
ET.indent(root, space="    ")
tree = ET.ElementTree(root)
tree.write(path, encoding="unicode", xml_declaration=False)
PY
}

pp_replica_config_staging() {
  echo "${PP_E2E_LIB_DIR}/dist/${REPLICA_CONTAINER}-conf"
}

pp_sync_replica_config_from_primary() {
  local preserve_mount="${1:-false}"
  local staging
  staging="$(pp_replica_config_staging)"
  if [[ "${preserve_mount}" == "true" ]]; then
    mkdir -p "${staging}/conf" "${staging}/webapp-conf"
  else
    rm -rf "${staging}"
    mkdir -p "${staging}/conf" "${staging}/webapp-conf"
  fi
  docker cp "${CONTAINER}:/opt/ranger/audit-ingestor/conf/." "${staging}/conf/" >/dev/null
  if docker exec "${CONTAINER}" test -f "${SITE_XMLS[1]}" 2>/dev/null; then
    docker cp "${CONTAINER}:${SITE_XMLS[1]}" "${staging}/webapp-conf/ranger-audit-ingestor-site.xml" >/dev/null
  else
    cp "${staging}/conf/ranger-audit-ingestor-site.xml" "${staging}/webapp-conf/ranger-audit-ingestor-site.xml"
  fi
  pp_patch_replica_site_file "${staging}/conf/ranger-audit-ingestor-site.xml"
  pp_patch_replica_site_file "${staging}/webapp-conf/ranger-audit-ingestor-site.xml"
}

pp_start_ingestor_replica() {
  local image="${1:-ranger-audit-ingestor:latest}"
  local staging
  if docker ps -a --format '{{.Names}}' | grep -qx "${REPLICA_CONTAINER}"; then
    docker rm -f "${REPLICA_CONTAINER}" >/dev/null 2>&1 || true
  fi
  pp_ensure_replica_keytab
  pp_sync_replica_config_from_primary
  staging="$(pp_replica_config_staging)"
  if ! docker run -d --name "${REPLICA_CONTAINER}" \
    --network rangernw \
    --hostname "${REPLICA_CONTAINER}.rangernw" \
    -p 7082:7081 \
    -e JAVA_HOME=/opt/java/openjdk \
    -e AUDIT_INGESTOR_HOME_DIR=/opt/ranger/audit-ingestor \
    -e AUDIT_INGESTOR_CONF_DIR=/opt/ranger/audit-ingestor/conf \
    -e AUDIT_INGESTOR_LOG_DIR=/var/log/ranger/audit-ingestor \
    -e AUDIT_INGESTOR_HEAP="-Xms512m -Xmx2g" \
    -e KERBEROS_ENABLED=true \
    -e KAFKA_BOOTSTRAP_SERVERS=ranger-kafka.rangernw:9092 \
    -v "${PP_E2E_LIB_DIR}/dist/keytabs/${REPLICA_CONTAINER}:/etc/keytabs" \
    -v "${staging}/conf:/opt/ranger/audit-ingestor/conf:ro" \
    "${image}" >/dev/null; then
    echo "ERROR: docker run failed for ${REPLICA_CONTAINER}" >&2
    return 1
  fi
  PP_RESTART_SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sleep 60
  if ! pp_wait_health 7082 "Ingestor replica" 300; then
    return 1
  fi
  pp_install_replica_webapp_site
}

pp_install_replica_webapp_site() {
  local staging
  staging="$(pp_replica_config_staging)"
  if ! docker exec "${REPLICA_CONTAINER}" test -d \
      /opt/ranger/audit-ingestor/webapp/audit-ingestor/WEB-INF/classes/conf 2>/dev/null; then
    return 0
  fi
  docker cp "${staging}/webapp-conf/ranger-audit-ingestor-site.xml" \
    "${REPLICA_CONTAINER}:/opt/ranger/audit-ingestor/webapp/audit-ingestor/WEB-INF/classes/conf/ranger-audit-ingestor-site.xml" >/dev/null
  PP_RESTART_SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  docker restart "${REPLICA_CONTAINER}" >/dev/null
  sleep 60
  pp_wait_health 7082 "Ingestor replica after webapp site sync" 300
}

pp_stop_ingestor_replica() {
  docker rm -f "${REPLICA_CONTAINER}" >/dev/null 2>&1 || true
}

pp_copy_site_to_replica() {
  local staging restart="${2:-true}"
  pp_sync_replica_config_from_primary "true"
  staging="$(pp_replica_config_staging)"
  docker cp "${staging}/conf/ranger-audit-ingestor-site.xml" \
    "${REPLICA_CONTAINER}:/opt/ranger/audit-ingestor/conf/ranger-audit-ingestor-site.xml"
  if docker exec "${REPLICA_CONTAINER}" test -d \
      /opt/ranger/audit-ingestor/webapp/audit-ingestor/WEB-INF/classes/conf 2>/dev/null; then
    docker cp "${staging}/webapp-conf/ranger-audit-ingestor-site.xml" \
      "${REPLICA_CONTAINER}:/opt/ranger/audit-ingestor/webapp/audit-ingestor/WEB-INF/classes/conf/ranger-audit-ingestor-site.xml"
  fi
  if [[ "${restart}" == "true" ]]; then
    PP_RESTART_SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker restart "${REPLICA_CONTAINER}" >/dev/null
    sleep 45
    pp_wait_health 7082 "Ingestor replica after config copy" 300
  fi
}

pp_print_results() {
  echo ""
  echo "Results: ${PP_PASS} passed, ${PP_FAIL} failed"
  if [[ "${PP_FAIL}" -ne 0 ]]; then
    echo "See: audit-server/README-KAFKA-PARTITION-PLAN-E2E-TEST-PLAN.md" >&2
    return 1
  fi
  return 0
}

pp_preflight_tier3() {
  chmod +x "${PP_E2E_LIB_DIR}/scripts/audit/wait-for-audit-health.sh" 2>/dev/null || true
  "${PP_E2E_LIB_DIR}/scripts/audit/wait-for-audit-health.sh" --tier 3 --timeout "${1:-120}"
  pp_require_container "${CONTAINER}"
  pp_require_container "${KAFKA_CONTAINER}"
}
