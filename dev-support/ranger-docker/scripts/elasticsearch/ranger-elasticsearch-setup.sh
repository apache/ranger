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

cp ${RANGER_SCRIPTS}/elasticsearch.yml ${ELASTICSEARCH_HOME}/config/elasticsearch.yml

if [ ! -f "${ELASTICSEARCH_HOME}/config/elasticsearch.keystore" ]; then
  su -s /bin/bash elasticsearch -c "${ELASTICSEARCH_HOME}/bin/elasticsearch-keystore create"
  echo "${ELASTICSEARCH_BOOTSTRAP_PASSWORD:-rangerR0cks!}" | \
    su -s /bin/bash elasticsearch -c "${ELASTICSEARCH_HOME}/bin/elasticsearch-keystore add -x bootstrap.password"
fi

chown -R elasticsearch:hadoop ${ELASTICSEARCH_HOME}

# File-realm users for authorization smoke tests (offline; Ranger blocks security REST APIs).
ES_BOOTSTRAP_PASSWORD="${ELASTICSEARCH_BOOTSTRAP_PASSWORD:-rangerR0cks!}"
cat > "${ELASTICSEARCH_HOME}/config/roles.yml" <<'EOF'
ranger_test_index_reader:
  indices:
  - names: ['test-index']
    privileges: ['read', 'view_index_metadata']
EOF
chown elasticsearch:hadoop "${ELASTICSEARCH_HOME}/config/roles.yml"
chmod 640 "${ELASTICSEARCH_HOME}/config/roles.yml"
for smoke_user in testuser_2 testuser_denied; do
  su -s /bin/bash elasticsearch -c "${ELASTICSEARCH_HOME}/bin/elasticsearch-users useradd ${smoke_user} -p \"${ES_BOOTSTRAP_PASSWORD}\" -r ranger_test_index_reader" \
    2>/dev/null || true
done

# ES plugin security manager allows read but not write under /etc/ranger; keep cache under ES data.
export POLICY_CACHE_FILE_PATH="${ELASTICSEARCH_HOME}/data/ranger-policycache"
mkdir -p "${POLICY_CACHE_FILE_PATH}"
chown elasticsearch:hadoop "${POLICY_CACHE_FILE_PATH}"
chmod 750 "${POLICY_CACHE_FILE_PATH}"

AUDIT_SPOOL_DIR="${ELASTICSEARCH_HOME}/data/ranger-audit-spool"
mkdir -p "${AUDIT_SPOOL_DIR}"
chown elasticsearch:hadoop "${AUDIT_SPOOL_DIR}"
chmod 750 "${AUDIT_SPOOL_DIR}"

cd ${RANGER_HOME}/ranger-elasticsearch-plugin
./enable-elasticsearch-plugin.sh

# enable-agent.sh resets POLICY_CACHE_FILE_PATH to /etc/ranger/...; patch Ranger config for docker.
SECURITY_XML="${ELASTICSEARCH_HOME}/config/ranger-elasticsearch-plugin/ranger-elasticsearch-security.xml"
if [ -f "${SECURITY_XML}" ]; then
  python3 "${RANGER_SCRIPTS}/patch-ranger-security-xml.py" "${SECURITY_XML}" \
    --cache-dir "${POLICY_CACHE_FILE_PATH}" \
    --poll-interval-ms "86400000" \
    --admin-user "${RANGER_ADMIN_USER:-admin}" \
    --admin-password "${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
fi

AUDIT_XML="${ELASTICSEARCH_HOME}/config/ranger-elasticsearch-plugin/ranger-elasticsearch-audit.xml"
if [ -f "${AUDIT_XML}" ]; then
  python3 "${RANGER_SCRIPTS}/patch-ranger-audit-xml.py" "${AUDIT_XML}" \
    --auditserver-url "http://ranger-audit-ingestor.rangernw:7081" \
    --spool-dir "${AUDIT_SPOOL_DIR}"
fi

# Replace plugin symlinks with local copies; ES security manager cannot read jars via /opt/ranger symlinks.
RANGER_PLUGIN_LIB="${RANGER_HOME}/ranger-elasticsearch-plugin/lib/ranger-elasticsearch-plugin"
PLUGIN_DIR="${ELASTICSEARCH_HOME}/plugins/ranger-elasticsearch-plugin"
rm -rf "${PLUGIN_DIR}"
mkdir -p "${PLUGIN_DIR}"
cp -a "${RANGER_PLUGIN_LIB}/." "${PLUGIN_DIR}/"
# ES 7.17+ does not allow plugins to create classloaders; load impl jars on the plugin classpath.
if [ -d "${RANGER_PLUGIN_LIB}/ranger-elasticsearch-plugin-impl" ]; then
  cp "${RANGER_PLUGIN_LIB}/ranger-elasticsearch-plugin-impl/"*.jar "${PLUGIN_DIR}/"
fi
# Shim runtime deps (exclude jars already provided by x-pack-security / x-pack-core / ES lib)
for jar in commons-lang3 commons-collections hadoop-client-api hadoop-client-runtime commons-configuration gson jackson-databind jackson-annotations; do
  cp "${RANGER_HOME}/ranger-elasticsearch-plugin/install/lib/${jar}-"*.jar "${PLUGIN_DIR}/" 2>/dev/null || true
done
if [ -d "${RANGER_SCRIPTS}/elasticsearch-lib" ]; then
  cp "${RANGER_SCRIPTS}/elasticsearch-lib/"*.jar "${PLUGIN_DIR}/" 2>/dev/null || true
fi
cp "${RANGER_HOME}/ranger-elasticsearch-plugin/lib/ranger-elasticsearch-plugin/plugin-security.policy" "${PLUGIN_DIR}/" 2>/dev/null || true
chown -R elasticsearch:hadoop "${PLUGIN_DIR}" "${ELASTICSEARCH_HOME}/config/ranger-elasticsearch-plugin"

echo "Elasticsearch Ranger plugin setup completed successfully"
