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

if [ ! -e ${ELASTICSEARCH_HOME}/.setupDone ]
then
  if "${RANGER_SCRIPTS}"/ranger-elasticsearch-setup.sh;
  then
    touch "${ELASTICSEARCH_HOME}"/.setupDone
  else
    echo "Ranger Elasticsearch Setup Script didn't complete proper execution." >&2
    exit 1
  fi
fi

AUDIT_SPOOL_DIR="${ELASTICSEARCH_HOME}/data/ranger-audit-spool"
mkdir -p "${AUDIT_SPOOL_DIR}"
chown elasticsearch:hadoop "${AUDIT_SPOOL_DIR}" 2>/dev/null || true
chmod 750 "${AUDIT_SPOOL_DIR}" 2>/dev/null || true

AUDIT_XML="${ELASTICSEARCH_HOME}/config/ranger-elasticsearch-plugin/ranger-elasticsearch-audit.xml"
if [ -f "${AUDIT_XML}" ] && [ -f "${RANGER_SCRIPTS}/patch-ranger-audit-xml.py" ]; then
  KERBEROS_PATCH_ARG=""
  if [ "${KERBEROS_ENABLED}" = "true" ]; then
    KERBEROS_PATCH_ARG="--kerberos-enabled"
  fi
  python3 "${RANGER_SCRIPTS}/patch-ranger-audit-xml.py" "${AUDIT_XML}" \
    --auditserver-url "http://ranger-audit-ingestor.rangernw:7081" \
    --spool-dir "${AUDIT_SPOOL_DIR}" \
    ${KERBEROS_PATCH_ARG}
  chown elasticsearch:hadoop "${AUDIT_XML}" 2>/dev/null || true
fi

if [ ! -e ${ELASTICSEARCH_HOME}/.postSetupDone ]
then
  if ! "${RANGER_SCRIPTS}"/ranger-elasticsearch-post-setup.sh seed;
  then
    echo "Ranger Elasticsearch Post-Setup Script didn't complete proper execution." >&2
    exit 1
  fi
fi

su -s /bin/bash elasticsearch -c "cd ${ELASTICSEARCH_HOME} && ES_JAVA_OPTS='${ES_JAVA_OPTS}' ./bin/elasticsearch" &
ES_PID=$!

if [ ! -e ${ELASTICSEARCH_HOME}/.postSetupDone ]
then
  if "${RANGER_SCRIPTS}"/ranger-elasticsearch-post-setup.sh fixtures;
  then
    touch "${ELASTICSEARCH_HOME}"/.postSetupDone
  else
    echo "Ranger Elasticsearch Post-Setup Script didn't complete proper execution." >&2
    kill "${ES_PID}" 2>/dev/null || true
    exit 1
  fi
fi

if ! ps -p "${ES_PID}" > /dev/null 2>&1
then
  echo "The Elasticsearch process exited unexpectedly." >&2
  exit 1
fi

tail --pid="${ES_PID}" -f /dev/null
