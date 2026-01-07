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

KERBEROS_ENABLED="${KERBEROS_ENABLED:-false}"
AUDIT_SERVER_HOME_DIR="${AUDIT_SERVER_HOME_DIR:-/opt/ranger-audit}"
AUDIT_SERVER_CONF_DIR="${AUDIT_SERVER_CONF_DIR:-/opt/ranger-audit/conf}"
KEYTABS_DIR=/etc/keytabs

echo "=========================================="
echo "Ranger Audit Server Setup"
echo "=========================================="
echo "KERBEROS_ENABLED: ${KERBEROS_ENABLED}"
echo "AUDIT_SERVER_HOME_DIR: ${AUDIT_SERVER_HOME_DIR}"
echo "AUDIT_SERVER_CONF_DIR: ${AUDIT_SERVER_CONF_DIR}"
echo "=========================================="

# Wait for keytabs if Kerberos is enabled
if [ "${KERBEROS_ENABLED}" == "true" ]; then
  echo "Kerberos is enabled. Waiting for keytabs..."

  # Wait for HTTP keytab (for incoming SPNEGO authentication)
  if [ -f "${KEYTABS_DIR}/HTTP.keytab" ]; then
    echo "[INFO] Found HTTP keytab: ${KEYTABS_DIR}/HTTP.keytab"
  else
    echo "[WARN] HTTP keytab not found: ${KEYTABS_DIR}/HTTP.keytab"
    echo "[INFO] Waiting for keytab to be created..."
    ${RANGER_SCRIPTS}/wait_for_keytab.sh HTTP.keytab
  fi

  # Wait for rangerauditserver keytab (for outgoing authentication to Kafka/HDFS/Solr)
  if [ -f "${KEYTABS_DIR}/rangerauditserver.keytab" ]; then
    echo "[INFO] Found rangerauditserver keytab: ${KEYTABS_DIR}/rangerauditserver.keytab"
  else
    echo "[WARN] rangerauditserver keytab not found: ${KEYTABS_DIR}/rangerauditserver.keytab"
    echo "[INFO] Waiting for keytab to be created..."
    ${RANGER_SCRIPTS}/wait_for_keytab.sh rangerauditserver.keytab
  fi

  echo "[INFO] All required keytabs are present"

  # The Ranger Audit framework handles Kerberos authentication internally
  # based on the properties in audit-server-site.xml
  echo "[INFO] Kerberos keytabs verified. Audit framework will handle authentication."
else
  echo "[INFO] Kerberos is disabled. Skipping keytab setup."
fi

# Update audit-server-site.xml with runtime values
echo "[INFO] Configuring audit server..."

# Replace _HOST with actual hostname in configuration
HOSTNAME=$(hostname -f)
echo "[INFO] Hostname: ${HOSTNAME}"

# Consumer thread configuration is set in audit-server-site.xml
# Edit the file before building to adjust worker counts:
#   - xasecure.audit.destination.solr.consumer.thread.count
#   - xasecure.audit.destination.hdfs.consumer.thread.count

echo "[INFO] Audit server setup completed successfully"

