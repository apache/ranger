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

AUDIT_SERVER_HOME_DIR="${AUDIT_SERVER_HOME_DIR:-/opt/ranger-audit}"
AUDIT_SERVER_CONF_DIR="${AUDIT_SERVER_CONF_DIR:-/opt/ranger-audit/conf}"
AUDIT_SERVER_LOG_DIR="${AUDIT_SERVER_LOG_DIR:-/var/log/ranger/audit-server}"

# Create log directory if it doesn't exist
mkdir -p ${AUDIT_SERVER_LOG_DIR}
chown -R ranger:ranger /var/log/ranger 2>/dev/null || true

echo "=========================================="
echo "Starting Ranger Audit Server"
echo "=========================================="
echo "AUDIT_SERVER_HOME_DIR: ${AUDIT_SERVER_HOME_DIR}"
echo "AUDIT_SERVER_CONF_DIR: ${AUDIT_SERVER_CONF_DIR}"
echo "AUDIT_SERVER_LOG_DIR: ${AUDIT_SERVER_LOG_DIR}"
echo "=========================================="

# Run setup script if it exists
if [ -f /home/ranger/scripts/ranger-audit-setup.sh ]; then
  echo "[INFO] Running ranger-audit-setup.sh..."
  bash /home/ranger/scripts/ranger-audit-setup.sh
fi

# Start the audit server
echo "[INFO] Starting Ranger Audit Server..."
cd ${AUDIT_SERVER_HOME_DIR}

# Export environment variables for Java and audit server
export JAVA_HOME=${JAVA_HOME:-/opt/java/openjdk}
export PATH=$JAVA_HOME/bin:$PATH
export AUDIT_SERVER_LOG_DIR=${AUDIT_SERVER_LOG_DIR}
export AUDIT_SERVER_PID_DIR=${AUDIT_SERVER_LOG_DIR}

# Set heap size
AUDIT_SERVER_HEAP="${AUDIT_SERVER_HEAP:--Xms512m -Xmx2g}"
export AUDIT_SERVER_HEAP

# Set JVM options including log4j2 configuration
if [ -z "$AUDIT_SERVER_OPTS" ]; then
  AUDIT_SERVER_OPTS="-Dlog4j2.configurationFile=${AUDIT_SERVER_CONF_DIR}/audit-log4j2.properties"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Djava.net.preferIPv4Stack=true -server"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -XX:InitiatingHeapOccupancyPercent=35"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -XX:ConcGCThreads=4 -XX:ParallelGCThreads=8"
fi

# Point to krb5.conf for Kerberos
if [ "${KERBEROS_ENABLED}" == "true" ]; then
  export AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Djava.security.krb5.conf=/etc/krb5.conf"
fi

export AUDIT_SERVER_OPTS

echo "[INFO] JAVA_HOME: ${JAVA_HOME}"
echo "[INFO] AUDIT_SERVER_HEAP: ${AUDIT_SERVER_HEAP}"
echo "[INFO] AUDIT_SERVER_OPTS: ${AUDIT_SERVER_OPTS}"

# Start the audit server
${AUDIT_SERVER_HOME_DIR}/bin/ranger-audit-server start

# Keep the container running by tailing logs
tail -f ${AUDIT_SERVER_LOG_DIR}/catalina.out ${AUDIT_SERVER_LOG_DIR}/catalina.err 2>/dev/null

