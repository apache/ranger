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

AUDIT_SERVER_HOME_DIR="${AUDIT_SERVER_HOME_DIR:-/opt/ranger-audit-server}"
AUDIT_SERVER_CONF_DIR="${AUDIT_SERVER_CONF_DIR:-/opt/ranger-audit-server/conf}"
AUDIT_SERVER_LOG_DIR="${AUDIT_SERVER_LOG_DIR:-/var/log/ranger/ranger-audit-server}"

# Create log directory if it doesn't exist
mkdir -p ${AUDIT_SERVER_LOG_DIR}
chown -R ranger:ranger /var/log/ranger 2>/dev/null || true

echo "=========================================="
echo "Starting Ranger Audit Server Service..."
echo "=========================================="
echo "AUDIT_SERVER_HOME_DIR: ${AUDIT_SERVER_HOME_DIR}"
echo "AUDIT_SERVER_CONF_DIR: ${AUDIT_SERVER_CONF_DIR}"
echo "AUDIT_SERVER_LOG_DIR: ${AUDIT_SERVER_LOG_DIR}"
echo "=========================================="

# Source service check functions
source /home/ranger/scripts/service-check-functions.sh

# Quick check for Kafka availability
# The audit server has a built-in recovery/spool mechanism for when Kafka is unavailable
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-ranger-kafka.rangernw:9092}"
if check_tcp_port "Kafka" "${KAFKA_BOOTSTRAP_SERVERS}" 30; then
  echo "[INFO] Kafka is available at startup"
else
  echo "[INFO] Kafka not immediately available - audit server will use recovery/spool mechanism"
fi

# Start the audit server
echo "[INFO] Starting Ranger Audit Server Service..."
cd ${AUDIT_SERVER_HOME_DIR}

# Export environment variables for Java and audit server
export JAVA_HOME=${JAVA_HOME:-/opt/java/openjdk}
export PATH=$JAVA_HOME/bin:$PATH
export AUDIT_SERVER_LOG_DIR=${AUDIT_SERVER_LOG_DIR}

# Set heap size
AUDIT_SERVER_HEAP="${AUDIT_SERVER_HEAP:--Xms512m -Xmx2g}"
export AUDIT_SERVER_HEAP

# Set JVM options including logback configuration
if [ -z "$AUDIT_SERVER_OPTS" ]; then
  AUDIT_SERVER_OPTS="-Dlogback.configurationFile=${AUDIT_SERVER_CONF_DIR}/logback.xml"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Daudit.server.log.dir=${AUDIT_SERVER_LOG_DIR}"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Daudit.server.log.file=ranger-audit-server.log"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Djava.net.preferIPv4Stack=true -server"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -XX:InitiatingHeapOccupancyPercent=35"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -XX:ConcGCThreads=4 -XX:ParallelGCThreads=8"
fi

# Point to krb5.conf for Kerberos
if [ "${KERBEROS_ENABLED}" == "true" ]; then
  export AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Djava.security.krb5.conf=/etc/krb5.conf"

  # Wait for keytabs if Kerberos is enabled
  if [ -f /home/ranger/scripts/wait_for_keytab.sh ]; then
    echo "[INFO] Waiting for Kerberos keytabs..."
    bash /home/ranger/scripts/wait_for_keytab.sh HTTP.keytab
    bash /home/ranger/scripts/wait_for_keytab.sh rangerauditserver.keytab
    echo "[INFO] ✓ Keytabs are available"
  fi
fi

export AUDIT_SERVER_OPTS

echo "[INFO] JAVA_HOME: ${JAVA_HOME}"
echo "[INFO] AUDIT_SERVER_HEAP: ${AUDIT_SERVER_HEAP}"
echo "[INFO] AUDIT_SERVER_OPTS: ${AUDIT_SERVER_OPTS}"

# Build classpath from WAR file
WEBAPP_ROOT="${AUDIT_SERVER_HOME_DIR}/webapp"
WAR_FILE="${WEBAPP_ROOT}/ranger-audit-server-service.war"
WEBAPP_DIR="${WEBAPP_ROOT}/ranger-audit-server-service"

# Extract WAR if not already extracted
if [ -f "${WAR_FILE}" ] && [ ! -d "${WEBAPP_DIR}" ]; then
  echo "[INFO] Extracting WAR file..."
  mkdir -p "${WEBAPP_DIR}"
  cd "${WEBAPP_DIR}"
  jar xf "${WAR_FILE}"
  cd -
fi

# Build classpath
RANGER_CLASSPATH="${WEBAPP_DIR}/WEB-INF/classes"
for jar in "${WEBAPP_DIR}"/WEB-INF/lib/*.jar; do
  RANGER_CLASSPATH="${RANGER_CLASSPATH}:${jar}"
done

# Add libext directory if it exists
if [ -d "${AUDIT_SERVER_HOME_DIR}/libext" ]; then
  for jar in "${AUDIT_SERVER_HOME_DIR}"/libext/*.jar; do
    if [ -f "${jar}" ]; then
      RANGER_CLASSPATH="${RANGER_CLASSPATH}:${jar}"
    fi
  done
fi

export RANGER_CLASSPATH

echo "[INFO] Starting Ranger Audit Server Service..."
echo "[INFO] Webapp dir: ${WEBAPP_DIR}"
java ${AUDIT_SERVER_HEAP} ${AUDIT_SERVER_OPTS} \
  -Daudit.config=${AUDIT_SERVER_CONF_DIR}/ranger-audit-server-site.xml \
  -cp "${RANGER_CLASSPATH}" \
  org.apache.ranger.audit.server.AuditServerApplication \
  >> ${AUDIT_SERVER_LOG_DIR}/catalina.out 2>&1 &

PID=$!
echo $PID > ${AUDIT_SERVER_LOG_DIR}/ranger-audit-server.pid

echo "[INFO] Ranger Audit Server started with PID: $PID"

# Keep the container running by tailing logs
tail -f ${AUDIT_SERVER_LOG_DIR}/catalina.out 2>/dev/null
