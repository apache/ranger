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

AUDIT_CONSUMER_HOME_DIR="${AUDIT_CONSUMER_HOME_DIR:-/opt/ranger-audit-consumer-solr}"
AUDIT_CONSUMER_CONF_DIR="${AUDIT_CONSUMER_CONF_DIR:-/opt/ranger-audit-consumer-solr/conf}"
AUDIT_CONSUMER_LOG_DIR="${AUDIT_CONSUMER_LOG_DIR:-/var/log/ranger/ranger-audit-consumer-solr}"

# Create log directory if it doesn't exist
mkdir -p ${AUDIT_CONSUMER_LOG_DIR}
chown -R ranger:ranger /var/log/ranger 2>/dev/null || true

echo "=========================================="
echo "Starting Ranger Audit Consumer - Solr"
echo "=========================================="
echo "AUDIT_CONSUMER_HOME_DIR: ${AUDIT_CONSUMER_HOME_DIR}"
echo "AUDIT_CONSUMER_CONF_DIR: ${AUDIT_CONSUMER_CONF_DIR}"
echo "AUDIT_CONSUMER_LOG_DIR: ${AUDIT_CONSUMER_LOG_DIR}"
echo "=========================================="

# Source service check functions
source /home/ranger/scripts/service-check-functions.sh

# Wait for Kafka to be available
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-ranger-kafka.rangernw:9092}"
check_tcp_port "Kafka" "${KAFKA_BOOTSTRAP_SERVERS}" 60

# Wait for Solr to be available
SOLR_URL="${SOLR_URL:-http://ranger-solr.rangernw:8983/solr}"
check_http_service "Solr" "${SOLR_URL}/" 60

# Start the Solr consumer
echo "[INFO] Starting Ranger Audit Consumer - Solr..."
cd ${AUDIT_CONSUMER_HOME_DIR}

# Export environment variables for Java
export JAVA_HOME=${JAVA_HOME:-/opt/java/openjdk}
export PATH=$JAVA_HOME/bin:$PATH
export AUDIT_CONSUMER_LOG_DIR=${AUDIT_CONSUMER_LOG_DIR}

# Set heap size
AUDIT_CONSUMER_HEAP="${AUDIT_CONSUMER_HEAP:--Xms512m -Xmx2g}"
export AUDIT_CONSUMER_HEAP

# Set JVM options including logback configuration
if [ -z "$AUDIT_CONSUMER_OPTS" ]; then
  AUDIT_CONSUMER_OPTS="-Dlogback.configurationFile=${AUDIT_CONSUMER_CONF_DIR}/logback.xml"
  AUDIT_CONSUMER_OPTS="${AUDIT_CONSUMER_OPTS} -Daudit.consumer.solr.log.dir=${AUDIT_CONSUMER_LOG_DIR}"
  AUDIT_CONSUMER_OPTS="${AUDIT_CONSUMER_OPTS} -Daudit.consumer.solr.log.file=ranger-audit-consumer-solr.log"
  AUDIT_CONSUMER_OPTS="${AUDIT_CONSUMER_OPTS} -Djava.net.preferIPv4Stack=true -server"
  AUDIT_CONSUMER_OPTS="${AUDIT_CONSUMER_OPTS} -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
  AUDIT_CONSUMER_OPTS="${AUDIT_CONSUMER_OPTS} -XX:InitiatingHeapOccupancyPercent=35"
  AUDIT_CONSUMER_OPTS="${AUDIT_CONSUMER_OPTS} -XX:ConcGCThreads=4 -XX:ParallelGCThreads=8"
fi

# Point to krb5.conf for Kerberos
if [ "${KERBEROS_ENABLED}" == "true" ]; then
  export AUDIT_CONSUMER_OPTS="${AUDIT_CONSUMER_OPTS} -Djava.security.krb5.conf=/etc/krb5.conf"

  # Wait for keytabs if Kerberos is enabled
  if [ -f /home/ranger/scripts/wait_for_keytab.sh ]; then
    echo "[INFO] Waiting for Kerberos keytabs..."
    bash /home/ranger/scripts/wait_for_keytab.sh HTTP.keytab
    bash /home/ranger/scripts/wait_for_keytab.sh rangerauditserver.keytab
    echo "[INFO] ✓ Keytabs are available"
  fi
fi

export AUDIT_CONSUMER_OPTS

echo "[INFO] JAVA_HOME: ${JAVA_HOME}"
echo "[INFO] AUDIT_CONSUMER_HEAP: ${AUDIT_CONSUMER_HEAP}"
echo "[INFO] AUDIT_CONSUMER_OPTS: ${AUDIT_CONSUMER_OPTS}"

# Build classpath from WAR file
WEBAPP_ROOT="${AUDIT_CONSUMER_HOME_DIR}/webapp"
WAR_FILE="${WEBAPP_ROOT}/ranger-audit-consumer-solr.war"
WEBAPP_DIR="${WEBAPP_ROOT}/ranger-audit-consumer-solr"

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
if [ -d "${AUDIT_CONSUMER_HOME_DIR}/libext" ]; then
  for jar in "${AUDIT_CONSUMER_HOME_DIR}"/libext/*.jar; do
    if [ -f "${jar}" ]; then
      RANGER_CLASSPATH="${RANGER_CLASSPATH}:${jar}"
    fi
  done
fi

export RANGER_CLASSPATH

echo "[INFO] Starting Ranger Audit Consumer - Solr..."
echo "[INFO] Webapp dir: ${WEBAPP_DIR}"
java ${AUDIT_CONSUMER_HEAP} ${AUDIT_CONSUMER_OPTS} \
  -Daudit.config=${AUDIT_CONSUMER_CONF_DIR}/ranger-audit-consumer-solr-site.xml \
  -Dranger.audit.consumer.webapp.dir="${WAR_FILE}" \
  -cp "${RANGER_CLASSPATH}" \
  org.apache.ranger.audit.consumer.SolrConsumerApplication \
  >> ${AUDIT_CONSUMER_LOG_DIR}/catalina.out 2>&1 &

PID=$!
echo $PID > ${AUDIT_CONSUMER_LOG_DIR}/ranger-audit-consumer-solr.pid

echo "[INFO] Ranger Audit Consumer - Solr started with PID: $PID"

# Keep the container running by tailing logs
tail -f ${AUDIT_CONSUMER_LOG_DIR}/catalina.out 2>/dev/null
