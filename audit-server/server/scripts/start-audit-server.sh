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

# ========================================
# Ranger Audit Server Service Start Script
# ========================================
# This script starts the Ranger Audit Server service
# The service receives audit events from Ranger plugins and produces them to Kafka

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$(dirname "$SCRIPT_DIR")"

# Default directories - can be overridden by environment variables
AUDIT_SERVER_HOME_DIR="${AUDIT_SERVER_HOME_DIR:-${SERVICE_DIR}/target}"
AUDIT_SERVER_CONF_DIR="${AUDIT_SERVER_CONF_DIR:-${SERVICE_DIR}/src/main/resources/conf}"
AUDIT_SERVER_LOG_DIR="${AUDIT_SERVER_LOG_DIR:-${SERVICE_DIR}/logs}"

# Create log directory if it doesn't exist
mkdir -p "${AUDIT_SERVER_LOG_DIR}"

echo "=========================================="
echo "Starting Ranger Audit Server Service"
echo "=========================================="
echo "Service Directory: ${SERVICE_DIR}"
echo "Home Directory: ${AUDIT_SERVER_HOME_DIR}"
echo "Config Directory: ${AUDIT_SERVER_CONF_DIR}"
echo "Log Directory: ${AUDIT_SERVER_LOG_DIR}"
echo "=========================================="

# Check if Java is available
if [ -z "$JAVA_HOME" ]; then
  JAVA_CMD=$(which java 2>/dev/null || true)
  if [ -z "$JAVA_CMD" ]; then
    echo "[ERROR] JAVA_HOME is not set and java is not in PATH"
    exit 1
  fi
  JAVA_HOME=$(dirname $(dirname $(readlink -f "$JAVA_CMD")))
  echo "[INFO] JAVA_HOME not set, detected: ${JAVA_HOME}"
fi

export JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH

echo "[INFO] Java version:"
java -version

# Set heap size (default: 512MB to 2GB)
AUDIT_SERVER_HEAP="${AUDIT_SERVER_HEAP:--Xms512m -Xmx2g}"

# Set JVM options
if [ -z "$AUDIT_SERVER_OPTS" ]; then
  AUDIT_SERVER_OPTS="-Dlogback.configurationFile=${AUDIT_SERVER_CONF_DIR}/logback.xml"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Daudit.server.log.dir=${AUDIT_SERVER_LOG_DIR}"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Daudit.server.log.file=ranger-audit-server.log"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Djava.net.preferIPv4Stack=true -server"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -XX:InitiatingHeapOccupancyPercent=35"
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -XX:ConcGCThreads=4 -XX:ParallelGCThreads=8"
fi

# Add Kerberos configuration if needed
if [ "${KERBEROS_ENABLED}" == "true" ]; then
  AUDIT_SERVER_OPTS="${AUDIT_SERVER_OPTS} -Djava.security.krb5.conf=/etc/krb5.conf"
  echo "[INFO] Kerberos is enabled"
fi

export AUDIT_SERVER_OPTS
export AUDIT_SERVER_LOG_DIR

echo "[INFO] JAVA_HOME: ${JAVA_HOME}"
echo "[INFO] HEAP: ${AUDIT_SERVER_HEAP}"
echo "[INFO] JVM_OPTS: ${AUDIT_SERVER_OPTS}"

# Find the WAR file
WAR_FILE=$(find "${AUDIT_SERVER_HOME_DIR}" -name "ranger-audit-server-service*.war" | head -1)

if [ -z "$WAR_FILE" ] || [ ! -f "$WAR_FILE" ]; then
  echo "[ERROR] WAR file not found in ${AUDIT_SERVER_HOME_DIR}"
  echo "[ERROR] Please build the project first using: mvn clean package"
  exit 1
fi

echo "[INFO] Using WAR file: ${WAR_FILE}"

# Extract WAR if not already extracted
WEBAPP_DIR="${AUDIT_SERVER_HOME_DIR}/webapp/ranger-audit-server-service"
if [ ! -d "${WEBAPP_DIR}" ]; then
  echo "[INFO] Extracting WAR file..."
  mkdir -p "${WEBAPP_DIR}"
  cd "${WEBAPP_DIR}"
  jar xf "${WAR_FILE}"
  cd - > /dev/null
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

# Check if already running
PID_FILE="${AUDIT_SERVER_LOG_DIR}/ranger-audit-server.pid"
if [ -f "${PID_FILE}" ]; then
  OLD_PID=$(cat "${PID_FILE}")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "[WARNING] Ranger Audit Server is already running (PID: ${OLD_PID})"
    echo "[INFO] Use stop-audit-server.sh to stop it first"
    exit 1
  else
    echo "[INFO] Removing stale PID file"
    rm -f "${PID_FILE}"
  fi
fi

# Start the service
echo "[INFO] Starting Ranger Audit Server Service..."
nohup java ${AUDIT_SERVER_HEAP} ${AUDIT_SERVER_OPTS} \
  -Daudit.config=${AUDIT_SERVER_CONF_DIR}/ranger-audit-server-site.xml \
  -cp "${RANGER_CLASSPATH}" \
  org.apache.ranger.audit.server.AuditServerApplication \
  >> "${AUDIT_SERVER_LOG_DIR}/catalina.out" 2>&1 &

PID=$!
echo $PID > "${PID_FILE}"

echo "[INFO] ✓ Ranger Audit Server started successfully"
echo "[INFO] PID: ${PID}"
echo "[INFO] Log file: ${AUDIT_SERVER_LOG_DIR}/ranger-audit-server.log"
echo "[INFO] Catalina out: ${AUDIT_SERVER_LOG_DIR}/catalina.out"
echo "[INFO] Health check: http://localhost:7081/api/audit/health"
echo ""
echo "To monitor logs: tail -f ${AUDIT_SERVER_LOG_DIR}/ranger-audit-server.log"
echo "To stop service: ${SCRIPT_DIR}/stop-audit-server.sh"
