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

# Resolve real path
realScriptPath=$(readlink -f "$0" 2>/dev/null)
if [ $? -eq 0 ]; then
    scriptDir=$(dirname "$realScriptPath")
else
    scriptDir=$(cd "$(dirname "$0")"; pwd)
fi

AUDIT_DISPATCHER_HOME_DIR=${AUDIT_DISPATCHER_HOME_DIR:-$(cd "${scriptDir}/.."; pwd)}
AUDIT_DISPATCHER_CONF_DIR=${AUDIT_DISPATCHER_CONF_DIR:-"${AUDIT_DISPATCHER_HOME_DIR}/conf"}
AUDIT_DISPATCHER_LOG_DIR=${AUDIT_DISPATCHER_LOG_DIR:-"${AUDIT_DISPATCHER_HOME_DIR}/logs"}

COMMON_CONF_FILE="${AUDIT_DISPATCHER_CONF_DIR}/ranger-audit-dispatcher-site.xml"

# Determine the dispatcher type for starting the dispatcher of that type
DISPATCHER_TYPE=$1

CONF_FILE="${COMMON_CONF_FILE}"

# Set default heap size if not set
if [ -z "${AUDIT_DISPATCHER_HEAP}" ]; then
  AUDIT_DISPATCHER_HEAP="-Xms512m -Xmx2g"
fi

# Set default Java options if not set
if [ -z "${AUDIT_DISPATCHER_OPTS}" ]; then
  AUDIT_DISPATCHER_OPTS="-Dlogback.configurationFile=${AUDIT_DISPATCHER_CONF_DIR}/logback.xml \
    -Daudit.dispatcher.log.dir=${AUDIT_DISPATCHER_LOG_DIR} \
    -Daudit.dispatcher.log.file=ranger-audit-dispatcher.log \
    -Djava.net.preferIPv4Stack=true \
    -server -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
    -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=8"
fi

export AUDIT_DISPATCHER_OPTS

echo "[INFO] JAVA_HOME: ${JAVA_HOME}"
echo "[INFO] AUDIT_DISPATCHER_HEAP: ${AUDIT_DISPATCHER_HEAP}"
echo "[INFO] AUDIT_DISPATCHER_OPTS: ${AUDIT_DISPATCHER_OPTS}"

if [ ! -f "${CONF_FILE}" ]; then
    echo "[ERROR] Configuration file not found: ${CONF_FILE}"
    exit 1
fi

if [ -n "${DISPATCHER_TYPE}" ]; then
    echo "[INFO] Starting dispatcher of type: ${DISPATCHER_TYPE}"
else
    echo "[INFO] Starting dispatcher (type will be determined from configuration)"
fi

# Helper function to read property from XML
get_property_value() {
  local prop_name=$1
  local file_path=$2
  if [ -f "$file_path" ]; then
    local val=$(grep -A 1 "<name>${prop_name}</name>" "$file_path" | grep "<value>" | sed -e 's/.*<value>\(.*\)<\/value>.*/\1/')
    # Trim whitespace
    echo "${val}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
  fi
}

WAR_FILE_NAME=$(get_property_value "ranger.audit.dispatcher.war.file" "${COMMON_CONF_FILE}")
if [ -z "${WAR_FILE_NAME}" ]; then
    WAR_FILE_NAME="ranger-audit-dispatcher.war"
fi

MAIN_CLASS=$(get_property_value "ranger.audit.dispatcher.launcher.class" "${COMMON_CONF_FILE}")
if [ -z "${MAIN_CLASS}" ]; then
    MAIN_CLASS="org.apache.ranger.audit.dispatcher.AuditDispatcherLauncher"
fi

WEBAPP_ROOT="${AUDIT_DISPATCHER_HOME_DIR}/webapp"
WAR_FILE="${WEBAPP_ROOT}/${WAR_FILE_NAME}"

if [ -n "${DISPATCHER_TYPE}" ]; then
    WEBAPP_DIR="${WEBAPP_ROOT}/audit-dispatcher-${DISPATCHER_TYPE}"
else
    WEBAPP_DIR="${WEBAPP_ROOT}/audit-dispatcher"
fi

if [ ! -f "${WAR_FILE}" ]; then
    echo "[ERROR] WAR file not found: ${WAR_FILE}"
    exit 1
fi

# Extract the specific WAR
if [ -f "${WAR_FILE}" ] && [ ! -d "${WEBAPP_DIR}/WEB-INF" ]; then
  echo "[INFO] Extracting ${WAR_FILE}..."
  mkdir -p "${WEBAPP_DIR}"
  cd "${WEBAPP_DIR}"
  jar xf "${WAR_FILE}"
  cd - > /dev/null
fi

# Build minimal classpath for the Launcher
RANGER_CLASSPATH="${AUDIT_DISPATCHER_CONF_DIR}:${WEBAPP_DIR}/WEB-INF/classes"
for jar in "${WEBAPP_DIR}"/WEB-INF/lib/*.jar; do
  if [ -f "${jar}" ]; then
    RANGER_CLASSPATH="${RANGER_CLASSPATH}:${jar}"
  fi
done
for jar in "${AUDIT_DISPATCHER_HOME_DIR}"/lib/common/*.jar; do
  if [ -f "${jar}" ]; then
    RANGER_CLASSPATH="${RANGER_CLASSPATH}:${jar}"
  fi
done

# Add libext directory if it exists
if [ -d "${AUDIT_DISPATCHER_HOME_DIR}/libext" ]; then
  for jar in "${AUDIT_DISPATCHER_HOME_DIR}"/libext/*.jar; do
    if [ -f "${jar}" ]; then
      RANGER_CLASSPATH="${RANGER_CLASSPATH}:${jar}"
    fi
  done
fi

echo "[INFO] Webapp dir: ${WEBAPP_DIR}"
echo "[INFO] Main class: ${MAIN_CLASS}"

JAVA_CMD="java ${AUDIT_DISPATCHER_HEAP} ${AUDIT_DISPATCHER_OPTS}"

if [ -n "${DISPATCHER_TYPE}" ]; then
    JAVA_CMD="${JAVA_CMD} -Dranger.audit.dispatcher.type=${DISPATCHER_TYPE}"
fi

JAVA_CMD="${JAVA_CMD} -Dranger.audit.dispatcher.webapp.dir=${WEBAPP_DIR} -cp ${RANGER_CLASSPATH} ${MAIN_CLASS}"

# Start the Java application
if [ -n "${HADOOP_HOME}" ]; then
  export HADOOP_HOME=${HADOOP_HOME}
else
  # Set a default HADOOP_HOME if not set, typical for Ranger docker
  export HADOOP_HOME=/opt/ranger/audit-dispatcher
fi

exec ${JAVA_CMD}