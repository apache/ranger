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

AUDIT_CONSUMER_HOME_DIR=${AUDIT_CONSUMER_HOME_DIR:-$(cd "${scriptDir}/.."; pwd)}
AUDIT_CONSUMER_CONF_DIR=${AUDIT_CONSUMER_CONF_DIR:-"${AUDIT_CONSUMER_HOME_DIR}/conf"}
AUDIT_CONSUMER_LOG_DIR=${AUDIT_CONSUMER_LOG_DIR:-"${AUDIT_CONSUMER_HOME_DIR}/logs"}

# Determine the consumer type for starting the consumer of that type
CONSUMER_TYPE=$1

if [ -z "${CONSUMER_TYPE}" ]; then
    echo "[ERROR] Consumer type not provided. Usage: $0 <type> (e.g., $0 solr)"
    exit 1
fi

CONF_FILE="${AUDIT_CONSUMER_CONF_DIR}/ranger-audit-consumer-${CONSUMER_TYPE}-site.xml"

# Set default heap size if not set
if [ -z "${AUDIT_CONSUMER_HEAP}" ]; then
  AUDIT_CONSUMER_HEAP="-Xms512m -Xmx2g"
fi

# Set default Java options if not set
if [ -z "${AUDIT_CONSUMER_OPTS}" ]; then
  AUDIT_CONSUMER_OPTS="-Dlogback.configurationFile=${AUDIT_CONSUMER_CONF_DIR}/logback.xml \
    -Daudit.consumer.log.dir=${AUDIT_CONSUMER_LOG_DIR} \
    -Daudit.consumer.log.file=ranger-audit-consumer.log \
    -Djava.net.preferIPv4Stack=true \
    -server -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
    -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=8"
fi

export AUDIT_CONSUMER_OPTS

echo "[INFO] JAVA_HOME: ${JAVA_HOME}"
echo "[INFO] AUDIT_CONSUMER_HEAP: ${AUDIT_CONSUMER_HEAP}"
echo "[INFO] AUDIT_CONSUMER_OPTS: ${AUDIT_CONSUMER_OPTS}"

if [ ! -f "${CONF_FILE}" ]; then
    echo "[ERROR] Configuration file not found: ${CONF_FILE}"
    exit 1
fi

echo "[INFO] Starting consumer of type: ${CONSUMER_TYPE}"

# Dynamically construct the property names based on the consumer type.
WAR_PROP_NAME="ranger.audit.consumer.${CONSUMER_TYPE}.war.file"
CLASS_PROP_NAME="ranger.audit.consumer.${CONSUMER_TYPE}.main.class"

# Read the WAR file name and Main Class dynamically
WAR_FILE_NAME=$(grep -A 1 "<name>${WAR_PROP_NAME}</name>" "${CONF_FILE}" | grep "<value>" | sed -e 's/.*<value>\(.*\)<\/value>.*/\1/')
MAIN_CLASS=$(grep -A 1 "<name>${CLASS_PROP_NAME}</name>" "${CONF_FILE}" | grep "<value>" | sed -e 's/.*<value>\(.*\)<\/value>.*/\1/')

if [ -z "${WAR_FILE_NAME}" ] || [ -z "${MAIN_CLASS}" ]; then
    echo "[ERROR] Missing WAR file or Main Class configuration for type '${CONSUMER_TYPE}'"
    echo "[ERROR] Please ensure ${WAR_PROP_NAME} and ${CLASS_PROP_NAME} are set in ${CONF_FILE}"
    exit 1
fi

WEBAPP_ROOT="${AUDIT_CONSUMER_HOME_DIR}/webapp"
WAR_FILE="${WEBAPP_ROOT}/${WAR_FILE_NAME}"
WEBAPP_DIR="${WEBAPP_ROOT}/audit-consumer-${CONSUMER_TYPE}"

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

# Build classpath from the extracted WAR
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

echo "[INFO] Webapp dir: ${WEBAPP_DIR}"
echo "[INFO] Main class: ${MAIN_CLASS}"

# Start the Java application using the dynamically loaded Main Class
java ${AUDIT_CONSUMER_HEAP} ${AUDIT_CONSUMER_OPTS} \
  -Daudit.config=${CONF_FILE} \
  -Dranger.audit.consumer.webapp.dir="${WEBAPP_DIR}" \
  -cp "${RANGER_CLASSPATH}" \
  ${MAIN_CLASS}
