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
# -------------------------------------------------------------------------------------

if [ -z "${JAVA_HOME}" ]; then
   echo "PLEASE EXPORT VARIABLE JAVA_HOME"
exit;
else
   echo "JAVA_HOME : "$JAVA_HOME
fi

if [ -z "${RANGER_KMS_HOME}" ]; then
   echo "PLEASE EXPORT VARIABLE RANGER_KMS_HOME"
exit;
else
   echo "RANGER_KMS_HOME : "$RANGER_KMS_HOME
fi

if [ -z "${RANGER_KMS_CONF}" ]; then
   echo "PLEASE EXPORT VARIABLE RANGER_KMS_CONF"
exit;
else
   echo "RANGER_KMS_CONF : "$RANGER_KMS_CONF
fi

if [ -z "${SQL_CONNECTOR_JAR}" ]; then
   echo "PLEASE EXPORT VARIABLE SQL_CONNECTOR_JAR"
exit;
else
   echo "SQL_CONNECTOR_JAR : "$SQL_CONNECTOR_JAR
fi

if [ -z "${HADOOP_CREDSTORE_PASSWORD}" ]; then
   echo "PLEASE EXPORT VARIABLE HADOOP_CREDSTORE_PASSWORD"
exit;
else
   echo "HADOOP_CREDSTORE_PASSWORD : "$HADOOP_CREDSTORE_PASSWORD
fi


if [ -z "${LIQUI_SEARCH_PATH}" ]; then
   echo "PLEASE EXPORT VARIABLE LIQUI_SEARCH_PATH"
exit;
else
   echo "LIQUI_SEARCH_PATH : "$LIQUI_SEARCH_PATH
fi


if [ -z "${LOG4J_PROPERTIES_FILE_PATH}" ]; then
   echo "PLEASE EXPORT VARIABLE LOG4J_PROPERTIES_FILE_PATH"
exit;
else
   echo "LOG4J_PROPERTIES_FILE_PATH : "$LOG4J_PROPERTIES_FILE_PATH
fi

set -x

cp="${RANGER_KMS_HOME}/cred/lib/*:${RANGER_KMS_CONF}:${RANGER_KMS_HOME}/ews/webapp/WEB-INF/classes/lib/*:${SQL_CONNECTOR_JAR}:${RANGER_KMS_HOME}/ews/webapp/config:${RANGER_KMS_HOME}/ews/lib/*:${RANGER_KMS_HOME}/ews/webapp/lib/*:${RANGER_KMS_HOME}/ews/webapp/META-INF:${RANGER_KMS_CONF}/*:${LIQUI_SEARCH_PATH}/*:${LIQUI_SEARCH_PATH}"

output=$(${JAVA_HOME}/bin/java -Dlog4j.configurationFile=file:${LOG4J_PROPERTIES_FILE_PATH} -cp "${cp}" org.apache.ranger.db.upgrade.LiquibaseDBUtilsMain -serviceName ${1} -op ${2} -tag ${3})

json_output=$(echo "$output" | tail -n 1)

OP_STATUS=$(echo "$json_output" | jq -r '.op_status // ""')
IS_FINALIZE_COMPLETE=$(echo "$json_output" | jq -r '.isFinalizeComplete // ""')
IS_UPGRADE_COMPLETE=$(echo "$json_output" | jq -r '.isUpgradeComplete // ""')

set +x

if [ -n "$OP_STATUS" ]; then
  echo "OP_STATUS=$OP_STATUS"
fi

if [ -n "$IS_UPGRADE_COMPLETE" ]; then
  echo "IS_UPGRADE_COMPLETE=$IS_UPGRADE_COMPLETE"
fi

if [ -n "$IS_FINALIZE_COMPLETE" ]; then
  echo "IS_FINALIZE_COMPLETE=$IS_FINALIZE_COMPLETE"
fi
