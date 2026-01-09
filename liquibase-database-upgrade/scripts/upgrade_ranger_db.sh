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
set -x

if [ -z "${JAVA_HOME_RANGER}" ]; then
   echo "PLEASE EXPORT VARIABLE JAVA_HOME_RANGER"
exit;
else
   echo "JAVA_HOME_RANGER : "$JAVA_HOME_RANGER
fi

if [ -z "${RANGER_HOME}" ]; then
   echo "PLEASE EXPORT VARIABLE RANGER_HOME"
exit;
else
   echo "RANGER_HOME : "$RANGER_HOME
fi

if [ -z "${RANGER_CONF}" ]; then
   echo "PLEASE EXPORT VARIABLE RANGER_CONF"
exit;
else
   echo "RANGER_CONF : "$RANGER_CONF
fi

if [ -z "${SQL_CONNECTOR_JAR_RANGER}" ]; then
   echo "PLEASE EXPORT VARIABLE SQL_CONNECTOR_JAR_RANGER"
exit;
else
   echo "SQL_CONNECTOR_JAR_RANGER : "$SQL_CONNECTOR_JAR_RANGER
fi

if [ -z "${HADOOP_CREDSTORE_PASSWORD}" ]; then
   echo "PLEASE EXPORT VARIABLE HADOOP_CREDSTORE_PASSWORD"
exit;
else
   echo "HADOOP_CREDSTORE_PASSWORD : "$HADOOP_CREDSTORE_PASSWORD
fi


if [ -z "${LIQUI_SEARCH_PATH_RANGER}" ]; then
   echo "PLEASE EXPORT VARIABLE LIQUI_SEARCH_PATH_RANGER"
exit;
else
   echo "LIQUI_SEARCH_PATH_RANGER : "$LIQUI_SEARCH_PATH_RANGER
fi


if [ -z "${LOG4J_PROPERTIES_FILE_PATH_RANGER}" ]; then
   echo "PLEASE EXPORT VARIABLE LOG4J_PROPERTIES_FILE_PATH_RANGER"
exit;
else
   echo "LOG4J_PROPERTIES_FILE_PATH_RANGER : "$LOG4J_PROPERTIES_FILE_PATH_RANGER
fi

cp="${RANGER_HOME}/cred/lib/*:${RANGER_CONF}:${RANGER_HOME}/ews/webapp/WEB-INF/classes/:${RANGER_HOME}/ews/webapp/WEB-INF/classes/META-INF:${RANGER_HOME}/ews/webapp/WEB-INF/lib/*:${SQL_CONNECTOR_JAR_RANGER}:${RANGER_HOME}/conf:${RANGER_HOME}/ews/lib/*:${RANGER_HOME}/ews/webapp/libs/*:${RANGER_HOME}/ews/webapp/META-INF/:${RANGER_HOME}/ews/ranger_jaas/*:${RANGER_CONF}/*:${LIQUI_SEARCH_PATH_RANGER}/*:${LIQUI_SEARCH_PATH_RANGER}/lib/*:${LIQUI_SEARCH_PATH_RANGER}/lib/:${RANGER_HOME}/ews/webapp/WEB-INF/:${RANGER_HOME}/ews/webapp/WEB-INF/classes/lib/*:${RANGER_HOME}/ews/webapp/WEB-INF/conf"

${JAVA_HOME_RANGER}/bin/java -DapplicationContext.files=applicationContext.xml,security-applicationContext.xml,asynctask-applicationContext.xml -Dlog4j.configurationFile=file:${LOG4J_PROPERTIES_FILE_PATH_RANGER} -cp "${cp}" org.apache.ranger.db.upgrade.LiquibaseUpdateDriverMain -serviceName ${1} -op ${2} -tag ${3}