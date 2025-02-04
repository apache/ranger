#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Check if required environment variables are set
for var in RANGER_ADMIN_HOME RANGER_ADMIN_CONF RANGER_ADMIN_LOG_DIR; do
    if [ -z "${!var}" ]; then
        echo "Environment variable not found: ${var}"
        exit 1
    else
        echo "${var} : ${!var}"
    fi
done

RANGER_ADMIN_HEAP_DEFAULT_SIZE=1g
RANGER_ADMIN_HEAP_SIZE=${RANGER_ADMIN_HEAP_SIZE:-$RANGER_ADMIN_HEAP_DEFAULT_SIZE}
echo "Using heap size ${RANGER_ADMIN_HEAP_SIZE}"

# Define JAVA_OPTS
JAVA_OPTS=" ${JAVA_OPTS} -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx${RANGER_ADMIN_HEAP_SIZE} -Xms${RANGER_ADMIN_HEAP_SIZE} -Xloggc:${RANGER_ADMIN_LOG_DIR}/gc-worker.log -verbose:gc -XX:+PrintGCDetails "

# Construct CLASSPATH
CLASSPATH="${RANGER_ADMIN_CONF}:${RANGER_ADMIN_HOME}/ews/webapp/WEB-INF/classes/:${SQL_CONNECTOR_JAR}:${RANGER_ADMIN_HOME}/ews/webapp/WEB-INF/classes/lib/*:${RANGER_ADMIN_HOME}/ews/webapp/WEB-INF/classes/META-INF:${RANGER_ADMIN_HOME}/ews/webapp/WEB-INF/lib/*:${RANGER_ADMIN_HOME}/ews/webapp/META-INF:${RANGER_ADMIN_HOME}/ews/lib/*:${JAVA_HOME}/lib/*"

# Start Migration
nohup ${JAVA_HOME}/bin/java ${JAVA_OPTS} -Dlogdir="${RANGER_ADMIN_LOG_DIR}" -cp "${CLASSPATH}" org.apache.ranger.patch.cliutil.TrxLogV2MigrationUtil  > ${RANGER_ADMIN_LOG_DIR}/trxlog_v1_migration.out 2>&1 &
VALUE_OF_PID=$!

# Check if the command succeeded
if [ $? -ne 0 ]; then
    echo "[Error] Migration failed. Please check ${RANGER_ADMIN_LOG_DIR} for details."
    exit 1
fi

echo "Migrating Transaction logs has started with PID - ${VALUE_OF_PID}"
