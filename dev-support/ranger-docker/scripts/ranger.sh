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


if [ ! -e ${RANGER_HOME}/.setupDone ]
then
  SETUP_RANGER=true
else
  SETUP_RANGER=false
fi

if [ "${SETUP_RANGER}" == "true" ]
then
  su -c "cd ${RANGER_HOME}/admin && ./setup.sh" ranger

  touch ${RANGER_HOME}/.setupDone
fi

su -c "cd ${RANGER_HOME}/admin && ./ews/ranger-admin-services.sh start" ranger

if [ "${SETUP_RANGER}" == "true" ]
then
  # Wait for Ranger Admin to become ready
  sleep 30

  python3 ${RANGER_SCRIPTS}/ranger-hdfs-service-dev_hdfs.py
  python3 ${RANGER_SCRIPTS}/ranger-yarn-service-dev_yarn.py
  python3 ${RANGER_SCRIPTS}/ranger-hive-service-dev_hive.py
  python3 ${RANGER_SCRIPTS}/ranger-hbase-service-dev_hbase.py
  python3 ${RANGER_SCRIPTS}/ranger-kafka-service-dev_kafka.py
  python3 ${RANGER_SCRIPTS}/ranger-knox-service-dev_knox.py
fi

RANGER_ADMIN_PID=`ps -ef  | grep -v grep | grep -i "org.apache.ranger.server.tomcat.EmbeddedServer" | awk '{print $2}'`

# prevent the container from exiting
tail --pid=$RANGER_ADMIN_PID -f /dev/null
