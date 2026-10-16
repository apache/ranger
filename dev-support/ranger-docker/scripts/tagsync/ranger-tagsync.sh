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
  if [ "${KERBEROS_ENABLED}" == "true" ]
  then
    ${RANGER_SCRIPTS}/wait_for_keytab.sh rangertagsync.keytab
    ${RANGER_SCRIPTS}/wait_for_testusers_keytab.sh
  fi

  cd "${RANGER_HOME}"/tagsync || exit
  if ./setup.sh;
  then
    if [ "${KERBEROS_ENABLED}" == "true" ]
    then
      cp ${RANGER_SCRIPTS}/core-site.xml ${RANGER_HOME}/tagsync/conf/core-site.xml
    fi

    touch "${RANGER_HOME}"/.setupDone
  else
    echo "Ranger TagSync Setup Script didn't complete proper execution."
  fi
fi

# Atlas Kafka consumer authenticates with the dev SASL/OAUTHBEARER token minted by ranger-kafka unless
# TAG_SOURCE_ATLAS_KAFKA_SASL_MECHANISM selects GSSAPI; give the minter a moment on a fresh stack
if [ "${KERBEROS_ENABLED}" == "true" ] && grep -qiE '^TAG_SOURCE_ATLAS_ENABLED *= *true' ${RANGER_HOME}/tagsync/install.properties && ! grep -qiE '^TAG_SOURCE_ATLAS_KAFKA_SASL_MECHANISM *= *GSSAPI' ${RANGER_HOME}/tagsync/install.properties
then
  for i in $(seq 1 30)
  do
    [ -s /etc/kafka-oauth/rangertagsync.token ] && break
    echo "Waiting for Kafka OAUTHBEARER token /etc/kafka-oauth/rangertagsync.token ($i/30)"
    sleep 2
  done
fi

cd ${RANGER_HOME}/tagsync && ./ranger-tagsync-services.sh start

RANGER_TAGSYNC_PID=`ps -ef  | grep -v grep | grep -i "org.apache.ranger.tagsync.server.RangerTagSyncServer" | awk '{print $2}'`

# prevent the container from exiting
if [ -z "$RANGER_TAGSYNC_PID" ]
then
  echo "The TagSync process probably exited, no process id found!"
else
  tail --pid=$RANGER_TAGSYNC_PID -f /dev/null
fi
