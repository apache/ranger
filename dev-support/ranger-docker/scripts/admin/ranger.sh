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
    ${RANGER_SCRIPTS}/wait_for_keytab.sh rangeradmin.keytab
    ${RANGER_SCRIPTS}/wait_for_keytab.sh rangerlookup.keytab
    ${RANGER_SCRIPTS}/wait_for_keytab.sh HTTP.keytab
    ${RANGER_SCRIPTS}/wait_for_testusers_keytab.sh
  fi

  # Select audit store from RANGER_AUDIT_STORE env-var (mirrors RANGER_DB_TYPE pattern).
  # Default is opensearch; set RANGER_AUDIT_STORE=solr to switch to Solr.
  # Rewrites install.properties in-place so setup.sh picks up the right audit block.
  if [ "${RANGER_AUDIT_STORE}" = "solr" ]; then
    sed -i \
      -e 's|^audit_store=opensearch|# audit_store=opensearch|' \
      -e 's|^audit_opensearch_urls=|# audit_opensearch_urls=|' \
      -e 's|^audit_opensearch_port=|# audit_opensearch_port=|' \
      -e 's|^audit_opensearch_protocol=|# audit_opensearch_protocol=|' \
      -e 's|^audit_opensearch_user=|# audit_opensearch_user=|' \
      -e 's|^audit_opensearch_password=|# audit_opensearch_password=|' \
      -e 's|^audit_opensearch_index=|# audit_opensearch_index=|' \
      -e 's|^audit_opensearch_bootstrap_enabled=|# audit_opensearch_bootstrap_enabled=|' \
      -e 's|^# audit_store=solr|audit_store=solr|' \
      -e 's|^# audit_solr_urls=|audit_solr_urls=|' \
      -e 's|^# audit_solr_collection_name=|audit_solr_collection_name=|' \
      "${RANGER_HOME}/admin/install.properties"
    echo "Audit store set to solr"
  else
    echo "Audit store set to opensearch (default)"
  fi

  cd "${RANGER_HOME}"/admin || exit
  if ./setup.sh;
  then
    if [ "${KERBEROS_ENABLED}" == "true" ]
    then
      cp ${RANGER_SCRIPTS}/core-site.xml ${RANGER_HOME}/admin/conf/core-site.xml
    fi

    touch "${RANGER_HOME}"/.setupDone
  else
    echo "Ranger Admin Setup Script didn't complete proper execution."
  fi
fi

cd ${RANGER_HOME}/admin && ./ews/ranger-admin-services.sh start

if [ "${SETUP_RANGER}" == "true" ]
then
  # Wait for Ranger Admin to become ready
  sleep 30
  python3 ${RANGER_SCRIPTS}/create-ranger-services.py
fi

RANGER_ADMIN_PID=`ps -ef  | grep -v grep | grep -i "org.apache.ranger.server.tomcat.EmbeddedServer" | awk '{print $2}'`

# prevent the container from exiting
if [ -z "$RANGER_ADMIN_PID" ]
then
  echo "Ranger Admin process probably exited, no process id found!"
else
  tail --pid=$RANGER_ADMIN_PID -f /dev/null
fi
