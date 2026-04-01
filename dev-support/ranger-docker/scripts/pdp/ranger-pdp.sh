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
    ${RANGER_SCRIPTS}/wait_for_keytab.sh HTTP.keytab
    ${RANGER_SCRIPTS}/wait_for_testusers_keytab.sh
    cp ${RANGER_SCRIPTS}/core-site.xml ${RANGER_HOME}/pdp/conf/core-site.xml
  fi

  touch "${RANGER_HOME}"/.setupDone
fi

cd ${RANGER_HOME}/pdp || exit 1
exec ./ranger-pdp-services.sh run
