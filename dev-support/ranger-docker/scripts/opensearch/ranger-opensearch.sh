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

if [ ! -e ${OPENSEARCH_HOME}/.setupDone ]
then
  if "${RANGER_SCRIPTS}"/ranger-opensearch-setup.sh;
  then
    touch "${OPENSEARCH_HOME}"/.setupDone
  else
    echo "OpenSearch Setup Script didn't complete proper execution."
  fi
fi

# Start OpenSearch as opensearch user with Kerberos enabled by default
if [ "${KERBEROS_ENABLED}" != "false" ]; then
  echo "Starting OpenSearch with Kerberos authentication enabled..."
  su -c "cd ${OPENSEARCH_HOME} && OPENSEARCH_JAVA_OPTS=\"${OPENSEARCH_JAVA_OPTS} -Djava.security.krb5.conf=/etc/krb5.conf -Djava.security.auth.login.config=/opt/opensearch/config/opensearch-jaas.conf\" ./bin/opensearch" opensearch
else
  echo "Starting OpenSearch without Kerberos..."
  su -c "cd ${OPENSEARCH_HOME} && ./bin/opensearch" opensearch
fi

