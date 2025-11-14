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

# Wait for Kerberos keytabs (enabled by default)
if [ "${KERBEROS_ENABLED}" != "false" ]
then
  echo "Kerberos is enabled, waiting for keytabs..."
  ${RANGER_SCRIPTS}/wait_for_keytab.sh opensearch.keytab
  ${RANGER_SCRIPTS}/wait_for_keytab.sh HTTP.keytab
  ${RANGER_SCRIPTS}/wait_for_testusers_keytab.sh
else
  echo "Kerberos is disabled"
fi

# Set ownership
chown -R opensearch:hadoop ${OPENSEARCH_HOME}/
echo "OpenSearch setup completed successfully"
