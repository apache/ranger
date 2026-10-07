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

# Docker entrypoint: renders the mounted site XML template for the dispatcher type
# (first argument: solr | opensearch | hdfs) into the conf dir, then hands over to the
# product start script.

set -e

DISPATCHER_TYPE="${1:-}"
AUDIT_DISPATCHER_CONF_DIR="${AUDIT_DISPATCHER_CONF_DIR:-/opt/ranger/audit-dispatcher/conf}"

source /home/ranger/scripts/service-check-functions.sh

if [ -n "${DISPATCHER_TYPE}" ]; then
  SITE_XML_TEMPLATE="/home/ranger/scripts/ranger-audit-dispatcher-${DISPATCHER_TYPE}-site.xml"

  if [ -f "${SITE_XML_TEMPLATE}" ]; then
    render_kafka_sasl_site_xml "${SITE_XML_TEMPLATE}" "${AUDIT_DISPATCHER_CONF_DIR}/ranger-audit-dispatcher-${DISPATCHER_TYPE}-site.xml"
  fi
fi

wait_for_kafka_oauth_token /etc/kafka-oauth/rangerauditserver.token 60

exec /home/ranger/scripts/start-audit-dispatcher.sh "$@"
