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

set -euo pipefail

PROP_NAME='ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition'
FLAG_VALUE="${RANGER_OZONE_ENABLE_ACTION_MATCHER:-false}"

if [ "${FLAG_VALUE}" != "true" ] && [ "${FLAG_VALUE}" != "false" ]; then
  echo "WARN: RANGER_OZONE_ENABLE_ACTION_MATCHER='${FLAG_VALUE}' is invalid; using false"
  FLAG_VALUE=false
fi

ADMIN_DIR="${RANGER_HOME}/admin"
UPDATE_PROP="${ADMIN_DIR}/update_property.py"
CONF_DIR="${ADMIN_DIR}/ews/webapp/WEB-INF/classes/conf"
CONF_DIST="${ADMIN_DIR}/ews/webapp/WEB-INF/classes/conf.dist"

if [ ! -f "${UPDATE_PROP}" ]; then
  echo "WARN: ${UPDATE_PROP} not found; skipping ozone action-matcher flag"
  exit 0
fi

updated=false
for site_file in \
  "${CONF_DIR}/ranger-admin-site.xml" \
  "${CONF_DIR}/ranger-admin-default-site.xml" \
  "${CONF_DIST}/ranger-admin-default-site.xml"; do
  if [ -f "${site_file}" ] && grep -q "<name>${PROP_NAME}</name>" "${site_file}"; then
    if python3 "${UPDATE_PROP}" "${PROP_NAME}" "${FLAG_VALUE}" "${site_file}"; then
      echo "Updated ${PROP_NAME}=${FLAG_VALUE} in ${site_file}"
      updated=true
    fi
  fi
done

if [ "${updated}" != "true" ]; then
  echo "WARN: no ranger admin site XML found; skipping ozone action-matcher flag"
fi
