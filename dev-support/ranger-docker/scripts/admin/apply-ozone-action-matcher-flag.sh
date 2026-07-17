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

ADMIN_SITE="${RANGER_HOME}/admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-site.xml"

if [ ! -f "${ADMIN_SITE}" ]; then
  echo "WARN: ${ADMIN_SITE} not found; skipping ozone action-matcher flag"
  exit 0
fi

python3 - "${ADMIN_SITE}" "${PROP_NAME}" "${FLAG_VALUE}" <<'PY'
import sys
from xml.etree import ElementTree as ET

site_path, prop_name, prop_value = sys.argv[1:4]
tree = ET.parse(site_path)
root = tree.getroot()

for prop in root.findall('property'):
    name_el = prop.find('name')
    if name_el is not None and name_el.text and name_el.text.strip() == prop_name:
        value_el = prop.find('value')
        if value_el is None:
            value_el = ET.SubElement(prop, 'value')
        value_el.text = prop_value
        tree.write(site_path, encoding='unicode', xml_declaration=False)
        print(f"Updated {prop_name}={prop_value} in {site_path}")
        sys.exit(0)

prop = ET.SubElement(root, 'property')
ET.SubElement(prop, 'name').text = prop_name
ET.SubElement(prop, 'value').text = prop_value
tree.write(site_path, encoding='unicode', xml_declaration=False)
print(f"Added {prop_name}={prop_value} to {site_path}")
PY
