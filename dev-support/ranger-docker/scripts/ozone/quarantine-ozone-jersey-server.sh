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

# Move jersey-server out of Ozone's lib dir so Ranger's Jersey client (in
# RangerPluginClassLoader) does not SPI-load WadlAutoDiscoverable from the
# Ozone app classpath (ClassCastException on policy/role download).
#
# Set OZONE_JERSEY_SERVER_QUARANTINE=false to skip (not recommended when
# Kerberos secure download is enabled).

quarantine_ozone_jersey_server() {
  local ozone_lib_dir="${1:?Ozone lib directory required}"

  if [ "${OZONE_JERSEY_SERVER_QUARANTINE:-true}" != "true" ]; then
    echo "OZONE_JERSEY_SERVER_QUARANTINE=false; skipping jersey-server quarantine"
    return 0
  fi

  if [ ! -d "${ozone_lib_dir}" ]; then
    echo "WARN: Ozone lib directory not found: ${ozone_lib_dir}; skipping jersey-server quarantine" >&2
    return 0
  fi

  local workaround_dir="${ozone_lib_dir}/.ranger-jersey-workaround"
  local moved=0

  mkdir -p "${workaround_dir}"

  shopt -s nullglob
  for jar in "${ozone_lib_dir}"/jersey-server-*.jar; do
    mv "${jar}" "${workaround_dir}/"
    echo "Quarantined $(basename "${jar}") -> ${workaround_dir}/"
    moved=1
  done
  shopt -u nullglob

  if [ "${moved}" -eq 0 ]; then
    echo "No jersey-server jar under ${ozone_lib_dir} (already quarantined?)"
  fi
}

if [ "${BASH_SOURCE[0]}" = "${0}" ] && [ -n "${1:-}" ]; then
  quarantine_ozone_jersey_server "$1"
fi
