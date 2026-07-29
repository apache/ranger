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

cd "${OZONE_HOME}"/ranger-ozone-plugin || exit

# SCM Ratis leader election can lag compose depends_on; avoid OM failing with
# Connection refused / ServerNotLeaderException on first SCM RPC.
wait_for_scm() {
  local scm_host="${OZONE_SCM_HOST:-scm}"
  local scm_port="${OZONE_SCM_CLIENT_PORT:-9863}"
  local max_wait_sec="${OZONE_SCM_WAIT_SEC:-120}"
  local deadline=$((SECONDS + max_wait_sec))

  echo "Waiting for SCM at ${scm_host}:${scm_port} (up to ${max_wait_sec}s)..."
  while (( SECONDS < deadline )); do
    if (echo > "/dev/tcp/${scm_host}/${scm_port}") 2>/dev/null; then
      sleep 5
      echo "SCM port open; proceeding with Ozone plugin enable"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: timed out waiting for SCM at ${scm_host}:${scm_port}" >&2
  exit 1
}
wait_for_scm

if [[ ! -f "${OZONE_HOME}"/.setupDone ]];
then
  if [ ! -d conf ]; then
    mkdir -p conf
    echo "conf directory created!"
  else
    echo "conf directory exists already!"
  fi
  enable_java_home="${JAVA_HOME:-/usr/lib/jvm/jre/}"
  echo "export JAVA_HOME=${enable_java_home}" >> conf/ozone-env.sh
  sudo JAVA_HOME="${enable_java_home}" ./enable-ozone-plugin.sh
  touch "${OZONE_HOME}"/.setupDone
else
  echo "Ranger Ozone Plugin Installation is already complete!"
fi
