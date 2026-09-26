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

RANGER_ADMIN_DIR=${RANGER_HOME}/admin
export RANGER_ADMIN_CONFIGS=${RANGER_ADMIN_DIR}/configs
export RANGER_ADMIN_START_TIME=$(date +%s)

# colors only when attached to a terminal (e.g. docker compose with tty), plain text otherwise (e.g. log collectors)
[ -t 1 ] && [ -z "${NO_COLOR}" ] && COLOR=true

style() {
  if [ "${COLOR}" == "true" ]; then printf '\033[%sm%s\033[0m' "$2" "$1"; else printf '%s' "$1"; fi
}

section() {
  local title="━━━ ▶ $1 "
  local pad

  printf -v pad '%*s' $((110 - ${#title})) ''
  echo "$(style "${title}${pad// /━}" '1;36')"
}

log() {
  local code='32'

  [ "$1" == "ERROR" ] && code='1;31'
  echo "$(style "$(date +%H:%M:%S)" 2) $(style "$(printf '%-5s' "$1")" "${code}") $2"
}

section "ranger.sh · Ranger Admin container"

if [ "${KERBEROS_ENABLED}" == "true" ]
then
  log INFO "Kerberos is enabled, waiting for keytabs"

  ${RANGER_SCRIPTS}/wait_for_keytab.sh rangeradmin.keytab
  ${RANGER_SCRIPTS}/wait_for_keytab.sh rangerlookup.keytab
  ${RANGER_SCRIPTS}/wait_for_keytab.sh HTTP.keytab
  ${RANGER_SCRIPTS}/wait_for_testusers_keytab.sh
fi

# runtime conf is rebuilt on every start: defaults from conf.dist, then files mounted in ${RANGER_ADMIN_CONFIGS},
# including ranger-admin-site.xml when it is provided; dba.py renders that file from YAML only when it is not.
# -L and the hidden-name filter handle the symlinks of a mounted Kubernetes ConfigMap.
mkdir -p "${RANGER_CONF_DIR}"
cp -r "${RANGER_ADMIN_DIR}"/ews/webapp/WEB-INF/classes/conf.dist/. "${RANGER_CONF_DIR}"/
echo "<configuration></configuration>" > "${RANGER_CONF_DIR}"/core-site.xml

find -L "${RANGER_ADMIN_CONFIGS}" -maxdepth 1 -type f ! -name '.*' ! -name 'ranger-admin-site-*.yaml' -exec cp {} "${RANGER_CONF_DIR}"/ \;

if [ "${KERBEROS_ENABLED}" == "true" ]
then
  cp "${RANGER_SCRIPTS}"/core-site.xml "${RANGER_CONF_DIR}"/core-site.xml
fi

if [ "${DEBUG_ADMIN}" == "true" ]
then
  xmlstarlet ed -L -u "//root/@level" -v "debug" "${RANGER_CONF_DIR}"/logback.xml

  log INFO "DEBUG_ADMIN is enabled: Ranger Admin logs at debug level"
fi

# loggers writing to Ranger Admin log (xa_log_appender) and DB patch log (patch_logger) write to console as well,
# which reaches container logs: from Java patches run by dba.py directly and from Ranger Admin via catalina.out
xmlstarlet ed -L \
  -i "/configuration/appender[1]" -t elem -n console_appender \
  -s "//console_appender" -t attr -n name -v console \
  -s "//console_appender" -t attr -n class -v ch.qos.logback.core.ConsoleAppender \
  -s "//console_appender" -t elem -n encoder \
  -s "//console_appender/encoder" -t elem -n pattern -v "%date [%thread] %level{5} [%file:%line] %msg%n" \
  -r "//console_appender" -v appender \
  -s "//*[appender-ref/@ref='xa_log_appender' or appender-ref/@ref='patch_logger']" -t elem -n console_appender_ref \
  -s "//console_appender_ref" -t attr -n ref -v console \
  -r "//console_appender_ref" -v appender-ref \
  "${RANGER_CONF_DIR}"/logback.xml

# service-def registration logs one line per service-def on every start; they are kept in Ranger Admin log only,
# the count of loaded service-defs is logged by create-ranger-services.py
for logger_name in org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil org.apache.ranger.plugin.store.AbstractServiceStore
do
  xmlstarlet ed -L \
    -i "/configuration/root" -t elem -n file_only_logger \
    -s "//file_only_logger" -t attr -n name -v "${logger_name}" \
    -s "//file_only_logger" -t attr -n additivity -v false \
    -s "//file_only_logger" -t attr -n level -v info \
    -s "//file_only_logger" -t elem -n appender-ref \
    -s "//file_only_logger/appender-ref" -t attr -n ref -v xa_log_appender \
    -r "//file_only_logger" -v logger \
    "${RANGER_CONF_DIR}"/logback.xml
done

log INFO "✔ Prepared ${RANGER_CONF_DIR} from conf.dist and ${RANGER_ADMIN_CONFIGS}, Ranger Admin log is sent to console too"

# AUDIT_INDEX_STORE (solr|opensearch), when set, overrides ranger.audit.source.type; it must match docker-compose.ranger-audit-service.yml.
if ! python3 "${RANGER_SCRIPTS}"/dba.py
then
  log ERROR "✖ Ranger Admin configuration or database bootstrap failed, see dba.py logs above"
  exit 1
fi

# JDBC driver of the configured DB flavor; ranger-admin-services.sh appends CLASSPATH to Ranger Admin classpath
export CLASSPATH=$(xmlstarlet sel -t -v "/configuration/property[name='ranger.jdbc.sqlconnectorjar']/value" "${RANGER_CONF_DIR}"/ranger-admin-site.xml)

section "Ranger Admin server · starting"

cd "${RANGER_ADMIN_DIR}" && ./ews/ranger-admin-services.sh start

RANGER_ADMIN_PID=`ps -ef  | grep -v grep | grep -i "org.apache.ranger.server.tomcat.EmbeddedServer" | awk '{print $2}'`

if [ -z "$RANGER_ADMIN_PID" ]
then
  cat "${RANGER_ADMIN_LOG_DIR}"/catalina.out
  log ERROR "✖ Ranger Admin process exited, see its output above"
  exit 1
fi

# services are created before Ranger Admin log is streamed, to keep these logs together
python3 "${RANGER_SCRIPTS}"/create-ranger-services.py

# Ranger Admin log file name is built by logback from -Dhostname and -Duser passed by ranger-admin-services.sh
section "Ranger Admin server log · ${RANGER_ADMIN_LOG_DIR}/{catalina.out, ranger-admin-${HOSTNAME}-${USER}.log}"

# stream Ranger Admin console output to container logs, from the start of this run; this also keeps the
# container running until Ranger Admin exits
tail --pid="${RANGER_ADMIN_PID}" -n +1 -F "${RANGER_ADMIN_LOG_DIR}"/catalina.out &

wait
