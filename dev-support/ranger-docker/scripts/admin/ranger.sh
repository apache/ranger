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
set -euo pipefail

RANGER_HOME="${RANGER_HOME:-/opt/ranger}"
RANGER_ADMIN_DIR="${RANGER_HOME}/admin"
RANGER_DIST="${RANGER_DIST:-/home/ranger/dist}"
USER_PASSWORD_BOOTSTRAP_HELPER="/home/ranger/scripts/user_password_bootstrap.py"
SERVICES_MARKER="/opt/ranger/.rangeradminservicescreated"

# Filenames for JDBC jars are exported by Dockerfile.ranger ENV; keep fallbacks for local runs.
RANGER_JDBC_POSTGRES_JAR="${RANGER_JDBC_POSTGRES_JAR:-postgresql-42.2.16.jre7.jar}"
RANGER_JDBC_MYSQL_JAR="${RANGER_JDBC_MYSQL_JAR:-mysql-connector-java-8.0.28.jar}"
RANGER_JDBC_ORACLE_JAR="${RANGER_JDBC_ORACLE_JAR:-ojdbc8.jar}"
RANGER_JDBC_LOG4JDBC_JAR="${RANGER_JDBC_LOG4JDBC_JAR:-log4jdbc-1.2.jar}"

# Select JDBC driver from RANGER_DIST at container start (RANGER_DB_TYPE); export SQL_CONNECTOR_JAR.
copy_jdbc_driver() {
  local share_java="/usr/share/java"
  local db_raw="${RANGER_DB_TYPE:-postgres}"
  local t
  t=$(echo "$db_raw" | tr '[:upper:]' '[:lower:]')

  local src=""
  local dest_name=""
  case "$t" in
    postgres|postgresql)
      src="${RANGER_DIST}/${RANGER_JDBC_POSTGRES_JAR}"
      dest_name="postgresql.jar"
      ;;
    mysql|mariadb)
      src="${RANGER_DIST}/${RANGER_JDBC_MYSQL_JAR}"
      dest_name="mysql-connector.jar"
      ;;
    oracle)
      src="${RANGER_DIST}/${RANGER_JDBC_ORACLE_JAR}"
      dest_name="oracle.jar"
      ;;
    *)
      echo "ranger.sh: unsupported RANGER_DB_TYPE='${db_raw}' (use postgres, mysql, or oracle)" >&2
      exit 1
      ;;
  esac

  local dest="${share_java}/${dest_name}"
  if [ ! -f "$src" ]; then
    echo "ranger.sh: JDBC driver not found: ${src}" >&2
    exit 1
  fi
  cp -f "$src" "$dest"
  export SQL_CONNECTOR_JAR="$dest"
}

copy_sql_connector_to_webapp_lib() {
  local lib="${RANGER_HOME}/admin/ews/webapp/WEB-INF/lib"
  local db_type
  db_type=$(echo "${RANGER_DB_TYPE:-postgres}" | tr '[:upper:]' '[:lower:]')

  mkdir -p "${lib}"
  if [ -n "${SQL_CONNECTOR_JAR:-}" ] && [ -f "${SQL_CONNECTOR_JAR}" ]; then
    cp -f "${SQL_CONNECTOR_JAR}" "${lib}/"
  fi
  if [ "${db_type}" = "mysql" ] || [ "${db_type}" = "mariadb" ]; then
    local log4jdbc_jar="${RANGER_DIST}/${RANGER_JDBC_LOG4JDBC_JAR}"
    if [ ! -f "${log4jdbc_jar}" ]; then
      echo "ranger.sh: log4jdbc jar not found: ${log4jdbc_jar}" >&2
      exit 1
    fi
    cp -f "${log4jdbc_jar}" "${lib}/"
  fi
}

# if kerberos is enabled, below keytabs must be mounted: rangeradmin.keytab, rangerlookup.keytab, HTTP.keytab, and testusers.keytab
configure_java_opts() {
  local db_password="${RANGER_ADMIN_DB_PASSWORD:-}"

  if [ -n "${db_password}" ]; then
    export JAVA_OPTS="${JAVA_OPTS:-} -Dranger.jpa.jdbc.password=${db_password}"
  fi
}

admin_pid() {
  local pid_dir="${RANGER_PID_DIR_PATH:-/var/run/ranger}"
  local pid_name="${RANGER_ADMIN_PID_NAME:-rangeradmin.pid}"
  local pidf="${pid_dir}/${pid_name}"

  if [ -f "${pidf}" ]; then
    cat "${pidf}" 2>/dev/null || true
    return 0
  fi

  ps -ef | grep java | grep -- '-Dproc_rangeradmin' | grep -v grep | awk '{ print $2 }' | head -n 1
}

wait_for_admin() {
  local timeout_s="${1:-180}"
  local start
  start="$(date +%s)"

  while true; do
    local pid
    pid="$(admin_pid || true)"
    if [ -n "${pid}" ] && ps -p "${pid}" >/dev/null 2>&1; then
      if command -v curl >/dev/null 2>&1; then
        # login.jsp returns 200 once the webapp is fully initialized
        if curl -fsS "http://127.0.0.1:6080/login.jsp" >/dev/null 2>&1; then
          return 0
        fi
      else
        return 0
      fi
    fi

    if [ $(( $(date +%s) - start )) -ge "${timeout_s}" ]; then
      return 1
    fi
    sleep 3
  done
}

copy_jdbc_driver
copy_sql_connector_to_webapp_lib

cd "${RANGER_ADMIN_DIR}"
configure_java_opts

python3 "/home/ranger/scripts/dba.py"
./ews/ranger-admin-services.sh start

if [ ! -f "${SERVICES_MARKER}" ]; then
  if wait_for_admin 240; then
    if python3 "${USER_PASSWORD_BOOTSTRAP_HELPER}"; then
      if python3 "/home/ranger/scripts/create_ranger_services.py"; then
        touch "${SERVICES_MARKER}" 2>/dev/null || true
      else
        echo "Warning: service creation failed" >&2
      fi
    else
      echo "Warning: admin bootstrap failed; skipping service creation" >&2
    fi
  else
    echo "ERROR: Ranger Admin did not become ready in time; skipping service creation" >&2
  fi
fi

pid="$(admin_pid || true)"
if [ -n "${pid}" ]; then
  tail --pid="${pid}" -f /dev/null
fi

echo "Ranger Admin process id not found; keeping container alive for debugging" >&2
tail -f /dev/null
