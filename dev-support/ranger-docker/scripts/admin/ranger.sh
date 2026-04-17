#!/bin/bash

set -euo pipefail

RANGER_HOME="${RANGER_HOME:-/opt/ranger}"
RANGER_ADMIN_DIR="${RANGER_HOME}/admin"
CONF_DIR="${RANGER_ADMIN_CONF:-${RANGER_ADMIN_DIR}/ews/webapp/WEB-INF/classes/conf}"
CONFIGS_DIR="${RANGER_ADMIN_DIR}/configs"
CONFIG_XML_DEST="${CONF_DIR}/ranger-admin-site.xml"
ADMIN_XML_HELPER="/home/ranger/scripts/ranger_admin_xml_config.py"
USER_PASSWORD_BOOTSTRAP_HELPER="/home/ranger/scripts/user_password_bootstrap.py"
SERVICES_MARKER="/opt/ranger/.rangeradminservicescreated"

# if kerberos is enabled, below keytabs must be mounted:
# rangeradmin.keytab, rangerlookup.keytab, HTTP.keytab, and testusers.keytab

sync_admin_configs() {
  local conf_file
  mkdir -p "${CONF_DIR}"

  for conf_file in \
    "ranger-admin-site.xml" \
    "core-site.xml" \
    "ranger-admin-default-site.xml"; do
    if [ -f "${CONFIGS_DIR}/${conf_file}" ]; then
      cp -f "${CONFIGS_DIR}/${conf_file}" "${CONF_DIR}/${conf_file}"
    fi
  done
}

xml_prop() {
  local key="$1"
  local file="${2:-${CONFIG_XML_DEST}}"

  [ -f "${file}" ] || return 0
  python3 "${ADMIN_XML_HELPER}" get-property --file "${file}" --name "${key}"
}

get_config_value() {
  local env_key="$1"
  local xml_key="$2"
  local default_value="${3:-}"
  local value="${!env_key:-}"

  if [ -n "${value}" ]; then
    printf '%s\n' "${value}"
    return 0
  fi

  value="$(xml_prop "${xml_key}")"
  if [ -n "${value}" ]; then
    printf '%s\n' "${value}"
    return 0
  fi

  printf '%s\n' "${default_value}"
}

db_config_field() {
  local field="$1"
  python3 "${ADMIN_XML_HELPER}" get-db-field --file "${CONFIG_XML_DEST}" --field "${field}"
}

sync_db_password_property() {
  local pass="${RANGER_ADMIN_DB_PASSWORD:-}"

  if [ -z "${pass}" ] || [ ! -f "${CONFIG_XML_DEST}" ]; then
    return 0
  fi

  python3 "${ADMIN_XML_HELPER}" set-property \
    --file "${CONFIG_XML_DEST}" \
    --name "ranger.jpa.jdbc.password" \
    --value "${pass}" \
    --create
}

ensure_jdbc_driver() {
  # ensure the JDBC driver is visible to the webapp classloader.
  local src
  src="$(get_config_value "SQL_CONNECTOR_JAR" "ranger.jdbc.sqlconnectorjar" "/usr/share/java/postgresql.jar")"
  local libdir="${RANGER_ADMIN_DIR}/ews/webapp/WEB-INF/lib"

  if [ -f "${src}" ]; then
    mkdir -p "${libdir}" 2>/dev/null || true
    if [ ! -f "${libdir}/$(basename "${src}")" ]; then
      cp -f "${src}" "${libdir}/" 2>/dev/null || true
    fi
  fi
}

prepare_admin_runtime() {
  local log_dir="${RANGER_ADMIN_LOG_DIR:-/var/log/ranger}"
  local logback_conf="${RANGER_ADMIN_LOGBACK_CONF_FILE:-${CONF_DIR}/logback.xml}"
  local pid_dir="${RANGER_PID_DIR_PATH:-/var/run/ranger}"
  local env_logdir="${CONF_DIR}/ranger-admin-env-logdir.sh"
  local env_logback="${CONF_DIR}/ranger-admin-env-logback-conf-file.sh"
  local legacy_log_dir="${RANGER_ADMIN_DIR}/ews/logs"
  local conf_dist_dir="${RANGER_ADMIN_DIR}/ews/webapp/WEB-INF/classes/conf.dist"

  export RANGER_ADMIN_LOG_DIR="${log_dir}"
  export RANGER_ADMIN_LOGBACK_CONF_FILE="${logback_conf}"
  export RANGER_PID_DIR_PATH="${pid_dir}"

  mkdir -p "${CONF_DIR}" "${log_dir}" "${pid_dir}"

  for conf_file in "security-applicationContext.xml" "logback.xml"; do
    if [ -f "${conf_dist_dir}/${conf_file}" ]; then
      cp -f "${conf_dist_dir}/${conf_file}" "${CONF_DIR}/${conf_file}"
    fi
  done

  if [ ! -e "${legacy_log_dir}" ]; then
    ln -s "${log_dir}" "${legacy_log_dir}" 2>/dev/null || mkdir -p "${legacy_log_dir}"
  fi

  printf 'export RANGER_ADMIN_LOG_DIR=%s\n' "${RANGER_ADMIN_LOG_DIR}" > "${env_logdir}"
  chmod 755 "${env_logdir}"

  printf 'export RANGER_ADMIN_LOGBACK_CONF_FILE=%s\n' "${RANGER_ADMIN_LOGBACK_CONF_FILE}" > "${env_logback}"
  chmod 755 "${env_logback}"
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

port_open() {
  local host="$1"
  local port="$2"

  if command -v nc >/dev/null 2>&1; then
    nc -z -w 2 "${host}" "${port}" >/dev/null 2>&1
    return $?
  fi

  # Fallback: bash /dev/tcp (may be disabled in some environments)
  (exec 3<>"/dev/tcp/${host}/${port}") >/dev/null 2>&1
}

check_db_ready() {
  local flavor="$1"
  local host="$2"
  local port="$3"
  local db="$4"
  local user="$5"
  local pass="$6"

  case "${flavor}" in
    POSTGRES|postgres|Postgres|POSTGRESQL|postgresql)
      if command -v pg_isready >/dev/null 2>&1; then
        PGPASSWORD="${pass}" pg_isready -h "${host}" -p "${port}" -U "${user}" -d "${db}" >/dev/null 2>&1
        return $?
      fi
      if command -v psql >/dev/null 2>&1; then
        PGPASSWORD="${pass}" psql "host=${host} port=${port} user=${user} dbname=${db} sslmode=disable" \
          -v ON_ERROR_STOP=1 -tAc "select 1" >/dev/null 2>&1
        return $?
      fi
      port_open "${host}" "${port}"
      return $?
      ;;
    MYSQL|mysql|MySQL|MARIADB|mariadb)
      if command -v mysqladmin >/dev/null 2>&1; then
        MYSQL_PWD="${pass}" mysqladmin ping -h "${host}" -P "${port}" -u "${user}" --silent >/dev/null 2>&1
        return $?
      fi
      if command -v mysql >/dev/null 2>&1; then
        MYSQL_PWD="${pass}" mysql -h "${host}" -P "${port}" -u "${user}" -D "${db}" -e "select 1" >/dev/null 2>&1
        return $?
      fi
      port_open "${host}" "${port}"
      return $?
      ;;
    *)
      port_open "${host}" "${port}"
      return $?
      ;;
  esac
}

wait_for_db_ready_or_timeout() {
  local timeout_s="${1:-600}"

  local flavor host port db user pass
  flavor="${DB_FLAVOR:-$(db_config_field flavor)}"
  host="${RANGER_ADMIN_DB_HOSTNAME:-$(db_config_field host)}"
  port="${RANGER_ADMIN_DB_PORT:-$(db_config_field port)}"
  db="${RANGER_ADMIN_DB_DATABASE:-$(db_config_field database)}"
  user="${RANGER_ADMIN_DB_USERNAME:-$(db_config_field user)}"
  pass="${RANGER_ADMIN_DB_PASSWORD:-}"

  if [ -z "${host}" ] || [ -z "${port}" ]; then
    echo "WARNING: DB host/port not configured in ranger-admin-site.xml; skipping DB wait" >&2
    return 0
  fi
  if [ -z "${flavor}" ]; then
    flavor="POSTGRES"
  fi

  echo "Waiting for DB connectivity (flavor=${flavor} host=${host} port=${port} db=${db:-<unset>}) with timeout ${timeout_s}s" >&2

  local start now elapsed
  start="$(date +%s)"

  while true; do
    if check_db_ready "${flavor}" "${host}" "${port}" "${db:-postgres}" "${user:-postgres}" "${pass:-}"; then
      echo "DB is reachable" >&2
      return 0
    fi

    now="$(date +%s)"
    elapsed=$(( now - start ))
    if [ "${elapsed}" -ge "${timeout_s}" ]; then
      echo "ERROR: Timed out after ${timeout_s}s waiting for DB connectivity (flavor=${flavor} host=${host} port=${port} db=${db:-<unset>})" >&2
      return 1
    fi

    echo "Waiting for DB connectivity... elapsed=${elapsed}s remaining=$(( timeout_s - elapsed ))s" >&2
    sleep 5
  done
}

cd "${RANGER_ADMIN_DIR}"
sync_admin_configs
sync_db_password_property
ensure_jdbc_driver
prepare_admin_runtime

wait_for_db_ready_or_timeout 600
python3 "/home/ranger/scripts/dba.py"
./ews/ranger-admin-services.sh start

if [ ! -f "${SERVICES_MARKER}" ]; then
  if wait_for_admin 240; then
    if python3 "${USER_PASSWORD_BOOTSTRAP_HELPER}"; then
      if python3 "/home/ranger/scripts/create_services.py"; then
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
