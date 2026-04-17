#!/bin/bash

set -euo pipefail

RANGER_HOME="${RANGER_HOME:-/opt/ranger}"
RANGER_ADMIN_DIR="${RANGER_HOME}/admin"
USER_PASSWORD_BOOTSTRAP_HELPER="/home/ranger/scripts/user_password_bootstrap.py"
SERVICES_MARKER="/opt/ranger/.rangeradminservicescreated"

# if kerberos is enabled, below keytabs must be mounted:
# rangeradmin.keytab, rangerlookup.keytab, HTTP.keytab, and testusers.keytab

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

cd "${RANGER_ADMIN_DIR}"
configure_java_opts

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
