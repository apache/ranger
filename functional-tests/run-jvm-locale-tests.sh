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

set -u

DB_TYPE="${1:-}"
JVM_LOCALE_INPUT="${2:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RANGER_DOCKER_PATH="${SCRIPT_DIR}/../dev-support/ranger-docker"
JVM_LOCALE_TEST_SUITE="jvm_locale_test"
JVM_LOCALE_JAVA_OPTS=""
AUDIT_INDEX_STORE="${AUDIT_INDEX_STORE:-none}"
SELECTED_CONTAINERS="${SELECTED_CONTAINERS:-none}"

_prompt_inputs() {
  if [[ -z "${DB_TYPE}" ]]; then
    echo "Available DB types: postgres, mysql, oracle"
    printf "Enter DB type (press Enter to default to postgres): "
    read -r DB_TYPE
    DB_TYPE="${DB_TYPE:-postgres}"
  fi

  if [[ -z "${JVM_LOCALE_INPUT}" && -z "${JVM_LOCALE:-}" ]]; then
    echo
    echo "Available JVM locales: us, uk, france, china"
    printf "Enter JVM locale (press Enter to default to france): "
    read -r JVM_LOCALE
    JVM_LOCALE="${JVM_LOCALE:-france}"
  else
    JVM_LOCALE="${JVM_LOCALE_INPUT:-${JVM_LOCALE:-france}}"
  fi
}

_validate_db_type() {
  case "${DB_TYPE}" in
    postgres|mysql|oracle)
      ;;
    *)
      echo "ERROR: Unsupported DB type '${DB_TYPE}'. Use one of: postgres, mysql, oracle."
      return 1
      ;;
  esac
}

_resolve_jvm_locale() {
  case "$(printf '%s' "${JVM_LOCALE}" | tr '[:upper:]' '[:lower:]')" in
    us|usa|en_us)
      JVM_LOCALE_LANGUAGE="en"
      JVM_LOCALE_COUNTRY="US"
      ;;
    uk|gb|britain|en_gb)
      JVM_LOCALE_LANGUAGE="en"
      JVM_LOCALE_COUNTRY="GB"
      ;;
    fr|fra|france)
      JVM_LOCALE_LANGUAGE="fr"
      JVM_LOCALE_COUNTRY="FR"
      ;;
    china|cn|zh|zh_cn)
      JVM_LOCALE_LANGUAGE="zh"
      JVM_LOCALE_COUNTRY="CN"
      ;;
    *)
      echo "ERROR: Unsupported JVM locale '${JVM_LOCALE}'. Use one of: us, uk, france, china."
      return 1
      ;;
  esac

  JVM_LOCALE_JAVA_OPTS="-Duser.language=${JVM_LOCALE_LANGUAGE} -Duser.country=${JVM_LOCALE_COUNTRY}"

  export JVM_LOCALE_EXPECTED_LANGUAGE="${JVM_LOCALE_LANGUAGE}"
  export JVM_LOCALE_EXPECTED_COUNTRY="${JVM_LOCALE_COUNTRY}"
}

_base_java_opts() {
  local value="${JAVA_OPTS:-}"
  local env_file="${RANGER_DOCKER_PATH}/.env"

  if [[ -z "${value}" && -f "${env_file}" ]]; then
    value="$(awk -F= '/^JAVA_OPTS=/{sub(/^JAVA_OPTS="/,""); sub(/"$/,""); print; exit}' "${env_file}")"
  fi

  printf '%s' "${value}"
}

_wait_for_ranger_admin() {
  local timeout="${1:-240}"
  local deadline=$((SECONDS + timeout))
  local password="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"

  echo "Waiting for Ranger Admin to be ready..."
  while (( SECONDS < deadline )); do
    if docker exec ranger bash -c "curl -sf -u admin:${password} http://localhost:6080/service/xusers/users/userName/admin >/dev/null" 2>/dev/null; then
      echo "Ranger Admin is ready."
      return 0
    fi
    sleep 5
  done

  echo "ERROR: Ranger Admin did not become ready within ${timeout} seconds."
  return 1
}

_recreate_ranger() {
  local java_opts="$1"
  local reason="$2"
  local compose_status=0
  local compose_files=(
    "-f" "docker-compose.ranger.yml"
    "-f" "docker-compose.ranger-usersync.yml"
    "-f" "docker-compose.ranger-tagsync.yml"
    "-f" "docker-compose.ranger-pdp.yml"
    "-f" "docker-compose.ranger-kms.yml"
  )

  if [[ "${AUDIT_INDEX_STORE}" != "none" ]]; then
    compose_files+=("-f" "docker-compose.ranger-audit-service.yml")
  fi

  echo "Recreating ranger container (${reason})..."
  cd "${RANGER_DOCKER_PATH}" || return 1

  if [[ "${AUDIT_INDEX_STORE}" != "none" ]]; then
    RANGER_DB_TYPE="${DB_TYPE}" JAVA_OPTS="${java_opts}" docker compose --profile "audit-store-${AUDIT_INDEX_STORE}" "${compose_files[@]}" up -d --force-recreate --no-deps ranger
    compose_status=$?
  else
    RANGER_DB_TYPE="${DB_TYPE}" JAVA_OPTS="${java_opts}" docker compose "${compose_files[@]}" up -d --force-recreate --no-deps ranger
    compose_status=$?
  fi

  if [[ "${compose_status}" -ne 0 ]]; then
    return "${compose_status}"
  fi

  _wait_for_ranger_admin
}

_ensure_test_env() {
  cd "${SCRIPT_DIR}" || return 1

  if [[ ! -d "myenv" ]]; then
    python3 -m venv myenv || return 1
    source myenv/bin/activate || return 1
    pip install --upgrade pip || return 1
  else
    source myenv/bin/activate || return 1
  fi

  pip install -q -r requirements.txt
}

_prompt_inputs
_validate_db_type || exit 1
_resolve_jvm_locale || exit 1

BASE_JAVA_OPTS="$(_base_java_opts)"
LOCALE_JAVA_OPTS="${BASE_JAVA_OPTS} ${JVM_LOCALE_JAVA_OPTS}"
RESTORE_NEEDED=0

_restore_ranger_on_exit() {
  local status=$?
  local restore_status=0

  if [[ "${RESTORE_NEEDED}" -eq 1 ]]; then
    _recreate_ranger "${BASE_JAVA_OPTS}" "restoring default JVM locale"
    restore_status=$?
  fi

  if [[ "${status}" -eq 0 ]]; then
    exit "${restore_status}"
  fi

  exit "${status}"
}

trap _restore_ranger_on_exit EXIT

echo "Bringing up base Ranger services..."
echo "DB Type           : ${DB_TYPE}"
echo "JVM Locale        : ${JVM_LOCALE} (${JVM_LOCALE_JAVA_OPTS})"
RUN_TESTS=0 \
AUDIT_INDEX_STORE="${AUDIT_INDEX_STORE}" \
SELECTED_CONTAINERS="${SELECTED_CONTAINERS}" \
JAVA_OPTS="${BASE_JAVA_OPTS}" \
bash "${SCRIPT_DIR}/run-tests.sh" "${DB_TYPE}" || exit 1

RESTORE_NEEDED=1
_recreate_ranger "${LOCALE_JAVA_OPTS}" "JVM locale settings" || exit 1
_ensure_test_env || exit 1

cd "${SCRIPT_DIR}" || exit 1
echo "Running ${JVM_LOCALE_TEST_SUITE} with Ranger Admin JVM locale: ${JVM_LOCALE_JAVA_OPTS}"
pytest -vs "${JVM_LOCALE_TEST_SUITE}/" --html="report_${JVM_LOCALE_TEST_SUITE}.html"
exit $?
