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

# Verify RANGER-5720 / patch 078 outcomes in a Ranger Admin database.
#
# Usage (from dev-support/ranger-docker):
#   export RANGER_DB_TYPE=postgres   # postgres|mysql|oracle|sqlserver
#   ./scripts/db-test/verify-patch-078.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RANGER_DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DB_TYPE="${RANGER_DB_TYPE:-postgres}"

PASS=0
FAIL=0

pass() { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }

check_eq() {
  local label="$1" expected="$2" actual="$3"
  if [ "${actual}" = "${expected}" ]; then
    pass "${label} (${actual})"
  else
    fail "${label} expected '${expected}', got '${actual}'"
  fi
}

check_contains() {
  local label="$1" needle="$2" haystack="$3"
  if echo "${haystack}" | grep -q "${needle}"; then
    pass "${label}"
  else
    fail "${label} (missing '${needle}' in '${haystack}')"
  fi
}

verify_audit_config_common() {
  check_eq "ingestor.url" "https://ranger-audit-ingestor:8765" "${INGESTOR_URL}"
  check_eq "service.hive.allowed.users" "hive" "${HIVE_USERS}"
  check_contains "audit.partition.plan JSON" "ranger_audits" "${PLAN_JSON}"
  check_contains "audit.partition.plan buffer" "buffer" "${PLAN_JSON}"
  if [ -n "${GLOBAL_PLAN}" ]; then
    fail "x_ranger_global_state partition plan row should be absent (got '${GLOBAL_PLAN}')"
  else
    pass "no x_ranger_global_state partition plan row"
  fi
}

echo "=== Patch 078 verification (${DB_TYPE}) ==="

case "${DB_TYPE}" in
  postgres)
    DB_CONTAINER="${DB_CONTAINER:-ranger-postgres}"
    run_sql() {
      docker exec "${DB_CONTAINER}" psql -U rangeradmin -d ranger -t -A -c "$1" 2>/dev/null | tr -d '\r'
    }
    STATUS="$(run_sql "SELECT status FROM x_portal_user WHERE login_id='rangerauditserver';")"
    PASSWORD="$(run_sql "SELECT COALESCE(password,'') FROM x_portal_user WHERE login_id='rangerauditserver';")"
    ROLE="$(run_sql "SELECT user_role FROM x_portal_user_role WHERE user_id=(SELECT id FROM x_portal_user WHERE login_id='rangerauditserver');")"
    INGESTOR_URL="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='ingestor.url';")"
    HIVE_USERS="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='service.hive.allowed.users';")"
    PLAN_JSON="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='audit.partition.plan';")"
    GLOBAL_PLAN="$(run_sql "SELECT state_name FROM x_ranger_global_state WHERE state_name='RangerAuditPartitionPlan';")"
    PATCH078="$(run_sql "SELECT active FROM x_db_version_h WHERE version='078';")"
    COLTYPE="$(run_sql "SELECT data_type FROM information_schema.columns WHERE table_name='x_ranger_global_state' AND column_name='app_data';")"
    check_eq "rangerauditserver status" "0" "${STATUS}"
    check_eq "rangerauditserver password" "" "${PASSWORD}"
    check_eq "rangerauditserver role" "ROLE_ADMIN_AUDITOR" "${ROLE}"
    verify_audit_config_common
    check_eq "patch 078 applied" "Y" "${PATCH078}"
    if [ "${COLTYPE}" = "character varying" ]; then
      pass "x_ranger_global_state.app_data type (${COLTYPE})"
    else
      fail "x_ranger_global_state.app_data type expected character varying, got '${COLTYPE}'"
    fi
    ;;

  mysql)
    DB_CONTAINER="${DB_CONTAINER:-ranger-mysql}"
    run_sql() {
      docker exec "${DB_CONTAINER}" mysql -urangeradmin -prangerR0cks! ranger -N -s -e "$1" 2>/dev/null
    }
    STATUS="$(run_sql "SELECT status FROM x_portal_user WHERE login_id='rangerauditserver';")"
    PASSWORD="$(run_sql "SELECT IFNULL(password,'') FROM x_portal_user WHERE login_id='rangerauditserver';")"
    ROLE="$(run_sql "SELECT user_role FROM x_portal_user_role WHERE user_id=(SELECT id FROM x_portal_user WHERE login_id='rangerauditserver');")"
    INGESTOR_URL="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='ingestor.url';")"
    HIVE_USERS="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='service.hive.allowed.users';")"
    PLAN_JSON="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='audit.partition.plan';")"
    GLOBAL_PLAN="$(run_sql "SELECT state_name FROM x_ranger_global_state WHERE state_name='RangerAuditPartitionPlan';")"
    PATCH078="$(run_sql "SELECT active FROM x_db_version_h WHERE version='078';")"
    COLTYPE="$(run_sql "SELECT data_type FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='x_ranger_global_state' AND column_name='app_data';")"
    check_eq "rangerauditserver status" "0" "${STATUS}"
    check_eq "rangerauditserver password" "" "${PASSWORD}"
    check_eq "rangerauditserver role" "ROLE_ADMIN_AUDITOR" "${ROLE}"
    verify_audit_config_common
    check_eq "patch 078 applied" "Y" "${PATCH078}"
    if [ "${COLTYPE}" = "varchar" ]; then
      pass "x_ranger_global_state.app_data type (${COLTYPE})"
    else
      fail "x_ranger_global_state.app_data type expected varchar, got '${COLTYPE}'"
    fi
    ;;

  oracle)
    DB_CONTAINER="${DB_CONTAINER:-ranger-oracle}"
    if ! docker ps --format '{{.Names}}' | grep -qx "${DB_CONTAINER}"; then
      fail "Oracle DB container ${DB_CONTAINER} is not running"
    else
    run_sql() {
      docker exec "${DB_CONTAINER}" bash -lc "sqlplus -s rangeradmin/rangerR0cks!@//localhost:1521/FREEPDB1 <<'EOSQL'
SET HEADING OFF FEEDBACK OFF PAGES 0 VERIFY OFF
WHENEVER SQLERROR EXIT SQL.SQLCODE
$1
EXIT
EOSQL" 2>/dev/null | grep -viE 'SP2-0157|ORA-[0-9]+|ERROR|Help:' | grep -v '^$' | tail -1 | tr -d '\r\t' | xargs || true
    }
    run_sql_clob() {
      docker exec "${DB_CONTAINER}" bash -lc "sqlplus -s rangeradmin/rangerR0cks!@//localhost:1521/FREEPDB1 <<'EOSQL'
SET HEADING OFF FEEDBACK OFF PAGES 0 LONG 4000 LONGCHUNKSIZE 4000 LINESIZE 32767 TRIMOUT ON
$1
EXIT
EOSQL" 2>/dev/null | grep -v '^$' | tr -d '\r\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || true
    }
    STATUS="$(run_sql "SELECT status FROM x_portal_user WHERE login_id='rangerauditserver';")"
    PASSWORD="$(run_sql "SELECT NVL(password,' ') FROM x_portal_user WHERE login_id='rangerauditserver';")"
    PASSWORD="$(echo "${PASSWORD}" | xargs)"
    [ -z "${PASSWORD}" ] || [ "${PASSWORD}" = " " ] && PASSWORD=""
    ROLE="$(run_sql "SELECT user_role FROM x_portal_user_role WHERE user_id=(SELECT id FROM x_portal_user WHERE login_id='rangerauditserver');")"
    INGESTOR_URL="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='ingestor.url';")"
    HIVE_USERS="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='service.hive.allowed.users';")"
    PLAN_JSON="$(run_sql_clob "SELECT DBMS_LOB.SUBSTR(cfg_value, 4000, 1) FROM x_audit_config WHERE cfg_name='audit.partition.plan';")"
    GLOBAL_PLAN="$(run_sql "SELECT state_name FROM x_ranger_global_state WHERE state_name='RangerAuditPartitionPlan';")"
    PATCH078="$(run_sql "SELECT active FROM x_db_version_h WHERE version='078';")"
    COLTYPE="$(run_sql "SELECT data_type FROM user_tab_columns WHERE table_name='X_RANGER_GLOBAL_STATE' AND column_name='APP_DATA';")"
    check_eq "rangerauditserver status" "0" "${STATUS}"
    check_eq "rangerauditserver password" "" "${PASSWORD}"
    check_eq "rangerauditserver role" "ROLE_ADMIN_AUDITOR" "${ROLE}"
    verify_audit_config_common
    check_eq "patch 078 applied" "Y" "${PATCH078}"
    if [ "${COLTYPE}" = "VARCHAR2" ]; then
      pass "x_ranger_global_state.app_data type (${COLTYPE})"
    else
      fail "x_ranger_global_state.app_data type expected VARCHAR2, got '${COLTYPE}'"
    fi
    fi
    ;;

  sqlserver)
    DB_CONTAINER="${DB_CONTAINER:-ranger-sqlserver}"
    run_sql() {
      docker exec "${DB_CONTAINER}" /opt/mssql-tools18/bin/sqlcmd -S localhost -U rangeradmin -P rangerR0cks! -d ranger -C -h -1 -W -Q "SET NOCOUNT ON; $1" 2>/dev/null | tr -d '\r' | sed '/^$/d' | head -1
    }
    STATUS="$(run_sql "SELECT status FROM x_portal_user WHERE login_id='rangerauditserver'")"
    PASSWORD="$(run_sql "SELECT ISNULL(password,'') FROM x_portal_user WHERE login_id='rangerauditserver'")"
    ROLE="$(run_sql "SELECT user_role FROM x_portal_user_role WHERE user_id=(SELECT id FROM x_portal_user WHERE login_id='rangerauditserver')")"
    INGESTOR_URL="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='ingestor.url'")"
    HIVE_USERS="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='service.hive.allowed.users'")"
    PLAN_JSON="$(run_sql "SELECT cfg_value FROM x_audit_config WHERE cfg_name='audit.partition.plan'")"
    GLOBAL_PLAN="$(run_sql "SELECT state_name FROM x_ranger_global_state WHERE state_name='RangerAuditPartitionPlan'")"
    PATCH078="$(run_sql "SELECT active FROM x_db_version_h WHERE version='078'")"
    COLTYPE="$(run_sql "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='x_ranger_global_state' AND COLUMN_NAME='app_data'")"
    check_eq "rangerauditserver status" "0" "${STATUS}"
    check_eq "rangerauditserver password" "" "${PASSWORD}"
    check_eq "rangerauditserver role" "ROLE_ADMIN_AUDITOR" "${ROLE}"
    verify_audit_config_common
    check_eq "patch 078 applied" "Y" "${PATCH078}"
    if [ "${COLTYPE}" = "varchar" ]; then
      pass "x_ranger_global_state.app_data type (${COLTYPE})"
    else
      fail "x_ranger_global_state.app_data type expected varchar, got '${COLTYPE}'"
    fi
    ;;

  sqlanywhere)
    echo "SQL Anywhere is not supported in ranger-docker (no DB container)."
    echo "Apply patch 078 manually with db_setup/jisql on a SQL Anywhere host."
    exit 2
    ;;

  *)
    echo "Unknown RANGER_DB_TYPE: ${DB_TYPE}" >&2
    exit 1
    ;;
esac

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="
[ "${FAIL}" -eq 0 ]
