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

# Fresh-install and 077->078 upgrade E2E for patch 078 (all docker DB backends).
#
# Usage (from dev-support/ranger-docker):
#   ./scripts/db-test/test-patch-078-e2e-docker.sh
#   ./scripts/db-test/test-patch-078-e2e-docker.sh postgres mysql

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RANGER_DOCKER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPO_ROOT="$(cd "${RANGER_DOCKER_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
source "${RANGER_DOCKER_DIR}/.env"

DB_TYPES=("$@")
if [ "${#DB_TYPES[@]}" -eq 0 ]; then
  DB_TYPES=(postgres mysql oracle sqlserver)
fi

DIST_TARBALL="${RANGER_DOCKER_DIR}/dist/ranger-${RANGER_VERSION}-admin.tar.gz"
MASTER_TARBALL="${RANGER_DOCKER_DIR}/dist/ranger-${RANGER_VERSION}-admin-master.tar.gz"
BRANCH_TARBALL="${RANGER_DOCKER_DIR}/dist/ranger-${RANGER_VERSION}-admin-branch.tar.gz"

build_admin_tarball_with_db() {
  local db_dir="$1"
  local out_tarball="$2"
  local base_tarball="${DIST_TARBALL}"

  if [ ! -f "${base_tarball}" ]; then
    echo "[error] Base admin tarball missing: ${base_tarball}" >&2
    return 1
  fi

  local workdir
  workdir="$(mktemp -d)"
  tar -xzf "${base_tarball}" -C "${workdir}"
  local admin_dir
  admin_dir="$(find "${workdir}" -maxdepth 1 -type d -name 'ranger-*-admin' | head -1)"
  chmod -R u+w "${admin_dir}"
  rsync -a --delete --exclude='._*' --exclude='.DS_Store' "${db_dir}/" "${admin_dir}/db/"
  cp "${REPO_ROOT}/security-admin/scripts/dba_script.py" "${admin_dir}/dba_script.py"
  COPYFILE_DISABLE=1 tar -czf "${out_tarball}" -C "${workdir}" "$(basename "${admin_dir}")"
  rm -rf "${workdir}"
  echo "[ok] Built ${out_tarball}"
}

fix_master_sqlserver_schema() {
  local master_db="$1"
  local schema="${master_db}/sqlserver/optimized/current/ranger_core_db_sqlserver.sql"
  # master @077: patch 077 dropped x_policy_ref_user audit FKs but left an orphaned
  # CHECK CONSTRAINT line in optimized schema (fixed on branch in RANGER-5720).
  if [ -f "${schema}" ]; then
    sed -i '' '/x_policy_ref_user\] CHECK CONSTRAINT \[x_policy_ref_user_FK_upd_by\]/d' "${schema}" 2>/dev/null \
      || sed -i '/x_policy_ref_user\] CHECK CONSTRAINT \[x_policy_ref_user_FK_upd_by\]/d' "${schema}"
  fi
}

prepare_tarballs() {
  echo "[prep] Building master (077) and branch (078) admin tarballs..."
  local master_db="${RANGER_DOCKER_DIR}/.patch078-master-db"
  local branch_db="${REPO_ROOT}/security-admin/db"
  rm -rf "${master_db}"
  mkdir -p "${master_db}"
  git -C "${REPO_ROOT}" archive master:security-admin/db | tar -x -C "${master_db}"
  fix_master_sqlserver_schema "${master_db}"
  build_admin_tarball_with_db "${master_db}" "${MASTER_TARBALL}"
  build_admin_tarball_with_db "${branch_db}" "${BRANCH_TARBALL}"
  cp "${BRANCH_TARBALL}" "${DIST_TARBALL}"
}

use_tarball() {
  cp "$1" "${DIST_TARBALL}"
}

teardown_stack() {
  local db_type="$1"
  echo "[teardown] ${db_type} stack..."
  export RANGER_DB_TYPE="${db_type}"
  docker compose -f "${RANGER_DOCKER_DIR}/docker-compose.ranger.yml" down -v --remove-orphans 2>/dev/null || true
  docker rm -f ranger ranger-postgres ranger-mysql ranger-oracle ranger-sqlserver \
    ranger-kdc ranger-zk ranger-solr ranger-db 2>/dev/null || true
  docker network rm rangernw 2>/dev/null || true
  sleep 2
}

wait_for_db_setup() {
  local db_type="$1"
  export RANGER_DB_TYPE="${db_type}"
  local max_wait=90
  case "${db_type}" in
    oracle|sqlserver) max_wait=120 ;;
  esac
  local i
  for i in $(seq 1 "${max_wait}"); do
    if "${SCRIPT_DIR}/verify-patch-078.sh" >/dev/null 2>&1; then
      if "${SCRIPT_DIR}/verify-patch-078.sh"; then
        echo "  verified after ~$((i * 10))s"
        return 0
      fi
    fi
    if [ "${i}" -eq "${max_wait}" ]; then
      echo "[error] verification timed out; last verify attempt:"
      "${SCRIPT_DIR}/verify-patch-078.sh" || true
      docker logs ranger 2>&1 | tail -20 || true
      return 1
    fi
    sleep 10
  done
}

wait_for_master_at_077() {
  local db_type="$1"
  export RANGER_DB_TYPE="${db_type}"
  local max_wait=120
  case "${db_type}" in
    oracle|sqlserver) max_wait=150 ;;
  esac
  local i patch077=""
  for i in $(seq 1 "${max_wait}"); do
    patch077=""
    case "${db_type}" in
      postgres)
        patch077="$(docker exec ranger-postgres psql -U rangeradmin -d ranger -t -A -c "SELECT COALESCE(active,'') FROM x_db_version_h WHERE version='077';" 2>/dev/null | tr -d '\r')"
        ;;
      mysql)
        patch077="$(docker exec ranger-mysql mysql -urangeradmin -prangerR0cks! ranger -N -s -e "SELECT COALESCE(active,'') FROM x_db_version_h WHERE version='077';" 2>/dev/null || true)"
        ;;
      oracle)
        patch077="$(docker exec ranger-oracle bash -lc "sqlplus -s rangeradmin/rangerR0cks!@//localhost:1521/FREEPDB1 <<'EOSQL'
SET HEADING OFF FEEDBACK OFF PAGES 0
SELECT NVL(active,' ') FROM x_db_version_h WHERE version='077';
EXIT
EOSQL" 2>/dev/null | grep -v '^$' | tail -1 | tr -d '\r\t' | xargs || true)"
        ;;
      sqlserver)
        patch077="$(docker exec ranger-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U rangeradmin -P rangerR0cks! -d ranger -C -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(active,'') FROM x_db_version_h WHERE version='077'" 2>/dev/null | tr -d '\r' | sed '/^$/d' | head -1)"
        ;;
    esac
    if [ "${patch077}" = "Y" ]; then
      echo "  master DB at patch 077 after ~$((i * 10))s"
      return 0
    fi
    if [ "${i}" -eq "${max_wait}" ]; then
      echo "[error] master install did not reach patch 077"
      docker logs ranger 2>&1 | tail -30
      return 1
    fi
    sleep 10
  done
}

start_stack() {
  local db_type="$1"
  export RANGER_DB_TYPE="${db_type}"
  export KERBEROS_ENABLED=false
  teardown_stack "${db_type}"
  docker compose -f "${RANGER_DOCKER_DIR}/docker-compose.ranger.yml" \
    build ranger-db ranger ranger-kdc ranger-zk
  docker compose -f "${RANGER_DOCKER_DIR}/docker-compose.ranger.yml" up -d
}

verify_patch_077_only() {
  local db_type="$1"
  export RANGER_DB_TYPE="${db_type}"
  local patch078=""
  case "${db_type}" in
    postgres)
      patch078="$(docker exec ranger-postgres psql -U rangeradmin -d ranger -t -A -c "SELECT COALESCE(active,'') FROM x_db_version_h WHERE version='078';" 2>/dev/null | tr -d '\r')"
      ;;
    mysql)
      patch078="$(docker exec ranger-mysql mysql -urangeradmin -prangerR0cks! ranger -N -s -e "SELECT COALESCE(active,'') FROM x_db_version_h WHERE version='078';" 2>/dev/null || true)"
      ;;
    oracle)
      patch078="$(docker exec ranger-oracle bash -lc "sqlplus -s rangeradmin/rangerR0cks!@//localhost:1521/FREEPDB1 <<'EOSQL'
SET HEADING OFF FEEDBACK OFF PAGES 0
SELECT NVL(active,' ') FROM x_db_version_h WHERE version='078';
EXIT
EOSQL" 2>/dev/null | grep -v '^$' | tail -1 | tr -d '\r\t' | xargs || true)"
      ;;
    sqlserver)
      patch078="$(docker exec ranger-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U rangeradmin -P rangerR0cks! -d ranger -C -h -1 -W -Q "SET NOCOUNT ON; SELECT ISNULL(active,'') FROM x_db_version_h WHERE version='078'" 2>/dev/null | tr -d '\r' | sed '/^$/d' | head -1)"
      ;;
  esac
  if [ "${patch078}" = "Y" ]; then
    echo "[error] patch 078 already applied before upgrade step" >&2
    return 1
  fi
  echo "  [ok] patch 078 not yet applied (expected pre-upgrade)"
}

clear_patch_078_marker() {
  local db_type="$1"
  case "${db_type}" in
    postgres)
      docker exec ranger-postgres psql -U rangeradmin -d ranger -c "DELETE FROM x_db_version_h WHERE version='078';"
      ;;
    mysql)
      docker exec ranger-mysql mysql -urangeradmin -prangerR0cks! ranger -e "DELETE FROM x_db_version_h WHERE version='078';"
      ;;
    oracle)
      docker exec ranger-oracle bash -lc "sqlplus -s rangeradmin/rangerR0cks!@//localhost:1521/FREEPDB1 <<'EOSQL'
DELETE FROM x_db_version_h WHERE version='078';
COMMIT;
EXIT
EOSQL"
      ;;
    sqlserver)
      docker exec ranger-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U rangeradmin -P rangerR0cks! -d ranger -C -Q "DELETE FROM x_db_version_h WHERE version='078';"
      ;;
  esac
}

clear_db_patches_marker() {
  local db_type="$1"
  case "${db_type}" in
    postgres)
      docker exec ranger-postgres psql -U rangeradmin -d ranger -c "DELETE FROM x_db_version_h WHERE version='DB_PATCHES';"
      ;;
    mysql)
      docker exec ranger-mysql mysql -urangeradmin -prangerR0cks! ranger -e "DELETE FROM x_db_version_h WHERE version='DB_PATCHES';"
      ;;
    oracle)
      docker exec ranger-oracle bash -lc "sqlplus -s rangeradmin/rangerR0cks!@//localhost:1521/FREEPDB1 <<'EOSQL'
DELETE FROM x_db_version_h WHERE version='DB_PATCHES';
COMMIT;
EXIT
EOSQL"
      ;;
    sqlserver)
      docker exec ranger-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U rangeradmin -P rangerR0cks! -d ranger -C -Q "DELETE FROM x_db_version_h WHERE version='DB_PATCHES';"
      ;;
  esac
}

run_db_setup_in_container() {
  docker exec ranger bash -lc '
    set -e
    ADMIN=$(find /opt/ranger -maxdepth 2 -type d -name "ranger-*-admin" | head -1)
    cd "${ADMIN}"
    python3 db_setup.py
  '
}

apply_upgrade_in_container() {
  local branch_db="$1"
  local db_type="$2"
  local admin_path patch_file="${branch_db}/${db_type}/patches/078-add-x_audit_config.sql"
  admin_path="$(docker exec ranger bash -lc 'find /opt/ranger -maxdepth 2 -type d -name "ranger-*-admin" | head -1')"
  if [ ! -f "${patch_file}" ]; then
    echo "[error] missing patch file: ${patch_file}" >&2
    return 1
  fi
  docker cp "${patch_file}" "ranger:${admin_path}/db/${db_type}/patches/078-add-x_audit_config.sql"
  docker exec ranger bash -lc "rm -f ${admin_path}/db/${db_type}/patches/078-audit-partition-plan-global-state.sql ${admin_path}/db/${db_type}/patches/._078* 2>/dev/null || true"
  clear_patch_078_marker "${db_type}"
  clear_db_patches_marker "${db_type}"
  run_db_setup_in_container
}

test_fresh_install() {
  local db_type="$1"
  echo ""
  echo "======== Fresh install: ${db_type} ========"
  use_tarball "${BRANCH_TARBALL}"
  start_stack "${db_type}"
  wait_for_db_setup "${db_type}"
  teardown_stack "${db_type}"
}

test_upgrade() {
  local db_type="$1"
  echo ""
  echo "======== Upgrade 077->078: ${db_type} ========"
  use_tarball "${MASTER_TARBALL}"
  start_stack "${db_type}"
  wait_for_master_at_077 "${db_type}" || { teardown_stack "${db_type}"; return 1; }
  verify_patch_077_only "${db_type}" || { teardown_stack "${db_type}"; return 1; }
  apply_upgrade_in_container "${REPO_ROOT}/security-admin/db" "${db_type}" || { teardown_stack "${db_type}"; return 1; }
  wait_for_db_setup "${db_type}" || { teardown_stack "${db_type}"; return 1; }
  teardown_stack "${db_type}"
}

prepare_tarballs

FRESH_FAILED=()
UPGRADE_FAILED=()

if [ "${SKIP_FRESH:-0}" != "1" ]; then
  for db in "${DB_TYPES[@]}"; do
    if ! test_fresh_install "${db}"; then
      FRESH_FAILED+=("${db}")
    fi
  done
else
  echo "[skip] Fresh install tests skipped (SKIP_FRESH=1)"
fi

for db in "${DB_TYPES[@]}"; do
  if ! test_upgrade "${db}"; then
    UPGRADE_FAILED+=("${db}")
  fi
done

echo ""
echo "=========================================="
if [ "${SKIP_FRESH:-0}" != "1" ]; then
  echo "Fresh install: $([ ${#FRESH_FAILED[@]} -eq 0 ] && echo PASS || echo "FAIL (${FRESH_FAILED[*]})")"
fi
echo "Upgrade 077->078: $([ ${#UPGRADE_FAILED[@]} -eq 0 ] && echo PASS || echo "FAIL (${UPGRADE_FAILED[*]})")"
echo "=========================================="

if [ "${SKIP_FRESH:-0}" != "1" ] && [ "${#FRESH_FAILED[@]}" -ne 0 ]; then
  exit 1
fi
if [ "${#UPGRADE_FAILED[@]}" -ne 0 ]; then
  exit 1
fi
