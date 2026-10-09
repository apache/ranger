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

"""
Authorization is asserted via beeline output only. Access-audit asserts are deferred until
Hive plugin audits are reliably indexed in Docker — use ranger_test_utils.access_audit_utils
when ready.

AUDIT_FILTERS (future):
  test_01_01: accessType=CREATE, accessResult=0, aclEnforcer=ranger-acl,
              resourcePath=<db>/<table>, repoName=dev_hive, requestUser=hrt_21
  test_01_02: accessType=USE, accessResult=0, aclEnforcer=ranger-acl,
              resourcePath=<db>, repoName=dev_hive, requestUser=hrt_21
"""

import pytest

from ranger_test_utils.hive_utils import (
    HIVE_AUTHZ_DENIED_SUBSTR,
    HIVE_SERVICE_NAME,
    HIVE_TEST_USER,
    cleanup_hive_test_namespace,
    create_audit_only_policy,
    delete_policies_by_id,
    ensure_hive_test_user_in_ranger,
    ensure_kerberos_user,
    preflight_hive_stack,
    prepare_hive_test_namespace,
    run_beeline,
    set_all_policies_enabled,
    wait_for_policy_propagation,
)
from ranger_test_utils.utils import (
    configure_test_logging,
    create_ranger_admin_session,
    get_test_logger,
    log_testcase_begin_for_pytest,
    unique_suffix,
)

pytestmark = pytest.mark.hive

logger = get_test_logger(__name__)

_admin_session = None
_audit_policy_id = None
_audit_suffix = None
_run_suffix = None
_test_database = None
_test_table = None


def setup_module():
    configure_test_logging(module_name=__name__)
    logger.info("=== setup_module: start ===")
    preflight_hive_stack()

    global _admin_session, _audit_policy_id, _audit_suffix
    global _run_suffix, _test_database, _test_table
    _admin_session = create_ranger_admin_session()
    _run_suffix = unique_suffix(4)
    _test_database = "db_" + _run_suffix
    _test_table = "employee_" + _run_suffix
    logger.info(
        "Test resources: database=%s table=%s",
        _test_database,
        _test_table,
    )

    logger.info("Ensuring Ranger user and Kerberos principal for %s", HIVE_TEST_USER)
    ensure_hive_test_user_in_ranger(_admin_session)
    ensure_kerberos_user(HIVE_TEST_USER)

    logger.info(
        "Hive service setup: create database %s and drop table %s.%s if present",
        _test_database,
        _test_database,
        _test_table,
    )
    prepare_hive_test_namespace(_test_database, _test_table)

    logger.info("Disabling all policies on %s and creating audit-only policy", HIVE_SERVICE_NAME)
    set_all_policies_enabled(_admin_session, False, HIVE_SERVICE_NAME)
    _audit_policy_id, _audit_suffix = create_audit_only_policy(_admin_session)
    logger.info("Created audit policy id=%s suffix=%s", _audit_policy_id, _audit_suffix)
    wait_for_policy_propagation()
    logger.info("=== setup_module: done ===")


def teardown_module():
    if _admin_session is None:
        return

    logger.info("=== teardown_module: start ===")
    if _test_database is not None and _test_table is not None:
        logger.info(
            "Hive service cleanup: drop %s.%s and database %s",
            _test_database,
            _test_table,
            _test_database,
        )
        cleanup_hive_test_namespace(_test_database, _test_table)

    if _audit_policy_id is not None:
        logger.info("Deleting audit policy id=%s", _audit_policy_id)
        delete_policies_by_id(_admin_session, [_audit_policy_id])

    logger.info("Re-enabling all policies on %s", HIVE_SERVICE_NAME)
    set_all_policies_enabled(_admin_session, True, HIVE_SERVICE_NAME)
    wait_for_policy_propagation()
    logger.info("=== teardown_module: done ===")


def test_01_01_table_create_employee_no_policy_as_user1_deny(request):
    log_testcase_begin_for_pytest(logger, request.node.name)
    sql = (
        "create table "
        + _test_database
        + "."
        + _test_table
        + "(name String);"
    )
    logger.info(
        "Running beeline CREATE TABLE as %s (expect deny): %s",
        HIVE_TEST_USER,
        sql,
    )
    exit_code, output = run_beeline(
        HIVE_TEST_USER,
        sql,
        expect_substring=HIVE_AUTHZ_DENIED_SUBSTR,
    )
    assert HIVE_AUTHZ_DENIED_SUBSTR in output, (
        "Expected authorization denial in beeline output; exit_code="
        + str(exit_code)
        + " output="
        + output
    )


def test_01_02_database_use_default_no_policy_as_user1_deny(request):
    log_testcase_begin_for_pytest(logger, request.node.name)
    sql = "use " + _test_database + ";"
    logger.info(
        "Running beeline USE database as %s (expect deny): %s",
        HIVE_TEST_USER,
        sql,
    )
    exit_code, output = run_beeline(
        HIVE_TEST_USER,
        sql,
        expect_substring=HIVE_AUTHZ_DENIED_SUBSTR,
    )
    assert HIVE_AUTHZ_DENIED_SUBSTR in output, (
        "Expected authorization denial in beeline output; exit_code="
        + str(exit_code)
        + " output="
        + output
    )
