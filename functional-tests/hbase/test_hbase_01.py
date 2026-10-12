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
Authorization is asserted via HBase shell output only. Access-audit asserts are deferred until
HBase plugin audits are reliably indexed in Docker — use ranger_test_utils.access_audit_utils
when ready.

AUDIT_FILTERS (future):
  test_01_01: accessType=createTable, accessResult=0, aclEnforcer=ranger-acl,
              resourceName=<table>, repoName=dev_hbase, requestUser=hrt_21
"""

import pytest

from ranger_test_utils.hbase_utils import (
    HBASE_AUTHZ_DENIED_SUBSTR,
    HBASE_SERVICE_NAME,
    HBASE_TEST_USER,
    cleanup_hbase_test_table,
    create_audit_only_policy,
    ensure_kerberos_user,
    preflight_hbase_stack,
    prepare_hbase_test_table,
    run_hbase_shell,
    wait_for_hbase_policy_propagation,
)
from ranger_test_utils.hive_utils import delete_policies_by_id, set_all_policies_enabled
from ranger_test_utils.utils import (
    DOCKER_PLUGIN_TEST_USER_PASSWORD,
    configure_test_logging,
    create_ranger_admin_session,
    ensure_user_exists,
    get_test_logger,
    log_testcase_begin_for_pytest,
    unique_suffix,
)

pytestmark = pytest.mark.hbase

logger = get_test_logger(__name__)

_admin_session = None
_audit_policy_id = None
_audit_suffix = None
_run_suffix = None
_test_table = None


def setup_module():
    configure_test_logging(module_name=__name__)
    logger.info("=== setup_module: start ===")
    preflight_hbase_stack()

    global _admin_session, _audit_policy_id, _audit_suffix
    global _run_suffix, _test_table
    _admin_session = create_ranger_admin_session()
    _run_suffix = unique_suffix(4)
    _test_table = "iemployee_" + _run_suffix
    logger.info("Test table: %s", _test_table)

    logger.info("Ensuring Ranger user and Kerberos principal for %s", HBASE_TEST_USER)
    ensure_user_exists(
        _admin_session,
        HBASE_TEST_USER,
        password=DOCKER_PLUGIN_TEST_USER_PASSWORD,
        last_name="hive_test",
    )
    ensure_kerberos_user(HBASE_TEST_USER)

    logger.info("HBase setup: drop table %s if present", _test_table)
    prepare_hbase_test_table(_test_table)

    logger.info(
        "Disabling all policies on %s and creating audit-only policy",
        HBASE_SERVICE_NAME,
    )
    set_all_policies_enabled(_admin_session, False, HBASE_SERVICE_NAME)
    _audit_policy_id, _audit_suffix = create_audit_only_policy(_admin_session)
    logger.info("Created audit policy id=%s suffix=%s", _audit_policy_id, _audit_suffix)
    wait_for_hbase_policy_propagation()
    logger.info("=== setup_module: done ===")


def teardown_module():
    if _admin_session is None:
        return

    logger.info("=== teardown_module: start ===")
    if _test_table is not None:
        logger.info("HBase cleanup: drop table %s", _test_table)
        cleanup_hbase_test_table(_test_table)

    if _audit_policy_id is not None:
        logger.info("Deleting audit policy id=%s", _audit_policy_id)
        delete_policies_by_id(_admin_session, [_audit_policy_id])

    logger.info("Re-enabling all policies on %s", HBASE_SERVICE_NAME)
    set_all_policies_enabled(_admin_session, True, HBASE_SERVICE_NAME)
    wait_for_hbase_policy_propagation()
    logger.info("=== teardown_module: done ===")


def test_01_01_create_hbase_table_case1a_deny(request):
    log_testcase_begin_for_pytest(logger, request.node.name, suite="HBASE")
    create_cmd = (
        "create '"
        + _test_table
        + "','personal','payroll','medical'"
    )
    logger.info(
        "Running HBase CREATE TABLE as %s (expect deny): %s",
        HBASE_TEST_USER,
        create_cmd,
    )
    exit_code, output = run_hbase_shell(
        HBASE_TEST_USER,
        [create_cmd],
        expect_substring=HBASE_AUTHZ_DENIED_SUBSTR,
    )
    assert HBASE_AUTHZ_DENIED_SUBSTR in output, (
        "Expected authorization denial in HBase shell output; exit_code="
        + str(exit_code)
        + " output="
        + output
    )
