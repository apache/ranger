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
Hive plugin enforcement via beeline and optional access-audit validation.

Set ``AUDIT_INDEX_STORE`` to ``opensearch`` or ``solr`` when starting Docker
(see functional-tests readme) so access audits are indexed and asserted.
"""

import pytest

from ranger_test_utils.hive_utils import (
    HIVE_AUTHZ_DENIED_SUBSTR,
    HIVE_SERVICE_NAME,
    HivePluginTest,
    HivePluginTestContext,
    create_audit_only_policy,
    delete_policies_by_id,
    preflight_hive_stack,
    run_beeline,
    set_all_policies_enabled,
    wait_for_policy_propagation,
)
from ranger_test_utils.plugin_enforcement_utils import (
    configure_test_logging,
    create_ranger_admin_session,
    logger,
)

pytestmark = pytest.mark.hive

TEST_CASE = None

_admin_session = None
_audit_policy_id = None
_audit_suffix = None


def setup_module():
    configure_test_logging(module_name=__name__)
    logger.info("=== setup_module: start ===")
    preflight_hive_stack()

    global TEST_CASE, _admin_session, _audit_policy_id, _audit_suffix
    TEST_CASE = HivePluginTestContext()
    _admin_session = create_ranger_admin_session()
    TEST_CASE.admin_session = _admin_session

    logger.info(
        "Plugin user %s (Ranger + Kerberos) ensured by functional-tests/conftest.py",
        HivePluginTestContext.USER_USER1,
    )

    logger.info("Hive service setup: drop default.employee if present")
    run_beeline(None, "drop table if exists default.employee;", service_user=True)

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
    if _audit_policy_id is not None:
        logger.info("Deleting audit policy id=%s", _audit_policy_id)
        delete_policies_by_id(_admin_session, [_audit_policy_id])

    logger.info("Re-enabling all policies on %s", HIVE_SERVICE_NAME)
    set_all_policies_enabled(_admin_session, True, HIVE_SERVICE_NAME)
    wait_for_policy_propagation()
    logger.info("=== teardown_module: done ===")


@pytest.mark.parametrize(
    ("validateImpala", "verifyStr"),
    [(False, HIVE_AUTHZ_DENIED_SUBSTR)],
)
def test_01_01_TableCreateEmployeeNoPolicyAsUser1Deny(validateImpala, verifyStr):
    HivePluginTest(
        "HIVE.test_01_01_TableCreateEmployeeNoPolicyAsUser1Deny",
        TEST_CASE,
        "create table default.employee(name String);",
        HivePluginTestContext.USER_USER1,
        HivePluginTestContext.PASS_USER1,
        verifyCriteria=HivePluginTest.STDERR_CONTAINS,
        verifyStr=verifyStr,
        newAuditLogMustExist=True,
        validateImpala=validateImpala,
        auditLogFilter={
            "accessType": TEST_CASE.access_type["CREATE"],
            "accessResult": TEST_CASE.result_denied,
            "accessEnforcer": "ranger-acl",
            "resourceName": "default/employee",
        },
    ).execute()


def test_01_02_DatabaseUseDefaultNoPolicyAsUser1Deny():
    HivePluginTest(
        "HIVE.test_01_02_DatabaseUseDefaultNoPolicyAsUser1Deny",
        TEST_CASE,
        "use default;",
        HivePluginTestContext.USER_USER1,
        HivePluginTestContext.PASS_USER1,
        verifyCriteria=HivePluginTest.STDERR_CONTAINS,
        verifyStr=HIVE_AUTHZ_DENIED_SUBSTR,
        newAuditLogMustExist=True,
        auditLogFilter={
            "accessType": TEST_CASE.access_type["USE"],
            "accessResult": TEST_CASE.result_denied,
            "accessEnforcer": "ranger-acl",
            "resourceName": "default",
        },
    ).execute()
