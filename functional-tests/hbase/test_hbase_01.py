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
HBase plugin enforcement via HBase shell and optional access-audit validation.

Set ``AUDIT_INDEX_STORE`` to ``opensearch`` or ``solr`` when starting Docker
(see functional-tests readme) so access audits are indexed and asserted.
"""

import pytest

from ranger_test_utils.hbase_utils import (
    HBASE_AUTHZ_DENIED_SUBSTR,
    HBASE_SERVICE_NAME,
    HBasePluginTest,
    HBasePluginTestContext,
    cleanup_hbase_test_table,
    create_audit_only_policy,
    preflight_hbase_stack,
    prepare_hbase_test_table,
    wait_for_hbase_policy_propagation,
)
from ranger_test_utils.hive_utils import delete_policies_by_id, set_all_policies_enabled
from ranger_test_utils.plugin_enforcement_utils import (
    configure_test_logging,
    create_ranger_admin_session,
    logger,
)

pytestmark = pytest.mark.hbase

TEST_CASE = None

HBASE_TEST_TABLE = "iemployee"

_admin_session = None
_audit_policy_id = None
_audit_suffix = None


def setup_module():
    configure_test_logging(module_name=__name__)
    logger.info("=== setup_module: start ===")
    preflight_hbase_stack()

    global TEST_CASE, _admin_session, _audit_policy_id, _audit_suffix
    TEST_CASE = HBasePluginTestContext()
    _admin_session = create_ranger_admin_session()
    TEST_CASE.admin_session = _admin_session

    logger.info(
        "Plugin user %s (Ranger + Kerberos) ensured by functional-tests/conftest.py",
        HBasePluginTestContext.USER_USER1,
    )

    logger.info("HBase setup: drop table %s if present", HBASE_TEST_TABLE)
    prepare_hbase_test_table(HBASE_TEST_TABLE)

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
    logger.info("HBase cleanup: drop table %s", HBASE_TEST_TABLE)
    cleanup_hbase_test_table(HBASE_TEST_TABLE)

    if _audit_policy_id is not None:
        logger.info("Deleting audit policy id=%s", _audit_policy_id)
        delete_policies_by_id(_admin_session, [_audit_policy_id])

    logger.info("Re-enabling all policies on %s", HBASE_SERVICE_NAME)
    set_all_policies_enabled(_admin_session, True, HBASE_SERVICE_NAME)
    wait_for_hbase_policy_propagation()
    logger.info("=== teardown_module: done ===")


def test_01_01_CreateHbaseTableCase1A():
    HBasePluginTest(
        "HBASE.test_01_01_CreateHbaseTableCase1A",
        TEST_CASE,
        hBaseCmds=["create 'iemployee','personal','payroll','medical'"],
        reqUser=HBasePluginTestContext.USER_USER1,
        verifyCriteria=HBasePluginTest.STDOUT_CONTAINS,
        verifyStr=HBASE_AUTHZ_DENIED_SUBSTR,
        checkAuditLog=False,
        newAuditLogMustExist=True,
        auditLogFilter={
            "accessType": TEST_CASE.access_type["CREATE_TABLE"],
            "accessResult": TEST_CASE.result_denied,
            "accessEnforcer": "ranger-acl",
            "resourceName": "iemployee",
        },
    ).execute()
