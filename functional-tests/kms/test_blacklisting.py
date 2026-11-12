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

"""KMS dbks-site.xml user blacklisting for CREATE / ROLLOVER / DELETE (keyadmin)."""

import time

from ranger_test_utils.kms_test_utils import init_kms_test_module, logger
from ranger_test_utils.kms_utils import (
    KmsXaTestCase,
    blacklist_op_users,
    container,
    ensure_keyadmin_can_list_kms_keys,
    ensure_ticket,
    execute_kms_rest_test,
    fetch_kms_key_names,
    teardown_kms_keys_created_since,
    unblacklist_op_users,
)

TEST_CASE = None
list_of_keys_before_test = None

BLACKLIST_USER = "keyadmin"
_BLACKLIST_OPS = ("CREATE", "ROLLOVER", "DELETE")


def setup_module():
    global TEST_CASE, list_of_keys_before_test
    init_kms_test_module(module_name=__name__)

    ensure_keyadmin_can_list_kms_keys()
    list_of_keys_before_test = fetch_kms_key_names()
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)

    TEST_CASE = KmsXaTestCase()
    TEST_CASE.setUpModule()

    for op in _BLACKLIST_OPS:
        unblacklist_op_users(op, [BLACKLIST_USER])
    _restart_kms_after_blacklist()


def teardown_module():
    global TEST_CASE
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)
    for op in _BLACKLIST_OPS:
        unblacklist_op_users(op, [BLACKLIST_USER])
    _restart_kms_after_blacklist()

    if TEST_CASE is not None:
        TEST_CASE.deleteKeysForThisTest()
        TEST_CASE.tearDownModule()
        TEST_CASE = None
    teardown_kms_keys_created_since(list_of_keys_before_test)


def _restart_kms_after_blacklist():
    container.restart()
    time.sleep(5)
    ensure_ticket()


def _delete_key_quiet(key_name):
    execute_kms_rest_test(
        "DELETE",
        f"/key/{key_name}",
        expected_status=None,
        step=f"KMS.test_blacklisting.delete_quiet.{key_name}",
    )


def _rollover_empty(key_name, expected_status, step):
    execute_kms_rest_test(
        "POST",
        f"/key/{key_name}",
        json_body={},
        expected_status=expected_status,
        step=step,
    )


def test_user_keyOperation_before_blacklist():
    admin = TEST_CASE.hadoopAdminUser
    key_name = "blacklist-key1"

    TEST_CASE.createEncryptionZoneKey(
        "test_user_keyOperation_before_blacklist",
        admin,
        key_name,
        expected_status=201,
        test_desc="test_user_keyOperation_before_blacklist_create",
    )
    _rollover_empty(
        key_name,
        200,
        step="test_user_keyOperation_before_blacklist_rollover",
    )
    TEST_CASE.deleteEncryptionZoneKey(
        "test_user_keyOperation_before_blacklist",
        admin,
        key_name,
        expected_status=200,
        test_desc="test_user_keyOperation_before_blacklist_delete",
    )


def test_blacklist_create():
    blacklist_op_users("CREATE", [BLACKLIST_USER])
    _restart_kms_after_blacklist()

    key_name = "blacklist-key2"
    key_data = {"name": key_name}

    execute_kms_rest_test(
        "POST",
        "/keys",
        json_body=key_data,
        expected_status=403,
        step="test_blacklist_create.blocked",
    )

    unblacklist_op_users("CREATE", [BLACKLIST_USER])
    _restart_kms_after_blacklist()

    _delete_key_quiet(key_name)

    execute_kms_rest_test(
        "POST",
        "/keys",
        json_body=key_data,
        expected_status=201,
        step="test_blacklist_create.allowed",
    )

    _delete_key_quiet(key_name)
    if key_name in TEST_CASE.createdKeys:
        TEST_CASE.createdKeys.remove(key_name)


def test_blacklist_rollOver():
    blacklist_op_users("ROLLOVER", [BLACKLIST_USER])
    _restart_kms_after_blacklist()

    admin = TEST_CASE.hadoopAdminUser
    key_name = "blacklist-key3"

    TEST_CASE.createEncryptionZoneKey(
        "test_blacklist_rollOver",
        admin,
        key_name,
        expected_status=201,
        test_desc="test_blacklist_rollOver_create",
    )

    _rollover_empty(
        key_name,
        403,
        step="test_blacklist_rollOver.blocked",
    )

    unblacklist_op_users("ROLLOVER", [BLACKLIST_USER])
    _restart_kms_after_blacklist()

    _rollover_empty(
        key_name,
        200,
        step="test_blacklist_rollOver.allowed",
    )

    TEST_CASE.deleteEncryptionZoneKey(
        "test_blacklist_rollOver",
        admin,
        key_name,
        expected_status=200,
        test_desc="test_blacklist_rollOver_cleanup",
    )


def test_blacklist_delete():
    blacklist_op_users("DELETE", [BLACKLIST_USER])
    _restart_kms_after_blacklist()

    admin = TEST_CASE.hadoopAdminUser
    key_name = "blacklist-key4"

    TEST_CASE.createEncryptionZoneKey(
        "test_blacklist_delete",
        admin,
        key_name,
        expected_status=201,
        test_desc="test_blacklist_delete_create",
    )

    execute_kms_rest_test(
        "DELETE",
        f"/key/{key_name}",
        expected_status=403,
        step="test_blacklist_delete.blocked",
    )

    unblacklist_op_users("DELETE", [BLACKLIST_USER])
    _restart_kms_after_blacklist()

    TEST_CASE.deleteEncryptionZoneKey(
        "test_blacklist_delete",
        admin,
        key_name,
        expected_status=200,
        test_desc="test_blacklist_delete.allowed",
    )
