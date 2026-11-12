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

"""Incremental KMS Ranger policy enforcement for ``hrt_21`` (``dev_kms``)."""

import json
import random

from ranger_test_utils.kms_test_utils import init_kms_test_module, logger
from ranger_test_utils.kms_utils import (
    KmsXaTestCase,
    create_kms_04_module_policy,
    ensure_hrt_21_keytab,
    ensure_keyadmin_can_list_kms_keys,
    fetch_kms_key_names,
    teardown_kms_04_module_policy,
    teardown_kms_keys_created_since,
    update_kms_policy,
)
from ranger_test_utils.plugin_test_users import ensure_kerberos_principal
from ranger_test_utils.utils import create_ranger_admin_session, ensure_user_exists

TEST_CASE = None
test_name = None
list_of_keys_before_test = None


def setup_module():
    global TEST_CASE, list_of_keys_before_test
    init_kms_test_module(module_name=__name__)
    ensure_kerberos_principal(KmsXaTestCase.hadoopUser_1)
    ensure_hrt_21_keytab()
    admin_session = create_ranger_admin_session()
    ensure_user_exists(admin_session, KmsXaTestCase.hadoopUser_1)

    ensure_keyadmin_can_list_kms_keys()
    list_of_keys_before_test = fetch_kms_key_names()
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)

    TEST_CASE = KmsXaTestCase()
    TEST_CASE.policy_name = __name__
    TEST_CASE.setUpModule()
    TEST_CASE.policy = create_kms_04_module_policy(
        TEST_CASE.policy_name,
        TEST_CASE.hadoopUser_1,
        TEST_CASE.hdfs_service_user,
    )


def teardown_module():
    global TEST_CASE
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)
    teardown_kms_04_module_policy(
        TEST_CASE.policy if TEST_CASE is not None else None
    )
    if TEST_CASE is not None:
        TEST_CASE.deleteKeysForThisTest()
        TEST_CASE.tearDownModule()
        TEST_CASE = None
    teardown_kms_keys_created_since(list_of_keys_before_test)


def setup_function(test_func):
    global test_name
    test_name = test_func.__name__


def teardown_function(test_func):
    global test_name
    del test_func
    test_name = None


def test_04_Ranger_KMS_TestCase1A():
    username = TEST_CASE.hadoopUser_1
    key_name = test_name.lower() + str(random.randint(0, 2**30))
    expected_status = 201

    key_version = TEST_CASE.createEncryptionZoneKey(
        test_name,
        username,
        key_name,
        expected_status,
        test_desc=test_name + "_01",
        checkAuditLog=True,
        newAuditLogMustExist=True,
    )
    if key_version is not None:
        logger.info("Created key:%s", json.dumps(key_version))

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyCurrentVersion(
        test_name, username, key_name, expected_status, test_desc=test_name + "_02"
    )

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyMetadata(
        test_name, username, key_name, expected_status, test_desc=test_name + "_03"
    )

    expected_status = 403
    TEST_CASE.rolloverEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_04"
    )

    expected_status = 403
    TEST_CASE.setEncryptionZoneKeyMaterial(
        test_name,
        username,
        key_name,
        TEST_CASE.keyMaterial_2,
        expected_status,
        test_desc=test_name + "_05",
    )

    expected_status = 403
    TEST_CASE.deleteEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_06"
    )

    expected_status = 403
    num_keys = 1
    TEST_CASE.generateEncryptedDEKs(
        test_name, username, key_name, num_keys, expected_status, test_desc=test_name + "_07"
    )


def test_04_Ranger_KMS_TestCase2A():
    access = {"isAllowed": True, "type": "delete"}
    TEST_CASE.policy["policyItems"][1]["accesses"].append(access)
    TEST_CASE.policy = update_kms_policy(TEST_CASE.policy)

    username = TEST_CASE.hadoopUser_1
    key_name = test_name.lower() + str(random.randint(0, 2**30))
    expected_status = 201

    key_version = TEST_CASE.createEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_01"
    )
    if key_version is not None:
        logger.info("Created key:%s", json.dumps(key_version))

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyCurrentVersion(
        test_name, username, key_name, expected_status, test_desc=test_name + "_03"
    )

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyMetadata(
        test_name, username, key_name, expected_status, test_desc=test_name + "_04"
    )

    expected_status = 403
    TEST_CASE.rolloverEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_05"
    )

    expected_status = 403
    TEST_CASE.setEncryptionZoneKeyMaterial(
        test_name,
        username,
        key_name,
        TEST_CASE.keyMaterial_2,
        expected_status,
        test_desc=test_name + "_06",
    )

    expected_status = 403
    num_keys = 1
    TEST_CASE.generateEncryptedDEKs(
        test_name, username, key_name, num_keys, expected_status, test_desc=test_name + "_07"
    )

    expected_status = 200
    TEST_CASE.deleteEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_02"
    )


def test_04_Ranger_KMS_TestCase3A():
    access = {"isAllowed": True, "type": "rollover"}
    TEST_CASE.policy["policyItems"][1]["accesses"].append(access)
    TEST_CASE.policy = update_kms_policy(TEST_CASE.policy)

    username = TEST_CASE.hadoopUser_1
    key_name = test_name.lower() + str(random.randint(0, 2**30))
    expected_status = 201

    key_version = TEST_CASE.createEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_01"
    )
    if key_version is not None:
        logger.info("Created key:%s", json.dumps(key_version))

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyCurrentVersion(
        test_name, username, key_name, expected_status, test_desc=test_name + "_02"
    )

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyMetadata(
        test_name, username, key_name, expected_status, test_desc=test_name + "_03"
    )

    expected_status = 200
    TEST_CASE.rolloverEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_04"
    )

    expected_status = 403
    TEST_CASE.setEncryptionZoneKeyMaterial(
        test_name,
        username,
        key_name,
        TEST_CASE.keyMaterial_2,
        expected_status,
        test_desc=test_name + "_05",
    )

    expected_status = 403
    num_keys = 1
    TEST_CASE.generateEncryptedDEKs(
        test_name, username, key_name, num_keys, expected_status, test_desc=test_name + "_06"
    )

    expected_status = 200
    TEST_CASE.deleteEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_07"
    )


def test_04_Ranger_KMS_TestCase4A():
    access = {"isAllowed": True, "type": "setkeymaterial"}
    TEST_CASE.policy["policyItems"][1]["accesses"].append(access)
    TEST_CASE.policy = update_kms_policy(TEST_CASE.policy)

    username = TEST_CASE.hadoopUser_1
    key_name = test_name.lower() + str(random.randint(0, 2**30))
    expected_status = 201

    key_version = TEST_CASE.createEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_01"
    )
    if key_version is not None:
        logger.info("Created key:%s", json.dumps(key_version))

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyCurrentVersion(
        test_name, username, key_name, expected_status, test_desc=test_name + "_02"
    )

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyMetadata(
        test_name, username, key_name, expected_status, test_desc=test_name + "_03"
    )

    expected_status = 200
    TEST_CASE.rolloverEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_04"
    )

    expected_status = 200
    TEST_CASE.setEncryptionZoneKeyMaterial(
        test_name,
        username,
        key_name,
        TEST_CASE.keyMaterial_2,
        expected_status,
        test_desc=test_name + "_05",
    )

    expected_status = 403
    num_keys = 1
    TEST_CASE.generateEncryptedDEKs(
        test_name, username, key_name, num_keys, expected_status, test_desc=test_name + "_06"
    )

    expected_status = 200
    TEST_CASE.deleteEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_07"
    )


def test_04_Ranger_KMS_TestCase5A():
    access = {"isAllowed": True, "type": "get"}
    TEST_CASE.policy["policyItems"][1]["accesses"].append(access)
    TEST_CASE.policy = update_kms_policy(TEST_CASE.policy)

    username = TEST_CASE.hadoopUser_1
    key_name = test_name.lower() + str(random.randint(0, 2**30))

    expected_status = 201
    key_version = TEST_CASE.createEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_01"
    )
    if key_version is not None:
        logger.info("Created key:%s", json.dumps(key_version))

    expected_status = 200
    TEST_CASE.getEncryptionZoneKeyCurrentVersion(
        test_name, username, key_name, expected_status, test_desc=test_name + "_02"
    )

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyMetadata(
        test_name, username, key_name, expected_status, test_desc=test_name + "_03"
    )

    expected_status = 200
    TEST_CASE.rolloverEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_04"
    )

    expected_status = 200
    TEST_CASE.setEncryptionZoneKeyMaterial(
        test_name,
        username,
        key_name,
        TEST_CASE.keyMaterial_2,
        expected_status,
        test_desc=test_name + "_05",
    )

    expected_status = 403
    num_keys = 1
    TEST_CASE.generateEncryptedDEKs(
        test_name, username, key_name, num_keys, expected_status, test_desc=test_name + "_06"
    )

    expected_status = 200
    TEST_CASE.deleteEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_07"
    )


def test_04_Ranger_KMS_TestCase6A():
    access = {"isAllowed": True, "type": "generateeek"}
    TEST_CASE.policy["policyItems"][1]["accesses"].append(access)
    TEST_CASE.policy = update_kms_policy(TEST_CASE.policy)

    username = TEST_CASE.hadoopUser_1
    key_name = test_name.lower() + str(random.randint(0, 2**30))

    expected_status = 201
    key_version = TEST_CASE.createEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_01"
    )
    if key_version is not None:
        logger.info("Created key:%s", json.dumps(key_version))

    expected_status = 200
    TEST_CASE.getEncryptionZoneKeyCurrentVersion(
        test_name, username, key_name, expected_status, test_desc=test_name + "_02"
    )

    expected_status = 403
    TEST_CASE.getEncryptionZoneKeyMetadata(
        test_name, username, key_name, expected_status, test_desc=test_name + "_03"
    )

    expected_status = 200
    TEST_CASE.rolloverEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_04"
    )

    expected_status = 200
    TEST_CASE.setEncryptionZoneKeyMaterial(
        test_name,
        username,
        key_name,
        TEST_CASE.keyMaterial_2,
        expected_status,
        test_desc=test_name + "_05",
    )

    expected_status = 200
    num_keys = 1
    TEST_CASE.generateEncryptedDEKs(
        test_name, username, key_name, num_keys, expected_status, test_desc=test_name + "_06"
    )

    expected_status = 200
    TEST_CASE.deleteEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_07"
    )


def test_04_Ranger_KMS_TestCase7A():
    access = {"isAllowed": True, "type": "getmetadata"}
    TEST_CASE.policy["policyItems"][1]["accesses"].append(access)
    TEST_CASE.policy = update_kms_policy(TEST_CASE.policy)

    username = TEST_CASE.hadoopUser_1
    key_name = test_name.lower() + str(random.randint(0, 2**30))

    expected_status = 201
    key_version = TEST_CASE.createEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_01"
    )
    if key_version is not None:
        logger.info("Created key:%s", json.dumps(key_version))

    expected_status = 200
    TEST_CASE.getEncryptionZoneKeyCurrentVersion(
        test_name, username, key_name, expected_status, test_desc=test_name + "_02"
    )

    expected_status = 200
    TEST_CASE.getEncryptionZoneKeyMetadata(
        test_name, username, key_name, expected_status, test_desc=test_name + "_03"
    )

    expected_status = 200
    TEST_CASE.rolloverEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_04"
    )

    expected_status = 200
    TEST_CASE.setEncryptionZoneKeyMaterial(
        test_name,
        username,
        key_name,
        TEST_CASE.keyMaterial_2,
        expected_status,
        test_desc=test_name + "_05",
    )

    expected_status = 200
    num_keys = 1
    TEST_CASE.generateEncryptedDEKs(
        test_name, username, key_name, num_keys, expected_status, test_desc=test_name + "_06"
    )

    expected_status = 200
    TEST_CASE.deleteEncryptionZoneKey(
        test_name, username, key_name, expected_status, test_desc=test_name + "_07"
    )
