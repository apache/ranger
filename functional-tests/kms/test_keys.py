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

"""KMS REST key creation and name validation (keyadmin, seed ``dev_kms`` policy)."""

import pytest

from ranger_test_utils.kms_test_utils import init_kms_test_module, logger
from ranger_test_utils.kms_utils import (
    KmsXaTestCase,
    ensure_keyadmin_can_list_kms_keys,
    fetch_kms_key_names,
    teardown_kms_keys_created_since,
)

TEST_CASE = None
list_of_keys_before_test = None

DEFAULT_KEY_BODY = {
    "cipher": "AES/CTR/NoPadding",
    "length": 128,
}


def setup_module():
    global TEST_CASE, list_of_keys_before_test
    init_kms_test_module(module_name=__name__)

    ensure_keyadmin_can_list_kms_keys()
    list_of_keys_before_test = fetch_kms_key_names()
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)

    TEST_CASE = KmsXaTestCase()
    TEST_CASE.setUpModule()


def teardown_module():
    global TEST_CASE
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)
    if TEST_CASE is not None:
        TEST_CASE.deleteKeysForThisTest()
        TEST_CASE.tearDownModule()
        TEST_CASE = None
    teardown_kms_keys_created_since(list_of_keys_before_test)


def test_create_key():
    key_data = {
        **DEFAULT_KEY_BODY,
        "name": "key2",
        "description": "New key for checking key creation functionality",
    }
    TEST_CASE.createKmsKey(
        "test_create_key",
        TEST_CASE.hadoopAdminUser,
        key_data,
        expected_status=201,
        test_desc="test_create_key",
    )
    TEST_CASE.deleteEncryptionZoneKey(
        "test_create_key",
        TEST_CASE.hadoopAdminUser,
        "key2",
        expected_status=200,
        test_desc="test_create_key_cleanup",
    )


@pytest.mark.parametrize(
    "name, expected_status",
    [
        ("valid-key", 201),
        ("", 400),
        ("@invalid!", 400),
        ("invalid--key", 400),
    ],
)
def test_key_name_validation(name, expected_status):
    key_data = {
        **DEFAULT_KEY_BODY,
        "name": name,
        "description": "Validation test",
    }
    TEST_CASE.createKmsKey(
        "test_key_name_validation",
        TEST_CASE.hadoopAdminUser,
        key_data,
        expected_status=expected_status,
        test_desc=f"test_key_name_validation_{name!r}",
    )

    if expected_status == 201:
        TEST_CASE.deleteEncryptionZoneKey(
            "test_key_name_validation",
            TEST_CASE.hadoopAdminUser,
            name,
            expected_status=200,
            test_desc=f"test_key_name_validation_cleanup_{name!r}",
        )


def test_duplicate_key_creation():
    key_name = "duplicate-key"
    key_data = {
        **DEFAULT_KEY_BODY,
        "name": key_name,
        "description": "Testing duplicate key creation",
    }

    TEST_CASE.createKmsKey(
        "test_duplicate_key_creation",
        TEST_CASE.hadoopAdminUser,
        key_data,
        expected_status=201,
        test_desc="test_duplicate_key_creation_first",
    )

    TEST_CASE.createKmsKey(
        "test_duplicate_key_creation",
        TEST_CASE.hadoopAdminUser,
        key_data,
        expected_status=500,
        test_desc="test_duplicate_key_creation_duplicate",
    )

    TEST_CASE.deleteEncryptionZoneKey(
        "test_duplicate_key_creation",
        TEST_CASE.hadoopAdminUser,
        key_name,
        expected_status=200,
        test_desc="test_duplicate_key_creation_cleanup",
    )
