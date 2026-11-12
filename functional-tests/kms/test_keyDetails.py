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

"""KMS key metadata, versions, bulk metadata, and keyversion REST checks."""

import pytest

from ranger_test_utils.kms_test_utils import init_kms_test_module, logger
from ranger_test_utils.kms_utils import (
    KmsXaTestCase,
    ensure_keyadmin_can_list_kms_keys,
    execute_kms_rest_test,
    fetch_kms_key_names,
    teardown_kms_keys_created_since,
)

TEST_CASE = None
list_of_keys_before_test = None

KEY1_BODY = {
    "name": "key1",
    "cipher": "AES/CTR/NoPadding",
    "length": 128,
    "description": "Test key",
}

_BLANK_RESPONSES = {"", "{}", "{ }", "[]", "[ ]"}


def setup_module():
    global TEST_CASE, list_of_keys_before_test
    init_kms_test_module(module_name=__name__)

    ensure_keyadmin_can_list_kms_keys()
    list_of_keys_before_test = fetch_kms_key_names()
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)

    TEST_CASE = KmsXaTestCase()
    TEST_CASE.setUpModule()

    _delete_key_quiet("key1")
    TEST_CASE.createKmsKey(
        "setup_module",
        TEST_CASE.hadoopAdminUser,
        KEY1_BODY,
        expected_status=201,
        test_desc="setup_module_key1",
    )


def teardown_module():
    global TEST_CASE
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)
    if TEST_CASE is not None:
        TEST_CASE.deleteKeysForThisTest()
        TEST_CASE.tearDownModule()
        TEST_CASE = None
    teardown_kms_keys_created_since(list_of_keys_before_test)


def _delete_key_quiet(key_name):
    execute_kms_rest_test(
        "DELETE",
        f"/key/{key_name}",
        expected_status=None,
        step=f"KMS.test_keyDetails.delete_quiet.{key_name}",
    )


def test_get_key_names():
    key_names = fetch_kms_key_names()
    logger.info("Key names: %s", key_names)
    assert KEY1_BODY["name"] in key_names


@pytest.mark.parametrize(
    "key_name, expected_status, expected_response",
    [
        ("key1", 200, "valid"),
        ("non-existent-key", 200, "invalid"),
    ],
)
def test_get_key_metadata(key_name, expected_status, expected_response):
    response = execute_kms_rest_test(
        "GET",
        f"/key/{key_name}/_metadata",
        expected_status=expected_status,
        step=f"test_get_key_metadata.{key_name}",
    )

    if expected_response == "invalid":
        assert response.text.strip() in _BLANK_RESPONSES, (
            f"Expected blank response for non-existent key, got: {response.text}"
        )


@pytest.mark.parametrize(
    "key_name, expected_status, expected_response",
    [
        ("key1", 200, "valid"),
        ("non-existent-key", 200, "invalid"),
    ],
)
def test_get_key_versions(key_name, expected_status, expected_response):
    response = execute_kms_rest_test(
        "GET",
        f"/key/{key_name}/_versions",
        expected_status=expected_status,
        step=f"test_get_key_versions.{key_name}",
    )

    if expected_response == "invalid":
        assert response.text.strip() in _BLANK_RESPONSES, (
            f"Expected blank response for non-existent key, got: {response.text}"
        )


def test_get_keys_metadata():
    admin = TEST_CASE.hadoopAdminUser
    key_name = "key2"

    TEST_CASE.createEncryptionZoneKey(
        "test_get_keys_metadata",
        admin,
        key_name,
        expected_status=201,
        test_desc="test_get_keys_metadata_create_key2",
    )

    try:
        existing_keys = ["key1", "key2"]
        params = [("key", k) for k in existing_keys]
        params.append(("user.name", "keyadmin"))

        response = execute_kms_rest_test(
            "GET",
            "/keys/metadata",
            params=params,
            expected_status=200,
            step="test_get_keys_metadata.existing",
        )
        metadata = response.json()
        returned_keys = [entry["name"] for entry in metadata]
        for key in existing_keys:
            assert key in returned_keys, (
                f"Expected key '{key}' not found in metadata response"
            )

        fake_keys = ["nonExistent_key_1", "nonExistent_key_2"]
        params = [("key", k) for k in fake_keys]
        params.append(("user.name", "keyadmin"))

        response = execute_kms_rest_test(
            "GET",
            "/keys/metadata",
            params=params,
            expected_status=200,
            step="test_get_keys_metadata.nonexistent",
        )
        assert response.json() == [{}, {}], (
            f"Expected blank response for non-existent keys, got: {response.text}"
        )
    finally:
        TEST_CASE.deleteEncryptionZoneKey(
            "test_get_keys_metadata",
            admin,
            key_name,
            expected_status=200,
            test_desc="test_get_keys_metadata_cleanup_key2",
        )


@pytest.mark.parametrize(
    "version_name, expected_status, expected_valid",
    [
        ("key1@0", 200, True),
        ("non-existent-key@0", 200, False),
    ],
)
def test_get_key_version(version_name, expected_status, expected_valid):
    response = execute_kms_rest_test(
        "GET",
        f"/keyversion/{version_name}",
        expected_status=expected_status,
        step=f"test_get_key_version.{version_name}",
    )

    if expected_valid:
        version_data = response.json()
        assert "versionName" in version_data, "versionName missing in response"
        assert version_data["versionName"] == version_name, (
            f"Mismatch in version name: expected {version_name}"
        )
    else:
        assert response.text.strip() in _BLANK_RESPONSES, (
            f"Expected blank response for non-existent key, got: {response.text}"
        )
