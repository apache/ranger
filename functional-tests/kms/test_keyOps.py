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

"""KMS key operations: rollover, DEK/EDEK, re-encrypt batch, cache invalidation."""

import time

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

ROLLOVER_KEY_BODY = {
    "name": "rollover-key",
    "cipher": "AES/CTR/NoPadding",
    "length": 128,
    "description": "Key to check roll over functionality",
}


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
        step=f"KMS.test_keyOps.delete_quiet.{key_name}",
    )


def _rollover_empty_body(key_name, expected_status, step):
    """POST ``/key/{name}`` with empty JSON (legacy ``test_keyOps`` rollover shape)."""
    return execute_kms_rest_test(
        "POST",
        f"/key/{key_name}",
        json_body={},
        expected_status=expected_status,
        step=step,
    )


def _get_key_metadata(key_name, step):
    response = execute_kms_rest_test(
        "GET",
        f"/key/{key_name}/_metadata",
        expected_status=200,
        step=step,
    )
    return response.json()


def test_temp_key():
    admin = TEST_CASE.hadoopAdminUser
    _delete_key_quiet("rollover-key")
    TEST_CASE.createKmsKey(
        "test_temp_key",
        admin,
        ROLLOVER_KEY_BODY,
        expected_status=201,
        test_desc="test_temp_key_create",
    )
    TEST_CASE.deleteEncryptionZoneKey(
        "test_temp_key",
        admin,
        "rollover-key",
        expected_status=200,
        test_desc="test_temp_key_cleanup",
    )


@pytest.mark.parametrize(
    "key_name, expected_status",
    [
        ("rollover-key", 200),
        ("non-existent-key", 500),
    ],
)
def test_roll_over_key(key_name, expected_status):
    admin = TEST_CASE.hadoopAdminUser
    if key_name == "rollover-key":
        _delete_key_quiet("rollover-key")
        TEST_CASE.createKmsKey(
            "test_roll_over_key",
            admin,
            ROLLOVER_KEY_BODY,
            expected_status=201,
            test_desc="test_roll_over_key_create",
        )

    _rollover_empty_body(
        key_name,
        expected_status,
        step=f"test_roll_over_key.rollover.{key_name}",
    )

    if key_name == "rollover-key":
        _delete_key_quiet("rollover-key")
        if "rollover-key" in TEST_CASE.createdKeys:
            TEST_CASE.createdKeys.remove("rollover-key")


def test_roll_over_new_material():
    old_metadata = _get_key_metadata("key1", "test_roll_over_new_material.before")
    logger.info("Old metadata: %s", old_metadata)

    _rollover_empty_body(
        "key1",
        200,
        step="test_roll_over_new_material.rollover",
    )

    new_metadata = _get_key_metadata("key1", "test_roll_over_new_material.after")
    logger.info("New metadata: %s", new_metadata)

    assert old_metadata != new_metadata, "Key rollover should create new key material."


def test_generate_data_key_and_decrypt():
    key_name = KEY1_BODY["name"]
    generate_body = TEST_CASE.generateDekWithEdek(
        "test_generate_data_key_and_decrypt",
        TEST_CASE.hadoopAdminUser,
        key_name,
        test_desc="test_generate_data_key_and_decrypt_generate",
    )

    dek = generate_body.get("dek")
    edek = generate_body.get("edek")
    assert dek is not None, "Generated DEK should not be None"
    assert edek is not None, "Generated EDEK should not be None"

    encrypted_key_version = edek.get("encryptedKeyVersion")
    decrypt_payload = {
        "name": encrypted_key_version.get("name"),
        "iv": edek.get("iv"),
        "material": encrypted_key_version.get("material"),
    }
    version_name = edek.get("versionName")

    decrypt_response = execute_kms_rest_test(
        "POST",
        f"/keyversion/{version_name}/_eek",
        json_body=decrypt_payload,
        params={"user.name": "keyadmin", "eek_op": "decrypt"},
        expected_status=200,
        step="test_generate_data_key_and_decrypt_decrypt",
    )
    decrypted_data = decrypt_response.json()
    logger.info("Decrypted data: %s", decrypted_data)

    assert decrypted_data == dek, "Decrypted DEK should match the original DEK"


def test_reencrypt_encrypted_keys():
    admin = TEST_CASE.hadoopAdminUser
    key_name = "reencrypt-key"

    TEST_CASE.createEncryptionZoneKey(
        "test_reencrypt_encrypted_keys",
        admin,
        key_name,
        expected_status=201,
        test_desc="test_reencrypt_encrypted_keys_create",
    )

    try:
        generate_body = TEST_CASE.generateDekWithEdek(
            "test_reencrypt_encrypted_keys",
            admin,
            key_name,
            test_desc="test_reencrypt_encrypted_keys_generate_dek",
        )
        logger.info("Generate DEK response: %s", generate_body)

        edek = generate_body["edek"]
        encrypted_key_version = edek["encryptedKeyVersion"]
        edek_payload = [
            {
                "versionName": edek["versionName"],
                "iv": edek["iv"],
                "encryptedKeyVersion": {
                    "versionName": "EEK",
                    "material": encrypted_key_version["material"],
                },
            }
        ]

        _rollover_empty_body(
            key_name,
            200,
            step="test_reencrypt_encrypted_keys.rollover",
        )

        reencrypt_response = execute_kms_rest_test(
            "POST",
            f"/key/{key_name}/_reencryptbatch",
            json_body=edek_payload,
            expected_status=200,
            step="test_reencrypt_encrypted_keys.reencrypt",
        )
        reencrypted_edeks = reencrypt_response.json()
        logger.info("Re-encrypted EDEKs: %s", reencrypted_edeks)

        assert isinstance(reencrypted_edeks, list), "Expected list of re-encrypted EDEKs"
        assert len(reencrypted_edeks) == 1, "Expected exactly one re-encrypted EDEK"
        assert reencrypted_edeks[0]["versionName"] != edek["versionName"], (
            "Expected EDEK version to change after re-encryption"
        )
    finally:
        TEST_CASE.deleteEncryptionZoneKey(
            "test_reencrypt_encrypted_keys",
            admin,
            key_name,
            expected_status=200,
            test_desc="test_reencrypt_encrypted_keys_cleanup",
        )


def test_generate_data_key_after_invalidate_cache():
    admin = TEST_CASE.hadoopAdminUser
    key_name = "cache_key"

    TEST_CASE.createEncryptionZoneKey(
        "test_generate_data_key_after_invalidate_cache",
        admin,
        key_name,
        expected_status=201,
        test_desc="test_generate_data_key_after_invalidate_cache_create",
    )

    _rollover_empty_body(
        key_name,
        200,
        step="test_generate_data_key_after_invalidate_cache.rollover",
    )

    TEST_CASE.deleteEncryptionZoneKey(
        "test_generate_data_key_after_invalidate_cache",
        admin,
        key_name,
        expected_status=200,
        test_desc="test_generate_data_key_after_invalidate_cache_delete",
    )

    time.sleep(5)

    TEST_CASE.createEncryptionZoneKey(
        "test_generate_data_key_after_invalidate_cache",
        admin,
        key_name,
        expected_status=201,
        test_desc="test_generate_data_key_after_invalidate_cache_recreate",
    )

    execute_kms_rest_test(
        "POST",
        f"/key/{key_name}",
        json_body={},
        params={"user.name": "keyadmin", "action": "invalidateCache"},
        expected_status=200,
        step="test_generate_data_key_after_invalidate_cache.invalidate",
    )

    TEST_CASE.generateDekWithEdek(
        "test_generate_data_key_after_invalidate_cache",
        admin,
        key_name,
        test_desc="test_generate_data_key_after_invalidate_cache_dek",
    )

    TEST_CASE.deleteEncryptionZoneKey(
        "test_generate_data_key_after_invalidate_cache",
        admin,
        key_name,
        expected_status=200,
        test_desc="test_generate_data_key_after_invalidate_cache_final_cleanup",
    )
