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

"""Encryption zone key create, rollover, delete, and EDEK/DEK flows."""

from ranger_test_utils.api_test_utils import return_random_str
from ranger_test_utils.kms_test_utils import init_kms_test_module, logger
from ranger_test_utils.kms_utils import (
    KmsXaTestCase,
    ensure_keyadmin_can_list_kms_keys,
    fetch_kms_key_names,
    teardown_kms_keys_created_since,
)

TEST_CASE = None
list_of_keys_before_test = None
kms_test_service_name = "dev_kms"


def setup_module():
    global TEST_CASE, list_of_keys_before_test
    init_kms_test_module(module_name=__name__)

    ensure_keyadmin_can_list_kms_keys()
    list_of_keys_before_test = get_keys_list(kms_test_service_name)
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


def get_keys_list(provider_name):
    del provider_name
    return fetch_kms_key_names()


def create_kms_encryption_zone_key_for_testing(
    kms_service_name,
    key_name,
    description=None,
    cipher="AES/CTR/NoPadding",
    length=128,
    versions=1,
    attributes=None,
):
    del kms_service_name, versions, attributes
    if description is None:
        description = return_random_str(10)

    payload = {
        "name": key_name,
        "description": description,
        "cipher": cipher,
        "length": length,
    }
    admin = TEST_CASE.hadoopAdminUser
    TEST_CASE.createKmsKey(
        "create_kms_encryption_zone_key_for_testing",
        admin,
        payload,
        expected_status=201,
        test_desc="create_kms_encryption_zone_key_for_testing",
    )
    kms_ez_key_obj = TEST_CASE.getKmsEncryptionZoneKey(
        "create_kms_encryption_zone_key_for_testing",
        admin,
        key_name,
        test_desc="create_kms_encryption_zone_key_for_testing_fetch",
    )
    assert kms_ez_key_obj.name == key_name, "Encryption Zone key creation failed"
    return kms_ez_key_obj


def roll_over_key(kms_ez_key, provider_name):
    del provider_name
    admin = TEST_CASE.hadoopAdminUser
    TEST_CASE.rolloverEncryptionZoneKey(
        "roll_over_key",
        admin,
        kms_ez_key.name,
        expected_status=200,
        test_desc="roll_over_key",
    )
    return TEST_CASE.getKmsEncryptionZoneKey(
        "roll_over_key",
        admin,
        kms_ez_key.name,
        test_desc="roll_over_key_fetch",
    )


def delete_key_by_name(key_name, provider_name):
    del provider_name
    TEST_CASE.deleteEncryptionZoneKey(
        "delete_key_by_name",
        TEST_CASE.hadoopAdminUser,
        key_name,
        expected_status=200,
        test_desc="delete_key_by_name",
    )


def test_01_kms_encryption_zone_key_creation():
    key_name = "test_key_" + return_random_str(7)
    kms_ez_key_obj = create_kms_encryption_zone_key_for_testing(
        kms_test_service_name, key_name
    )
    assert kms_ez_key_obj.name == key_name, "Encryption Zone key creation failed"


def test_02_get_kms_encryption_zone_key():
    key_name = "test_key_" + return_random_str(7)
    kms_ez_key_obj = create_kms_encryption_zone_key_for_testing(
        kms_test_service_name, key_name
    )
    key_obj_fetched_by_name = TEST_CASE.getKmsEncryptionZoneKey(
        "test_02_get_kms_encryption_zone_key",
        TEST_CASE.hadoopAdminUser,
        key_name,
        test_desc="test_02_get_kms_encryption_zone_key",
    )
    assert kms_ez_key_obj.description == key_obj_fetched_by_name.description, (
        "Fetch KMS encryption zone key by name not successful"
    )


def test_03_roll_over_encryption_zone_key():
    key_name = "test_key_" + return_random_str(7)
    kms_ez_key_obj = create_kms_encryption_zone_key_for_testing(
        kms_test_service_name, key_name
    )
    rolled_over_key = roll_over_key(kms_ez_key_obj, kms_test_service_name)
    assert kms_ez_key_obj.name == rolled_over_key.name, (
        "Encryption zone key rollover not successful"
    )
    assert kms_ez_key_obj.versions + 1 == rolled_over_key.versions, (
        "Version of rolled over encryption zone key not equal to the expected value"
    )


def test_04_delete_kms_encryption_zone_key_by_name():
    key_name = "test_key_" + return_random_str(7)
    _ = create_kms_encryption_zone_key_for_testing(kms_test_service_name, key_name)
    delete_key_by_name(key_name, kms_test_service_name)
    kms_ez_keys_list = get_keys_list(kms_test_service_name)
    assert key_name not in kms_ez_keys_list, (
        "KMS Encryption Zone Key deletion not successful"
    )


def test_05_generate_edek_and_dek_and_verify():
    key_name = "test_key_edek_dek_" + return_random_str(7)
    kms_ez_key_obj = create_kms_encryption_zone_key_for_testing(
        kms_test_service_name, key_name
    )
    assert kms_ez_key_obj.name == key_name, "Failed to create Encryption Zone Key"

    admin = TEST_CASE.hadoopAdminUser
    generate_body = TEST_CASE.generateDekWithEdek(
        "test_05_generate_edek_and_dek_and_verify",
        admin,
        key_name,
        test_desc="test_05_generate_edek_and_dek_and_verify_generate_dek",
    )

    edek_material = generate_body["edek"]["encryptedKeyVersion"]["material"]
    edek_iv = generate_body["edek"]["iv"]
    dek_material = generate_body["dek"]["material"]

    decrypt_body = TEST_CASE.decryptEncryptedKeyExport(
        "test_05_generate_edek_and_dek_and_verify",
        admin,
        key_name,
        {
            "name": key_name,
            "iv": edek_iv,
            "material": edek_material,
        },
        test_desc="test_05_generate_edek_and_dek_and_verify_decrypt_edek",
    )

    decrypted_dek_material = decrypt_body["material"]
    assert decrypted_dek_material == dek_material, (
        "Mismatch between generated DEK and decrypted DEK"
    )
