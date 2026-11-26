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
KMS REST API key lifecycle tests including rollover, material validation, deletion,
and bulk operations.

Requires ranger-kms and ranger-hadoop containers to be running.
"""

import pytest

from ranger_test_utils.kms_utils import (
    BASE_URL,
    PARAMS,
    ensure_keyadmin_ticket,
    krb_requests,
)
from ranger_test_utils.plugin_enforcement_utils import logger

pytestmark = pytest.mark.kms

# Global list to track keys created during tests for cleanup
_created_keys = []


def setup_module():
    """
    Module setup: ensure Kerberos ticket for keyadmin user.
    
    Container validation is handled by session fixture (runs once for all KMS tests).
    This module uses keyadmin for all operations.
    """
    logger.info("=== setup_module: start ===")
    logger.info("Ensuring keyadmin Kerberos ticket for KMS REST operations")
    ensure_keyadmin_ticket()
    logger.info("=== setup_module: done ===")


def teardown_module():
    """
    Module teardown: clean up any keys that were not properly deleted during test execution.
    
    This cleanup runs after all tests in this module complete, even if tests fail.
    """
    logger.info("=== teardown_module: start ===")
    
    if _created_keys:
        logger.info("Cleaning up remaining keys: %s", _created_keys)
        for key_name in _created_keys:
            try:
                response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
                if response.status_code in (200, 404):
                    logger.info("Deleted key: %s", key_name)
                else:
                    logger.warning(
                        "Failed to delete key %s: status=%s", key_name, response.status_code
                    )
            except Exception as e:
                logger.error("Error deleting key %s: %s", key_name, e)
    
    logger.info("=== teardown_module: done ===")


def test_versionIncrement_after_rollover():
    """
    Test key version increments correctly after rollover.
    
    This test verifies that when a key is rolled over, its version number
    increments by exactly 1 (e.g., key@0 becomes key@1).
    
    Expected behavior: new version = old version + 1
    """
    key_name = "key_roll"
    _created_keys.append(key_name)
    
    key_data = {
        "name": key_name
    }
    
    # Create key
    logger.info("Creating key: %s", key_name)
    response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # Check version before rollover
    response_before = krb_requests.get(
        f"{BASE_URL}/key/{key_name}/_currentversion", params=PARAMS
    )
    assert response_before.status_code == 200, (
        f"Failed to get current version. Response: {response_before.text}"
    )

    # Extract version number
    version_before = response_before.json().get("versionName")  # e.g "test-key@0"
    version_num_before = int(version_before.split("@")[1])
    logger.info("Version before rollover: %s", version_num_before)

    # Perform rollover
    logger.info("Performing key rollover for: %s", key_name)
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params=PARAMS)
    assert response.status_code == 200, (
        f"Failed to perform rollover. Response: {response.text}"
    )

    # Check version after rollover
    response_after = krb_requests.get(
        f"{BASE_URL}/key/{key_name}/_currentversion", params=PARAMS
    )
    assert response_after.status_code == 200, (
        f"Failed to get current version. Response: {response_after.text}"
    )

    # Extract new version number
    version_after = response_after.json().get("versionName")
    version_num_after = int(version_after.split("@")[1])
    logger.info("Version after rollover: %s", version_num_after)

    assert version_num_after == version_num_before + 1, (
        f"Expected version to increment by 1. Before: {version_before}, After: {version_after}"
    )

    # Cleanup key after test
    logger.info("Cleaning up key: %s", key_name)
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    _created_keys.remove(key_name)


def test_key_material():
    """
    Test that key material used during creation matches the material retrieved via API.
    
    This test verifies that when a key is created with specific key material,
    the same material can be retrieved through the current version endpoint.
    
    Expected behavior: Created material equals retrieved material
    """
    key_name = "test-key"
    key_material = "G90ZtTKOWIICXG_wpqx0tA"
    _created_keys.append(key_name)

    key_data = {
        "name": key_name,
        "material": key_material,
        "length": 128
    }

    # Create key with specific material
    logger.info("Creating key %s with custom material", key_name)
    response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # Check material from current version
    logger.info("Retrieving key material for validation")
    version_response = krb_requests.get(
        f"{BASE_URL}/key/{key_name}/_currentversion", params=PARAMS
    )
    assert version_response.status_code == 200, (
        f"Failed to get current version. Response: {version_response.text}"
    )

    response_key_material = version_response.json().get("material")

    assert key_material == response_key_material, (
        f"Key material mismatch. Expected: {key_material}, Got: {response_key_material}"
    )

    # Cleanup key after test
    logger.info("Cleaning up key: %s", key_name)
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    _created_keys.remove(key_name)


def test_deleted_key_not_in_list():
    """
    Test that a deleted key is no longer present in the key list.
    
    This test verifies the deletion operation by confirming that a key
    removed via the DELETE endpoint does not appear in subsequent key list queries.
    
    Expected behavior: Deleted key name absent from key list
    """
    key_name = "Delete-key"
    _created_keys.append(key_name)

    key_data = {
        "name": key_name,
    }

    # Create key
    logger.info("Creating key: %s", key_name)
    response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # Delete key
    logger.info("Deleting key: %s", key_name)
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    _created_keys.remove(key_name)
    
    # Verify key is not in list
    logger.info("Verifying key deletion from key list")
    list_response = krb_requests.get(f"{BASE_URL}/keys/names", params=PARAMS)

    key_list = list_response.json()

    assert key_name not in key_list, (
        f"Deleted key '{key_name}' still exists in key list. Deletion might have failed."
    )


def test_bulk_key_operation():
    """
    Test bulk key operations: creation, retrieval, and deletion.
    
    This test verifies that the KMS can handle multiple keys in sequence,
    ensuring all CRUD operations work correctly when dealing with multiple keys.
    It creates 5 keys, verifies their presence, retrieves their metadata,
    and finally deletes all of them.
    
    Expected behavior: All operations succeed for all keys
    """
    key_names = [f"key{i}" for i in range(5)]
    created_keys = []

    # Create 5 keys
    logger.info("Creating 5 keys for bulk operation test")
    for name in key_names:
        _created_keys.append(name)
        key_data = {
            "name": name,
        }

        response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
        assert response.status_code == 201, f"Failed to create key {name}: {response.text}"
        created_keys.append(name)
        logger.info("Created key: %s", name)

    # Get all keys and verify they exist
    logger.info("Verifying all created keys are in the key list")
    list_response = krb_requests.get(f"{BASE_URL}/keys/names", params=PARAMS)
    assert list_response.status_code == 200, f"Fetching key list failed: {list_response.text}"

    all_keys = list_response.json()

    for name in created_keys:
        assert name in all_keys, f"Key '{name}' not found in key list."

    # Get metadata for each key
    logger.info("Retrieving metadata for all created keys")
    for name in created_keys:
        meta_response = krb_requests.get(f"{BASE_URL}/key/{name}", params=PARAMS)
        assert meta_response.status_code == 200, f"Failed to get metadata for key {name}"

    # Delete all 5 keys
    logger.info("Deleting all created keys")
    for name in created_keys:
        del_response = krb_requests.delete(f"{BASE_URL}/key/{name}", params=PARAMS)
        assert del_response.status_code == 200, (
            f"Failed to delete key {name}: {del_response.text}"
        )
        _created_keys.remove(name)
        logger.info("Deleted key: %s", name)

    # Verify keys are deleted
    logger.info("Verifying all keys have been removed from the key list")
    final_list_response = krb_requests.get(f"{BASE_URL}/keys/names", params=PARAMS)
    assert final_list_response.status_code == 200, (
        "Fetching key list after deletion failed"
    )
    final_keys = final_list_response.json()

    for name in created_keys:
        assert name not in final_keys, f"Deleted key '{name}' still found in key list"
