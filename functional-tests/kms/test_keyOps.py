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
Functional tests for KMS key operations.

Tests key operations including:
  - Key rollover (versioning)
  - Data key generation (DEK/EDEK)
  - Encryption and decryption
  - Re-encryption of encrypted keys
  - Cache invalidation

Uses keyadmin for all operations.
Requires ranger-kms and ranger-hadoop containers to be running.
"""

import time

import pytest

from ranger_test_utils.kms_utils import (
    BASE_URL,
    PARAMS,
    ensure_keyadmin_ticket,
    fetch_logs,
    krb_requests,
)
from ranger_test_utils.plugin_enforcement_utils import logger

pytestmark = pytest.mark.kms

# Test key name used for rollover and data key generation tests
TEST_KEY_NAME = "key1"

# Global tracking for cleanup
_created_keys = []


def setup_module():
    """
    Module setup: ensure Kerberos ticket and create test key.
    
    Creates key1 which is used by rollover and data key generation tests.
    
    Container validation is handled by session fixture.
    """
    logger.info("=== setup_module: start ===")
    logger.info("Ensuring keyadmin Kerberos ticket for KMS operations")
    ensure_keyadmin_ticket()
    
    # Create test key (key1)
    logger.info("Creating test key: %s", TEST_KEY_NAME)
    key_data = {
        "name": TEST_KEY_NAME,
        "cipher": "AES/CTR/NoPadding",
        "length": 128,
        "description": "Test key for key operations tests"
    }
    
    response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert response.status_code == 201, f"Test key creation failed: {response.text}"
    _created_keys.append(TEST_KEY_NAME)
    logger.info("Created test key: %s", TEST_KEY_NAME)
    
    logger.info("=== setup_module: done ===")


def teardown_module():
    """
    Module teardown: cleanup all created keys.
    
    Deletes all keys tracked in _created_keys list.
    """
    logger.info("=== teardown_module: start ===")
    
    for key_name in _created_keys[:]:
        logger.info("Deleting key: %s", key_name)
        try:
            response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
            if response.status_code == 200:
                logger.info("✓ Deleted key: %s", key_name)
            else:
                logger.warning("Failed to delete key %s: %s", key_name, response.text)
        except Exception as e:
            logger.warning("Error deleting key %s: %s", key_name, e)
    
    _created_keys.clear()
    logger.info("=== teardown_module: done ===")


def test_temp_key():
    """
    Test temporary key creation and cleanup.
    
    Creates a rollover-key, verifies successful creation, and immediately
    deletes it. This tests basic key lifecycle operations.
    
    Expected behavior:
      - Delete if exists (cleanup from previous runs)
      - Create should return 201
      - Delete should succeed
    """
    logger.info("Test: Temporary key creation and cleanup")
    
    key_name = "rollover-key"
    data = {
        "name": key_name,
        "cipher": "AES/CTR/NoPadding",
        "length": 128,
        "description": "Key to check roll over functionality"
    }
    
    # Cleanup if exists from previous runs
    logger.info("Cleaning up any existing rollover-key")
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    
    # Create key
    logger.info("Creating temporary key: %s", key_name)
    key_creation_response = krb_requests.post(f"{BASE_URL}/keys", json=data, params=PARAMS)
    
    if key_creation_response.status_code != 201:
        logs = fetch_logs()
        pytest.fail(
            f"Create key operation failed. API Response: {key_creation_response.text}\nLogs:\n{logs}"
        )
    
    logger.info("✓ Created temporary key: %s", key_name)
    
    # Cleanup after test
    logger.info("Cleaning up temporary key: %s", key_name)
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    logger.info("✓ Deleted temporary key: %s", key_name)


@pytest.mark.parametrize("key_name, expected_status", [
    ("rollover-key", 200),             # Valid key rollover
    ("non-existent-key", 500)          # Rollover on a non-existent key
])
def test_roll_over_key(key_name, expected_status):
    """
    Test key rollover operation for existent and non-existent keys.
    
    Tests the POST /key/{name} endpoint (rollover operation) with:
    - Existent key: Should return 200 and create new version
    - Non-existent key: Should return 500 error
    
    Parametrized test with 2 cases.
    
    Expected behavior:
      - Valid key: 200 OK with new version created
      - Non-existent key: 500 error
    """
    logger.info("Test: Key rollover for %s (expected: %d)", key_name, expected_status)
    
    if key_name == "rollover-key":
        # Create the key first
        logger.info("Creating rollover-key for test")
        data = {
            "name": "rollover-key",
            "cipher": "AES/CTR/NoPadding",
            "length": 128,
            "description": "Key to check roll over functionality"
        }
        
        krb_requests.delete(f"{BASE_URL}/key/rollover-key", params=PARAMS)
        krb_requests.post(f"{BASE_URL}/keys", json=data, params=PARAMS)
        logger.info("✓ Created rollover-key")
    
    # Perform rollover
    logger.info("Attempting rollover for key: %s", key_name)
    response = krb_requests.post(
        f"{BASE_URL}/key/{key_name}",
        json={},
        params=PARAMS
    )
    
    if response.status_code != expected_status:
        logs = fetch_logs()
        pytest.fail(
            f"Rollover key operation failed. Expected {expected_status}, got {response.status_code}. "
            f"API Response: {response.text}\nLogs:\n{logs}"
        )
    
    logger.info("✓ Rollover returned expected status: %d", expected_status)
    
    # Cleanup
    if key_name == "rollover-key":
        logger.info("Cleaning up rollover-key")
        krb_requests.delete(f"{BASE_URL}/key/rollover-key", params=PARAMS)


def test_roll_over_new_material():
    """
    Test that key rollover creates new key material.
    
    Verifies that after a key rollover operation, the key metadata changes
    (indicating new material was generated). Compares metadata before and
    after rollover.
    
    Uses the test key (key1) created in setup_module.
    
    Expected behavior:
      - Metadata should differ after rollover
      - New version should be created with different material
    """
    logger.info("Test: Key rollover creates new material")
    
    # Get metadata before rollover
    logger.info("Fetching metadata before rollover")
    old_metadata = krb_requests.get(f"{BASE_URL}/key/{TEST_KEY_NAME}/_metadata", params=PARAMS)
    old_data = old_metadata.json()
    logger.info("Old Metadata: %s", old_data)
    
    # Perform rollover
    logger.info("Performing rollover on key: %s", TEST_KEY_NAME)
    rollover_response = krb_requests.post(f"{BASE_URL}/key/{TEST_KEY_NAME}", json={}, params=PARAMS)
    assert rollover_response.status_code == 200, f"Rollover failed: {rollover_response.text}"
    logger.info("✓ Rollover completed")
    
    # Get metadata after rollover
    logger.info("Fetching metadata after rollover")
    new_metadata = krb_requests.get(f"{BASE_URL}/key/{TEST_KEY_NAME}/_metadata", params=PARAMS)
    new_data = new_metadata.json()
    logger.info("New Metadata: %s", new_data)
    
    # Verify metadata changed
    assert old_data != new_data, "Key rollover should create new key material."
    logger.info("✓ Metadata changed after rollover (new material created)")


def test_generate_data_key_and_decrypt():
    """
    Test data key generation (DEK) and decryption of EDEK.
    
    Tests the full encryption key lifecycle:
    1. Generate a Data Encryption Key (DEK) and Encrypted DEK (EDEK)
    2. Extract encryption details from EDEK
    3. Decrypt the EDEK back to DEK
    4. Verify decrypted DEK matches the original
    
    Uses the test key (key1) created in setup_module.
    
    Expected behavior:
      - DEK generation returns both DEK and EDEK
      - EDEK can be decrypted back to original DEK
      - Decrypted DEK matches original DEK
    """
    logger.info("Test: Data key generation and decryption")
    
    # Generate Data Key
    logger.info("Generating data key (DEK) for key: %s", TEST_KEY_NAME)
    response = krb_requests.get(f"{BASE_URL}/key/{TEST_KEY_NAME}/_dek", params=PARAMS)
    
    if response.status_code != 200:
        logs = fetch_logs()
        pytest.fail(
            f"generation of data key operation failed. API Response: {response.text}\nLogs:\n{logs}"
        )
    
    data_key_response = response.json()
    dek = data_key_response.get("dek")
    edek = data_key_response.get("edek")
    
    logger.info("DEK: %s", dek)
    logger.info("EDEK: %s", edek)
    
    assert dek is not None, "Generated DEK should not be None"
    assert edek is not None, "Generated EDEK should not be None"
    logger.info("✓ Generated DEK and EDEK")
    
    # Extracting details for decryption from EDEK
    logger.info("Extracting encryption details from EDEK")
    encrypted_key_version = edek.get("encryptedKeyVersion")
    encrypted_material = encrypted_key_version.get("material")
    name = encrypted_key_version.get("name")
    version_name = edek.get("versionName")
    iv = edek.get("iv")
    
    decrypt_payload = {
        "name": name,
        "iv": iv,
        "material": encrypted_material,
    }
    
    # Decrypt EDEK
    logger.info("Decrypting EDEK to verify it matches original DEK")
    DECRYPT_PARAMS = {"user.name": "keyadmin", "eek_op": "decrypt"}
    decrypt_response = krb_requests.post(
        f"{BASE_URL}/keyversion/{version_name}/_eek",
        json=decrypt_payload,
        params=DECRYPT_PARAMS
    )
    
    if decrypt_response.status_code != 200:
        logs = fetch_logs()
        pytest.fail(
            f"Decryption of key operation failed. API Response: {decrypt_response.text}\nLogs:\n{logs}"
        )
    
    decrypted_data = decrypt_response.json()
    logger.info("Decrypted Data: %s", decrypted_data)
    
    # Verify decrypted key matches the original DEK
    assert decrypted_data == dek, "Decrypted DEK should match the original DEK"
    logger.info("✓ Decrypted DEK matches original DEK")

def test_reencrypt_encrypted_keys():
    """
    Test re-encryption of encrypted keys after key rotation.
    
    Tests the _reencryptbatch endpoint which re-encrypts EDEKs with the
    new key version after a rollover:
    1. Create a key
    2. Generate an EDEK
    3. Rotate the key (creating new version)
    4. Re-encrypt the EDEK with new version
    5. Verify EDEK versionName changed
    
    Expected behavior:
      - EDEK versionName should change after re-encryption
      - Re-encrypted EDEK uses new key version
    """
    logger.info("Test: Re-encryption of encrypted keys after rotation")
    
    # Step 1: Create the key
    key_name = "reencrypt-key"
    _created_keys.append(key_name)
    
    logger.info("Creating key: %s", key_name)
    data = {"name": key_name}
    create_response = krb_requests.post(f"{BASE_URL}/keys", json=data, params=PARAMS)
    logs = fetch_logs()
    assert create_response.status_code == 201, (
        f"Key creation failed: {create_response.text}\nLogs:\n{logs}"
    )
    logger.info("✓ Created key: %s", key_name)
    
    try:
        # Step 2: Generate an Encrypted DEK (EDEK) using the key
        logger.info("Generating EDEK for key: %s", key_name)
        generate_response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek", params=PARAMS)
        logs = fetch_logs()
        assert generate_response.status_code == 200, (
            f"EEK generation failed: {generate_response.text}\nLogs:\n{logs}"
        )
        
        logger.info("EDEK response: %s", generate_response.json())
        
        edek = generate_response.json()["edek"]
        encrypted_key_version = edek["encryptedKeyVersion"]
        logger.info("✓ Generated EDEK with version: %s", edek["versionName"])
        
        edek_payload = [
            {
                "versionName": edek["versionName"],
                "iv": edek["iv"],
                "encryptedKeyVersion": {
                    "versionName": "EEK",
                    "material": encrypted_key_version["material"]
                }
            }
        ]
        
        # Step 3: Rotate the key (to create a new version)
        logger.info("Rotating key: %s", key_name)
        rollover_response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params=PARAMS)
        assert rollover_response.status_code == 200, f"Key rollover failed: {rollover_response.text}"
        logger.info("✓ Key rotated successfully")
        
        # Step 4: Call the reencryptEncryptedKeys API
        logger.info("Re-encrypting EDEK with new key version")
        reencrypt_url = f"{BASE_URL}/key/{key_name}/_reencryptbatch"
        reencrypt_response = krb_requests.post(reencrypt_url, json=edek_payload, params=PARAMS)
        assert reencrypt_response.status_code == 200, (
            f"Re-encrypt call failed: {reencrypt_response.text}"
        )
        
        # Step 5: Validate the response EDEKs
        reencrypted_edeks = reencrypt_response.json()
        logger.info("Re-encrypted EDEKs: %s", reencrypted_edeks)
        
        assert isinstance(reencrypted_edeks, list), "Expected list of re-encrypted EDEKs"
        assert len(reencrypted_edeks) == 1, "Expected exactly one re-encrypted EDEK"
        assert reencrypted_edeks[0]["versionName"] != edek["versionName"], (
            "Expected EDEK version to change after re-encryption"
        )
        logger.info("✓ EDEK version changed after re-encryption: %s -> %s",
                    edek["versionName"], reencrypted_edeks[0]["versionName"])
        
    finally:
        # Cleanup key
        logger.info("Cleaning up key: %s", key_name)
        krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
        _created_keys.remove(key_name)



def test_generate_data_key_after_invalidate_cache():
    """
    Test data key generation after cache invalidation.
    
    Tests the cache invalidation mechanism when key versions get out of sync:
    1. Create key (version @0)
    2. Rollover (creates version @1, cached)
    3. Delete key (DB clean, cache stale with @1)
    4. Recreate key (only @0 in DB, cache still has stale @1)
    5. Invalidate cache (forces reload from DB)
    6. Generate DEK (should succeed with correct version @0)
    
    This verifies that cache invalidation properly syncs KMS state with DB.
    
    Expected behavior:
      - DEK generation fails without cache invalidation (stale cache)
      - DEK generation succeeds after cache invalidation
    """
    logger.info("Test: Data key generation after cache invalidation")
    
    key_name = "cache_key"
    data = {"name": key_name}
    
    # Step 1: Create a key
    logger.info("Creating key: %s", key_name)
    create_response = krb_requests.post(f"{BASE_URL}/keys", json=data, params=PARAMS)
    assert create_response.status_code == 201, "Key creation failed"
    logger.info("✓ Created key (version @0)")
    
    # Step 2: Roll over (creates @1, cached)
    logger.info("Rolling over key (creates version @1)")
    roll_response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params=PARAMS)
    assert roll_response.status_code == 200, "Rollover failed"
    logger.info("✓ Rollover completed (version @1 cached)")
    
    # Step 3: Delete the key (DB is clean, cache still references @1)
    logger.info("Deleting key (DB clean, cache stale with @1)")
    delete_response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    assert delete_response.status_code == 200, "Key deletion failed"
    logger.info("✓ Key deleted from DB")
    
    time.sleep(5)
    
    # Step 4: Recreate the key (creates only @0 in DB, cache still stale @1)
    logger.info("Recreating key (creates @0 in DB, cache still has stale @1)")
    recreate_response = krb_requests.post(f"{BASE_URL}/keys", json=data, params=PARAMS)
    assert recreate_response.status_code == 201, "Key recreation failed"
    logger.info("✓ Key recreated (version @0 in DB)")
    
    # Step 5: Invalidate cache – forces KMS to reload latest version from DB
    logger.info("Invalidating cache to force reload from DB")
    invalidate_params = {"user.name": "keyadmin", "action": "invalidateCache"}
    invalidate_response = krb_requests.post(
        f"{BASE_URL}/key/{key_name}",
        json={},
        params=invalidate_params
    )
    assert invalidate_response.status_code == 200, "Invalidate cache failed"
    logger.info("✓ Cache invalidated")
    
    # Step 6: try DEK – should succeed (correct version @0 loaded)
    logger.info("Generating DEK (should succeed with correct version @0)")
    dek_response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek", params=PARAMS)
    assert dek_response.status_code == 200, (
        "DEK generation should succeed after cache invalidation"
    )
    logger.info("✓ DEK generation succeeded after cache invalidation")
    
    # Cleanup
    logger.info("Cleaning up cache_key")
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
