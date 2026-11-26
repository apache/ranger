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
Functional tests for KMS key details retrieval endpoints.

Tests various endpoints for retrieving key information:
  - Get key names (list all keys)
  - Get key metadata (single and multiple keys)
  - Get key versions
  - Get specific key version details

Tests both existent and non-existent keys to verify error handling.

Uses keyadmin for all operations.
Requires ranger-kms and ranger-hadoop containers to be running.
"""

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

# Test key name used across all tests
TEST_KEY_NAME = "key1"

# Global tracking for cleanup
_created_keys = []


def setup_module():
    """
    Module setup: ensure Kerberos ticket and create test key.
    
    Creates key1 which is used by most tests in this module to verify
    key detail retrieval operations.
    
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
        "description": "Test key for key details tests"
    }
    
    response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert response.status_code == 201, f"Test key creation failed: {response.text}"
    _created_keys.append(TEST_KEY_NAME)
    logger.info("Created test key: %s", TEST_KEY_NAME)
    
    logger.info("=== setup_module: done ===")


def teardown_module():
    """
    Module teardown: clean up all created keys.
    """
    logger.info("=== teardown_module: start ===")
    
    if _created_keys:
        logger.info("Cleaning up keys: %s", _created_keys)
        ensure_keyadmin_ticket()
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

def test_get_key_names():
    """
    Test retrieving list of all key names.
    
    Verifies that the GET /keys/names endpoint returns a list containing
    all existing key names, including the test key created in setup_module.
    
    Expected behavior: 200 OK with list containing test key name
    """
    logger.info("Test: Get key names")
    
    logger.info("Retrieving list of all key names")
    response = krb_requests.get(f"{BASE_URL}/keys/names", params=PARAMS)
    
    if response.status_code != 200:
        logs = fetch_logs()
        pytest.fail(f"Get key operation failed. API Response: {response.text}\nLogs:\n{logs}")
    
    key_names = response.json()
    logger.info("Retrieved %d key names", len(key_names))
    
    assert TEST_KEY_NAME in key_names, (
        f"Test key '{TEST_KEY_NAME}' not found in key list: {key_names}"
    )
    logger.info("✓ Test key '%s' found in key list", TEST_KEY_NAME)


@pytest.mark.parametrize("key_name, expected_status, expected_response", [
    ("key1", 200, "valid"),                   # Key exists, should return valid metadata
    ("non-existent-key", 200, "invalid"),     # Key does not exist but returns 200 with [] should give 404
])
def test_get_key_metadata(key_name, expected_status, expected_response):
    """
    Test retrieving key metadata for existent and non-existent keys.
    
    Tests the GET /key/{name}/_metadata endpoint with:
    - Existent key: Should return 200 with valid metadata
    - Non-existent key: Should return 200 with empty/blank response
    
    Expected behavior:
      - Existent key: 200 with metadata
      - Non-existent key: 200 with blank response ([], {}, or empty)
    """
    logger.info("Test: Get key metadata for %s (expected: %s)", key_name, expected_response)
    
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params=PARAMS)
    
    if response.status_code != expected_status:
        logs = fetch_logs()
        pytest.fail(f"Get key metadata operation failed. API Response: {response.text}\nLogs:\n{logs}")
    
    if expected_response == "invalid":
        logger.info("Verifying blank response for non-existent key")
        assert response.text.strip() in ["", "{}", "{ }", "[]", "[ ]"], (
            f"Expected blank response for non-existent key, got: {response.text}"
        )
        logger.info("✓ Blank response received for non-existent key")
    else:
        logger.info("✓ Valid metadata received for existent key")


@pytest.mark.parametrize("key_name, expected_status, expected_response", [
    ("key1", 200, "valid"),               # Key exists
    ("non-existent-key", 200, "invalid"),  # Key does not exist but returns 200 with [] should give 404
])
def test_get_key_versions(key_name, expected_status, expected_response):
    """
    Test retrieving key versions for existent and non-existent keys.
    
    Tests the GET /key/{name}/_versions endpoint with:
    - Existent key: Should return 200 with version list
    - Non-existent key: Should return 200 with empty/blank response
    
    Expected behavior:
      - Existent key: 200 with version list
      - Non-existent key: 200 with blank response ([], {}, or empty)
    """
    logger.info("Test: Get key versions for %s (expected: %s)", key_name, expected_response)
    
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_versions", params=PARAMS)
    
    if response.status_code != expected_status:
        logs = fetch_logs()
        pytest.fail(f"Get key version operation failed. API Response: {response.text}\nLogs:\n{logs}")
    
    if expected_response == "invalid":
        logger.info("Verifying blank response for non-existent key")
        assert response.text.strip() in ["", "{}", "{ }", "[]", "[ ]"], (
            f"Expected blank response for non-existent key, got: {response.text}"
        )
        logger.info("✓ Blank response received for non-existent key")
    else:
        logger.info("✓ Valid version list received for existent key")


def test_get_keys_metadata():
    """
    Test retrieving metadata for multiple keys at once.
    
    Tests the GET /keys/metadata endpoint with multiple key parameters:
    - Existent keys: Should return metadata for all requested keys
    - Non-existent keys: Should return empty objects for each non-existent key
    
    This test creates a temporary second key (key2) to test multi-key retrieval,
    then cleans it up.
    
    Expected behavior:
      - Existent keys: 200 with array of metadata objects
      - Non-existent keys: 200 with array of empty objects [{}, {}]
    """
    logger.info("Test: Get metadata for multiple keys")
    
    # Create second key (key2)
    key_name = "key2"
    _created_keys.append(key_name)
    
    logger.info("Creating temporary key: %s", key_name)
    data = {"name": key_name}
    create_response = krb_requests.post(f"{BASE_URL}/keys", json=data, params=PARAMS)
    assert create_response.status_code == 201, f"Key2 creation failed: {create_response.text}"
    logger.info("✓ Created key: %s", key_name)
    
    try:
        # Check metadata for existing keys (key1 and key2)
        logger.info("Fetching metadata for existing keys: key1, key2")
        existing_keys = ["key1", "key2"]
        params = [("key", k) for k in existing_keys]
        params.append(("user.name", "keyadmin"))
        
        response = krb_requests.get(f"{BASE_URL}/keys/metadata", params=params)
        assert response.status_code == 200, f"Metadata fetch failed: {response.status_code}"
        
        metadata = response.json()
        returned_keys = [entry["name"] for entry in metadata]
        logger.info("Received metadata for keys: %s", returned_keys)
        
        for key in existing_keys:
            assert key in returned_keys, f"Expected key '{key}' not found in metadata response"
        logger.info("✓ All existing keys found in metadata response")
        
        # Check metadata for non-existent keys
        logger.info("Fetching metadata for non-existent keys")
        fake_keys = ["nonExistent_key_1", "nonExistent_key_2"]
        params = [("key", k) for k in fake_keys]
        params.append(("user.name", "keyadmin"))
        
        response = krb_requests.get(f"{BASE_URL}/keys/metadata", params=params)
        assert response.status_code == 200, (
            f"Metadata fetch failed for non-existent keys: {response.status_code}"
        )
        
        assert response.json() == [{}, {}], (
            f"Expected blank response for non-existent keys, got: {response.text}"
        )
        logger.info("✓ Empty objects received for non-existent keys")
        
    finally:
        # Cleanup key2
        logger.info("Cleaning up temporary key: %s", key_name)
        krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
        _created_keys.remove(key_name)


@pytest.mark.parametrize("version_name, expected_status, expected_valid", [
    ("key1@0", 200, True),                      # Valid version
    ("non-existent-key@0", 200, False),         # Key does not exist but returns 200 with [] should give 404
])
def test_get_key_version(version_name, expected_status, expected_valid):
    """
    Test retrieving specific key version details.
    
    Tests the GET /keyversion/{versionName} endpoint with:
    - Existent version: Should return 200 with version details including versionName
    - Non-existent version: Should return 200 with empty/blank response
    
    Version name format: keyname@versionNumber (e.g., key1@0)
    
    Expected behavior:
      - Existent version: 200 with version details
      - Non-existent version: 200 with blank response ([], {}, or empty)
    """
    logger.info("Test: Get key version details for %s (expected valid: %s)", 
                version_name, expected_valid)
    
    response = krb_requests.get(f"{BASE_URL}/keyversion/{version_name}", params=PARAMS)
    
    if response.status_code != expected_status:
        logs = fetch_logs()
        pytest.fail(f"Get key version failed. Response: {response.text}\nLogs:\n{logs}")
    
    if expected_valid:
        logger.info("Verifying valid version details")
        try:
            version_data = response.json()
            assert "versionName" in version_data, "versionName missing in response"
            assert version_data["versionName"] == version_name, (
                f"Mismatch in version name: expected {version_name}, got {version_data.get('versionName')}"
            )
            logger.info("✓ Valid version details received with correct versionName")
        except ValueError:
            pytest.fail(f"Expected valid JSON response, got: {response.text}")
    else:
        logger.info("Verifying blank response for non-existent version")
        assert response.text.strip() in ["", "{}", "{ }", "[]", "[ ]"], (
            f"Expected blank response for non-existent key version, got: {response.text}"
        )
        logger.info("✓ Blank response received for non-existent version")



