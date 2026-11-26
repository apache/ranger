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
Functional tests for KMS key management operations.

Tests basic key creation and validation:
  - Key creation with cleanup
  - Key name validation (valid and invalid patterns)
  - Duplicate key creation (negative test)

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


def setup_module():
    """
    Module setup: ensure Kerberos ticket for keyadmin.
    
    Container validation is handled by session fixture.
    Tests in this module create and delete keys inline (no persistent test keys).
    """
    logger.info("=== setup_module: start ===")
    logger.info("Ensuring keyadmin Kerberos ticket for KMS operations")
    ensure_keyadmin_ticket()
    logger.info("=== setup_module: done ===")


def teardown_module():
    """
    Module teardown: no cleanup needed.
    
    Tests in this module clean up keys inline.
    """
    logger.info("=== teardown_module: done ===")



def test_create_key():
    """
    Test basic key creation and cleanup.
    
    Creates a test key (key2), verifies successful creation (201),
    and immediately deletes it.
    
    Expected behavior:
      - Key creation returns 201
      - Key can be deleted successfully
    """
    logger.info("Test: Basic key creation")
    
    key_name = "key2"
    key_data = {
        "name": key_name,
        "cipher": "AES/CTR/NoPadding",
        "length": 128,
        "description": "New key for checking key creation functionality"
    }
    
    logger.info("Creating key: %s", key_name)
    response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    
    if response.status_code != 201:
        error_logs = fetch_logs()
        pytest.fail(
            f"Key creation failed. API Response: {response.text}\nLogs:\n{error_logs}"
        )
    
    logger.info("✓ Key created successfully: %s", key_name)
    
    # Cleanup
    logger.info("Cleaning up key: %s", key_name)
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    logger.info("✓ Key deleted: %s", key_name)


@pytest.mark.parametrize("name, expected_status", [
    ("valid-key", 201),           # Valid key name
    ("", 400),                    # Invalid: Empty name
    ("@invalid!", 400),           # Invalid: Special characters
    ("invalid--key", 400)         # Invalid: -- or __ or _- -_ not allowed
])
def test_key_name_validation(name, expected_status):
    """
    Test key name validation with valid and invalid patterns.
    
    Tests the POST /keys endpoint with various key names:
    - Valid key name: Should return 201
    - Empty name: Should return 400
    - Special characters (@, !): Should return 400
    - Invalid patterns (--): Should return 400
    
    Parametrized test with 4 cases.
    
    Expected behavior:
      - Valid names: 201 (key created and cleaned up)
      - Invalid names: 400 (validation error)
    """
    logger.info("Test: Key name validation for '%s' (expected: %d)", name, expected_status)
    
    key_data = {
        "name": name,
        "cipher": "AES/CTR/NoPadding",
        "length": 128,
        "description": "Validation test"
    }
    
    logger.info("Creating key with name: '%s'", name)
    response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    
    if response.status_code != expected_status:
        error_logs = fetch_logs()
        pytest.fail(
            f"Key validation failed. Expected {expected_status}, got {response.status_code}. "
            f"API Response: {response.text}\nLogs:\n{error_logs}"
        )
    
    logger.info("✓ Key name validation returned expected status: %d", expected_status)
    
    # Cleanup if key was created successfully
    if expected_status == 201:
        logger.info("Cleaning up successfully created key: %s", name)
        krb_requests.delete(f"{BASE_URL}/key/{name}", params=PARAMS)
        logger.info("✓ Key deleted: %s", name)



def test_duplicate_key_creation():
    """
    Test duplicate key creation (negative test).
    
    Verifies that attempting to create a key with an existing name fails:
    1. Create a key successfully (201)
    2. Try to create the same key again (should fail with 500)
    3. Cleanup the original key
    
    Expected behavior:
      - First creation: 201
      - Duplicate creation: 500 (error)
    """
    logger.info("Test: Duplicate key creation (negative test)")
    
    key_name = "duplicate-key"
    key_data = {
        "name": key_name,
        "cipher": "AES/CTR/NoPadding",
        "length": 128,
        "description": "Testing duplicate key creation"
    }
    
    # Create key first time
    logger.info("Creating key (first time): %s", key_name)
    response1 = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert response1.status_code == 201, f"Initial key creation failed: {response1.text}"
    logger.info("✓ Key created successfully: %s", key_name)
    
    # Try creating the same key again (should fail)
    logger.info("Attempting to create duplicate key: %s (should fail)", key_name)
    response2 = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    
    assert response2.status_code == 500, (
        f"Duplicate key got created (expected to fail). Status: {response2.status_code}, "
        f"Response: {response2.text}"
    )
    logger.info("✓ Duplicate key creation failed as expected (500)")
    
    # Cleanup
    logger.info("Cleaning up key: %s", key_name)
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    logger.info("✓ Key deleted: %s", key_name)




