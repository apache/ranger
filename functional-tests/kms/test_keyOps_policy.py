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
Functional tests for KMS policy-based access control.

Tests various permission combinations to verify KMS policy enforcement:
  - CREATE only
  - CREATE + DELETE
  - CREATE + DELETE + ROLLOVER
  - CREATE + DELETE + ROLLOVER + GET
  - CREATE + DELETE + ROLLOVER + GET + GETMETADATA
  - CREATE + DELETE + ROLLOVER + GET + GETMETADATA + GENERATEEEK
  - Full access (all permissions)
  - No access (deny all operations)

Uses testuser for policy enforcement tests and keyadmin for cleanup.
Requires ranger-kms, ranger-hadoop, and ranger-admin containers.
"""

import time

import pytest
import requests

from ranger_test_utils.kms_utils import (
    BASE_URL,
    BASE_URL_RANGER,
    KMS_SERVICE_NAME,
    PARAMS,
    RANGER_KMS_AUTH,
    TESTUSER,
    delete_test_user,
    ensure_keyadmin_ticket,
    ensure_test_user_exists,
    ensure_testuser_ticket,
    find_conflicting_policies,
    krb_requests,
    update_kms_policy,
)
from ranger_test_utils.plugin_enforcement_utils import logger

pytestmark = pytest.mark.kms

# Policy and key name patterns
POLICY_NAME = "pytest-policy"
KEYNAME_PATTERN = "pytest-*"

# Module-level policy ID (shared across all tests)
_policy_id = None


def setup_module():
    """
    Module setup: create testuser, obtain tickets, and create shared policy.
    
    Creates:
      1. Ranger testuser (REST API)
      2. Kerberos principals for testuser and keyadmin
      3. Shared KMS policy (pytest-policy on pytest-* keys)
    
    All tests share the same policy and update its policyItems as needed.
    Container validation is handled by session fixture.
    """
    global _policy_id
    
    logger.info("=== setup_module: start ===")
    
    # Create testuser in Ranger Admin (REST API)
    logger.info("Creating testuser in Ranger Admin: %s", TESTUSER)
    ensure_test_user_exists(TESTUSER)
    
    # Obtain Kerberos tickets
    logger.info("Ensuring Kerberos tickets for testuser and keyadmin")
    ensure_testuser_ticket()
    ensure_keyadmin_ticket()
    
    # Clean up any stale policy on this resource pattern
    logger.info("Cleaning up stale policies for pattern: %s", KEYNAME_PATTERN)
    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, KEYNAME_PATTERN):
        logger.info("Deleting stale policy: %s", stale_id)
        requests.delete(f"{BASE_URL_RANGER}/{stale_id}", auth=RANGER_KMS_AUTH)
    
    if find_conflicting_policies(KMS_SERVICE_NAME, KEYNAME_PATTERN):
        logger.info("Waiting for stale policy deletion to propagate")
        time.sleep(5)
    
    # Create shared policy (empty policyItems - tests will update as needed)
    logger.info("Creating shared KMS policy: %s", POLICY_NAME)
    policy_data = {
        "policyName": POLICY_NAME,
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {
                "values": [KEYNAME_PATTERN],
                "isExcludes": False,
                "isRecursive": False
            }
        },
        "policyItems": []
    }
    
    response = requests.post(BASE_URL_RANGER, auth=RANGER_KMS_AUTH, json=policy_data)
    time.sleep(5)
    
    if response.status_code not in (200, 201):
        raise Exception(f"Failed to create initial policy: {response.text}")
    
    _policy_id = response.json()["id"]
    logger.info("Created shared policy with ID: %s", _policy_id)
    logger.info("=== setup_module: done ===")


def teardown_module():
    """
    Module teardown: delete shared policy and testuser.
    
    Cleans up:
      1. Shared KMS policy
      2. Ranger testuser
    """
    logger.info("=== teardown_module: start ===")
    
    # Delete shared policy
    if _policy_id:
        logger.info("Deleting shared policy: %s", _policy_id)
        try:
            response = requests.delete(f"{BASE_URL_RANGER}/{_policy_id}", auth=RANGER_KMS_AUTH)
            if response.status_code == 204:
                logger.info("✓ Deleted shared policy")
            else:
                logger.warning("Failed to delete policy %s: %s", _policy_id, response.text)
        except Exception as e:
            logger.warning("Error deleting policy %s: %s", _policy_id, e)
    
    # Delete testuser
    logger.info("Deleting testuser: %s", TESTUSER)
    delete_test_user(TESTUSER)
    
    logger.info("=== teardown_module: done ===")



def test_policy_01():
    """
    Test policy with CREATE access only.
    
    Verifies that with only CREATE permission:
    - CREATE: Allowed (201)
    - GET current version: Denied (403)
    - GET metadata: Denied (403)
    - ROLLOVER: Denied (403)
    - GENERATE DEK: Denied (403)
    - DELETE: Denied (403)
    
    Policy: pytest-* with CREATE only
    User: testuser
    """
    logger.info("Test: Policy with CREATE access only")
    
    update_kms_policy(_policy_id, TESTUSER, accesses=["create"])
    key_name = "pytest-key-01"
    
    # Cleanup any existing key
    logger.info("Cleaning up any existing key: %s", key_name)
    ensure_keyadmin_ticket()
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    
    # Switch to testuser
    ensure_testuser_ticket()
    
    # CREATE - should succeed
    logger.info("Testing CREATE (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name})
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    logger.info("✓ CREATE succeeded (201)")
    
    # GET current version - should be denied
    logger.info("Testing GET current version (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion")
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GET current version denied (403)")
    
    # GET metadata - should be denied
    logger.info("Testing GET metadata (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GET metadata denied (403)")
    
    # ROLLOVER - should be denied
    logger.info("Testing ROLLOVER (should be denied)")
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ ROLLOVER denied (403)")
    
    # GENERATE DEK - should be denied
    logger.info("Testing GENERATE DEK (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GENERATE DEK denied (403)")
    
    # DELETE - should be denied
    logger.info("Testing DELETE (should be denied)")
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ DELETE denied (403)")
    
    # Cleanup as keyadmin
    logger.info("Cleaning up key as keyadmin")
    ensure_keyadmin_ticket()
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)



def test_policy_02():
    """
    Test policy with CREATE + DELETE access.
    
    Verifies that with CREATE and DELETE permissions:
    - CREATE: Allowed (201)
    - GET current version: Denied (403)
    - GET metadata: Denied (403)
    - ROLLOVER: Denied (403)
    - GENERATE DEK: Denied (403)
    - DELETE: Allowed (200)
    
    Policy: pytest-* with CREATE, DELETE
    User: testuser
    """
    logger.info("Test: Policy with CREATE + DELETE access")
    
    update_kms_policy(_policy_id, TESTUSER, accesses=["create", "delete"])
    key_name = "pytest-key-02"
    
    ensure_testuser_ticket()
    
    # CREATE - should succeed
    logger.info("Testing CREATE (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name})
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    logger.info("✓ CREATE succeeded (201)")
    
    # GET current version - should be denied
    logger.info("Testing GET current version (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion")
    assert response.status_code == 403, f"Get current version failed: {response.text}"
    logger.info("✓ GET current version denied (403)")
    
    # GET metadata - should be denied
    logger.info("Testing GET metadata (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata")
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GET metadata denied (403)")
    
    # ROLLOVER - should be denied
    logger.info("Testing ROLLOVER (should be denied)")
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ ROLLOVER denied (403)")
    
    # GENERATE DEK - should be denied
    logger.info("Testing GENERATE DEK (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek")
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GENERATE DEK denied (403)")
    
    # DELETE - should succeed
    logger.info("Testing DELETE (should succeed)")
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}")
    assert response.status_code == 200, f"Key deletion failed: {response.text}"
    logger.info("✓ DELETE succeeded (200)")



def test_policy_03():
    """
    Test policy with CREATE + DELETE + ROLLOVER access.
    
    Verifies that with CREATE, DELETE, and ROLLOVER permissions:
    - CREATE: Allowed (201)
    - ROLLOVER: Allowed (200)
    - GET current version: Denied (403)
    - GET metadata: Denied (403)
    - GENERATE DEK: Denied (403)
    - DELETE: Allowed (200)
    
    Policy: pytest-* with CREATE, DELETE, ROLLOVER
    User: testuser
    """
    logger.info("Test: Policy with CREATE + DELETE + ROLLOVER access")
    
    update_kms_policy(_policy_id, TESTUSER, accesses=["CREATE", "DELETE", "ROLLOVER"])
    key_name = "pytest-key-03"
    
    # CREATE - should succeed
    logger.info("Testing CREATE (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name})
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    logger.info("✓ CREATE succeeded (201)")
    
    # ROLLOVER - should succeed
    logger.info("Testing ROLLOVER (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ ROLLOVER succeeded (200)")
    
    # GET current version - should be denied
    logger.info("Testing GET current version (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Get current version failed: {response.text}"
    logger.info("✓ GET current version denied (403)")
    
    # GET metadata - should be denied
    logger.info("Testing GET metadata (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GET metadata denied (403)")
    
    # GENERATE DEK - should be denied
    logger.info("Testing GENERATE DEK (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GENERATE DEK denied (403)")
    
    # DELETE - should succeed
    logger.info("Testing DELETE (should succeed)")
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Key deletion failed: {response.text}"
    logger.info("✓ DELETE succeeded (200)")



def test_policy_04():
    """
    Test policy with CREATE + DELETE + ROLLOVER + GET access.
    
    Verifies that with CREATE, DELETE, ROLLOVER, and GET permissions:
    - CREATE: Allowed (201)
    - ROLLOVER: Allowed (200)
    - GET current version: Allowed (200)
    - GET metadata: Denied (403)
    - GENERATE DEK: Denied (403)
    - DELETE: Allowed (200)
    
    Policy: pytest-* with CREATE, DELETE, ROLLOVER, GET
    User: testuser
    """
    logger.info("Test: Policy with CREATE + DELETE + ROLLOVER + GET access")
    
    update_kms_policy(_policy_id, TESTUSER, accesses=["CREATE", "DELETE", "ROLLOVER", "GET"])
    key_name = "pytest-key-04"
    
    # CREATE - should succeed
    logger.info("Testing CREATE (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": TESTUSER})
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    logger.info("✓ CREATE succeeded (201)")
    
    # ROLLOVER - should succeed
    logger.info("Testing ROLLOVER (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ ROLLOVER succeeded (200)")
    
    # GET current version - should succeed
    logger.info("Testing GET current version (should succeed)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Get current version failed: {response.text}"
    logger.info("✓ GET current version succeeded (200)")
    
    # GET metadata - should be denied
    logger.info("Testing GET metadata (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GET metadata denied (403)")
    
    # GENERATE DEK - should be denied
    logger.info("Testing GENERATE DEK (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GENERATE DEK denied (403)")
    
    # DELETE - should succeed
    logger.info("Testing DELETE (should succeed)")
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Key deletion failed: {response.text}"
    logger.info("✓ DELETE succeeded (200)")




def test_policy_05():
    """
    Test policy with CREATE + DELETE + ROLLOVER + GET + GETMETADATA access.
    
    Verifies that with CREATE, DELETE, ROLLOVER, GET, and GETMETADATA permissions:
    - CREATE: Allowed (201)
    - ROLLOVER: Allowed (200)
    - GET current version: Allowed (200)
    - GET metadata: Allowed (200)
    - GENERATE DEK: Denied (403)
    - DELETE: Allowed (200)
    
    Policy: pytest-* with CREATE, DELETE, ROLLOVER, GET, GETMETADATA
    User: testuser
    """
    logger.info("Test: Policy with CREATE + DELETE + ROLLOVER + GET + GETMETADATA access")
    
    update_kms_policy(_policy_id, TESTUSER, accesses=["CREATE", "DELETE", "ROLLOVER", "GET", "GETMETADATA"])
    key_name = "pytest-key-05"
    
    # CREATE - should succeed
    logger.info("Testing CREATE (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": TESTUSER})
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    logger.info("✓ CREATE succeeded (201)")
    
    # ROLLOVER - should succeed
    logger.info("Testing ROLLOVER (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ ROLLOVER succeeded (200)")
    
    # GET current version - should succeed
    logger.info("Testing GET current version (should succeed)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Get current version failed: {response.text}"
    logger.info("✓ GET current version succeeded (200)")
    
    # GET metadata - should succeed
    logger.info("Testing GET metadata (should succeed)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ GET metadata succeeded (200)")
    
    # GENERATE DEK - should be denied
    logger.info("Testing GENERATE DEK (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GENERATE DEK denied (403)")
    
    # DELETE - should succeed
    logger.info("Testing DELETE (should succeed)")
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Key deletion failed: {response.text}"
    logger.info("✓ DELETE succeeded (200)")




def test_policy_06():
    """
    Test policy with CREATE + DELETE + ROLLOVER + GET + GETMETADATA + GENERATEEEK access.
    
    Verifies that with CREATE, DELETE, ROLLOVER, GET, GETMETADATA, and GENERATEEEK permissions:
    - CREATE: Allowed (201)
    - ROLLOVER: Allowed (200)
    - GET current version: Allowed (200)
    - GET metadata: Allowed (200)
    - GENERATE EEK: Allowed (200)
    - DELETE: Allowed (200)
    
    Policy: pytest-* with CREATE, DELETE, ROLLOVER, GET, GETMETADATA, GENERATEEEK
    User: testuser
    """
    logger.info("Test: Policy with CREATE + DELETE + ROLLOVER + GET + GETMETADATA + GENERATEEEK access")
    
    update_kms_policy(_policy_id, TESTUSER, accesses=["CREATE", "DELETE", "ROLLOVER", "GET", "GETMETADATA", "GENERATEEEK"])
    key_name = "pytest-key-06"
    
    # CREATE - should succeed
    logger.info("Testing CREATE (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": TESTUSER})
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    logger.info("✓ CREATE succeeded (201)")
    
    # ROLLOVER - should succeed
    logger.info("Testing ROLLOVER (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ ROLLOVER succeeded (200)")
    
    # GET current version - should succeed
    logger.info("Testing GET current version (should succeed)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Get current version failed: {response.text}"
    logger.info("✓ GET current version succeeded (200)")
    
    # GET metadata - should succeed
    logger.info("Testing GET metadata (should succeed)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ GET metadata succeeded (200)")
    
    # GENERATE EEK - should succeed
    logger.info("Testing GENERATE EEK (should succeed)")
    DEK_PARAMS = {"eek_op": "generate", "num_keys": 1, "user.name": TESTUSER}
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_eek", params=DEK_PARAMS)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ GENERATE EEK succeeded (200)")
    
    # DELETE - should succeed
    logger.info("Testing DELETE (should succeed)")
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Key deletion failed: {response.text}"
    logger.info("✓ DELETE succeeded (200)")




def test_policy_07():
    """
    Test policy with full access (all permissions).
    
    Verifies that with all permissions (CREATE, DELETE, ROLLOVER, GET, GETMETADATA, 
    GENERATEEEK, DECRYPTEEK):
    - CREATE: Allowed (201)
    - ROLLOVER: Allowed (200)
    - GET current version: Allowed (200)
    - GET metadata: Allowed (200)
    - GENERATE EEK: Allowed (200)
    - DECRYPT EEK: Allowed (200)
    - DELETE: Allowed (200)
    
    Policy: pytest-* with all permissions
    User: testuser
    """
    logger.info("Test: Policy with full access (all permissions)")
    
    update_kms_policy(
        _policy_id, 
        TESTUSER, 
        accesses=["CREATE", "DELETE", "ROLLOVER", "GET", "GETMETADATA", "GENERATEEEK", "DECRYPTEEK"]
    )
    key_name = "pytest-key-07"
    
    # CREATE - should succeed
    logger.info("Testing CREATE (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": TESTUSER})
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    logger.info("✓ CREATE succeeded (201)")
    
    # ROLLOVER - should succeed
    logger.info("Testing ROLLOVER (should succeed)")
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ ROLLOVER succeeded (200)")
    
    # GET current version - should succeed
    logger.info("Testing GET current version (should succeed)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Get current version failed: {response.text}"
    logger.info("✓ GET current version succeeded (200)")
    
    # GET metadata - should succeed
    logger.info("Testing GET metadata (should succeed)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ GET metadata succeeded (200)")
    
    # GENERATE EEK - should succeed
    logger.info("Testing GENERATE EEK (should succeed)")
    DEK_PARAMS = {"eek_op": "generate", "num_keys": 1, "user.name": TESTUSER}
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_eek", params=DEK_PARAMS)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    logger.info("✓ GENERATE EEK succeeded (200)")
    
    # Extract EEK details for decryption
    eek_response = response.json()[0]
    material = eek_response["encryptedKeyVersion"]["material"]
    name = eek_response["encryptedKeyVersion"]["name"]
    iv = eek_response["iv"]
    version_name = eek_response["versionName"]
    logger.info("Extracted EEK details for decryption test")
    
    # DECRYPT EEK - should succeed
    logger.info("Testing DECRYPT EEK (should succeed)")
    decrypt_payload = {
        "name": name,
        "iv": iv,
        "material": material,
    }
    
    DECRYPT_PARAMS = {"eek_op": "decrypt", "user.name": TESTUSER}
    decrypt_response = krb_requests.post(
        f"{BASE_URL}/keyversion/{version_name}/_eek",
        params=DECRYPT_PARAMS,
        json=decrypt_payload
    )
    assert decrypt_response.status_code == 200, (
        f"Decryption of EDEK failed {decrypt_response.status_code}: {decrypt_response.text}"
    )
    logger.info("✓ DECRYPT EEK succeeded (200)")
    
    # DELETE - should succeed
    logger.info("Testing DELETE (should succeed)")
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params={"user.name": TESTUSER})
    assert response.status_code == 200, f"Key deletion failed: {response.text}"
    logger.info("✓ DELETE succeeded (200)")




def test_policy_08():
    """
    Test policy with no access.
    
    Verifies that with no permissions (empty policyItems):
    - CREATE: Denied (403)
    - ROLLOVER: Denied (403)
    - GET current version: Denied (403)
    - GET metadata: Denied (403)
    - GENERATE EEK: Denied (403)
    - DELETE: Denied (403)
    
    Policy: pytest-* with no permissions
    User: testuser
    """
    logger.info("Test: Policy with no access")
    
    # Update policy for this test - no accesses
    update_kms_policy(_policy_id, TESTUSER, accesses=None)
    key_name = "pytest-key-08"
    
    # CREATE - should be denied
    logger.info("Testing CREATE (should be denied)")
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Creation of key, Expected 403 but got {response.text}"
    logger.info("✓ CREATE denied (403)")
    
    # ROLLOVER - should be denied
    logger.info("Testing ROLLOVER (should be denied)")
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Rollover of key, Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ ROLLOVER denied (403)")
    
    # GET current version - should be denied
    logger.info("Testing GET current version (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Get current version, Expected 403 but got: {response.text}"
    logger.info("✓ GET current version denied (403)")
    
    # GET metadata - should be denied
    logger.info("Testing GET metadata (should be denied)")
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Get keyMetaData, Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GET metadata denied (403)")
    
    # GENERATE EEK - should be denied
    logger.info("Testing GENERATE EEK (should be denied)")
    DEK_PARAMS = {"eek_op": "generate", "num_keys": 1, "user.name": TESTUSER}
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_eek", params=DEK_PARAMS)
    assert response.status_code == 403, f"Generate DEK, Expected 403 but got {response.status_code}: {response.text}"
    logger.info("✓ GENERATE EEK denied (403)")
    
    # DELETE - should be denied
    logger.info("Testing DELETE (should be denied)")
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params={"user.name": TESTUSER})
    assert response.status_code == 403, f"Delete key, Expected 403 but got: {response.text}"
    logger.info("✓ DELETE denied (403)")