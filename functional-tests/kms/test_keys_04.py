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
Functional tests for KMS key name case normalization.

Verifies that key names supplied in any case (uppercase, lowercase,
mixed) are treated as the same key throughout the request lifecycle —
from authorization checks through to storage operations.

Uses testuser for access tests and keyadmin for setup/cleanup.
Requires ranger-kms and ranger-hadoop containers to be running.
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
)
from ranger_test_utils.plugin_enforcement_utils import logger

pytestmark = pytest.mark.kms

KEY_NAME = "testkey04"
KEY_NAME_UPPER = KEY_NAME.upper()
KEY_NAME_MIXED = "TestKey04"

# Global tracking for cleanup
_created_keys = []
_created_policies = []


def setup_module():
    """
    Module setup: create testuser, ensure tickets, create test key and policies.
    
    This module uses testuser for access tests (to verify policy enforcement)
    and keyadmin for setup/cleanup operations.
    
    Policy setup:
      Allow — testuser has GET access on all testkey* keys (wildcard)
      Deny  — testuser is denied GET on testkey04 (specific)
    
    This validates that policy enforcement uses the normalized (lowercase)
    key name regardless of the case supplied in the request.
    
    Container validation is handled by session fixture.
    """
    logger.info("=== setup_module: start ===")
    
    # Create testuser in Ranger Admin
    logger.info("Creating testuser in Ranger Admin")
    ensure_test_user_exists(TESTUSER)
    
    # Ensure Kerberos tickets for both users
    logger.info("Ensuring testuser and keyadmin Kerberos tickets")
    ensure_testuser_ticket()
    ensure_keyadmin_ticket()
    
    # Create test key
    logger.info("Creating test key: %s", KEY_NAME)
    krb_requests.delete(f"{BASE_URL}/key/{KEY_NAME}", params=PARAMS)
    resp = krb_requests.post(
        f"{BASE_URL}/keys",
        json={"name": KEY_NAME, "cipher": "AES/CTR/NoPadding", "length": 128},
        params=PARAMS,
    )
    assert resp.status_code == 201, f"Key creation failed: {resp.text}"
    _created_keys.append(KEY_NAME)
    logger.info("Created test key: %s", KEY_NAME)
    
    # Clean up any stale policies
    logger.info("Cleaning up stale policies")
    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, "testkey*"):
        logger.info("Deleting stale policy id=%s", stale_id)
        requests.delete(f"{BASE_URL_RANGER}/{stale_id}", auth=RANGER_KMS_AUTH)
    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, KEY_NAME):
        logger.info("Deleting stale policy id=%s", stale_id)
        requests.delete(f"{BASE_URL_RANGER}/{stale_id}", auth=RANGER_KMS_AUTH)
    
    # Create allow policy (wildcard testkey*)
    logger.info("Creating allow policy for testkey* pattern")
    allow_resp = requests.post(BASE_URL_RANGER, auth=RANGER_KMS_AUTH, json={
        "policyName": "pytest-casetest-allow",
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {"values": ["testkey*"], "isExcludes": False, "isRecursive": False}
        },
        "policyItems": [
            {"accesses": [{"type": "get", "isAllowed": True}], "users": [TESTUSER], "delegateAdmin": False}
        ],
    })
    assert allow_resp.status_code in (200, 201), f"Allow policy creation failed: {allow_resp.text}"
    allow_id = allow_resp.json()["id"]
    _created_policies.append(allow_id)
    logger.info("Created allow policy id=%s", allow_id)
    
    # Create deny policy (specific testkey04)
    logger.info("Creating deny policy for %s", KEY_NAME)
    deny_resp = requests.post(BASE_URL_RANGER, auth=RANGER_KMS_AUTH, json={
        "policyName": "pytest-casetest-deny",
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {"values": [KEY_NAME], "isExcludes": False, "isRecursive": False}
        },
        "denyPolicyItems": [
            {"accesses": [{"type": "get", "isAllowed": True}], "users": [TESTUSER], "delegateAdmin": False}
        ],
    })
    assert deny_resp.status_code in (200, 201), f"Deny policy creation failed: {deny_resp.text}"
    deny_id = deny_resp.json()["id"]
    _created_policies.append(deny_id)
    logger.info("Created deny policy id=%s", deny_id)
    
    # Wait for policies to propagate
    logger.info("Waiting 15 seconds for policies to propagate")
    time.sleep(15)
    
    logger.info("=== setup_module: done ===")


def teardown_module():
    """
    Module teardown: clean up keys, policies, and testuser.
    
    Uses keyadmin ticket for cleanup operations.
    """
    logger.info("=== teardown_module: start ===")
    
    # Cleanup policies
    if _created_policies:
        logger.info("Cleaning up policies: %s", _created_policies)
        for policy_id in _created_policies:
            try:
                response = requests.delete(
                    f"{BASE_URL_RANGER}/{policy_id}", auth=RANGER_KMS_AUTH
                )
                if response.status_code in (200, 204, 404):
                    logger.info("Deleted policy: %s", policy_id)
                else:
                    logger.warning(
                        "Failed to delete policy %s: status=%s", policy_id, response.status_code
                    )
            except Exception as e:
                logger.error("Error deleting policy %s: %s", policy_id, e)
    
    # Cleanup keys
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
    
    # Cleanup testuser from Ranger Admin
    logger.info("Deleting testuser from Ranger Admin")
    try:
        delete_test_user(TESTUSER)
        logger.info("Deleted testuser: %s", TESTUSER)
    except Exception as e:
        logger.error("Error deleting testuser: %s", e)
    
    logger.info("=== teardown_module: done ===")


def test_lowercase_key_access_denied():
    """
    Test baseline: policy correctly denies testuser access using lowercase key name.
    
    This establishes the baseline that the deny policy is working correctly
    when the key name is provided in lowercase (the normalized form).
    
    Expected behavior: 403 Forbidden (access denied by policy)
    """
    logger.info("Test: Lowercase key access denied")
    ensure_testuser_ticket()
    
    logger.info("Requesting key with lowercase name: %s", KEY_NAME)
    resp = krb_requests.get(f"{BASE_URL}/key/{KEY_NAME}/_currentversion", params=PARAMS)
    
    assert resp.status_code == 403, (
        f"Expected 403 for lowercase key access, got {resp.status_code}: {resp.text}"
    )
    logger.info("✓ Access correctly denied for lowercase key name")


@pytest.mark.parametrize("endpoint", [
    "/_currentversion",
    "/_versions",
    "/_metadata",
])
def test_uppercase_key_access_denied(endpoint):
    """
    Test uppercase key name normalization in authorization.
    
    Verifies that when a key name is provided in uppercase, it is normalized
    to lowercase before policy evaluation, resulting in the same deny outcome.
    
    Tests multiple endpoints to ensure normalization is consistent across
    different API operations.
    
    Expected behavior: 403 Forbidden (normalized to lowercase, then denied)
    """
    logger.info("Test: Uppercase key access denied on endpoint: %s", endpoint)
    ensure_testuser_ticket()
    
    logger.info("Requesting key with uppercase name: %s%s", KEY_NAME_UPPER, endpoint)
    resp = krb_requests.get(f"{BASE_URL}/key/{KEY_NAME_UPPER}{endpoint}", params=PARAMS)
    
    assert resp.status_code == 403, (
        f"Uppercase key name produced unexpected result on {endpoint}: "
        f"got {resp.status_code}: {resp.text}"
    )
    logger.info("✓ Access correctly denied for uppercase key name on %s", endpoint)


def test_mixedcase_key_access_denied():
    """
    Test mixed-case key name normalization in authorization.
    
    Verifies that when a key name is provided in mixed case (e.g., TestKey04),
    it is normalized to lowercase before policy evaluation, resulting in the
    same deny outcome.
    
    Expected behavior: 403 Forbidden (normalized to lowercase, then denied)
    """
    logger.info("Test: Mixed-case key access denied")
    ensure_testuser_ticket()
    
    logger.info("Requesting key with mixed-case name: %s", KEY_NAME_MIXED)
    resp = krb_requests.get(f"{BASE_URL}/key/{KEY_NAME_MIXED}/_currentversion", params=PARAMS)
    
    assert resp.status_code == 403, (
        f"Mixed-case key name produced unexpected result: "
        f"got {resp.status_code}: {resp.text}"
    )
    logger.info("✓ Access correctly denied for mixed-case key name")

def test_uppercase_create_normalizes_to_lowercase():
    """
    Test that creating a key with uppercase name stores it as lowercase.
    
    This test verifies that when a key is created with an uppercase name,
    KMS normalizes it to lowercase for storage. A subsequent attempt to
    create a key with the lowercase version of the same name must be
    rejected as a duplicate.
    
    Expected behavior:
      - Create with uppercase succeeds
      - Subsequent create with lowercase fails (duplicate)
    """
    logger.info("Test: Uppercase create normalizes to lowercase")
    upper_name = "TESTNORMKEY"
    lower_name = upper_name.lower()
    _created_keys.append(lower_name)
    
    ensure_keyadmin_ticket()
    logger.info("Cleaning up any existing key: %s", lower_name)
    krb_requests.delete(f"{BASE_URL}/key/{lower_name}", params=PARAMS)
    
    logger.info("Creating key with uppercase name: %s", upper_name)
    create_resp = krb_requests.post(
        f"{BASE_URL}/keys",
        json={"name": upper_name, "cipher": "AES/CTR/NoPadding", "length": 128},
        params=PARAMS,
    )
    assert create_resp.status_code == 201, (
        f"Key creation with uppercase name failed: {create_resp.text}"
    )
    logger.info("✓ Key created with uppercase name")
    
    logger.info("Attempting duplicate create with lowercase name: %s", lower_name)
    dup_resp = krb_requests.post(
        f"{BASE_URL}/keys",
        json={"name": lower_name, "cipher": "AES/CTR/NoPadding", "length": 128},
        params=PARAMS,
    )
    assert dup_resp.status_code in (400, 500), (
        f"Expected duplicate rejection, got {dup_resp.status_code}: {dup_resp.text}"
    )
    logger.info("✓ Duplicate create correctly rejected (key was normalized to lowercase)")
    
    logger.info("Cleaning up test key: %s", lower_name)
    krb_requests.delete(f"{BASE_URL}/key/{lower_name}", params=PARAMS)
    _created_keys.remove(lower_name)


def test_uppercase_keyversion_endpoint_access_denied():
    """
    Test that uppercase version names are normalized in /keyversion/ endpoint.
    
    This test verifies that when accessing a key version with an uppercase
    version name (e.g., TESTKEY04@0), KMS normalizes it to lowercase before
    policy evaluation, resulting in the same authorization outcome.
    
    Steps:
      1. Get current version name as keyadmin (e.g., testkey04@0)
      2. Convert to uppercase (e.g., TESTKEY04@0)
      3. Try to access as testuser with uppercase version name
      4. Verify access is denied (normalized, then policy applied)
    
    Expected behavior: 403 Forbidden (normalized to lowercase, then denied)
    """
    logger.info("Test: Uppercase keyversion endpoint access denied")
    
    # Get the current version name as keyadmin
    ensure_keyadmin_ticket()
    logger.info("Getting current version name for key: %s", KEY_NAME)
    ver_resp = krb_requests.get(f"{BASE_URL}/key/{KEY_NAME}/_currentversion", params=PARAMS)
    assert ver_resp.status_code == 200, f"Could not get version name: {ver_resp.text}"
    version_name = ver_resp.json().get("versionName", "")
    assert version_name, "versionName not found in current version response"
    logger.info("Current version name: %s", version_name)
    
    # Switch to testuser for the access check
    ensure_testuser_ticket()
    
    upper_version = version_name.upper()  # e.g., "TESTKEY04@0"
    logger.info("Requesting version with uppercase name: %s", upper_version)
    resp = krb_requests.get(f"{BASE_URL}/keyversion/{upper_version}", params=PARAMS)
    
    assert resp.status_code == 403, (
        f"Uppercase version name produced unexpected result: "
        f"got {resp.status_code}: {resp.text}"
    )
    logger.info("✓ Access correctly denied for uppercase version name")