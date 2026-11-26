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
Verifies that raw key material is correctly included or stripped from
createKey and rolloverKey responses based on the per-key GET policy
configured in Ranger.

Tests use testuser for key operations and keyadmin for cleanup.
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
    update_kms_policy,
)
from ranger_test_utils.plugin_enforcement_utils import logger

pytestmark = pytest.mark.kms

# Global tracking for cleanup
_created_keys = []
_created_policies = []
_base_policy_id = None


def setup_module():
    """
    Module setup: create testuser in Ranger, ensure Kerberos tickets.
    
    This module uses testuser for key operations (to test policy enforcement)
    and keyadmin for cleanup operations.
    
    Container validation is handled by session fixture.
    """
    global _base_policy_id
    
    logger.info("=== setup_module: start ===")
    
    # Create testuser in Ranger Admin (required for policies)
    logger.info("Creating testuser in Ranger Admin")
    ensure_test_user_exists(TESTUSER)
    
    # Ensure Kerberos tickets for both users
    logger.info("Ensuring testuser and keyadmin Kerberos tickets")
    ensure_testuser_ticket()
    ensure_keyadmin_ticket()
    
    # Create base policy for matkey-* pattern
    logger.info("Creating base policy for matkey-* pattern")
    key_pattern = "matkey-*"
    policy_name = "matkey-policy"
    
    # Clean up any stale policies
    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, key_pattern):
        logger.info("Deleting stale policy id=%s", stale_id)
        requests.delete(f"{BASE_URL_RANGER}/{stale_id}", auth=RANGER_KMS_AUTH)
    
    if find_conflicting_policies(KMS_SERVICE_NAME, key_pattern):
        time.sleep(5)
    
    policy_data = {
        "policyName": policy_name,
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {
                "values": [key_pattern],
                "isExcludes": False,
                "isRecursive": False,
            }
        },
        "policyItems": [],
    }
    
    response = requests.post(BASE_URL_RANGER, auth=RANGER_KMS_AUTH, json=policy_data)
    time.sleep(5)
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Failed to create base policy: {response.text}")
    
    _base_policy_id = response.json()["id"]
    _created_policies.append(_base_policy_id)
    logger.info("Created base policy id=%s", _base_policy_id)
    logger.info("=== setup_module: done ===")


def teardown_module():
    """
    Module teardown: clean up keys, policies, and testuser.
    
    Uses keyadmin ticket for cleanup operations.
    """
    logger.info("=== teardown_module: start ===")
    
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
    
    # Cleanup testuser from Ranger Admin
    logger.info("Deleting testuser from Ranger Admin")
    try:
        delete_test_user(TESTUSER)
        logger.info("Deleted testuser: %s", TESTUSER)
    except Exception as e:
        logger.error("Error deleting testuser: %s", e)
    
    logger.info("=== teardown_module: done ===")


def _cleanup_key(key_name):
    """Helper to cleanup a key using keyadmin."""
    ensure_keyadmin_ticket()
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)


def test_material_stripped_without_get():
    """
    Test that key material is stripped when user lacks GET permission.
    
    This test verifies that when a user has CREATE and ROLLOVER permissions
    but NOT GET permission on a key, the material field is absent from both
    the createKey and rolloverKey API responses.
    
    Expected behavior:
      - User has CREATE + ROLLOVER but no GET
      - material field absent from CREATE response
      - material field absent from ROLLOVER response
    """
    logger.info("Test 1: Material stripped without GET permission")
    update_kms_policy(_base_policy_id, TESTUSER, accesses=["create", "rollover"])
    
    key_name = "matkey-1"
    _created_keys.append(key_name)
    _cleanup_key(key_name)
    
    ensure_testuser_ticket()
    
    logger.info("Creating key without GET permission: %s", key_name)
    create_resp = krb_requests.post(
        f"{BASE_URL}/keys", json={"name": key_name}, params=PARAMS
    )
    assert create_resp.status_code == 201, f"Key creation failed: {create_resp.text}"
    assert create_resp.json().get("material") is None, (
        "material must not be returned in createKey response "
        "when the user does not hold GET access for this key"
    )
    logger.info("✓ CREATE response: material correctly absent")
    
    logger.info("Rolling over key without GET permission: %s", key_name)
    rollover_resp = krb_requests.post(
        f"{BASE_URL}/key/{key_name}", json={}, params=PARAMS
    )
    assert rollover_resp.status_code == 200, f"Rollover failed: {rollover_resp.text}"
    assert rollover_resp.json().get("material") is None, (
        "material must not be returned in rolloverKey response "
        "when the user does not hold GET access for this key"
    )
    logger.info("✓ ROLLOVER response: material correctly absent")
    
    _cleanup_key(key_name)
    _created_keys.remove(key_name)


def test_material_present_with_get():
    """
    Test that key material is included when user has GET permission.
    
    This test verifies that when a user has CREATE, ROLLOVER, and GET
    permissions on a key, the material field is present in both the
    createKey and rolloverKey API responses.
    
    Expected behavior:
      - User has CREATE + ROLLOVER + GET
      - material field present in CREATE response
      - material field present in ROLLOVER response
    """
    logger.info("Test 2: Material present with GET permission")
    update_kms_policy(_base_policy_id, TESTUSER, accesses=["create", "rollover", "get"])
    
    key_name = "matkey-2"
    _created_keys.append(key_name)
    _cleanup_key(key_name)
    
    ensure_testuser_ticket()
    
    logger.info("Creating key with GET permission: %s", key_name)
    create_resp = krb_requests.post(
        f"{BASE_URL}/keys", json={"name": key_name}, params=PARAMS
    )
    assert create_resp.status_code == 201, f"Key creation failed: {create_resp.text}"
    assert create_resp.json().get("material") is not None, (
        "material must be returned in createKey response "
        "when the user holds GET access for this key"
    )
    logger.info("✓ CREATE response: material correctly present")
    
    logger.info("Rolling over key with GET permission: %s", key_name)
    rollover_resp = krb_requests.post(
        f"{BASE_URL}/key/{key_name}", json={}, params=PARAMS
    )
    assert rollover_resp.status_code == 200, f"Rollover failed: {rollover_resp.text}"
    assert rollover_resp.json().get("material") is not None, (
        "material must be returned in rolloverKey response "
        "when the user holds GET access for this key"
    )
    logger.info("✓ ROLLOVER response: material correctly present")
    
    _cleanup_key(key_name)
    _created_keys.remove(key_name)


def test_get_on_different_key_pattern_does_not_expose_material():
    """
    Test that GET permission is evaluated per-key pattern, not globally.
    
    This test verifies that having GET permission on one key pattern (matkey-*)
    does NOT grant access to material on a different key pattern (diffkey-*).
    The GET check must be evaluated for the specific key being accessed, not as
    a blanket user-level permission.
    
    Expected behavior:
      - User has GET on 'matkey-*' pattern
      - User has CREATE (but NOT GET) on 'diffkey-*' pattern
      - Creating a 'diffkey-*' key returns NO material
      - Material is correctly scoped to the specific key pattern
    """
    logger.info("Test 3: GET permission evaluated per-key pattern")
    
    # base_policy (matkey-*): grant GET only
    update_kms_policy(_base_policy_id, TESTUSER, accesses=["get"])
    logger.info("Base policy (matkey-*): granted GET to testuser")
    
    # separate policy on diffkey-*: CREATE only, no GET
    other_pattern = "diffkey-*"
    logger.info("Creating secondary policy for %s pattern", other_pattern)
    
    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, other_pattern):
        logger.info("Deleting stale policy id=%s", stale_id)
        requests.delete(f"{BASE_URL_RANGER}/{stale_id}", auth=RANGER_KMS_AUTH)
    
    other_policy_resp = requests.post(
        BASE_URL_RANGER,
        auth=RANGER_KMS_AUTH,
        json={
            "policyName": "diffkey-policy",
            "service": KMS_SERVICE_NAME,
            "resources": {
                "keyname": {
                    "values": [other_pattern],
                    "isExcludes": False,
                    "isRecursive": False,
                }
            },
            "policyItems": [{
                "accesses": [{"type": "create", "isAllowed": True}],
                "users": [TESTUSER],
                "delegateAdmin": False,
            }],
        },
    )
    time.sleep(5)
    assert other_policy_resp.status_code in (200, 201), (
        f"Failed to create secondary policy: {other_policy_resp.text}"
    )
    other_policy_id = other_policy_resp.json()["id"]
    _created_policies.append(other_policy_id)
    logger.info("Created secondary policy id=%s (CREATE only, no GET)", other_policy_id)
    
    key_name = "diffkey-1"
    _created_keys.append(key_name)
    
    try:
        _cleanup_key(key_name)
        
        ensure_testuser_ticket()
        logger.info("Creating key under diffkey-* pattern: %s", key_name)
        create_resp = krb_requests.post(
            f"{BASE_URL}/keys", json={"name": key_name}, params=PARAMS
        )
        assert create_resp.status_code == 201, f"Key creation failed: {create_resp.text}"
        assert create_resp.json().get("material") is None, (
            "material must not be returned: user holds GET on 'matkey-*' "
            "but not on 'diffkey-*'. The GET check must be evaluated "
            "for the key being created, not for the user's broadest GET grant."
        )
        logger.info("✓ Material correctly absent (GET not granted on this pattern)")
    finally:
        _cleanup_key(key_name)
        _created_keys.remove(key_name)
        
        logger.info("Deleting secondary policy id=%s", other_policy_id)
        requests.delete(f"{BASE_URL_RANGER}/{other_policy_id}", auth=RANGER_KMS_AUTH)
        _created_policies.remove(other_policy_id)