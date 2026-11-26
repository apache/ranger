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
Functional tests for KMS operation blacklisting.

Tests the KMS blacklist functionality that allows blocking specific users
from performing specific operations (CREATE, ROLLOVER, DELETE) on keys.
Blacklisting is configured via dbks-site.xml and requires KMS container restart.

Uses keyadmin for all operations.
Requires ranger-kms and ranger-hadoop containers to be running.
"""

import time

import pytest

from ranger_test_utils.kms_utils import (
    BASE_URL,
    PARAMS,
    blacklist_op_users,
    ensure_keyadmin_ticket,
    get_kms_container,
    krb_requests,
    unblacklist_op_users,
)
from ranger_test_utils.plugin_enforcement_utils import logger

pytestmark = pytest.mark.kms

# User to be blacklisted in tests
BLACKLIST_USER = "keyadmin"

# Global tracking for cleanup
_created_keys = []


def setup_module():
    """
    Module setup: ensure Kerberos ticket for keyadmin.
    
    This module uses keyadmin for all operations (key creation + blacklist tests).
    
    Container validation is handled by session fixture.
    """
    logger.info("=== setup_module: start ===")
    logger.info("Ensuring keyadmin Kerberos ticket for KMS operations")
    ensure_keyadmin_ticket()
    logger.info("=== setup_module: done ===")


def teardown_module():
    """
    Module teardown: clean up keys and ensure no blacklists remain.
    
    Cleans up any keys created during tests and ensures the user is
    unblacklisted from all operations.
    """
    logger.info("=== teardown_module: start ===")
    
    # Ensure user is unblacklisted from all operations
    logger.info("Ensuring %s is unblacklisted from all operations", BLACKLIST_USER)
    try:
        for operation in ["CREATE", "ROLLOVER", "DELETE"]:
            unblacklist_op_users(operation, [BLACKLIST_USER])
            logger.info("Unblacklisted %s from %s", BLACKLIST_USER, operation)
    except Exception as e:
        logger.error("Error unblacklisting user: %s", e)
    
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
    
    logger.info("=== teardown_module: done ===")


def test_user_keyOperation_before_blacklist():
    """
    Test baseline: key operations succeed before applying blacklist.
    
    This test verifies that keyadmin can perform CREATE, ROLLOVER, and DELETE
    operations on keys before any blacklist is applied. This establishes the
    baseline that the user has the necessary permissions.
    
    Expected behavior:
      - CREATE succeeds (201)
      - ROLLOVER succeeds (200)
      - DELETE succeeds (200)
    """
    logger.info("Test 1: Key operations before blacklist")
    key_name = "blacklist-key1"
    _created_keys.append(key_name)
    
    key_data = {"name": key_name}
    
    logger.info("Creating key: %s", key_name)
    create_response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert create_response.status_code == 201, f"Key creation failed: {create_response.text}"
    logger.info("✓ CREATE succeeded")
    
    logger.info("Rolling over key: %s", key_name)
    rollover_response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params=PARAMS)
    assert rollover_response.status_code == 200, f"Rollover failed: {rollover_response.text}"
    logger.info("✓ ROLLOVER succeeded")
    
    logger.info("Deleting key: %s", key_name)
    delete_response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    assert delete_response.status_code == 200, f"Deletion failed: {delete_response.text}"
    logger.info("✓ DELETE succeeded")
    
    _created_keys.remove(key_name)


def test_blacklist_create():
    """
    Test CREATE operation blacklisting.
    
    This test verifies that when a user is blacklisted for CREATE operations:
    1. The user cannot create new keys (403 Forbidden)
    2. After unblacklisting, the user can create keys again (201 Created)
    
    The blacklist is configured in dbks-site.xml and requires KMS container restart
    to take effect.
    
    Expected behavior:
      - Blacklisted: CREATE returns 403
      - Unblacklisted: CREATE returns 201
    """
    logger.info("Test 2: Blacklist CREATE operation")
    key_name = "blacklist-key2"
    _created_keys.append(key_name)
    
    # Blacklist the user for CREATE operation
    logger.info("Blacklisting %s from CREATE operation", BLACKLIST_USER)
    blacklist_op_users("CREATE", [BLACKLIST_USER])
    
    logger.info("Restarting KMS container for blacklist to take effect")
    get_kms_container().restart()
    time.sleep(5)
    ensure_keyadmin_ticket()
    
    key_data = {"name": key_name}
    
    logger.info("Attempting to create key while blacklisted: %s", key_name)
    response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert response.status_code == 403, (
        f"User {BLACKLIST_USER} should be blocked from creating key, got {response.status_code}"
    )
    logger.info("✓ CREATE correctly blocked (403)")
    
    # Unblacklist the user
    logger.info("Unblacklisting %s from CREATE operation", BLACKLIST_USER)
    unblacklist_op_users("CREATE", [BLACKLIST_USER])
    
    logger.info("Restarting KMS container for unblacklist to take effect")
    get_kms_container().restart()
    time.sleep(5)
    ensure_keyadmin_ticket()
    
    # Delete before retry — KMS may have written the key even on a 403
    logger.info("Cleaning up any partially created key")
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    
    logger.info("Attempting to create key after unblacklisting: %s", key_name)
    response = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert response.status_code == 201, (
        f"User {BLACKLIST_USER} should be able to create key after unblacklisting, got {response.status_code}"
    )
    logger.info("✓ CREATE succeeded after unblacklisting (201)")
    
    logger.info("Cleaning up key: %s", key_name)
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    _created_keys.remove(key_name)


def test_blacklist_rollOver():
    """
    Test ROLLOVER operation blacklisting.
    
    This test verifies that when a user is blacklisted for ROLLOVER operations:
    1. The user cannot rollover existing keys (403 Forbidden)
    2. After unblacklisting, the user can rollover keys again (200 OK)
    
    The blacklist is configured in dbks-site.xml and requires KMS container restart
    to take effect.
    
    Steps:
      1. Create a key (not blacklisted yet)
      2. Blacklist user for ROLLOVER
      3. Attempt rollover - should fail (403)
      4. Unblacklist user
      5. Attempt rollover - should succeed (200)
    
    Expected behavior:
      - Blacklisted: ROLLOVER returns 403
      - Unblacklisted: ROLLOVER returns 200
    """
    logger.info("Test 3: Blacklist ROLLOVER operation")
    key_name = "blacklist-key3"
    _created_keys.append(key_name)
    
    # Create the key first (before blacklisting)
    logger.info("Creating key for rollover test: %s", key_name)
    key_data = {"name": key_name}
    create_resp = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert create_resp.status_code == 201, f"Key creation failed: {create_resp.text}"
    logger.info("✓ Key created")
    
    # Blacklist the user for ROLLOVER operation
    logger.info("Blacklisting %s from ROLLOVER operation", BLACKLIST_USER)
    blacklist_op_users("ROLLOVER", [BLACKLIST_USER])
    
    logger.info("Restarting KMS container for blacklist to take effect")
    get_kms_container().restart()
    time.sleep(5)
    ensure_keyadmin_ticket()
    
    logger.info("Attempting to rollover key while blacklisted: %s", key_name)
    response_after_blacklist = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params=PARAMS)
    assert response_after_blacklist.status_code == 403, (
        f"User {BLACKLIST_USER} should be blocked from rollover, got {response_after_blacklist.status_code}"
    )
    logger.info("✓ ROLLOVER correctly blocked (403)")
    
    # Unblacklist the user
    logger.info("Unblacklisting %s from ROLLOVER operation", BLACKLIST_USER)
    unblacklist_op_users("ROLLOVER", [BLACKLIST_USER])
    
    logger.info("Restarting KMS container for unblacklist to take effect")
    get_kms_container().restart()
    time.sleep(5)
    ensure_keyadmin_ticket()
    
    logger.info("Attempting to rollover key after unblacklisting: %s", key_name)
    response_after_unblacklist = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params=PARAMS)
    assert response_after_unblacklist.status_code == 200, (
        f"User {BLACKLIST_USER} should be able to rollover after unblacklisting, got {response_after_unblacklist.status_code}"
    )
    logger.info("✓ ROLLOVER succeeded after unblacklisting (200)")
    
    logger.info("Cleaning up key: %s", key_name)
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    _created_keys.remove(key_name)


def test_blacklist_delete():
    """
    Test DELETE operation blacklisting.
    
    This test verifies that when a user is blacklisted for DELETE operations:
    1. The user cannot delete existing keys (403 Forbidden)
    2. After unblacklisting, the user can delete keys again (200 OK)
    
    The blacklist is configured in dbks-site.xml and requires KMS container restart
    to take effect.
    
    Steps:
      1. Create a key (not blacklisted yet)
      2. Blacklist user for DELETE
      3. Attempt delete - should fail (403)
      4. Unblacklist user
      5. Attempt delete - should succeed (200)
    
    Expected behavior:
      - Blacklisted: DELETE returns 403
      - Unblacklisted: DELETE returns 200
    """
    logger.info("Test 4: Blacklist DELETE operation")
    key_name = "blacklist-key4"
    _created_keys.append(key_name)
    
    # Create the key first (before blacklisting)
    logger.info("Creating key for delete test: %s", key_name)
    key_data = {"name": key_name}
    create_resp = krb_requests.post(f"{BASE_URL}/keys", json=key_data, params=PARAMS)
    assert create_resp.status_code == 201, f"Key creation failed: {create_resp.text}"
    logger.info("✓ Key created")
    
    # Blacklist the user for DELETE operation
    logger.info("Blacklisting %s from DELETE operation", BLACKLIST_USER)
    blacklist_op_users("DELETE", [BLACKLIST_USER])
    
    logger.info("Restarting KMS container for blacklist to take effect")
    get_kms_container().restart()
    time.sleep(5)
    ensure_keyadmin_ticket()
    
    logger.info("Attempting to delete key while blacklisted: %s", key_name)
    delete_response_before = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    assert delete_response_before.status_code == 403, (
        f"User {BLACKLIST_USER} should be blocked from delete, got {delete_response_before.status_code}"
    )
    logger.info("✓ DELETE correctly blocked (403)")
    
    # Unblacklist the user
    logger.info("Unblacklisting %s from DELETE operation", BLACKLIST_USER)
    unblacklist_op_users("DELETE", [BLACKLIST_USER])
    
    logger.info("Restarting KMS container for unblacklist to take effect")
    get_kms_container().restart()
    time.sleep(5)
    ensure_keyadmin_ticket()
    
    logger.info("Attempting to delete key after unblacklisting: %s", key_name)
    delete_response_after = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    assert delete_response_after.status_code == 200, (
        f"User {BLACKLIST_USER} should be able to delete after unblacklisting, got {delete_response_after.status_code}"
    )
    logger.info("✓ DELETE succeeded after unblacklisting (200)")
    
    _created_keys.remove(key_name)

