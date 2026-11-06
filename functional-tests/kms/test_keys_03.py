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
"""

import requests
import pytest
import time

from kms.utils import (
    krb_requests, BASE_URL, PARAMS, find_conflicting_policies,
    ensure_testuser_ticket, ensure_keyadmin_ticket,
    BASE_URL_RANGER, RANGER_KMS_AUTH, KMS_SERVICE_NAME, TEST_USER,
    update_kms_policy,
)

@pytest.fixture(autouse=True)
def _use_testuser(kms_as_testuser, testuser_lifecycle, testuser_keytabs):
    pass


@pytest.fixture(scope="module", autouse=True)
def base_policy():
    """
    Single Ranger policy covering 'matkey-*' shared by all tests.
    Each test resets policyItems via update_kms_policy() — no cross-test leakage.
    """
    key_pattern = "matkey-*"
    policy_name  = "matkey-policy"

    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, key_pattern):
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

    policy_id = response.json()["id"]
    try:
        yield policy_id
    finally:
        requests.delete(f"{BASE_URL_RANGER}/{policy_id}", auth=RANGER_KMS_AUTH)


def _cleanup_key(key_name):
    ensure_keyadmin_ticket()
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)


# ── Test 1 ─────────────────────────────────────────────────────────────────────
# user has CREATE + ROLLOVER but no GET
# → material field must be absent from both CREATE and ROLLOVER responses
def test_material_stripped_without_get(base_policy, headers):
    update_kms_policy(base_policy, TEST_USER, accesses=["create", "rollover"])

    key_name = "matkey-1"
    _cleanup_key(key_name)

    ensure_testuser_ticket()

    create_resp = krb_requests.post(
        f"{BASE_URL}/keys", json={"name": key_name}, headers=headers
    )
    assert create_resp.status_code == 201, f"Key creation failed: {create_resp.text}"
    assert create_resp.json().get("material") is None, (
        "material must not be returned in createKey response "
        "when the user does not hold GET access for this key"
    )

    rollover_resp = krb_requests.post(
        f"{BASE_URL}/key/{key_name}", json={}, headers=headers
    )
    assert rollover_resp.status_code == 200, f"Rollover failed: {rollover_resp.text}"
    assert rollover_resp.json().get("material") is None, (
        "material must not be returned in rolloverKey response "
        "when the user does not hold GET access for this key"
    )

    _cleanup_key(key_name)


# ── Test 2 ─────────────────────────────────────────────────────────────────────
# user has CREATE + ROLLOVER + GET
# → material field must be present in both CREATE and ROLLOVER responses
def test_material_present_with_get(base_policy, headers):
    update_kms_policy(base_policy, TEST_USER, accesses=["create", "rollover", "get"])

    key_name = "matkey-2"
    _cleanup_key(key_name)

    ensure_testuser_ticket()

    create_resp = krb_requests.post(
        f"{BASE_URL}/keys", json={"name": key_name}, headers=headers
    )
    assert create_resp.status_code == 201, f"Key creation failed: {create_resp.text}"
    assert create_resp.json().get("material") is not None, (
        "material must be returned in createKey response "
        "when the user holds GET access for this key"
    )

    rollover_resp = krb_requests.post(
        f"{BASE_URL}/key/{key_name}", json={}, headers=headers
    )
    assert rollover_resp.status_code == 200, f"Rollover failed: {rollover_resp.text}"
    assert rollover_resp.json().get("material") is not None, (
        "material must be returned in rolloverKey response "
        "when the user holds GET access for this key"
    )

    _cleanup_key(key_name)


# ── Test 3 ─────────────────────────────────────────────────────────────────────
# user holds GET on pattern-A (matkey-*) but creates a key under pattern-B
# (diffkey-*) where no GET is granted.
# → material must be stripped for pattern-B even though the user has GET on pattern-A.
# This verifies that the GET check is evaluated per-key, not as a blanket check.
def test_get_on_different_key_pattern_does_not_expose_material(base_policy, headers):
    # base_policy (matkey-*): grant GET only
    update_kms_policy(base_policy, TEST_USER, accesses=["get"])

    # separate policy on diffkey-*: CREATE only, no GET
    other_pattern = "diffkey-*"
    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, other_pattern):
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
                "users": [TEST_USER],
                "delegateAdmin": False,
            }],
        },
    )
    time.sleep(5)
    assert other_policy_resp.status_code in (200, 201), (
        f"Failed to create secondary policy: {other_policy_resp.text}"
    )
    other_policy_id = other_policy_resp.json()["id"]

    key_name = "diffkey-1"
    try:
        _cleanup_key(key_name)

        ensure_testuser_ticket()
        create_resp = krb_requests.post(
            f"{BASE_URL}/keys", json={"name": key_name}, headers=headers
        )
        assert create_resp.status_code == 201, f"Key creation failed: {create_resp.text}"
        assert create_resp.json().get("material") is None, (
            "material must not be returned: user holds GET on 'matkey-*' "
            "but not on 'diffkey-*'. The GET check must be evaluated "
            "for the key being created, not for the user's broadest GET grant."
        )
    finally:
        _cleanup_key(key_name)
        requests.delete(f"{BASE_URL_RANGER}/{other_policy_id}", auth=RANGER_KMS_AUTH)