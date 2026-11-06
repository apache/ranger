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
"""

import requests
import pytest
import time

from kms.utils import (
    krb_requests, BASE_URL, find_conflicting_policies,
    ensure_keyadmin_ticket,
    BASE_URL_RANGER, RANGER_KMS_AUTH, KMS_SERVICE_NAME, TEST_USER,
)

KEY_NAME       = "testkey04"
KEY_NAME_UPPER = KEY_NAME.upper()
KEY_NAME_MIXED = "TestKey04"

@pytest.fixture(autouse=True)
def _use_testuser(kms_as_testuser, testuser_lifecycle, testuser_keytabs):
    pass


@pytest.fixture(scope="module", autouse=True)
def setup_key_and_policies():
    """
    Sets up the key and policy pair used across all tests in this module.

    Policy setup:
      Allow — testuser has GET access on all testkey* keys (wildcard)
      Deny  — testuser is denied GET on testkey04 (specific)

    This combination validates that policy enforcement uses the normalized
    (lowercase) key name regardless of the case supplied in the request.
    """
    ensure_keyadmin_ticket()

    krb_requests.delete(f"{BASE_URL}/key/{KEY_NAME}")
    resp = krb_requests.post(
        f"{BASE_URL}/keys",
        json={"name": KEY_NAME, "cipher": "AES/CTR/NoPadding", "length": 128},
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 201, f"Key creation failed: {resp.text}"

    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, "testkey*"):
        requests.delete(f"{BASE_URL_RANGER}/{stale_id}", auth=RANGER_KMS_AUTH)
    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, KEY_NAME):
        requests.delete(f"{BASE_URL_RANGER}/{stale_id}", auth=RANGER_KMS_AUTH)

    allow_resp = requests.post(BASE_URL_RANGER, auth=RANGER_KMS_AUTH, json={
        "policyName": "pytest-casetest-allow",
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {"values": ["testkey*"], "isExcludes": False, "isRecursive": False}
        },
        "policyItems": [
            {"accesses": [{"type": "get", "isAllowed": True}], "users": [TEST_USER], "delegateAdmin": False}
        ],
    })
    assert allow_resp.status_code in (200, 201), f"Allow policy creation failed: {allow_resp.text}"
    allow_id = allow_resp.json()["id"]

    deny_resp = requests.post(BASE_URL_RANGER, auth=RANGER_KMS_AUTH, json={
        "policyName": "pytest-casetest-deny",
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {"values": [KEY_NAME], "isExcludes": False, "isRecursive": False}
        },
        "denyPolicyItems": [
            {"accesses": [{"type": "get", "isAllowed": True}], "users": [TEST_USER], "delegateAdmin": False}
        ],
    })
    assert deny_resp.status_code in (200, 201), f"Deny policy creation failed: {deny_resp.text}"
    deny_id = deny_resp.json()["id"]

    time.sleep(15)

    try:
        yield
    finally:
        requests.delete(f"{BASE_URL_RANGER}/{allow_id}", auth=RANGER_KMS_AUTH)
        requests.delete(f"{BASE_URL_RANGER}/{deny_id}", auth=RANGER_KMS_AUTH)
        ensure_keyadmin_ticket()
        krb_requests.delete(f"{BASE_URL}/key/{KEY_NAME}")


def test_lowercase_key_access_denied():
    """Baseline: policy correctly denies testuser access using lowercase key name."""
    resp = krb_requests.get(f"{BASE_URL}/key/{KEY_NAME}/_currentversion")
    assert resp.status_code == 403, (
        f"Expected 403 for lowercase key access, got {resp.status_code}: {resp.text}"
    )

@pytest.mark.parametrize("endpoint", [
    "/_currentversion",
    "/_versions",
    "/_metadata",
])
def test_uppercase_key_access_denied(endpoint):
    """Uppercase key name must not change the authorization outcome."""
    resp = krb_requests.get(f"{BASE_URL}/key/{KEY_NAME_UPPER}{endpoint}")
    assert resp.status_code == 403, (
        f"Uppercase key name produced unexpected result on {endpoint}: "
        f"got {resp.status_code}: {resp.text}"
    )

def test_mixedcase_key_access_denied():
    """Mixed-case key names must also normalize and produce the same outcome."""
    resp = krb_requests.get(f"{BASE_URL}/key/{KEY_NAME_MIXED}/_currentversion")
    assert resp.status_code == 403, (
        f"Mixed-case key name produced unexpected result: "
        f"got {resp.status_code}: {resp.text}"
    )

def test_uppercase_create_normalizes_to_lowercase():
    """
    Creating a key with an uppercase name stores it under the normalized
    lowercase name. A subsequent lowercase create must be rejected as a duplicate.
    """
    upper_name = "TESTNORMKEY"
    lower_name = upper_name.lower()

    ensure_keyadmin_ticket()
    krb_requests.delete(f"{BASE_URL}/key/{lower_name}")

    create_resp = krb_requests.post(
        f"{BASE_URL}/keys",
        json={"name": upper_name, "cipher": "AES/CTR/NoPadding", "length": 128},
        headers={"Content-Type": "application/json"},
    )
    assert create_resp.status_code == 201, (
        f"Key creation with uppercase name failed: {create_resp.text}"
    )

    dup_resp = krb_requests.post(
        f"{BASE_URL}/keys",
        json={"name": lower_name, "cipher": "AES/CTR/NoPadding", "length": 128},
        headers={"Content-Type": "application/json"},
    )
    assert dup_resp.status_code in (400, 500), (
        f"Expected duplicate rejection, got {dup_resp.status_code}: {dup_resp.text}"
    )

    krb_requests.delete(f"{BASE_URL}/key/{lower_name}")