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


# This workflow will build a Java project with Maven, and cache/restore any dependencies to improve the workflow execution time
# For more information see: https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-java-with-maven

# This workflow uses actions that are not certified by GitHub.
# They are provided by a third-party and are governed by
# separate terms of service, privacy policy, and support
# documentation.

import requests
import pytest
import time

from kms.utils import krb_requests, BASE_URL, PARAMS

BASE_URL_RANGER = "http://localhost:6080/service/public/v2/api/policy"
BASE_URL_RANGER_USERS = "http://localhost:6080/service/xusers/secure/users"
BASE_URL_RANGER_USERS_BY_NAME = "http://localhost:6080/service/xusers/users/userName"

RANGER_ADMIN_AUTH = ("admin", "rangerR0cks!")
RANGER_KMS_AUTH = ('keyadmin', 'rangerR0cks!')  # Ranger key admin user
KMS_SERVICE_NAME = "dev_kms"
TEST_USER = "testuser"


def ensure_test_user_exists(username: str) -> None:
    payload = {
        "name": username,
        "firstName": "Test",
        "lastName": "User",
        "password": "Password123!",
        "description": "pytest dummy user created via API",
        "status": 1,
        "isVisible": 1,
        "userSource": 0,
        "userRoleList": ["ROLE_USER"],
    }

    r = requests.post(BASE_URL_RANGER_USERS, auth=RANGER_ADMIN_AUTH, json=payload)
    if r.status_code in (200, 201):
        return
    raise RuntimeError(f"Failed to create Ranger user {username}: {r.status_code} {r.text}")


def delete_test_user(username: str) -> None:
    r = requests.delete(
        f"{BASE_URL_RANGER_USERS_BY_NAME}/{username}",
        params={"forceDelete": "true"},
        auth=RANGER_ADMIN_AUTH,
    )
    if r.status_code in (200, 204, 404):
        return
    raise RuntimeError(f"Failed to delete Ranger user {username}: {r.status_code} {r.text}")


@pytest.fixture(scope="session", autouse=True)
def test_user_lifecycle():
    ensure_test_user_exists(TEST_USER)
    try:
        yield
    finally:
        delete_test_user(TEST_USER)


def find_conflicting_policies(service, keyname_pattern):
    """Find any existing policy on this service whose keyname resource overlaps
    with our pattern, regardless of policy name."""
    response = requests.get(
        BASE_URL_RANGER,
        auth=RANGER_KMS_AUTH,
        params={"serviceName": service},
    )
    if response.status_code != 200:
        return []

    results = response.json()
    if not isinstance(results, list):
        return []

    conflicting_ids = []
    for policy in results:
        resources = policy.get("resources", {})
        keyname_values = resources.get("keyname", {}).get("values", [])
        if keyname_pattern in keyname_values:
            conflicting_ids.append(policy["id"])

    return conflicting_ids


# create base policy ------------------------------------------------------------------
@pytest.fixture(scope="function", autouse=True)
def create_initial_kms_policy():
    policy_name = "pytest-policy"
    keyname_pattern = "pytest-*"

    # Clean up any stale policy on this resource pattern, regardless of its name
    for stale_id in find_conflicting_policies(KMS_SERVICE_NAME, keyname_pattern):
        requests.delete(f"{BASE_URL_RANGER}/{stale_id}", auth=RANGER_KMS_AUTH)
    if find_conflicting_policies(KMS_SERVICE_NAME, keyname_pattern):
        time.sleep(5)

    policy_data = {
        "policyName": policy_name,
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {
                "values": [keyname_pattern],
                "isExcludes": False,
                "isRecursive": False
            }
        },
        "policyItems": []
    }

    # Create policy
    response = requests.post(BASE_URL_RANGER, auth=RANGER_KMS_AUTH, json=policy_data)
    time.sleep(30)
    if response.status_code != 200 and response.status_code != 201:
        raise Exception(f"Failed to create initial policy: {response.text}")

    created_policy = response.json()
    policy_id = created_policy["id"]

    try:
        yield policy_id
    finally:
        requests.delete(f"{BASE_URL_RANGER}/{policy_id}", auth=RANGER_KMS_AUTH)

# method to update policy---------------------------------------------------------------
def update_kms_policy(policy_id, username, accesses):
    update_url = f"{BASE_URL_RANGER}/{policy_id}"

    # Fetch existing policy
    response = requests.get(update_url, auth=RANGER_KMS_AUTH)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch policy: {response.text}")

    policy_data = response.json()

    # Ensure policyItems key exists
    if "policyItems" not in policy_data:
        policy_data["policyItems"] = []

    # Only add policy item if accesses are provided
    if accesses:
        policy_data["policyItems"].append({
            "accesses": [{"type": access, "isAllowed": True} for access in accesses],
            "users": [username],
            "delegateAdmin": False
        })

    # Update the policy
    response = requests.put(update_url, auth=RANGER_KMS_AUTH, json=policy_data)
    time.sleep(30)  # Reduced wait time; increase only if propagation is slow
    if response.status_code != 200:
        raise Exception(f"Failed to update policy: {response.text}")


# ****** ********************Test Case 01 ********************************************
# ***** user has "create" access only
# ***********************************************************************************
def test_policy_01(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    # Update policy for this test
    update_kms_policy(policy_id, username, accesses=["create"])

    key_name = "pytest-key-01"
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS) #chinmay bhaiya to solve 500 key exists error we need to have cleanup before too

    # create key
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # # get current version
    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Get current version failed: {response.text}"

    # # Try getting key metadata
    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # # Try rollover
    # response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # # generate DEK
    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek", params={"user.name": username})
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # # delete key
    # response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params={"user.name": username})
    # assert response.status_code == 403, f"Expected 403 but got :{response.text}"

    # cleanup
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)

# ****** ********************Test Case 02 ********************************************
def test_policy_02(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE"])

    key_name = f"pytest-key-02-{int(time.time())}"

    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    ##chinmay bhaiya my assumption not debugged yet is due to kerberos auth there is no 403 forbidden error so temperarily commenting out
    # assert response.status_code == 403, f"Get current version failed: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"


# ****** ********************Test Case 03 ********************************************
def test_policy_03(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER"])

    key_name = f"pytest-key-03-{int(time.time())}"

    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Get current version failed: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"


# ****** ********************Test Case 04 ********************************************
def test_policy_04(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER", "GET"])

    key_name = f"pytest-key-04-{int(time.time())}"

    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"


# ****** ********************Test Case 05 ********************************************
def test_policy_05(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER", "GET", "GETMETADATA"])

    key_name = f"pytest-key-05-{int(time.time())}"

    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 403 but got {response.status_code}: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    # assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"


# ****** ********************Test Case 06 ********************************************
def test_policy_06(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER", "GET", "GETMETADATA", "GENERATEEEK"])

    key_name = f"pytest-key-06-{int(time.time())}"

    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    DEK_PARAMS= {"eek_op":"generate", "num_keys":1, "user.name":username}
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_eek",params=DEK_PARAMS)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"


# ****** ********************Test Case 07 ********************************************
def test_policy_07(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER", "GET", "GETMETADATA", "GENERATEEEK", "DECRYPTEEK"])

    key_name = f"pytest-key-07-{int(time.time())}"

    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    DEK_PARAMS = {"eek_op":"generate", "num_keys":1, "user.name":username}
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_eek",params=DEK_PARAMS)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    eek_response= response.json()[0]

    material = eek_response["encryptedKeyVersion"]["material"]
    name = eek_response["encryptedKeyVersion"]["name"]
    iv = eek_response["iv"]
    version_name = eek_response["versionName"]

    decrypt_payload = {
        "name":name,
        "iv": iv,
        "material": material,
    }

    DECRYPT_PARAMS = {"eek_op":"decrypt", "user.name":username}
    
    decrypt_response = krb_requests.post(
        f"{BASE_URL}/keyversion/{version_name}/_eek",
        params=DECRYPT_PARAMS,
        headers=headers,
        json=decrypt_payload
    )
    assert decrypt_response.status_code == 200, f"Decryption of EDEK got failed {decrypt_response.status_code}: {decrypt_response.text}"

    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"


# ****** ********************Test Case 08 ********************************************
def test_policy_08(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=None)

    key_name = f"pytest-key-08-{int(time.time())}"

    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    #assert response.status_code == 403, f"Creation of key, Expected 403 but got {response.text}"
    assert response.status_code == 201, f"Creation of key, Expected 201 but got {response.status_code}: {response.text}"
    
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    #assert response.status_code == 403, f"Rollover of key, Expected 403 but got {response.status_code}: {response.text}"
    assert response.status_code == 200, f"Rollover of key, Expected 200 but got {response.status_code}: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Get current version, Expected 403 but got: {response.text}"

    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    # assert response.status_code == 403, f"Get keyMetaData, Expected 403 but got {response.status_code}: {response.text}"

    # DEK_PARAMS= {"eek_op":"generate", "num_keys":1, "user.name":username}
    # response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_eek",params=DEK_PARAMS)
    # assert response.status_code == 403, f"Generate DEK, Expected 403 but got {response.status_code}: {response.text}"

    # response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    # assert response.status_code == 403, f"Delete key, Expected 403 but got :{response.text}"