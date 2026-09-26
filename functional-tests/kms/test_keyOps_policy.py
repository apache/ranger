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

import requests
import pytest
import time

from kms.utils import (
    krb_requests, BASE_URL, PARAMS, find_conflicting_policies,
    ensure_testuser_keytab, ensure_testuser_ticket, ensure_keyadmin_ticket,
    TESTUSER_PARAMS, ensure_test_user_exists, delete_test_user,
    BASE_URL_RANGER, BASE_URL_RANGER_USERS, BASE_URL_RANGER_USERS_BY_NAME,
    RANGER_ADMIN_AUTH, RANGER_KMS_AUTH, KMS_SERVICE_NAME, TEST_USER
)


@pytest.fixture(scope="session", autouse=True)
def test_user_lifecycle():
    ensure_test_user_exists(TEST_USER)
    try:
        yield
    finally:
        delete_test_user(TEST_USER)


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
    #time.sleep(30)
    time.sleep(5)
    if response.status_code != 200 and response.status_code != 201:
        raise Exception(f"Failed to create initial policy: {response.text}")

    created_policy = response.json()
    policy_id = created_policy["id"]

    try:
        yield policy_id
    finally:
        requests.delete(f"{BASE_URL_RANGER}/{policy_id}", auth=RANGER_KMS_AUTH)

@pytest.fixture(autouse=True)
def kms_as_testuser():
    ensure_testuser_ticket()
    yield
    ensure_keyadmin_ticket()

# method to update policy---------------------------------------------------------------
def update_kms_policy(policy_id, username, accesses):
    update_url = f"{BASE_URL_RANGER}/{policy_id}"

    # Fetch existing policy
    response = requests.get(update_url, auth=RANGER_KMS_AUTH)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch policy: {response.text}")

    policy_data = response.json()

    # Ensure policyItems key exists
    policy_data = response.json()
    policy_data["policyItems"] = []   # reset, don't append stale items
    if accesses:
        policy_data["policyItems"].append({
            "accesses": [
                {"type": access.lower(), "isAllowed": True} for access in accesses
            ],
            "users": [username],
            "delegateAdmin": False,
        })

    # Update the policy
    response = requests.put(update_url, auth=RANGER_KMS_AUTH, json=policy_data)
    #time.sleep(30)  # Reduced wait time; increase only if propagation is slow
    time.sleep(5)  # Wait for Ranger to process the policy update
    if response.status_code != 200:
        raise Exception(f"Failed to update policy: {response.text}")




# ****** ********************Test Case 01 ********************************************
# ***** user has "create" access only
# ***********************************************************************************
def test_policy_01(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER
    update_kms_policy(policy_id, username, accesses=["create"])
    key_name = "pytest-key-01"

    ensure_keyadmin_ticket()
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)

    ensure_testuser_ticket()
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion", headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # # Try getting key metadata
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # # Try rollover
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # # generate DEK
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek", params={"user.name": username})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # # delete key
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params={"user.name": username})
    assert response.status_code == 403, f"Expected 403 but got :{response.text}"

    #ensure_keyadmin_ticket()
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)


# ****** ********************Test Case 02 ********************************************
# ***** user has "create, delete" access only
# ***********************************************************************************
def test_policy_02(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["create", "delete"])

    key_name = f"pytest-key-02"

    ensure_testuser_ticket()

    # create — should succeed
    response = krb_requests.post(f"{BASE_URL}/keys",json={"name": key_name},headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    # get current version — should be denied (no GET access)
    response = krb_requests.get(
        f"{BASE_URL}/key/{key_name}/_currentversion",
        headers=headers,
    )
    assert response.status_code == 403, f"Get current version failed: {response.text}"

    response = krb_requests.get(
        f"{BASE_URL}/key/{key_name}/_metadata",
        headers=headers,
    )
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response = krb_requests.post(
        f"{BASE_URL}/key/{key_name}",
        json={},
        headers=headers,
    )
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek")
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    # delete — should succeed (testuser has DELETE)
    response = krb_requests.delete(f"{BASE_URL}/key/{key_name}")
    assert response.status_code == 200, f"Key deletion failed: {response.text}"


# ****** ********************Test Case 03 ********************************************
# ***** user has "create, rollover, delete" access only
# ***********************************************************************************
def test_policy_03(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER"])

    key_name = f"pytest-key-03"

    #response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    print(f"Key creation response: {response.status_code}, {response.text}")

    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"
    print(f"Key rollover response: {response.status_code}, {response.text}")

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Get current version failed: {response.text}"
    print(f"Get current version response: {response.status_code}, {response.text}")

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"


# ****** ********************Test Case 04 ********************************************
# ***** user has "create, rollover, getKeyVersion, delete" access only
# ***********************************************************************************
def test_policy_04(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER", "GET"])

    key_name = f"pytest-key-04"

    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"



# ****** ********************Test Case 05 ********************************************
# ***** user has "create, rollover, getKeyVersion, getMetadata, delete" access only
# ***********************************************************************************
def test_policy_05(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER", "GET", "GETMETADATA"])

    key_name = f"pytest-key-05"

    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 200 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Get current version failed: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 200, f"Expected 403 but got {response.status_code}: {response.text}"

    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_dek",params={"user.name": username})
    assert response.status_code == 403, f"Expected 403 but got {response.status_code}: {response.text}"

    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 200, f"Key deletion failed :{response.text}"



# ****** ********************Test Case 06 ********************************************
# ***** user has "create, rollover, getKeyVersion, getMetadata, generateeek, delete" access only
# ***********************************************************************************
def test_policy_06(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER", "GET", "GETMETADATA", "GENERATEEEK"])

    key_name = f"pytest-key-06"

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
# ***** user has all access "create, rollover, getKeyVersion, getMetadata, generateeek, decrypteek, delete" access
# ***********************************************************************************
def test_policy_07(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username = TEST_USER

    update_kms_policy(policy_id, username, accesses=["CREATE", "DELETE", "ROLLOVER", "GET", "GETMETADATA", "GENERATEEEK", "DECRYPTEEK"])

    key_name = f"pytest-key-07"

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
# ***** user has no access
# ***********************************************************************************
def test_policy_08(create_initial_kms_policy, headers):
    policy_id = create_initial_kms_policy
    username=TEST_USER

    #Update policy for this test
    update_kms_policy(policy_id, username, accesses=None)

    key_name = "pytest-key-08"

    # create key
    response = krb_requests.post(f"{BASE_URL}/keys", json={"name": key_name}, params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Creation of key, Expected 403 but got {response.text}"

    # Try rollover
    response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Rollover of key, Expected 403 but got {response.status_code}: {response.text}"

    #get current version
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_currentversion",params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Get current version, Expected 403 but got: {response.text}"

    # Try getting key metadata
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_metadata", params={"user.name": username}, headers=headers)
    assert response.status_code == 403, f"Get keyMetaData, Expected 403 but got {response.status_code}: {response.text}"

    #generate DEK
    DEK_PARAMS= {"eek_op":"generate","num_keys":1,"user.name":username}
    response = krb_requests.get(f"{BASE_URL}/key/{key_name}/_eek",params=DEK_PARAMS)
    assert response.status_code == 403, f"Generate DEK, Expected 403 but got {response.status_code}: {response.text}"

    #delete key
    response= krb_requests.delete(f"{BASE_URL}/key/{key_name}",params={"user.name": username})
    assert response.status_code == 403, f"Delete key, Expected 403 but got :{response.text}"