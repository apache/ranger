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

from kms.utils import (
    fetch_logs, krb_requests,
    ensure_keyadmin_keytab, ensure_ticket,
    BASE_URL, PARAMS
)

RANGER_AUTH = ('keyadmin', 'rangerR0cks!')
BASE_URL_RANGER = "http://localhost:6080/service/public/v2/api/policy"
KMS_SERVICE_NAME = "dev_kms"
TEST_USER = "keyadmin"
HEADERS = {"Content-Type": "application/json"}


# **************** Session-scoped: runs once for the entire test suite --------------

@pytest.fixture(scope="session", autouse=True)
def setup_kerberos():
    ensure_keyadmin_keytab()
    ensure_ticket()
    print("Kerberos ready.")


# **************** Shared fixtures available to all test files ---------------------

@pytest.fixture(scope="session")
def headers():
    return HEADERS


@pytest.fixture(scope="module")
def user1():
    return TEST_USER


# **************** create_test_key — krb_requests (KMS needs Kerberos) -------------

@pytest.fixture(scope="class")
def create_test_key(headers):
    data = {
        "name": "key1",
        "cipher": "AES/CTR/NoPadding",
        "length": 128,
        "description": "Test key"
    }

    key_creation_response = krb_requests.post(
        f"{BASE_URL}/keys", headers=headers, json=data, params=PARAMS
    )

    if key_creation_response.status_code != 201:
        error_logs = fetch_logs()
        pytest.fail(
            f"Key creation failed. API Response: {key_creation_response.text}\nLogs:\n{error_logs}"
        )

    yield data

    krb_requests.delete(f"{BASE_URL}/key/key1", params=PARAMS)


# **************** kms_policy — plain requests (Ranger uses basic auth) ------------

@pytest.fixture(scope="module")
def kms_policy(user1):
    policy_data = {
        "policyName": "blacklist-policy",
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {
                "values": ["blacklist-*"],
                "isExcludes": False,
                "isRecursive": False
            }
        },
        "policyItems": [{
            "accesses": [
                {"type": "CREATE",   "isAllowed": True},
                {"type": "ROLLOVER", "isAllowed": True},
                {"type": "DELETE",   "isAllowed": True}
            ],
            "users": [user1]
        }]
    }

    response = requests.post(BASE_URL_RANGER, auth=RANGER_AUTH, json=policy_data)
    time.sleep(30)
    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create policy: {response.text}")

    policy_id = response.json()["id"]
    yield policy_id

    requests.delete(f"{BASE_URL_RANGER}/{policy_id}", auth=RANGER_AUTH)