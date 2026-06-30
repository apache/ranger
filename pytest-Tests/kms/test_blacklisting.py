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


import pytest
import time

from kms.utils import (
    krb_requests, ensure_ticket, container,
    blacklist_op_users, unblacklist_op_users,
    BASE_URL, PARAMS
)


@pytest.fixture(scope="module", autouse=True)
def create_initial_kms_policy(kms_policy):
    """Pulls in the shared kms_policy fixture from conftest.py."""
    yield


# ****** Test Case 01 **************************************************************

def test_user_keyOperation_before_blacklist(headers):
    key_name = "blacklist-key1"
    key_data = {"name": key_name}

    create_response = krb_requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)
    assert create_response.status_code == 201, f"key creation failed"

    rollover_response = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=headers, params=PARAMS)
    assert rollover_response.status_code == 200, f"roll over failed"

    delete_response = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    assert delete_response.status_code == 200, f"deletion of key got failed"


# ****** Test Case 02 **************************************************************

def test_blacklist_create(headers, user1):
    blacklist_op_users('CREATE', [user1])
    container.restart()
    time.sleep(30)
    ensure_ticket()

    key_name = "blacklist-key2"
    key_data = {"name": key_name}

    response = krb_requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)
    assert response.status_code == 403, f"User {user1} should be blocked from creating the key but got succeeded"

    unblacklist_op_users('CREATE', [user1])
    container.restart()
    time.sleep(30)
    ensure_ticket()

    # delete before retry — KMS may have written the key even on a 403
    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)

    response = krb_requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)
    assert response.status_code == 201, f"User {user1} should be able to create the key after unblacklisting"

    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)


# ****** Test Case 03 **************************************************************

def test_blacklist_rollOver(headers, user1):
    blacklist_op_users('ROLLOVER', [user1])
    container.restart()
    time.sleep(30)
    ensure_ticket()

    key_name = "blacklist-key3"
    key_data = {"name": key_name}

    krb_requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)

    response_after_blacklist = krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=headers, params=PARAMS)
    assert response_after_blacklist.status_code == 403, f"User {user1} should be blocked from rolling over the key but got succeeded"

    unblacklist_op_users('ROLLOVER', [user1])
    container.restart()
    time.sleep(30)
    ensure_ticket()

    response_after_unblacklist = krb_requests.post(f"{BASE_URL}/key/{key_name}", headers=headers, json={}, params=PARAMS)
    assert response_after_unblacklist.status_code == 200, f"User {user1} should be able to roll over the key but failed"

    krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)


# ****** Test Case 04 **************************************************************

def test_blacklist_delete(headers, user1):
    blacklist_op_users('DELETE', [user1])
    container.restart()
    time.sleep(30)
    ensure_ticket()

    key_name = "blacklist-key4"
    key_data = {"name": key_name}

    krb_requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)

    delete_response_before = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    assert delete_response_before.status_code == 403, f"User {user1} should be blocked from deleting the key but got succeeded"

    unblacklist_op_users('DELETE', [user1])
    container.restart()
    time.sleep(30)
    ensure_ticket()

    delete_response_after = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    assert delete_response_after.status_code == 200, f"Deletion of key got failed"