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



import random
import string
import uuid
import pytest

import requests

import time
import os

import pytest
import requests

# Module-level constants — always available globally, even with pytest -n auto
CREDENTIALS = ("admin", "rangerR0cks!")
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}
KEYADMIN_CREDENTIALS = ("keyadmin", "rangerR0cks!")

@pytest.fixture(scope="session")
def credentials():
    return CREDENTIALS

@pytest.fixture(scope="session")
def default_headers():
    return DEFAULT_HEADERS

@pytest.fixture(scope="session")
def keyadmin_credentials():
    return KEYADMIN_CREDENTIALS


def create_user_with_retry(url, **kwargs):
    import time

    for attempt in range(5):
        response = requests.post(url, **kwargs)

        if response.status_code == 200:
            return response

        time.sleep(1 + attempt)

    raise AssertionError(f"User creation failed: {response.text}")

@pytest.fixture(scope="function")
def ranger_session(ranger_config):
    session = requests.Session()
    session.auth = ranger_config["auth"]
    session.headers.update(ranger_config["headers"])
    return session


@pytest.fixture(scope="session")
def ranger_config(credentials, default_headers):

    return {
        "base_url": "http://localhost:6080/service",
        "auth": credentials,
        "headers": default_headers
    }

@pytest.fixture(scope="session")
def ranger_key_admin_config(keyadmin_credentials, default_headers):

    return {
        "id" : 3,
        "base_url": "http://localhost:6080/service",
        "auth": keyadmin_credentials,
        "headers": default_headers
    }

@pytest.fixture()
def all_users(ranger_config):

    url = f"{ranger_config['base_url']}/xusers/users/"
    
    response = requests.get(
        url,
        auth=ranger_config["auth"],
        headers=ranger_config["headers"]
    )

    assert response.status_code == 200, f"Failed to fetch users list: {response.status_code}"

    data = response.json()
    assert "vXUsers" in data, "Invalid users list schema"

    users = data["vXUsers"]
    assert isinstance(users, list)
    assert len(users) > 0, "No users found in Ranger"

    return users


@pytest.fixture()
def all_schema_following_users(ranger_config):

    url = f"{ranger_config['base_url']}/xusers/users/"
    response = requests.get(
        url,
        auth=ranger_config["auth"],
        headers=ranger_config["headers"]
    )

    assert response.status_code == 200, f"Failed to fetch users list: {response.status_code}"

    data = response.json()
    users = data["vXUsers"]

    return [
        user for user in users
        if not (7 <= user["id"] <= 18)
    ]

@pytest.fixture(scope="session")
def client_roles(ranger_config):

    username = ranger_config["auth"][0]

    url = f"{ranger_config['base_url']}/xusers/users/userName/{username}"

    response = requests.get(
        url,
        auth=ranger_config["auth"],
        headers=ranger_config["headers"]
    )

    assert response.status_code == 200, \
        f"Failed to fetch user details for {username}"

    return response.json().get("userRoleList", [])



@pytest.fixture()
def get_user_by_id(ranger_config):

    def _get_user(user_id):
        response = requests.get(
            f"{ranger_config['base_url']}/xusers/secure/users/{user_id}",
            auth=ranger_config["auth"],
            headers=ranger_config["headers"]
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code in [400, 404]:
            return None
        else:
            pytest.fail(
                f"Unexpected response: {response.status_code} - {response.text}"
            )

    return _get_user

@pytest.fixture(scope="class")
def temp_secure_user(ranger_config, client_roles):

    if "ROLE_SYS_ADMIN" not in client_roles:
        pytest.fail("Admin privileges required to create secure user")

    created_user_ids = []

    def _create_user(role_list = None):

         
        worker = os.getenv("PYTEST_XDIST_WORKER", "gw0")
        username = f"pytest_{worker}_{uuid.uuid4().hex[:8]}"

        role_map = {
            "user": "ROLE_USER",
            "admin": "ROLE_SYS_ADMIN",
            "auditor": "ROLE_ADMIN_AUDITOR"
        }

        if role_list:
            unique_roles = {role_map.get(role.lower(), "ROLE_USER") for role in role_list}
        else:
            unique_roles = {"ROLE_USER"}

        final_role_list = list(unique_roles)

        payload = {
            "name": username,
            "firstName": "Fixture",
            "lastName": "User",
            "emailAddress": f"{username}@test.com",
            "password": "Test@123",
            "status": 1,
            "isVisible": 1,
            "userSource":1, # to ensure it's created as an external user
            "userRoleList": final_role_list,
            "groupIdList": [],
            "groupNameList": []
        }
 
        response = create_user_with_retry(
            f"{ranger_config['base_url']}/xusers/secure/users",
            json=payload,
            auth=ranger_config["auth"],
            headers=ranger_config["headers"]
            )
        #time.sleep(0.5)

        assert response.status_code == 200, f"User creation failed: {response.text}"

        created_user = response.json()
        created_user_ids.append(created_user["id"])

        print(f"\n[Fixture] Created user ID: {created_user['id']}, Roles: {final_role_list}")

        return created_user, created_user["id"]

    yield _create_user

    for user_id in created_user_ids:
        print(f"[Fixture] Cleaning up user ID: {user_id}")

        response = requests.delete(
            f"{ranger_config['base_url']}/xusers/users/{user_id}",
            params={"forceDelete": "true"},
            auth=ranger_config["auth"],
            headers={**ranger_config["headers"], "X-Requested-By": "ranger"}
        )
        print("DELETE RESPONSE:", response.status_code, response.text)
        if response.status_code in [200, 204]:
            print(f"[Fixture] Deleted user ID: {user_id}")

        elif response.status_code in [400, 404]:
            print(f"[Fixture] User ID {user_id} already deleted")

        else:
            pytest.fail(
                f"Unexpected error deleting user ID {user_id}: {response.text}"
            )

@pytest.fixture(scope="class")
def temp_keyadmin_user(ranger_config, client_roles, role = "keyadmin"):

    if "ROLE_SYS_ADMIN" not in client_roles:
        pytest.fail("Admin privileges required")

    created_user_ids = []

    def _create_keyadmin():

        #username = f"pytest_keyadmin_{uuid.uuid4().hex[:8]}"
        worker = os.getenv("PYTEST_XDIST_WORKER", "gw0")
        username = f"pytest_keyadmin_{worker}_{uuid.uuid4().hex[:8]}"

        payload = {
            "name": username,
            "firstName": "Fixture",
            "lastName": "KeyAdmin",
            "emailAddress": f"{username}@test.com",
            "password": "Test@123",
            "status": 1,
            "isVisible": 1,
            "userRoleList": ["ROLE_USER"],
            "groupIdList": [],
            "groupNameList": []
        }

        response = create_user_with_retry(
            f"{ranger_config['base_url']}/xusers/secure/users",
            json=payload,
            auth=ranger_config["auth"],
            headers=ranger_config["headers"]
        )

        assert response.status_code == 200

        user = response.json()
        user_id = user["id"]

        print(f"\n[Fixture] Created keyadmin base: {username} ({user_id})")

        # promote to keyadmin
        #requests.put(
        response = requests.put(
            f"{ranger_config['base_url']}/xusers/secure/users/roles/{user_id}",
            json={"vXStrings": [{"value": "ROLE_KEY_ADMIN"}]},
            auth=("keyadmin", "rangerR0cks!"),
            headers={**ranger_config["headers"], "X-Requested-By": "ranger"}
        )
        #time.sleep(0.5)

        created_user_ids.append(user_id)

        return user, user_id

    yield _create_keyadmin

    # cleanup
    for user_id in created_user_ids:
        print(f"[Fixture] Cleaning keyadmin {user_id}")

        requests.put(
            f"{ranger_config['base_url']}/xusers/secure/users/roles/{user_id}",
            json={"vXStrings": [{"value": "ROLE_USER"}]},
            auth=("keyadmin", "rangerR0cks!"),
            headers={**ranger_config["headers"], "X-Requested-By": "ranger"}
        )

        requests.delete(
            f"{ranger_config['base_url']}/xusers/secure/users/id/{user_id}",
            params={"forceDelete": "true"},
            auth=ranger_config["auth"],
            headers={**ranger_config["headers"], "X-Requested-By": "ranger"}
        )

@pytest.fixture(scope="class")
def temp_permission_module(ranger_config, client_roles):

    if "ROLE_SYS_ADMIN" not in client_roles:
        pytest.fail("Admin privileges required to create permission module")

    created_module_ids = []

    def _create_permission_module():


        module_name = f"pytest_permission_{uuid.uuid4().hex[:8]}"
        payload = {
            "module": module_name,
        }

        # Fetch existing permissions to get a valid module and permission ID
        response = requests.post(
            f"{ranger_config['base_url']}/xusers/permission",
            json=payload,
            auth=ranger_config["auth"],
            headers=ranger_config["headers"]
        )
        

        assert response.status_code in (200, 201), \
            f"Failed to create permission module: {response.text}"

        module = response.json()

        # Ranger may return id OR moduleId depending on version
        module_id = module.get("id") or module.get("moduleId")

        created_module_ids.append(module_id)

        print(f"\n[Fixture] Created permission module: {module_name} ({module_id})")

        return module, module_id

    yield _create_permission_module

    # ---------------- CLEANUP ----------------
    for module_id in created_module_ids:
        print(f"[Fixture] Cleaning permission module {module_id}")

        response = requests.delete(
            f"{ranger_config['base_url']}/xusers/permission/{module_id}",
            auth=ranger_config["auth"],
            headers={
                **ranger_config["headers"],
                "X-Requested-By": "ranger"
            }
        )

        if response.status_code in (200, 204):
            print(f"[Fixture] Deleted permission module {module_id}")
        elif response.status_code == 404:
            print(f"[Fixture] Permission module {module_id} already deleted")
        else:
            pytest.fail(
                f"Unexpected error deleting permission module {module_id}: {response.text}"
            )

@pytest.fixture(scope="class")
def temp_group(ranger_config):

    created_group_ids = []

    def _create_group():

        group_name = f"pytest_group_{uuid.uuid4().hex[:8]}"

        payload = {
            "name": group_name
        }

        response = requests.post(
            f"{ranger_config['base_url']}/xusers/groups",
            json=payload,
            auth=ranger_config["auth"],
            headers=ranger_config["headers"]
        )

        assert response.status_code in (200, 201), response.text

        data = response.json()
        group_id = data["id"]

        created_group_ids.append(group_id)

        print(f"[Fixture] Created group {group_name} ({group_id})")

        return data, group_id

    yield _create_group

    # -------- CLEANUP ----------
    for gid in created_group_ids:
        print(f"[Fixture] Cleaning group {gid}")

        requests.delete(
            f"{ranger_config['base_url']}/xusers/groups/{gid}",
            auth=ranger_config["auth"],
            headers={
                **ranger_config["headers"],
                "X-Requested-By": "ranger"
            }
        )

@pytest.fixture(scope="class")
def temp_permission_group(ranger_config):

    created_ids = []

    def _create_permission_group(group_id, module_id):

        payload = {
            "groupId": group_id,
            "moduleId": module_id,
            "isAllowed": 1
        }

        response = requests.post(
            f"{ranger_config['base_url']}/xusers/permission/group",
            json=payload,
            auth=ranger_config["auth"],
            headers=ranger_config["headers"]
        )

        print("CREATE PERMISSION GROUP:",
              response.status_code, response.text)

        if response.status_code in (200, 201):
            data = response.json()
            created_ids.append(data["id"])
            return data, data["id"]

        if response.status_code == 400 and "duplicate" in response.text.lower():
            print("[Fixture] Permission already exists — reusing")

            get_resp = requests.get(
                f"{ranger_config['base_url']}/xusers/permission/group",
                params={
                    "groupId": group_id,
                    "moduleId": module_id
                },
                auth=ranger_config["auth"],
                headers=ranger_config["headers"]
            )

            assert get_resp.status_code == 200

            data = get_resp.json()["vXGroupPermissions"][0]
            return data, data["id"]

        pytest.fail(response.text)

    yield _create_permission_group
    # cleanup
    for pid in created_ids:
        requests.delete(
            f"{ranger_config['base_url']}/xusers/permission/group/{pid}",
            auth=ranger_config["auth"],
            headers={
                **ranger_config["headers"],
                "X-Requested-By": "ranger"
            }
        )

@pytest.fixture(scope="class")
def temp_permission_user(ranger_config):

    created_ids = []

    def _create_permission_user(user_id, module_id):
        
        payload = {
            "userId": user_id,
            "moduleId": module_id,
            "isAllowed": 1
        }

        response = requests.post(
            f"{ranger_config['base_url']}/xusers/permission/user",
            json=payload,
            auth=ranger_config["auth"],
            headers=ranger_config["headers"]
        )

        print("CREATE PERMISSION USER:",
              response.status_code, response.text)

        if response.status_code in (200, 201):
            data = response.json()
            created_ids.append(data["id"])
            return data, data["id"]

        if response.status_code == 400 and "duplicate" in response.text.lower():
            print("[Fixture] Permission already exists — reusing")

            get_resp = requests.get(
                f"{ranger_config['base_url']}/xusers/permission/user",
                params={
                    "userId": user_id,
                    "moduleId": module_id
                },
                auth=ranger_config["auth"],
                headers=ranger_config["headers"]
            )

            assert get_resp.status_code == 200

            data = get_resp.json()["vXUserPermissions"][0]
            return data, data["id"]

        pytest.fail(response.text)

    yield _create_permission_user
    # cleanup
    for pid in created_ids:
        requests.delete(
            f"{ranger_config['base_url']}/xusers/permission/user/{pid}",
            auth=ranger_config["auth"],
            headers={
                **ranger_config["headers"],
                "X-Requested-By": "ranger"
            }
        )