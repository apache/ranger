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


from urllib import request, response
import pytest
import requests
import json
from datetime import datetime
#from utility.utils import fetch_logs
import random
import string
from utility.utils import (
    assert_response,
    validate_secure_user_schema,
    user_exists,
    delete_user,
    validate_external_user_schema,
    init_configs
)

BIGINT_MIN = -9223372036854775808
BIGINT_MAX =  9223372036854775807

@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
@pytest.mark.secure_endpoint
class TestSecureUserEndpoint:
    SERVICE_NAME = "admin"

    @pytest.fixture(autouse=True, scope="class")
    def _setup(
        self,
        request,
        temp_secure_user,
        temp_keyadmin_user,
        default_headers,
        ranger_config,
    ):
        cls = request.cls

        # ---------- primary users ----------
        cls.admin, _ = temp_secure_user(["admin"])
        cls.ranger_key_admin, _ = temp_keyadmin_user()
        cls.audit, _ = temp_secure_user(["auditor"])
        cls.user, _ = temp_secure_user(["user"])

        cls.ranger_admin_config = (cls.admin["name"], "Test@123")
        cls.ranger_key_admin_config = (cls.ranger_key_admin["name"], "Test@123")
        cls.ranger_auditor_config = (cls.audit["name"], "Test@123")
        cls.ranger_user_config = (cls.user["name"], "Test@123")

        cls.ranger_admin_id = cls.admin["id"]
        cls.ranger_key_admin_id = cls.ranger_key_admin["id"]
        cls.ranger_auditor_id = cls.audit["id"]
        cls.ranger_user_id = cls.user["id"]

        # ---------- secondary users ----------
        cls.admin1, _ = temp_secure_user(["admin"])
        cls.ranger_key_admin1, _ = temp_keyadmin_user()
        cls.audit1, _ = temp_secure_user(["auditor"])
        cls.user1, _ = temp_secure_user(["user"])

        cls.ranger_admin_config1 = (cls.admin1["name"], "Test@123")
        cls.ranger_key_admin_config1 = (cls.ranger_key_admin1["name"], "Test@123")
        cls.ranger_auditor_config1 = (cls.audit1["name"], "Test@123")
        cls.ranger_user_config1 = (cls.user1["name"], "Test@123")

        cls.ranger_admin_id1 = cls.admin1["id"]
        cls.ranger_key_admin_id1 = cls.ranger_key_admin1["id"]
        cls.ranger_auditor_id1 = cls.audit1["id"]
        cls.ranger_user_id1 = cls.user1["id"]

        # ---------- common ----------
        cls.headers = default_headers
        cls.base_url = ranger_config["base_url"]

        init_configs(
            cls.ranger_admin_config,
            cls.ranger_key_admin_config,
            cls.ranger_auditor_config,
            cls.ranger_user_config,
        )

        
    # POSITIVE TESTS
    @pytest.mark.get
    @pytest.mark.positive
    def test_get_secure_user_schema_validation(self, client_roles, all_schema_following_users):

        if client_roles == ["ROLE_USER"]:
            assert False, "Test requires elevated privileges to access all users"
            
        """ check only if you need to verify all users
        for user in all_schema_following_users:

            response = requests.get(
                f"{self.base_url}/xusers/secure/users/{user['id']}",
                auth=self.ranger_admin_config,
                headers=self.headers
            )

            assert_response(response, 200)

            assert response.headers["Content-Type"].startswith("application/json")
            assert response.elapsed.total_seconds() < 2

            data = response.json()

            validate_secure_user_schema(data)

            assert data["id"] == user["id"]"""
        user_id  = 1
        response = requests.get(
            f"{self.base_url}/xusers/secure/users/{user_id}",
            auth=self.ranger_admin_config,
            headers=self.headers
            )

        assert_response(response, 200)


        data = response.json()

        validate_secure_user_schema(data)
        

    
    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize("auth_role, target_role", [
        ("admin", "admin"),
        ("admin", "auditor"),
        ("auditor", "admin"),
        ("auditor", "auditor"),
        ("keyadmin", "keyadmin"),
        ("keyadmin", "user"),
        ("s_auditor", "auditor"),

        ])
    def test_get_secure_external_user(self, client_roles, auth_role, target_role, request):

        if not any(r in client_roles for r in ["ROLE_SYS_ADMIN", "ROLE_ADMIN_AUDITOR","ROLE_KEY_ADMIN"]):
            pytest.skip("Test requires admin or auditor privileges")
        if target_role == "keyadmin":
            target_id = self.ranger_key_admin_id
            assert user_exists(target_id, self.ranger_key_admin_config, self.base_url, self.headers), "Pre-requisite failed: Expected keyadmin user does not exist or inaccessible"
        elif target_role == "admin":
            target_id = self.ranger_admin_id
        elif target_role == "auditor":
            target_id = self.ranger_auditor_id
        elif target_role == "user":
            target_id = self.ranger_user_id
            

        if auth_role == "admin":
            authorization = self.ranger_admin_config

        elif auth_role == "auditor":
            auth_id = self.ranger_auditor_id1
            authorization = self.ranger_auditor_config1
        elif auth_role == "s_auditor":
            auth_id = self.ranger_auditor_id
            authorization = self.ranger_auditor_config
        elif auth_role == "keyadmin":
            authorization = self.ranger_key_admin_config
 

        response = requests.get(
        f"{self.base_url}/xusers/secure/users/external/{target_id}",
            auth=authorization,
            headers=self.headers
        )

        assert_response(response, 200)

        data = response.json()

        if auth_role == "admin":
            validate_external_user_schema(data)

        elif auth_role == "auditor":
            # If auditor fetching their own data
            if auth_id == target_id:
                validate_external_user_schema(data)
            else:
                assert data["vXStrings"] == [], \
                "Auditor should not see other users' external roles"


    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize("auth_role, target_role", [
        ("admin", "admin"),
        ("admin", "auditor"),
        ("keyadmin", "keyadmin"),
        ("keyadmin", "user"),
        ("s_auditor", "auditor"),
        ])
    def test_get_secure_user_roles_by_username(self, client_roles, auth_role, target_role, request): 
        #admin cant access keyadmin, can access the users, auditor, admin
        # keyadmin cant access admin, can access the users, keyadmin
        # auditor if it is with same name and creds it will return this is flaw in internal repo
 
        if not any(r in client_roles for r in ["ROLE_SYS_ADMIN", "ROLE_ADMIN_AUDITOR","ROLE_KEY_ADMIN"]):
            pytest.skip("Test requires admin or auditor privileges")
        if target_role == "auditor":  
            target_user, target_id = self.audit, self.ranger_auditor_id
            target_name = target_user["name"]
        elif target_role == "keyadmin":
            target_name = self.ranger_key_admin["name"]
            assert user_exists(self.ranger_key_admin_id, self.ranger_key_admin_config, self.base_url, self.headers), "Pre-requisite failed: Expected keyadmin user does not exist or inaccessible"
        elif target_role == "admin":
            target_name = self.admin["name"]
        elif target_role == "user":
            target_name = self.user["name"]
        else:
            pytest.fail(f"Unhandled target_role in test setup: {target_role}")

        if auth_role == "admin":
            authorization = self.ranger_admin_config
        elif auth_role == "auditor":
            auth_user, auth_id = self.audit1, self.ranger_auditor_id1
            authorization = self.ranger_auditor_config1
        elif auth_role == "s_auditor":
            auth_user, auth_id = self.audit, self.ranger_auditor_id
            authorization = self.ranger_auditor_config
            target_name = auth_user["name"]  # Auditor can access only their own roles, so target_name should be same as auth_user's name
        elif auth_role == "keyadmin":
            authorization = self.ranger_key_admin_config
        response = requests.get(
            f"{self.base_url}/xusers/secure/users/roles/userName/{target_name}",
            auth=authorization,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        print("Response text:", response.text, "Response code:", response.status_code, " for auth_role:", auth_role, " target_role:", target_role)

        assert_response(response, 200)

        data = response.json()

        if auth_role == "admin":
            validate_external_user_schema(data)

        elif auth_role == "auditor":
            # If auditor fetching their own data
            if auth_id == target_id:
                validate_external_user_schema(data)
            else:
                assert data["vXStrings"] == [], \
                "Auditor should not see other users' external roles"


    @pytest.mark.post
    @pytest.mark.positive
    def test_create_secure_user(self, client_roles):

        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        print("\nCreating a new secure user")

        random_suffix = ''.join(random.choices(string.ascii_lowercase, k=5))
        username = f"pytest_user_{random_suffix}"

        payload = {
            "name": username,
            "firstName": "PyTest",
            "lastName": "User",
            "emailAddress": f"{username}@test.com",
            "password": "Test@123",
            "status": 1,
            "isVisible": 1,
            "userRoleList": ["ROLE_USER"],
            "groupIdList": [],
            "groupNameList": []
        }

        response = requests.post(
            f"{self.base_url}/xusers/secure/users",
            json=payload,
            auth=self.ranger_admin_config,
            headers=self.headers
        )

        assert_response(response, 200)
        data = response.json()
        user_id = data["id"]

        print(f"\nCreated user: {username} | ID: {user_id}")

        try:
            validate_secure_user_schema(data)
            assert data["name"] == username
            # Verify persistence
            verify_response = requests.get(
                f"{self.base_url}/xusers/secure/users/{user_id}",
                auth=self.ranger_admin_config,
                headers=self.headers
            )

            assert_response(verify_response, 200)

            verify_data = verify_response.json()
            assert verify_data["id"] == user_id
        finally:
            if user_id:
                delete_user(user_id, force=True, ranger_admin_config=self.ranger_admin_config, base_url=self.base_url, headers=self.headers)

    @pytest.mark.put
    @pytest.mark.positive
    def test_update_secure_user_flow(self, request, client_roles):

        if not any(role in client_roles for role in ["ROLE_SYS_ADMIN", "ROLE_KEY_ADMIN"]):
            assert False, "Test requires admin or key-admin privileges"

        created_user, user_id = request.getfixturevalue("temp_secure_user")()
            
        print(f"Created user ID: {user_id}, firstName: {created_user['firstName']}")

        print("\n Updating created user")
        update_payload = created_user.copy()
        update_payload["firstName"] = "Updatedname"

        update_response = requests.put(
            f"{self.base_url}/xusers/secure/users/{user_id}",
            json=update_payload,
            auth=self.ranger_admin_config,
            headers=self.headers
            )

        assert_response(update_response, 200)
        updated_data = update_response.json()

        validate_secure_user_schema(updated_data)
        assert updated_data["firstName"] == "Updatedname"

        print(f"Update successful for user ID: {user_id}, firstName: {updated_data['firstName']}")

    @pytest.mark.put
    @pytest.mark.positive
    def test_update_secure_user_active_status(self, request, client_roles):

        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        created_user1, user_id1 = request.getfixturevalue("temp_secure_user")()
        created_user2, user_id2 = request.getfixturevalue("temp_secure_user")()

        print(f"\nTesting active status update")

        update_payload = {
                str(user_id1): 0,
                str(user_id2): 0
        }

        response = requests.put(
            f"{self.base_url}/xusers/secure/users/activestatus",
            json=update_payload,
            auth=(self.ranger_admin_config),
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            }
        )
        assert_response(response, 204, f"Failed to update active status")
        response1 = requests.get(
            f"{self.base_url}/xusers/secure/users/{user_id1}",
            auth=self.ranger_admin_config,
            headers=self.headers
        )
        response2 = requests.get(
            f"{self.base_url}/xusers/secure/users/{user_id2}",
            auth=self.ranger_admin_config,
            headers=self.headers
        )
        assert response1.json()["status"] == 0, f"User ID {user_id1} active status not updated"
        assert response2.json()["status"] == 0, f"User ID {user_id2} active status not updated"

    @pytest.mark.put
    @pytest.mark.positive
    def test_update_secure_user_visibility(self, request, client_roles):

        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        created_user1, user_id1 = request.getfixturevalue("temp_secure_user")()
        created_user2, user_id2 = request.getfixturevalue("temp_secure_user")()

        print(f"\nTesting visibility update")

        update_payload = {
                str(user_id1): 0,
                str(user_id2): 0
        }

        response = requests.put(
            f"{self.base_url}/xusers/secure/users/visibility",
            json=update_payload,
            auth=(self.ranger_admin_config),
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            }
        )
        assert_response(response, 204, f"Failed to update visibility")

        response1 = requests.get(
            f"{self.base_url}/xusers/secure/users/{user_id1}",
            auth=self.ranger_admin_config,
            headers=self.headers
        )
        response2 = requests.get(
            f"{self.base_url}/xusers/secure/users/{user_id2}",
            auth=self.ranger_admin_config,
            headers=self.headers
        )
        assert response1.json()["isVisible"] == 0, f"User ID {user_id1} visibility not updated"
        assert response2.json()["isVisible"] == 0, f"User ID {user_id2} visibility not updated"

    @pytest.mark.parametrize("auth_role, target_role", [
        ("admin", "admin"),
        ("admin", "auditor"),
        ("admin", "user"),
        ("keyadmin", "keyadmin"),
        ("keyadmin", "user")
        ])
    @pytest.mark.put
    @pytest.mark.positive
    def test_update_secure_role_using_username(self, auth_role, target_role, client_roles, request):

        if not any(role in client_roles for role in ["ROLE_SYS_ADMIN", "ROLE_KEY_ADMIN"]):
            assert False, "Test requires admin or key-admin privileges"
        
        if target_role == "keyadmin":
            target_user, target_id = request.getfixturevalue("temp_keyadmin_user")()

            auth_user = self.ranger_key_admin

            assert auth_user["name"] != target_user["name"], \
                "Auth user and target user must be different"

            authorization = (auth_user["name"], 'Test@123')
        else:
            target_user, target_id = request.getfixturevalue("temp_secure_user")([target_role])

        if auth_role == "admin":
            authorization = self.ranger_admin_config
        elif auth_role == "keyadmin" and target_role != "keyadmin":
            authorization = self.ranger_key_admin_config
        elif auth_role == "auditor":
            authorization = self.ranger_auditor_config
        elif auth_role == "user":
            authorization = self.ranger_user_config
            

        print(f"\nTesting role update for user: {target_user['name']}")

        update_payload = {
            "vXStrings": [
                {"value": "ROLE_USER"}
            ]
        }

        response = requests.put(
            f"{self.base_url}/xusers/secure/users/roles/userName/{target_user['name']}",
            json=update_payload,
            auth=authorization,
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            }
        )

        assert_response(response, 200)

        updated_data = response.json()
        validate_external_user_schema(updated_data)
        assert "ROLE_USER" in updated_data["vXStrings"][0]["value"]


    @pytest.mark.parametrize("auth_role, target_role", [
        ("admin", "admin"),
        ("admin", "auditor"),
        ("admin", "user"),
        ("keyadmin", "keyadmin"),
        ("keyadmin", "user")
        ])
    @pytest.mark.put
    @pytest.mark.positive
    def test_update_secure_role_using_id(self, auth_role, target_role, client_roles, request):

        if not any(role in client_roles for role in ["ROLE_SYS_ADMIN", "ROLE_KEY_ADMIN"]):
            assert False, "Test requires admin or key-admin privileges"

        if target_role == "keyadmin":
            target_user,target_id = request.getfixturevalue("temp_keyadmin_user")()
            assert user_exists(target_id, self.ranger_key_admin_config, self.base_url, self.headers), "Pre-requisite failed: Expected keyadmin user does not exist or inaccessible"
            
        else:
            target_user, target_id = request.getfixturevalue("temp_secure_user")([target_role])

        if auth_role == "admin":
            authorization = self.ranger_admin_config
        elif auth_role == "keyadmin":
            authorization = self.ranger_key_admin_config
        elif auth_role == "auditor":
            authorization = self.ranger_auditor_config
        elif auth_role == "user":
            authorization = self.ranger_user_config
            

        print(f"\nTesting role update for user: {target_user['name']}")

        update_payload = {
            "vXStrings": [
                {"value": "ROLE_USER"}
            ]
        }

        response = requests.put(
            f"{self.base_url}/xusers/secure/users/roles/{target_id}",
            json=update_payload,
            auth=authorization,
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            }
        )

        assert_response(response, 200)

        updated_data = response.json()
        validate_external_user_schema(updated_data)
        assert "ROLE_USER" in updated_data["vXStrings"][0]["value"]

    @pytest.mark.delete
    @pytest.mark.positive
    def test_delete_secure_user_by_id(self, request, client_roles):

        if "ROLE_SYS_ADMIN" not in client_roles:
            pytest.fail("Test requires admin privileges")

        target_user, user_id = request.getfixturevalue("temp_secure_user")(["ROLE_USER"])

        resp = requests.delete(
            f"{self.base_url}/xusers/secure/users/id/{user_id}?forceDelete=true",
            auth=self.ranger_admin_config,
            headers={
                **self.headers,
                "X-Requested-By": "ranger"  # Mandatory for DELETE
            }
        )

        assert_response(resp, 204, f"Failed to delete user: {target_user['name']}")


    @pytest.mark.delete
    @pytest.mark.positive
    def test_delete_secure_bulk_users(self, request, client_roles):

        if "ROLE_SYS_ADMIN" not in client_roles:
            pytest.fail("Test requires admin privileges")
            
        created_user1, user_id1 = request.getfixturevalue("temp_secure_user")()
        created_user2, user_id2 = request.getfixturevalue("temp_secure_user")()

        delete_payload = {
            "vXStrings": [
            {"value": created_user1["name"]},
            {"value": created_user2["name"]}
            ]
        }

        response = requests.delete(
            f"{self.base_url}/xusers/secure/users/delete?forceDelete=true",
            json=delete_payload,
            auth=self.ranger_admin_config,
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            }
        )

        assert_response(response, 204)
        assert not user_exists(user_id1, self.ranger_admin_config, self.base_url, self.headers), f"User ID {user_id1} should be deleted but still exists"
        assert not user_exists(user_id2, self.ranger_admin_config, self.base_url, self.headers), f"User ID {user_id2} should be deleted but still exists"

    @pytest.mark.delete
    @pytest.mark.positive
    def test_get_deleted_user_by_name(self, request, client_roles):
        if "ROLE_SYS_ADMIN" not in client_roles:
            pytest.fail("Test requires admin privileges")

        target_user, _ = request.getfixturevalue("temp_secure_user")(["ROLE_USER"])
        user_name = target_user["name"]

        resp =requests.delete(
            f"{self.base_url}/xusers/secure/users/{user_name}",
            auth=self.ranger_admin_config,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )

        assert_response(resp, 204, f"Failed to delete user: {target_user['name']}")

        response = requests.get(
            f"{self.base_url}/xusers/secure/users/{user_name}",
            auth=self.ranger_admin_config
        )
        assert_response(response, 404)
       

    # NEGATIVE TESTS
    
    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
    "user_id, auth, expected_codes",
        [
            (1, ("wrong_user", "wrong_pass"), {401, 403}),
            (1, ("admin", "wrong_pass"), {401, 403}),
            (1, ("", ""), {401, 403}),
            (1, None, {401, 403}),
            (-999999, "valid", {400, 404}),
            (-12345, "valid", {400, 404}),
            ("abc", "valid", {400, 404}),
            ("east or west ranger is the best", "valid", {400, 404}),
        ],
        ids=[
        "wrong-credentials",
        "wrong-password",
        "empty-credentials",
        "missing-auth",
        "non-existing-id",
        "negative-id",
        "string-id",
        "garbage-input",
        ],
    )
    def test_negative_access(self, user_id, auth, expected_codes):

        if auth == "valid":
            auth = self.ranger_admin_config

        response = requests.get(
            f"{self.base_url}/xusers/secure/users/{user_id}",
            auth=auth,
            headers=self.headers,
        )

        assert_response(response, expected_codes, f"Expected status codes {expected_codes} but got {response.status_code} for user_id: {user_id} with auth: {auth}")

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
    "role, password, user_id",
    [
        ("user", "Admin@123", 28),   # wrong password
        ("keyadmin", None, 1),       # missing auth
    ],
    ids=["role-user", "role-keyadmin"],
    )
    def test_role_access_restrictions(self, role, password, user_id, ranger_config, request):

        if role == "keyadmin":
            create_keyadmin = request.getfixturevalue("temp_keyadmin_user")
            keyadmin_user, _ = create_keyadmin()

            if password is None:
                auth = None
            else:
                auth = (keyadmin_user["name"], password)
        else:
            new_user, _ = request.getfixturevalue("temp_secure_user")([role])
            auth = (new_user["name"], password)

        response = requests.get(
            f"{self.base_url}/xusers/secure/users/{user_id}",
            auth=auth,
            headers=self.headers,
        )
        assert_response(response, [401, 403], f"Unauthorized role accessed secure endpoint: Role={role}, User ID={user_id}")

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize("target_role", ("user",  "keyadmin"),)
    def test_get_secure_external_user_invalid_role(self, request, target_role):
        
        if target_role == "keyadmin":
            authorization = self.ranger_key_admin_config
            # keyadmin cann't  access admin
            target_id = self.ranger_admin_id1
             
        else:
            target_id = self.ranger_user_id1
            authorization = self.ranger_user_config

        response = requests.get(
        f"{self.base_url}/xusers/secure/users/external/{target_id}",
            auth=authorization,
            headers=self.headers
        )

        assert_response(response, 403, f"{target_role} should not access external user endpoint")


    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize("auth_role, target_role", [
        ("admin", "keyadmin"),
        ("keyadmin", "auditor"),
        ("auditor", "auditor"),
        ("auditor", "admin"),
        ("auditor", "keyadmin"),
        ("auditor", "user"),
        ("user", "user"),
        ("user", "auditor"),
        ])
    def test_get_secure_user_roles_by_username_by_invalid_role(self, request, client_roles, auth_role, target_role): 
        # admin cant access keyadmin, can access the users, auditor, admin
        # keyadmin cant access admin, can access the users, keyadmin
        # auditor if it is with same name and creds it will return this is flaw in internal repo
 
        if not any(r in client_roles for r in ["ROLE_SYS_ADMIN", "ROLE_ADMIN_AUDITOR","ROLE_KEY_ADMIN"]):
            pytest.fail("Test requires admin or auditor privileges")
        if target_role == "keyadmin":
            target_user,target_id = self.ranger_key_admin, self.ranger_key_admin_id
            assert user_exists(target_id, self.ranger_key_admin_config, self.base_url, self.headers), "Pre-requisite failed: Expected keyadmin user does not exist or inaccessible"
            target_name = target_user["name"]
        elif target_role == "auditor":
            target_user, target_id = self.audit, self.ranger_auditor_id
            target_name = target_user["name"]
        elif target_role == "admin":
            target_user, target_id = self.admin, self.ranger_admin_id
            target_name = target_user["name"]
        elif target_role == "user":
            target_user, target_id = self.user, self.ranger_user_id
            target_name = target_user["name"]

        if auth_role == "admin":
            authorization = self.ranger_admin_config1
        elif auth_role == "auditor":
            authorization = self.ranger_auditor_config1
        elif auth_role == "keyadmin":
            authorization = self.ranger_key_admin_config1
        elif auth_role == "user":
            authorization = self.ranger_user_config1

        response = requests.get(
            f"{self.base_url}/xusers/secure/users/roles/userName/{target_name}",
            auth=authorization,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        print("Response text:", response.text, "Response code:", response.status_code, " for auth_role:", auth_role, " target_role:", target_role)

        if auth_role == "auditor" and target_role != "keyadmin":
            assert_response(response, 400) # it will show invalid input data
            
        else:
            assert_response(response, 403)


    @pytest.mark.post
    @pytest.mark.negative
    def test_create_secure_user_missing_name(self):

        payload = {
        # "name" missing
        "firstName": "PyTest",
        "lastName": "User",
        "emailAddress": "missingname@test.com",
        "password": "Test@123",
        "status": 1,
        "isVisible": 1,
        "userRoleList": ["ROLE_USER"],
        "groupIdList": [],
        "groupNameList": []
        }

        response = requests.post(
        f"{self.base_url}/xusers/secure/users",
        json=payload,
        auth=self.ranger_admin_config,
        headers=self.headers
        )

        assert_response(response, 400)


    @pytest.mark.post
    @pytest.mark.negative
    def test_create_secure_user_duplicate_username(self):
        

        random_suffix = ''.join(random.choices(string.ascii_lowercase, k=5))
        username = f"duplicate_test_user_{random_suffix}"

        payload = {
        "name": username,
        "firstName": "PyTest",
        "lastName": "User",
        "emailAddress": f"{username}@test.com",
        "password": "Test@123",
        "status": 1,
        "isVisible": 1,
        "userRoleList": ["ROLE_USER"],
        "groupIdList": [],
        "groupNameList": []
        }

        user_id = None

        try:
            # First creation → should succeed
            first_response = requests.post(
            f"{self.base_url}/xusers/secure/users",
            json=payload,
            auth=self.ranger_admin_config,
            headers=self.headers
            )

            assert_response(first_response, 200)

            user_id = first_response.json().get("id")
            assert user_id is not None, "User ID not returned"

            # Second creation → should fail
            second_response = requests.post(
            f"{self.base_url}/xusers/secure/users",
            json=payload,
            auth=self.ranger_admin_config,
            headers=self.headers
            )

            assert_response(second_response, 400)

        finally:
            delete_user(user_id, force=True, ranger_admin_config=self.ranger_admin_config, base_url=self.base_url, headers=self.headers) 

    @pytest.mark.post
    @pytest.mark.negative
    def test_create_secure_user_via_invalid_roles(self, request):

        normal_user, n_id = request.getfixturevalue("temp_secure_user")(["user"])

        auditor_user, a_id = request.getfixturevalue("temp_secure_user")(["auditor"])


        users_to_test = [
        (normal_user["name"], "Test@123"),
        (auditor_user["name"], "Test@123"),
        self.ranger_key_admin_config, 
        ]

        for username, password in users_to_test:

            random_suffix = ''.join(random.choices(string.ascii_lowercase, k=5))
            blocked_username = f"blocked_{random_suffix}"

            payload = {
            "name": blocked_username,
            "firstName": "Blocked",
            "lastName": "User",
            "emailAddress": f"{blocked_username}@test.com",
            "password": "Test@123",
            "status": 1,
            "isVisible": 1,
            "userRoleList": ["ROLE_USER"],
            "groupIdList": [],
            "groupNameList": []
            }

            response = requests.post(
            f"{self.base_url}/xusers/secure/users",
            json=payload,
            auth=(username, password),
            headers=self.headers
            )

            print(f"{username} → {response.status_code}")
            assert_response(response, 403, f"{username} should not have permission to create secure users, but got {response.status_code}")


    @pytest.mark.put
    @pytest.mark.negative
    def test_edit_secure_user_using_invalid_id(self): 

        invalid_id = -999999
        update_payload = {
        "firstName": "ShouldFail"
        }


        update_response = requests.put(
            f"{self.base_url}/xusers/secure/users/{invalid_id}",
            json=update_payload,
            auth=self.ranger_admin_config,
            headers=self.headers
            )
        assert_response(update_response, 404, "Expected status code 404 for non-existing user ID")

    @pytest.mark.put
    @pytest.mark.negative
    def test_edit_secure_user_using_invalid_roles(self, request):

        normal_user, n_id = self.user1, self.ranger_user_id1
        auditor_user, a_id = self.audit1, self.ranger_auditor_id1

        users_to_test = [
        self.ranger_user_config1,
        self.ranger_auditor_config1,
        ]

    
        update_payload = normal_user.copy()
        update_payload["firstName"] = "ShouldNotUpdate"

        for username, password in users_to_test:

            update_response = requests.put(
            f"{self.base_url}/xusers/secure/users/{n_id}",
            json=update_payload,
            auth=(username, password),
            headers=self.headers
            )

            print("we got response", update_response.status_code, " for username, ", username)

            assert_response(update_response, 403, f"{username} should not have permission to edit secure users, but got {update_response.status_code}")

    @pytest.mark.put
    @pytest.mark.negative
    def test_edit_secure_user_using_invalid_payload(self, request):

        created_user, user_id = request.getfixturevalue("temp_secure_user")(["user"]) # can use existing but for simplicity creating new user

        invalid_payload = {
                "name": created_user["name"],
                # missing required fields like firstName, lastName etc.
            }

        update_response = requests.put(
                f"{self.base_url}/xusers/secure/users/{user_id}",
                json=invalid_payload,
                auth=(created_user["name"], "Test@123"),
                headers=self.headers
            )

        assert update_response.status_code == 400, "Expected status code 400 for invalid payload"

    @pytest.mark.parametrize("field", [
    "name",
    "id",
    "createDate"
     ])
    @pytest.mark.put
    @pytest.mark.negative
    def test_edit_secure_mandatory_fields(self, request, field):

        created_user, user_id = request.getfixturevalue("temp_secure_user")(["user"])

        invalid_payload = created_user.copy()

        # Apply invalid modification depending on field type
        if field == "name":
            invalid_payload[field] = "shld not update"
        elif field == "id":
            invalid_payload[field] = 1  # must be an existing ID, but not the one being updated
        else:
            invalid_payload[field] = "4566:00:00Z" 
        update_response = requests.put(
        f"{self.base_url}/xusers/secure/users/{user_id}",
        json=invalid_payload,
        auth=self.ranger_admin_config,  # must use admin to test validation
        headers=self.headers
        )

        print(field, "update response code:", update_response.status_code)

        assert_response(update_response, 400, f"Expected status code 400 when trying to modify mandatory field: {field}")


    @pytest.mark.put
    @pytest.mark.negative
    def test_update_secure_user_active_status_with_invalid_roles(self, request):

        target_user, target_id = request.getfixturevalue("temp_secure_user")()
        normal_user, n_id = request.getfixturevalue("temp_secure_user")(["user"])
        auditor_user, a_id = request.getfixturevalue("temp_secure_user")(["auditor"])

        users_to_test = [
            (normal_user["name"], "Test@123"),
            (auditor_user["name"], "Test@123"),
            self.ranger_key_admin_config,
        ]

        print(f"\nTesting active status update with unauthorized roles")

        update_payload = {
            str(target_id): 0
        }

        for username, password in users_to_test:
            print(f"Attempting update with user: {username}")
            
            response = requests.put(
                f"{self.base_url}/xusers/secure/users/activestatus",
                json=update_payload,
                auth=(username, password), # Use the specific user's credentials
                headers={
                    **self.headers,
                    "X-Requested-By": "ranger"
                }
            )

            assert_response(response, 403, f"{username} should not have permission to update active status, but got {response.status_code}")

        verify_response = requests.get(
            f"{self.base_url}/xusers/secure/users/{target_id}",
            auth=self.ranger_admin_config,
            headers=self.headers
        )
        assert verify_response.json().get("status") != 0, "Security failure: Status was actually updated despite error code!"

    @pytest.mark.put
    @pytest.mark.negative
    def test_update_secure_user_visibility_with_invalid_roles(self, request):

        target_user, target_id = request.getfixturevalue("temp_secure_user")()
        normal_user, n_id = request.getfixturevalue("temp_secure_user")(["user"])
        auditor_user, a_id = request.getfixturevalue("temp_secure_user")(["auditor"])

        users_to_test = [
            (normal_user["name"], "Test@123"),
            (auditor_user["name"], "Test@123"),
            self.ranger_key_admin_config,
        ]

        print(f"\nTesting active status update with unauthorized roles")

        update_payload = {
            str(target_id): 0
        }

        for username, password in users_to_test:
            print(f"Attempting update with user: {username}")
            
            response = requests.put(
                f"{self.base_url}/xusers/secure/users/visibility",
                json=update_payload,
                auth=(username, password), # Use the specific user's credentials
                headers={
                    **self.headers,
                    "X-Requested-By": "ranger"
                }
            )

            assert_response(response, 403, f"{username} should not have permission to update visibility, but got {response.status_code}")

        verify_response = requests.get(
            f"{self.base_url}/xusers/secure/users/{target_id}",
            auth=self.ranger_admin_config,
            headers=self.headers
        )
        assert verify_response.json().get("isVisible") != 0, "Security failure: Visibility was actually updated despite error code!"

    @pytest.mark.parametrize("auth_role, target_role", [
        ("admin", "keyadmin"),
        ("keyadmin", "admin"),
        ("keyadmin", "auditor"),
        ("auditor", "admin"),
        ("auditor", "user"),
        ("auditor", "keyadmin"),
        ("user", "user"),        
        ("user", "admin"),
        ("user", "auditor"),
        ("user", "keyadmin"),
        ])
    @pytest.mark.put
    @pytest.mark.negative
    def test_update_secure_role_using_id_with_invalid_roles(self, auth_role, target_role, request):


        if target_role == "keyadmin":
            target_user,target_id = request.getfixturevalue("temp_keyadmin_user")()
            assert user_exists(target_id, self.ranger_key_admin_config, self.base_url, self.headers), "Pre-requisite failed: Expected keyadmin user does not exist or inaccessible"
            
        else:
            target_user, target_id = request.getfixturevalue("temp_secure_user")([target_role])

        if auth_role == "admin":
            authorization = self.ranger_admin_config
        elif auth_role == "keyadmin":
            authorization = self.ranger_key_admin_config
        else:
            auth_user, auth_id = request.getfixturevalue("temp_secure_user")(["auditor"])
            authorization = (auth_user["name"], "Test@123")
            

        print(f"\nTesting role update for id: {target_id}")

        update_payload = {
            "vXStrings": [
                {"value": "ROLE_USER"}
            ]
        }

        response = requests.put(
            f"{self.base_url}/xusers/secure/users/roles/{target_id}",
            json=update_payload,
            auth=authorization,
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            }
        )

        assert_response(response, 403)



    @pytest.mark.parametrize("auth_role, target_role", [
        ("admin", "keyadmin"),
        ("keyadmin", "admin"),
        ("keyadmin", "auditor"),
        ("auditor", "admin"),
        ("auditor", "user"),
        ("auditor", "keyadmin"),
        ("user", "user"),        
        ("user", "admin"),
        ("user", "auditor"),
        ("user", "keyadmin"),
        ])
    @pytest.mark.put
    @pytest.mark.negative
    def test_update_secure_role_using_username_with_invalid_roles(self, auth_role, target_role, request):


        if target_role == "keyadmin":
            target_user,target_id = request.getfixturevalue("temp_keyadmin_user")()
            assert user_exists(target_id, self.ranger_key_admin_config, self.base_url, self.headers), "Pre-requisite failed: Expected keyadmin user with ID 3 does not exist"
            
        else:
            target_user, target_id = request.getfixturevalue("temp_secure_user")([target_role])

        if auth_role == "admin":
            authorization = self.ranger_admin_config
        elif auth_role == "keyadmin":
            authorization = self.ranger_key_admin_config
        else:
            auth_user, auth_id = request.getfixturevalue("temp_secure_user")(["auditor"])
            authorization = (auth_user["name"], "Test@123")
            

        print(f"\nTesting role update for user: {target_user['name']}")

        update_payload = {
            "vXStrings": [
                {"value": "ROLE_USER"}
            ]
        }

        response = requests.put(
            f"{self.base_url}/xusers/secure/users/roles/userName/{target_user['name']}",
            json=update_payload,
            auth=authorization,
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            }
        )

        assert_response(response, 403)


    @pytest.mark.delete
    @pytest.mark.negative
    def test_delete_secure_users_invalid_payload(self, client_roles):

        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        print("\nTesting bulk delete with invalid payload")

        invalid_payload = {
        "invalid": "data"
        }

        response = requests.delete(
            f"{self.base_url}/xusers/secure/users/delete",
            auth=self.ranger_admin_config,
            headers={
            **self.headers,
            "X-Requested-By": "ranger"
            },
            json=invalid_payload
        )

        assert_response(response, 400)
    
    @pytest.mark.delete
    @pytest.mark.negative
    def test_delete_secure_user_using_invalid_id(self, client_roles):
        
        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        print("\nTesting delete with invalid user ID")

        invalid_user_id = -999999

        response = requests.delete(
            f"{self.base_url}/xusers/secure/users/id/{invalid_user_id}",
            auth=self.ranger_admin_config,
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            }
        )

        assert_response(response, 404)
    
    @pytest.mark.delete
    @pytest.mark.negative
    @pytest.mark.parametrize("role",["user", "auditor", "keyadmin"])
    def test_delete_secure_user_using_invalid_role(self, request, role):

        normal_user, n_id = request.getfixturevalue("temp_secure_user")(role)
        print(f"\nTesting secure delete with role: {role}")
        print(f"User ID: {n_id}")

        response = requests.delete(
            f"{self.base_url}/xusers/secure/users/{n_id}",
            auth=(normal_user["name"], "Test@123"),
            headers={
            **self.headers,
            "X-Requested-By": "ranger"
            }
        )

        assert_response(response, [403, 405])

    @pytest.mark.delete
    @pytest.mark.negative
    def test_delete_secure_user_using_invalid_payload(self, request):
        created_user, user_id = request.getfixturevalue("temp_secure_user")(["user"])

        print("\nTesting delete with invalid payload")

        invalid_payload = {
            "invalid": "data"
        }

        response = requests.delete(
            f"{self.base_url}/xusers/secure/users/delete",
            auth=(created_user["name"], "Test@123"),
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            },
            json=invalid_payload
        )

        assert_response(response, 400)

    @pytest.mark.delete
    @pytest.mark.negative
    @pytest.mark.parametrize("fatal_payload", [
        {"vXStrings": "this_is_a_string_not_a_list"},
        {"vXStrings": [{"value": 12345}]},
        {"vXStrings": [{"value": {"unexpected": "dictionary"}}]},
        [{"vXStrings": [{"value": "user1"}]}]
        ])
    def test_delete_secure_users_malformed_items(self, ranger_config, client_roles, fatal_payload):
        if "ROLE_SYS_ADMIN" not in client_roles:
            pytest.skip("Test requires admin privileges")

        response = requests.delete(
            f"{ranger_config['base_url']}/xusers/secure/users/delete?forceDelete=true",
            json=fatal_payload,
            auth=ranger_config["auth"],
            headers={**ranger_config["headers"], "X-Requested-By": "ranger"}
        )

        assert_response(response, 400)

    @pytest.mark.delete
    @pytest.mark.negative
    def test_delete_secure_user_using_invalid_username(self, client_roles):
        
        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        print("\nTesting delete with invalid username")

        invalid_username = "invalid_user7890#mkc"

        response = requests.delete(
            f"{self.base_url}/xusers/secure/users/{invalid_username}",
            auth=self.ranger_admin_config,
            headers={
                **self.headers,
                "X-Requested-By": "ranger"
            }
        )

        assert_response(response, 400, f"Expected 400 for invalid username, but got {response.status_code}. Response: {response.text}")
    
    @pytest.mark.delete
    @pytest.mark.negative
    @pytest.mark.parametrize("role",["user", "auditor", "keyadmin"])
    def test_delete_secure_user_by_username_using_invalid_role(self, ranger_config, request, role):

        normal_user, n_id = request.getfixturevalue("temp_secure_user")(role)
        print(f"\nTesting secure delete with role: {role}")
        print(f"User name: {normal_user['name']}")

        response = requests.delete(
            f"{ranger_config['base_url']}/xusers/secure/users/{normal_user['name']}",
            auth=(normal_user["name"], "Test@123"),
            headers={
            **ranger_config["headers"],
            "X-Requested-By": "ranger"
            }
        )

        assert_response(response, [403, 405])