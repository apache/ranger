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




import string

from utility.utils import (
    assert_response,
    validate_secure_user_schema,
    validate_user_schema,
    user_exists,
    delete_user,
    validate_external_user_schema,
    init_configs,
    validate_xgroup_schema,
    assign_groups_to_user
)
import uuid
import pytest
import requests
from datetime import datetime
#from utility.utils import fetch_logs
import random

@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestUsers:
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

    # Positive Test Cases
    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"),("keyadmin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")],
        ids=["admin", "keyadmin", "auditor", "user"],
    )
    def test_get_users(self, role, auth, request):
        auth = getattr(self, auth)

        url = f"{self.base_url}/xusers/users"
        response = requests.get(
            url,
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, f"Failed to fetch users list: {response.status_code}")

        data = response.json()
        assert "vXUsers" in data, "Invalid users list schema"

        users = data["vXUsers"]
        assert isinstance(users, list)
        assert len(users) > 0, "No users found in Ranger"

    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"),("keyadmin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")],
        ids=["admin", "keyadmin", "auditor", "user"],
    )
    def test_get_user_count(self, role, auth, request):
        auth = getattr(self, auth)

        url = f"{self.base_url}/xusers/users/count"
        response = requests.get(
            url,
            auth = auth,
            headers=self.headers
        )

        assert_response(response, 200, f"Failed to fetch user count: {response.status_code}")

        data = response.json()
        print(f"Total user count response: {data['value']}, auth : {auth[0]}")
        assert isinstance(data["value"], int), "User count not found in response"

    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize("auth_role, user_role", [
    ("admin", "user"), 
    ("admin", "auditor"), 
    ("admin", "admin"), 
    ("keyadmin", "keyadmin"), 
    ("keyadmin", "user"), 
    ("auditor", "admin"), 
    ("auditor", "auditor"), 
    ("auditor", "user"), 
    ("user", "same_user")
])
    def test_get_user_by_id(self, auth_role, user_role, request):
        if auth_role == "keyadmin":
            auth = self.ranger_key_admin_config
        elif auth_role == "admin":
            auth = self.ranger_admin_config
        elif auth_role == "auditor":
            auth = self.ranger_auditor_config
        elif auth_role == "user":
            auth = self.ranger_user_config
        
        if user_role == "same_user":
            user, user_id = self.user, self.ranger_user_id
        elif user_role == "keyadmin":  
            user, user_id = self.ranger_key_admin1, self.ranger_key_admin_id1
        elif user_role == "admin":
            user, user_id = self.admin1, self.ranger_admin_id1
        elif user_role == "auditor":
            user, user_id = self.audit1, self.ranger_auditor_id1
        elif user_role == "user":  
            user, user_id = self.user1, self.ranger_user_id1

        response = requests.get(
            f"{self.base_url}/xusers/users/{user_id}",
            auth = auth,
            headers=self.headers
            )
        
        assert_response(response, 200, f"Failed to fetch user of role {user_role} using {auth_role}")

        data = response.json()
        validate_user_schema(data)
        assert data["id"] == user_id, "Fetched user ID does not match requested ID"
        

    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize("auth_role, user_role", [
    ("admin", "user"), 
    ("admin", "auditor"), 
    ("admin", "admin"), 
    ("keyadmin", "keyadmin"), 
    ("keyadmin", "user"), 
    ("auditor", "admin"), 
    ("auditor", "auditor"), 
    ("auditor", "user"), 
    ("user", "same_user")
])
    def test_get_users_by_username(self, request, auth_role, user_role):
        if auth_role == "keyadmin":
            auth = self.ranger_key_admin_config
        elif auth_role == "admin":
            auth = self.ranger_admin_config
            if self.admin["name"].lower() == "rangerusersync":
                assert True, "If any admin's username is rangerusersync it has access to all users irrespective of role"
                return
        elif auth_role == "auditor":
            auth_user, auth_id = self.audit, self.ranger_auditor_id
            auth = self.ranger_auditor_config
        elif auth_role == "user":
            auth_user, auth_id = self.user, self.ranger_user_id
            auth = self.ranger_user_config
        
        if user_role == "same_user":
            user, user_id = auth_user, auth_id
        elif user_role == "keyadmin":  
            user, user_id = self.ranger_key_admin1, self.ranger_key_admin_id1
        elif user_role == "admin":
            user, user_id = self.admin1, self.ranger_admin_id1
        elif user_role == "auditor":
            user, user_id = self.audit1, self.ranger_auditor_id1
        elif user_role == "user":
            user, user_id = self.user1, self.ranger_user_id1

        response = requests.get(
            f"{self.base_url}/xusers/users/userName/{user['name']}",
            auth=auth,
            headers=self.headers
        )
        
        assert_response(response, 200, f"Failed to fetch user of role {user_role} using {auth_role}")

        data = response.json()
        validate_user_schema(data)
        assert data["id"] == user_id, "Fetched user ID does not match requested ID"

    @pytest.mark.positive 
    @pytest.mark.post 
    def test_create_user(self, client_roles): 
        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        print("\nCreating a new secure user")

        username = f"mine_pytest_fixture_{uuid.uuid4().hex[:8]}"

        payload = {
            "name": username,
            "password": "Test@123"
        }

        response = requests.post(
            f"{self.base_url}/xusers/users",
            json=payload,
            auth=self.ranger_admin_config,
            headers=self.headers
        )

        assert_response(response, 200)
        data = response.json()
        user_id = data["id"]

        print(f"\nCreated user: {username} | ID: {user_id}")

        try:
            validate_user_schema(data) # since it will always return default users skip this
            assert data["name"] == username
            # Verify persistence
            verify_response = requests.get(
                f"{self.base_url}/xusers/users/{user_id}",
                auth=self.ranger_admin_config,
                headers=self.headers
            )

            assert_response(verify_response, 200)

            verify_data = verify_response.json() 
            assert verify_data["id"] == user_id
            print(response.json())
        finally:

            delete_user(user_id, self.ranger_admin_config, self.base_url, self.headers)

    @pytest.mark.positive
    @pytest.mark.post
    def test_create_users_external(self, request, client_roles):
        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        print("\nCreating a new external user")

        username = f"external_pytest_fixture_{uuid.uuid4().hex[:8]}"

        payload = {
            "name": username
        }

        response = requests.post(
            f"{self.base_url}/xusers/users/external",
            json=payload,
            auth=self.ranger_admin_config,
            headers=self.headers
        )

        assert_response(response, 204) # since it wont return anything for new users.

        try :
            response = requests.post(
            f"{self.base_url}/xusers/users/external",
            json=payload,
            auth=self.ranger_admin_config,
            headers=self.headers
            )

            data = response.json()
            assert_response(response, 200) # since it will return existing user details for existing users.
            user_id = data["id"]

            print(f"\nCreated external user: {username} | ID: {user_id}")
        finally:
            response = requests.delete(
            f"{self.base_url}/xusers/users/{user_id}",
            params={"forceDelete": "true"},
            auth=self.ranger_admin_config,
            headers={**self.headers, "X-Requested-By": "ranger"}
            )
            assert_response(response, 204)
    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("auth_role", ["admin"])
    def test_create_user_userinfo(self, request, auth_role):
        if auth_role != "admin":
            assert False, "Test requires admin privileges"
        auth = self.ranger_admin_config
        test_user, test_id = request.getfixturevalue("temp_secure_user")(["user"])
        print(f"\nCreating a new secure user with userinfo endpoint")
        assert user_exists(test_id, self.ranger_admin_config, self.base_url, self.headers), "User should exist before performing userinfo based creation"
        grp_name = random.choice(string.ascii_letters) + ''.join(random.choices(string.ascii_letters + string.digits, k=7))
        payload = {
            "xuserInfo": {
                    "name": test_user["name"]  
                },
            "xgroupInfo": [
                    {
                    "name": grp_name,
                    "groupType": 1,
                    "groupSource": 1,
                    "description": "this is for test"
                    
                    }
                ]
        }

        assert payload["xuserInfo"]["name"] == test_user["name"], "User info name should match the test user name"
        assert len(payload["xgroupInfo"]) >= 1, "There should be at least one group info"

        response = requests.post(
            f"{self.base_url}/xusers/users/userinfo",
            json=payload,
            auth=auth,
            headers=self.headers
        )


        assert_response(response, 200)

        data = response.json()

        validate_xgroup_schema(data["xgroupInfo"][0])


        assert data["xgroupInfo"][0]["name"] == payload["xgroupInfo"][0]["name"], "Response group name should match the test user name"

        grp_id = data["xgroupInfo"][0]["id"]
        # cleanup  
        params = {"forceDelete": "true"}
        del_req = requests.delete(
            f"{self.base_url}/xusers/groups/{grp_id}",
            auth=auth,
            headers=self.headers,
            params=params
        )
        print(f"Deleted group {grp_name} created during test, status code: {del_req.status_code}")


    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("auth_role", ["admin"])
    def test_create_roleassignment(self, auth_role, request):

        if auth_role != "admin":
            pytest.fail("Test requires admin privileges")
        auth = self.ranger_admin_config

        users = [request.getfixturevalue("temp_secure_user")(["user"])[0] for _ in range(8)]
        for u in users:
            assert u["userSource"] == 1, f"{u['name']} should be external user"

        uA, uB, uC, uD, uE, uF, uG, uH = [u["name"] for u in users]

        group_admin = f"grp_admin_{uuid.uuid4().hex[:6]}"
        group_auditor = f"grp_aud_{uuid.uuid4().hex[:6]}"
        group_user = f"grp_user_{uuid.uuid4().hex[:6]}"

        assign_groups_to_user(uB, [group_auditor], auth, self.base_url, self.headers)  # groupMap
        assign_groups_to_user(uE, [group_admin], auth, self.base_url, self.headers)   # wlGroup
        assign_groups_to_user(uF, [group_admin], auth, self.base_url, self.headers)   # override userMap
        assign_groups_to_user(uG, [group_user], auth, self.base_url, self.headers)    # override groupMap
        assign_groups_to_user(uH, [group_admin], auth, self.base_url, self.headers)   # wlUser vs wlGroup

        user_map = {
            uA: "ROLE_SYS_ADMIN",
            uF: "ROLE_ADMIN_AUDITOR",  # will be overridden by whitelist group
        }

        group_map = {
            group_auditor: "ROLE_ADMIN_AUDITOR",
        }

        white_user_map = {
            uD: "ROLE_ADMIN_AUDITOR",
            uH: "ROLE_SYS_ADMIN",  # overrides whitelist group
        }

        white_group_map = {
            group_admin: "ROLE_SYS_ADMIN",
            group_user: "ROLE_USER",
        }

        payload = {
            "users": [uA, uB, uC, uD, uE, uF, uG, uH],
            "userRoleAssignments": user_map,
            "groupRoleAssignments": group_map,
            "whiteListUserRoleAssignments": white_user_map,
            "whiteListGroupRoleAssignments": white_group_map,
            "isReset": False,
            "isLastPage": False
        }

        response = requests.post(
            f"{self.base_url}/xusers/users/roleassignments",
            json=payload,
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200)

        expected = {
            uA: "ROLE_SYS_ADMIN",          # userMap
            uB: "ROLE_ADMIN_AUDITOR",      # groupMap
            uC: "ROLE_USER",               # default
            uD: "ROLE_ADMIN_AUDITOR",      # whitelist user
            uE: "ROLE_SYS_ADMIN",          # whitelist group
            uF: "ROLE_SYS_ADMIN",          # wlGroup > userMap
            uG: "ROLE_USER",               # wlGroup > groupMap
            uH: "ROLE_SYS_ADMIN",          # wlUser > wlGroup
        }

        for user, expected_role in expected.items():
            resp = requests.get(
            f"{self.base_url}/xusers/users/userName/{user}",
            auth=auth,
            headers=self.headers
            )

            assert resp.status_code == 200, f"Failed to fetch {user}"
            data = resp.json()
            validate_user_schema(data)

            actual_roles = data["userRoleList"]
            print(f"{user} → {actual_roles}")
            assert expected_role in actual_roles, \
                f"{user}: expected {expected_role}, got {actual_roles}"


        # cleanup of created groups
        for group in [group_admin, group_auditor, group_user]:
            resp = requests.get(
                f"{self.base_url}/xusers/groups/groupName/{group}",
                auth=auth,
                headers=self.headers
            )
            if resp.status_code == 200:
                group_id = resp.json()["id"]
                params = {"forceDelete": "true"}
                requests.delete(
                    f"{self.base_url}/xusers/groups/{group_id}",
                    params=params,
                    auth=auth,
                    headers={**self.headers, "X-Requested-By": "ranger"}
                )
                print(f"Deleted group {group} created during test")
                assert_response(resp, 200, f"Failed to delete group {group} during cleanup")
        
    @pytest.mark.positive
    @pytest.mark.put
    @pytest.mark.parametrize("auth_role", ["admin", "keyadmin"])
    def test_update_user(self, auth_role, request):

        user, user_id = request.getfixturevalue("temp_secure_user")(["user"])

        print(f"\nUpdating user: {user['name']} | ID: {user_id}")

        payload = {
            "id": user_id,
            "name": user["name"],
            "firstName": "updatedfirstname",
            "lastName": "updatedlastname",
            "emailAddress": f"updated_{user['emailAddress']}",
            "status": 1,
            "isVisible": 1,
            "userRoleList": ["ROLE_USER"],
            "groupIdList": [],
            "groupNameList": []
        }

        mandatory_fields = ["id", "name", "userRoleList", "firstName"]
        missing = [k for k in mandatory_fields if k not in payload]
        assert not missing, f"Missing mandatory fields in payload: {missing}"

        if auth_role == "admin":
            auth = self.ranger_admin_config
            assert "ROLE_KEY_ADMIN" not in payload["userRoleList"], "Admin can not promote any one to keyadmin role"
            assert "ROLE_KEY_ADMIN" not in user["userRoleList"], "Admin can not update keyadmin role"
        elif auth_role == "keyadmin":
            auth = self.ranger_key_admin_config
            assert "ROLE_SYS_ADMIN" not in payload["userRoleList"], "Key Admin can not promote any one to admin role"
            assert "ROLE_ADMIN_AUDITOR" not in payload["userRoleList"], "Key Admin can not promote any one to auditor role"
            assert "ROLE_SYS_ADMIN" not in user["userRoleList"], "Key Admin can not update admin role"
            assert "ROLE_ADMIN_AUDITOR" not in user["userRoleList"], "Key Admin can not update auditor role"
        

        resp = requests.get(
            f"{self.base_url}/xusers/users/{user_id}",
            auth=self.ranger_admin_config,
            headers=self.headers
        )
        resp = resp.json()

        for i in ["id", "name"]:
            assert resp[i] == payload[i], f"Miss-match mandatory field in response: {i}"

        response = requests.put(
            f"{self.base_url}/xusers/users",
            json=payload,
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200)

        data = response.json()
        validate_user_schema(data)
        assert data["firstName"].lower() == "updatedfirstname"
        assert data["lastName"].lower() == "updatedlastname"
        assert data["emailAddress"].lower() == f"updated_{user['emailAddress']}".lower()
        
    @pytest.mark.positive
    @pytest.mark.delete
    def test_delete_user(self, request, client_roles):
        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        user, user_id = request.getfixturevalue("temp_secure_user")(["user"])

        print(f"\nDeleting user: {user['name']} | ID: {user_id}")

        assert user_exists(user_id, self.ranger_admin_config, self.base_url, self.headers), "User should exist before deletion"

        response = requests.delete(
            f"{self.base_url}/xusers/users/{user_id}",
            params={"forceDelete": "true"},
            auth=self.ranger_admin_config,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        assert response, "User deletion failed"
        assert_response(response, 204)

    @pytest.mark.positive
    @pytest.mark.delete
    def test_delete_external_user_by_username(self, request, client_roles):
        if "ROLE_SYS_ADMIN" not in client_roles:
            assert False, "Test requires admin privileges"

        user, user_id = request.getfixturevalue("temp_secure_user")(["user"])
        userName = user["name"]

        print(f"\nDeleting external user: {user['name']} | ID: {user_id}")

        assert user_exists(user_id, self.ranger_admin_config, self.base_url, self.headers), "User should exist before deletion"

        response = requests.delete(
            f"{self.base_url}/xusers/users/userName/{userName}",
            params={"forceDelete": "true"},
            auth=self.ranger_admin_config,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        assert response, "External user deletion failed"
        assert_response(response, 204)
        

    # Negative Test Cases
    @pytest.mark.negative
    @pytest.mark.get
    def test_get_users_using_invalid_auth(self):
        random_num = random.randint(100000, 999999)
        url = self.base_url + f'/xusers/users/{random_num}'
        
        response = requests.get(
            url,
            auth=self.ranger_admin_config,
            headers=self.headers
        )

        assert_response(response, 400, "Expected status code not returned for invalid user ID")


    @pytest.mark.negative
    @pytest.mark.get
    @pytest.mark.parametrize("auth_role, user_role", [
    ("admin", "keyadmin"),
    ("auditor", "keyadmin"),
    ("keyadmin","admin"),
    ("keyadmin","auditor"),
    ("user", "admin"),
    ("user", "auditor"),
    ("user", "user"),
    ("user", "keyadmin"),
    ])
    def test_get_user_by_id_using_invalid_auth(self, auth_role, user_role, request):
        if auth_role == "keyadmin":
            auth = self.ranger_key_admin_config
        elif auth_role == "admin":
            auth = self.ranger_admin_config
        elif auth_role == "auditor":
            auth = self.ranger_auditor_config
        elif auth_role == "user":
            auth = self.ranger_user_config
        

        if user_role == "keyadmin":  
            user_id = self.ranger_key_admin_id1
        elif user_role == "admin": 
            user_id = self.ranger_admin_id1
        elif user_role == "auditor":
            user_id = self.ranger_auditor_id1
        elif user_role == "user":  
            user_id = self.ranger_user_id1
        
        
        response = requests.get(
            f"{self.base_url}/xusers/users/{user_id}",
            auth=auth,
            headers=self.headers
            )       
        
        assert_response(response, 403, f"{auth_role} should not have permission to access user of role {user_role}")


    @pytest.mark.negative
    @pytest.mark.get
    @pytest.mark.parametrize("auth_role, user_role", [
    ("admin", "keyadmin"), 
    ("keyadmin", "admin"), 
    ("keyadmin", "auditor"), 
    ("auditor", "keyadmin"),
    ("user", "admin"), 
    ("user", "auditor"), 
    ("user", "keyadmin"), 
    ("user", "user")
])
    def test_get_users_by_username_with_invalid_role(self, request, auth_role, user_role):
        if auth_role == "keyadmin":
            auth = self.ranger_key_admin_config
        elif auth_role == "admin":
            auth = self.ranger_admin_config
            if self.admin["name"].lower() == "rangerusersync":
                assert True, "If any admin's username is rangerusersync it has access to all users irrespective of role"
                return
        elif auth_role == "auditor":
            auth_user, auth_id = self.audit, self.ranger_auditor_id
            auth = self.ranger_auditor_config
        elif auth_role == "user":
            auth_user, auth_id = self.user, self.ranger_user_id
            auth = self.ranger_user_config
        
        if user_role == "keyadmin":  
            user, user_id = self.ranger_key_admin1, self.ranger_key_admin_id1
        elif user_role == "admin":
            user, user_id = self.admin1, self.ranger_admin_id1
        elif user_role == "auditor":
            user, user_id = self.audit1, self.ranger_auditor_id1
        elif user_role == "user":
            user, user_id = self.user1, self.ranger_user_id1

        userName = user["name"]
        response = requests.get(
            f"{self.base_url}/xusers/users/userName/{userName}",
            auth=auth,
            headers=self.headers
        )
        
        assert_response(response, 403, f"Unauthorized access should be denied for {auth_role} to access user of role {user_role}")

    @pytest.mark.post 
    @pytest.mark.negative
    def test_create_secure_user_missing_mandatory_field(self):

        payload = {
        "name" : "missing_night2d2",
        }

        response = requests.post(
        f"{self.base_url}/xusers/users",
        json=payload,
        auth=self.ranger_admin_config,
        headers=self.headers
        )

        assert_response(response, 400)


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
            f"{self.base_url}/xusers/users",
            json=payload,
            auth=(username, password),
            headers=self.headers
            )

            print(f"{username} → {response.status_code}")
            assert_response(response, 403, f"{username} should not have permission to create secure users, but got {response.status_code}")
    

    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("auth_role", ["auditor", "user", "keyadmin"]) 
    def test_create_external_user_using_invalid_roles(self, request, auth_role):
        if auth_role == "auditor":
            auth = self.ranger_auditor_config
        elif auth_role == "user":
            auth = self.ranger_user_config
        elif auth_role == "keyadmin":
            auth = self.ranger_key_admin_config
        random_suffix = ''.join(random.choices(string.ascii_lowercase, k=5))
        username = f"external_{random_suffix}"
        payload = {
            "name": username
        }
        response = requests.post(
            f"{self.base_url}/xusers/users/external",
            json=payload,
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 403, f"{auth_role} should not have permission to create external users, but got {response.status_code}")

    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("auth_role", ["auditor", "user", "keyadmin"])
    def test_create_user_userinfo_with_invalid_roles(self, request, auth_role):
        if auth_role == "auditor":
            auth = self.ranger_auditor_config
        elif auth_role == "user":
            auth = self.ranger_user_config
        elif auth_role == "keyadmin":
            auth = self.ranger_key_admin_config

        test_user, test_id = self.user, self.ranger_user_id
        assert user_exists(test_id, self.ranger_admin_config, self.base_url, self.headers), "User should exist before performing userinfo based creation"
        
        payload = {
            "xuserInfo": {
                    "name": test_user["name"]  
                },
            "xgroupInfo": [
                    {
                    "name": random.choice(string.ascii_letters) + ''.join(random.choices(string.ascii_letters + string.digits, k=7)),
                    "groupType": 1,
                    "groupSource": 1,
                    "description": "this is for test"
                    
                    }
                ]
        }

        response = requests.post(
            f"{self.base_url}/xusers/users/userinfo",
            json=payload,
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 403, f"{auth_role} should not have permission to create users using userinfo endpoint")

    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("missing_field", ["name", "xuserInfo_name"])
    def test_create_user_userinfo_with_invalid_payload(self, request, missing_field):
        auth = self.ranger_admin_config

        if missing_field == "name":
            username= f"non_exister_userinfo_{''.join(random.choices(string.ascii_lowercase, k=5))}"
            x_group_name  = "sample"
        elif missing_field == "xuserInfo_name":
            username = self.user["name"]
            x_group_name = None
        payload = {
            "xuserInfo": {
                    "name": username  
                },
            "xgroupInfo": [
                    {
                    "name": x_group_name,
                    }
                ]
        }

        response = requests.post(
            f"{self.base_url}/xusers/users/userinfo",
            json=payload,
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 404, f"Userinfo creation should not be allowed with missing field: {missing_field}")

    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("auth_role", ["auditor", "user", "keyadmin"])
    def test_create_roleassignment_with_invalid_roles(self, request, auth_role):
        
        if auth_role == "auditor":
            auth = self.ranger_auditor_config
        elif auth_role == "user":
            auth = self.ranger_user_config
        elif auth_role == "keyadmin":
            auth = self.ranger_key_admin_config 

        payload = {
            "users": [self.user1["name"]],
            "userRoleAssignments": {self.user1["name"]: "ROLE_SYS_ADMIN"},
            "groupRoleAssignments": {},
            "whiteListUserRoleAssignments": {},
            "whiteListGroupRoleAssignments": {},
            "isReset": False,
            "isLastPage": False
        }

        response = requests.post(
            f"{self.base_url}/xusers/users/roleassignments",
            json=payload,
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 403, f"{auth_role} should not have permission to assign roles, but got {response.status_code}")


    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("invalid_role", ["ROLE_NON_EXISTEN", "ROLE_KEY_ADMIN"])
    def test_create_roleassignment_to_invalid_assignment(self, invalid_role):
        auth = self.ranger_admin_config

        payload = {
            "users": [self.user1["name"]],
            "userRoleAssignments": {self.user1["name"]: invalid_role},
            "groupRoleAssignments": {},
            "whiteListUserRoleAssignments": {},
            "whiteListGroupRoleAssignments": {},
            "isReset": False,
            "isLastPage": False
        }

        response = requests.post(
            f"{self.base_url}/xusers/users/roleassignments",
            json=payload,
            auth=auth,
            headers=self.headers
        )
        if invalid_role == "ROLE_NON_EXISTEN":
            expected_status = 400
        elif invalid_role == "ROLE_KEY_ADMIN":
            expected_status = 403  # since ROLE_KEY_ADMIN is not a valid role but it has admin keyword so it will be blocked by permission check before role validation
        assert_response(response, expected_status, f"Assigning non-existent role should fail with {expected_status}, but got {response.status_code}")
    


    @pytest.mark.negative
    @pytest.mark.put
    @pytest.mark.parametrize(
        "auth_role,user_role",
        [
        ("admin", ["ROLE_KEY_ADMIN"]),
        ("auditor", ["ROLE_USER"]),
        ("user", ["ROLE_USER"]),
        ("keyadmin", ["ROLE_SYS_ADMIN"]),
        ("keyadmin", ["ROLE_ADMIN_AUDITOR"]),
            ],)    
    def test_update_user_using_invalid_access(self, temp_secure_user, auth_role, user_role):

        user, user_id = temp_secure_user(["user"])
        
        print(f"\nUpdating user: {user['name']} | ID: {user_id}")
        
        if auth_role == "auditor":
            auth_user = self.audit
            auth = self.ranger_auditor_config
            
            assert "ROLE_ADMIN_AUDITOR" in auth_user["userRoleList"], "User does not have auditor role"
        
        elif auth_role == "user":
            auth_user = self.user
            auth = self.ranger_user_config
            assert "ROLE_USER" in auth_user["userRoleList"], "User does not have user role"
        
        elif auth_role == "admin":
            auth = self.ranger_admin_config
           
        elif auth_role == "keyadmin":
            auth = self.ranger_key_admin_config

        payload = {
            "id": user_id,
            "name": user["name"],
            "firstName": "updatedfirstname",
            "lastName": "updatedlastname",
            "emailAddress": f"updated_{user['emailAddress']}",
            "status": 1,
            "isVisible": 1,
            "userRoleList": user_role ,
            "groupIdList": [],
            "groupNameList": []
        }

        response = requests.put(
            f"{self.base_url}/xusers/users",
            json=payload,
            auth=auth,
            headers=self.headers
        )
        if auth_role in ["auditor", "user"]:
            assert_response(response, 403, f"{auth_role} should not have permission to update users, but got {response.status_code}")
        elif auth_role in ["admin", "keyadmin"]:
            assert_response(response, 400, f"{auth_role} should not have permission to update {user_role} role, expected 400 but got {response.status_code}")
    


    @pytest.mark.negative
    @pytest.mark.put
    def test_update_user_using_invalid_data(self):  
        rand_id = random.randint(100000, 999999)
        payload = {
            "id": rand_id,
            "name": "name",
            "firstName": "updatedfirstname",
            "userRoleList": ["RANDOM_ROLE"]

        }
        response = requests.put(
            f"{self.base_url}/xusers/users",
            json=payload,
            auth=self.ranger_admin_config,
            headers=self.headers
        )
        print(response.status_code, response.text)
        assert_response(response, 403, "Operation is expected to be denied")


    @pytest.mark.negative
    @pytest.mark.delete
    def test_delete_user_using_invalid_id(self):

        #rand_id = random.randint(100000, 999999)
        rand_id = 89999
        
        response = requests.delete(
            f"{self.base_url}/xusers/users/{rand_id}",
            params={"forceDelete": "true"},
            auth=self.ranger_admin_config,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        assert_response(response, [400, 404], f"Expected status code not returned for invalid user ID, got {response.status_code}")
    
    @pytest.mark.negative
    @pytest.mark.delete
    @pytest.mark.parametrize("auth_role", ["auditor", "user", "keyadmin"])
    def test_delete_user_using_invalid_roles(self, auth_role, request):
        if auth_role == "auditor":
            auth_user = self.audit
            auth = self.ranger_auditor_config
            assert "ROLE_ADMIN_AUDITOR" in auth_user["userRoleList"], "User does not have auditor role"
        elif auth_role == "user":
            auth_user = self.user
            auth = self.ranger_user_config
            assert "ROLE_USER" in auth_user["userRoleList"], "User does not have user role"
        elif auth_role == "keyadmin":
            auth = self.ranger_key_admin_config 

        test_user, test_id = request.getfixturevalue("temp_secure_user")(["user"])
        

        response = requests.delete(
            f"{self.base_url}/xusers/users/{test_id}",
            params={"forceDelete": "true"},
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        assert_response(response, [403, 405], f"{auth_role} should not have permission to delete users")


    @pytest.mark.negative
    @pytest.mark.delete
    @pytest.mark.parametrize("auth_role", ["auditor", "user", "keyadmin"])
    def test_delete_external_user_with_invalid_role(self,request, auth_role):
        if auth_role == "auditor":
            auth_user = self.audit
            auth = self.ranger_auditor_config
            assert "ROLE_ADMIN_AUDITOR" in auth_user["userRoleList"], "User does not have auditor role"
        elif auth_role == "user":
            auth_user = self.user
            auth = self.ranger_user_config
            assert "ROLE_USER" in auth_user["userRoleList"], "User does not have user role"
        elif auth_role == "keyadmin":
            auth = self.ranger_key_admin_config 

        test_user, test_id = request.getfixturevalue("temp_secure_user")(["user"])
        
        userName = test_user["name"]
        response = requests.delete(
            f"{self.base_url}/xusers/users/userName/{userName}",
            params={"forceDelete": "true"},
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        assert_response(response, [403, 405], f"{auth_role} should not have permission to delete users")