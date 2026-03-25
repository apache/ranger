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
    init_configs
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

    @pytest.fixture(autouse=True)
    def _setup(self, request, default_headers):
        
        self.admin = request.getfixturevalue("temp_secure_user")(["admin"])[0]
        self.ranger_key_admin = request.getfixturevalue("temp_keyadmin_user")()[0]
        self.audit = request.getfixturevalue("temp_secure_user")(["auditor"])[0]
        self.user = request.getfixturevalue("temp_secure_user")(["user"])[0]


        self.ranger_admin_config = (self.admin["name"], "Test@123")
        self.ranger_key_admin_config = (self.ranger_key_admin["name"], "Test@123")
        self.ranger_auditor_config = (self.audit["name"], "Test@123")
        self.ranger_user_config = (self.user["name"], "Test@123")

        self.ranger_admin_id = self.admin["id"]
        self.ranger_key_admin_id = self.ranger_key_admin["id"]
        self.ranger_auditor_id = self.audit["id"]
        self.ranger_user_id = self.user["id"]

        self.admin1 = request.getfixturevalue("temp_secure_user")(["admin"])[0]
        self.ranger_key_admin1 = request.getfixturevalue("temp_keyadmin_user")()[0]
        self.audit1 = request.getfixturevalue("temp_secure_user")(["auditor"])[0]
        self.user1 = request.getfixturevalue("temp_secure_user")(["user"])[0]


        self.ranger_admin_config1 = (self.admin1["name"], "Test@123")
        self.ranger_key_admin_config1 = (self.ranger_key_admin1["name"], "Test@123")
        self.ranger_auditor_config1 = (self.audit1["name"], "Test@123")
        self.ranger_user_config1 = (self.user1["name"], "Test@123")

        self.ranger_admin_id1 = self.admin1["id"]
        self.ranger_key_admin_id1 = self.ranger_key_admin1["id"]
        self.ranger_auditor_id1 = self.audit1["id"]
        self.ranger_user_id1 = self.user1["id"]

        self.headers = default_headers
        self.base_url = request.getfixturevalue("ranger_config")["base_url"]

        init_configs(self.ranger_admin_config, self.ranger_key_admin_config, self.ranger_auditor_config, self.ranger_user_config)

        

    # Positive Test Cases
    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize("role", ["admin", "user", "auditor", "keyadmin"])
    def test_get_users(self, role, request):
        if role == "keyadmin":
            auth = self.ranger_key_admin_config
        elif role == "admin":
            auth = self.ranger_admin_config
        elif role == "auditor":
            auth = self.ranger_auditor_config
        elif role == "user":
            auth = self.ranger_user_config


        url = f"{self.base_url}/xusers/users"
        response = requests.get(
            url,
            auth=auth,
            headers=self.headers
        )

        assert response.status_code == 200, f"Failed to fetch users list: {response.status_code}"

        data = response.json()
        assert "vXUsers" in data, "Invalid users list schema"

        users = data["vXUsers"]
        assert isinstance(users, list)
        assert len(users) > 0, "No users found in Ranger"

    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize("role", ["admin", "user", "auditor", "keyadmin"])
    def test_get_user_count(self, role, request):
        if role == "keyadmin":
            auth = self.ranger_key_admin_config
        elif role == "admin":
            auth = self.ranger_admin_config
        elif role == "auditor":
            auth = self.ranger_auditor_config
        elif role == "user":
            auth = self.ranger_user_config
        

        url = f"{self.base_url}/xusers/users/count"
        response = requests.get(
            url,
            auth = auth,
            headers=self.headers
        )

        assert response.status_code == 200, f"Failed to fetch user count: {response.status_code}"

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
        
        assert response.status_code == 200, f"Failed to fetch user of role {user_role} using {auth_role}"

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
        
        assert response.status_code == 200, f"Failed to fetch user of role {user_role} using {auth_role}"

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
        finally:
            if user_id:
                delete_user(user_id,  self.ranger_admin_config, self.base_url, self.headers)
                print("status : ",verify_data["status"], "\n role list : ", verify_data["userRoleList"])

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
            if user_id:
                delete_user(user_id,  self.ranger_admin_config, self.base_url, self.headers)


    # @pytest.mark.positive
    # @pytest.mark.post
    # def test_create_roleassignment():
        
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
        assert response.status_code == 400, f"Expected status code not returned, got {response.status_code}"


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
        
        assert response.status_code == 403, f"{auth_role} should not have permission to access user of role {user_role}, but got {response.status_code}"


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
        
        assert response.status_code == 403, f"Unauthorized access should be denied for {auth_role} to access user of role {user_role}, but got {response.status_code}"

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
            assert response.status_code == 403, f"{username} should not have permission to create secure users"
    

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
        assert response.status_code == 403, f"{auth_role} should not have permission to create external users, but got {response.status_code}"

    

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
            assert response.status_code == 403, f"{auth_role} should not have permission to update secure users"
        elif auth_role in ["admin", "keyadmin"]:
            assert response.status_code == 400, f"{auth_role} should not have permission to update {user_role} role, expected 400 but got {response.status_code}"
    


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
        assert response.status_code == 403, f"Operation is expected to be denied but got {response.status_code}"


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
        assert response.status_code in [400, 404], f"Expected status code not returned, got {response.status_code}"
    
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
        assert response.status_code in [403, 405], f"{auth_role} should not have permission to delete users, but got {response.status_code}"


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
        assert response.status_code in [403, 405], f"{auth_role} should not have permission to delete users, but got {response.status_code}"