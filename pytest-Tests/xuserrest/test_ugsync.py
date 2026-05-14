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


from urllib import *
from wsgiref import headers
from xuserrest.utility.utils import *
import uuid
import string
import pytest
import requests
from datetime import datetime
import random

@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestUgsync:
    SERVICE_NAME = "admin"

    @pytest.fixture(autouse=True, scope="class")
    def _setup(
        self,
        request,
        temp_secure_user,
        temp_keyadmin_user,
        temp_group,
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


        # ---------- group details ----------
        cls.group, cls.group_id = temp_group()
        cls.group1, cls.group_id1 = temp_group()

        # ---------- common ----------
        cls.headers = default_headers
        cls.base_url = ranger_config["base_url"]

        init_configs(
            cls.ranger_admin_config,
            cls.ranger_key_admin_config,
            cls.ranger_auditor_config,
            cls.ranger_user_config,
        )

    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize("role, auth", [
        ("admin", "ranger_admin_config")
    ])
    def test_get_ugsync_groupusers(self, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not expected to have access to get group users list")
        
        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/xusers/ugsync/groupusers",
            auth = auth,
            headers = self.headers
        )
        assert_response(response, 200, f"Expected status code 200, got {response.status_code}")
        data = response.json()
        if data:
            assert isinstance(data, dict), f"Expected response to be a dict, got {type(data)}"

            first_key = next(iter(data))
            assert isinstance(first_key, str) and len(first_key) <= 767, "Expected first key to be non-empty"

            first_value = data[first_key]
            assert isinstance(first_value, list), f"Expected first value to be a list, got {type(first_value)}"

            if first_value:
                assert isinstance(first_value[0], str) and len(first_value[0]) <= 767, f"Expected elements of the list to be strings, got {type(first_value[0])}"


    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("role, auth", [
        ("admin", "ranger_admin_config")
    ])
    def test_create_ugsync_groupusers(self, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not expected to have access to create group users list")
        auth = getattr(self, auth)

        flg,_ = group_exists(self.group1["id"], auth, self.base_url, self.headers)
        assert flg, f"Group with name {self.group1['name']} does not exist which is not expected for test case: {role}"

        # assign group1 to user1
        assign_groups_to_user(self.user1["name"], [self.group1["name"]], auth, self.base_url, self.headers)
        data = fetch_groups_for_user_id(self.user1["id"], self.ranger_admin_config, self.base_url, self.headers)
        assert check_group_in_user_groups(self.group1['id'], data), f"Group with id {self.group1['id']} is not assigned to user with id {self.user['id']} which is not expected for test case: {role}"
        
        payload = [
            {
                "groupName": self.group1["name"],
                "addUsers": [self.user["name"]],
                "delUsers": [self.user1["name"]]
            }
        ]

        return_value = return_value_ugsync_groupusers(payload, auth, self.base_url, self.headers)

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/groupusers",
            auth = auth,
            json = payload,
            headers = self.headers
        )
        assert_response(response, 200, f"Expected status code 200, got {response.status_code}")
        
        data = response.json()
        assert data == return_value, f"Expected response to be {return_value}, got {data}"

        data = fetch_groups_for_user_id(self.user["id"], self.ranger_admin_config, self.base_url, self.headers)
        assert check_group_in_user_groups(self.group1['id'], data), \
            f"Group with id {self.group1['id']} is not assigned to user with id {self.user['id']} which is not expected for test case: {role}"

        data = fetch_groups_for_user_id(self.user1["id"], self.ranger_admin_config, self.base_url, self.headers)
        assert not check_group_in_user_groups(self.group1['id'], data), \
            f"Group with id {self.group1['id']} is still assigned to user1 with id {self.user1['id']} which is not expected for test case: {role}"
    


    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("role, auth, test_case", [
        ("admin", "ranger_admin_config", "ugsync via ldap"),
        ("admin", "ranger_admin_config", "ugsync via unix"),
        ("admin", "ranger_admin_config", "ugsync via file"),
        ("admin", "ranger_admin_config", "ugsync via multi sources")

    ])
    def test_create_ugsync_auditinfo(self, role, auth, test_case):
        if role != "admin":
            pytest.fail(f"Role {role} is not expected to have access to create audit info")
        auth = getattr(self, auth)


        payload = {
            "syncSource":  "",
            "noOfNewUsers": 10,
            "noOfNewGroups": 5,
            "noOfModifiedUsers": 3,
            "noOfModifiedGroups": 2,
        }

        if test_case == "ugsync via ldap":
            payload["syncSource"] = "LDAP"
            payload["ldapSyncSourceInfo"] = {}
        elif test_case == "ugsync via unix":
            payload["syncSource"] = "UNIX"
            payload["unixSyncSourceInfo"] = {}
        elif test_case == "ugsync via file":
            payload["syncSource"] = "FILE"
            payload["fileSyncSourceInfo"] = {}
        elif test_case == "ugsync via multi sources":
            payload["syncSource"] = "MULTI_SOURCES"
            payload["ldapSyncSourceInfo"] = {}
            payload["unixSyncSourceInfo"] = {}
            payload["fileSyncSourceInfo"] = {}

        validate_auditinfo_schema(payload)

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/auditinfo",
            auth = auth,
            json = payload,
            headers = self.headers
        )

        assert_response(response, 200, f"Expected status code 200, got {response.status_code}")
        data = response.json()
        validate_auditinfo_schema(data)
        validate_sync_source_info(payload, data)


    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("role, auth, test_case", [
        ("admin", "ranger_admin_config","existing groups in payload"),
        ("admin", "ranger_admin_config","new groups in payload")
    ])
    def test_create_ugsync_group(self, role, auth, test_case):
        if role != "admin":
            pytest.fail(f"Role {role} is not expected to have access to create group users list")
        
        auth = getattr(self, auth)
        
        payload = {
            "vXGroups":[
                {
                    "name": "testgroupugsync"+str(random.randint(1000,9999)),
                    "description": "test description for group created via ugsync"
                }
            ]
        }

        if test_case == "existing groups in payload":
            payload["vXGroups"][0]["name"] = self.group1["name"]
            flg, resp = group_exists(self.group1["id"], auth, self.base_url, self.headers)
    
            assert flg, f"Group with name {self.group1['name']} does not exist..."
            cur_description = resp.get("description") 
            assert cur_description != payload["vXGroups"][0]["description"], f"Group description is already same as payload for existing group in payload which is not expected for test case: {test_case}"


        for i in payload["vXGroups"]:
            validate_xgroup_schema(i) # Removed the trailing comma issue

        try:
            response = requests.post(
                f"{self.base_url}/xusers/ugsync/groups",
                auth = auth,
                json = payload,
                headers = self.headers
            )
    
            assert_response(response, 200, f"Expected status code 200, got {response.status_code}")
            data = response.json()
            assert data == len(payload["vXGroups"]), f"Expected response to be {len(payload['vXGroups'])}, got {data}"
    
            for group in payload["vXGroups"]:
                group_name = group["name"]
                
                resp = requests.get(
                    f"{self.base_url}/xusers/groups/groupName/{group_name}",
                    auth=auth,
                    headers=self.headers
                )
                assert resp.status_code == 200, f"Group '{group_name}' missing after sync"

                if test_case == "existing groups in payload":
                    synced_group = resp.json()
                    assert synced_group.get("description") == group["description"], "Group description failed to update during sync!"

        finally:
            # Cleanup only the random groups we created. 
            if test_case == "new groups in payload":
                for group in payload["vXGroups"]:
                    resp = requests.get(
                        f"{self.base_url}/xusers/groups/groupName/{group['name']}",
                        auth=self.ranger_admin_config,
                        headers=self.headers
                    )
                    if resp.status_code == 200:
                        delete_group(resp.json().get("id"), self.ranger_admin_config, self.base_url, self.headers)

    
    
    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("role, auth", [
        ("admin", "ranger_admin_config")
        ])
    def test_create_ugsync_group_visibility(self, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not expected to have access to create group users list")
        
        auth = getattr(self, auth)

        payload = [self.group1["name"]]
        
        return_value = len(set(payload))

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/groups/visibility",
            auth = auth,
            json = payload,
            headers = self.headers
        )

        assert_response(response, 200, f"Expected status code 200, got {response.status_code}")
        data = response.json()
        assert data == return_value, f"Expected response to be {return_value}, got {data}"
        
        flg, resp = group_exists(self.group1["id"], auth, self.base_url, self.headers)
        assert flg and resp.get("isVisible") == 0, f"Group visibility is not updated to 0 as expected for test case: {role}"


    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("role, auth, testcase", [
        ("admin", "ranger_admin_config", "single user in payload"),
        ("admin", "ranger_admin_config", "multiple users in payload"),
        ("admin", "ranger_admin_config", "existing user in payload")
        ])
    def test_create_ugsync_users(self, role, auth, testcase):
        if role != "admin":
            pytest.fail(f"Role {role} is not expected to have access to create group users list")
        
        auth = getattr(self, auth)

        payload = {
            "vXUsers":[
                {
                    "name": "testuserugsync"+str(random.randint(1000,9999)),
                    "firstName": "test",
                    "userRoleList": ["ROLE_USER"],
                }
            ]
        }

        if testcase in ["multiple users in payload", "existing user in payload"]:  
            payload["vXUsers"].append(
                payload["vXUsers"][0].copy()
            )

        return_value = 0
        for i in payload["vXUsers"]:
            assert mandatory_field_check_in_response(i, ["name", "firstName", "userRoleList"]), f"Mandatory fields missing in payload user {i} for test case: {role}"
            validate_user_schema(i) 
            return_value += 1 

        try:
            response = requests.post(
                f"{self.base_url}/xusers/ugsync/users",
                auth = auth,
                json = payload,
                headers = self.headers
            )

            assert_response(response, 200, f"Expected status code 200, got {response.status_code}")
            data = response.json()
            assert data == return_value, f"Expected response to be {return_value}, got {data}"

        finally:
            deleted_users = set()
            for i in payload["vXUsers"]:
                username = i['name']
                
                if username in deleted_users:
                    continue

                resp = requests.get(
                    f"{self.base_url}/xusers/users/userName/{username}",
                    auth = self.ranger_admin_config,
                    headers = self.headers,
                )
                
                if resp.status_code == 200:
                    delete_user(resp.json().get("id"), self.ranger_admin_config, self.base_url, self.headers)
                    deleted_users.add(username)
    
    
    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("role, auth", [
        ("admin", "ranger_admin_config")
        ])
    def test_create_ugsync_users_visibility(self, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not expected to have access to create group users list")
        
        auth = getattr(self, auth)

        payload = [self.user["name"]]
        
        return_value = len(set(payload))

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/users/visibility",
            auth = auth,
            json = payload,
            headers = self.headers
        )

        assert_response(response, 200, f"Expected status code 200, got {response.status_code}")
        data = response.json()
        assert data == return_value, f"Expected response to be {return_value}, got {data}"
        
        resp = requests.get(
            f"{self.base_url}/xusers/users/userName/{self.user['name']}",
            auth=auth,
            headers=self.headers
        )
        data = resp.json()
        assert resp.status_code == 200 and data.get("isVisible") == 0, f"User visibility is not updated to 0 as expected for test case: {role}"



    # NEGATIVE TESTS

    @pytest.mark.negative
    @pytest.mark.get
    @pytest.mark.parametrize("role, auth", [
        ("key admin", "ranger_key_admin_config"),
        ("auditor", "ranger_auditor_config"),
        ("user", "ranger_user_config")
    ])
    def test_get_ugsync_groupusers_unauthorized(self, role, auth):
        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/xusers/ugsync/groupusers",
            auth = auth,
            headers = self.headers
        )
        assert_response(response, 404, f"Expected status code 404 for role {role} due to spring's silent failure, got {response.status_code}")

    
    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("auth, testcase", [
        ("ranger_key_admin_config", "unauthorized access by key admin"),
        ("ranger_auditor_config", "unauthorized access by auditor"),
        ("ranger_user_config", "unauthorized access by user"),
        ("ranger_admin_config", "invalid group name in payload"),
        ("ranger_admin_config", "invalid user name in addUser payload"),
        ("ranger_admin_config", "invalid user name in delUser payload"),
    ])
    def test_create_ugsync_groupusers_negative(self, auth, testcase):
        auth = getattr(self, auth)
        invalid_name = "invalid_user_name"+str(random.randint(1000,9999))
        
        payload = [
                {
                    "groupName": self.group1["name"],
                    "addUsers": [self.user["name"]],
                    "delUsers": []
                }
            ]
        
        if testcase == "invalid group name in payload":
            payload[0]["groupName"] = invalid_name
            response_code = 200
            assert not group_exists_by_name(invalid_name, self.ranger_admin_config, self.base_url, self.headers)  


        elif testcase == "invalid user name in addUser payload":
            payload[0]["addUsers"] = [invalid_name]
            response_code = 200 # silent failure
            assert not user_exists_by_name(invalid_name, self.ranger_admin_config, self.base_url, self.headers) 

        elif testcase == "invalid user name in delUser payload":
            payload[0]["delUsers"] = [invalid_name]
            response_code = 200 # silent failure

        else:
            response_code = 404 # spring silent failure for unauthorized access to this API is returning 404 instead of 403 which is not expected but we are asserting based on actual response due to this reason

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/groupusers",
            auth = auth,
            json = payload,
            headers = self.headers
        )

        assert_response(response, response_code, f"Expected status code {response_code} for test case: {testcase}, got {response.status_code}")
        if testcase.startswith("unauthorized access"):
            return
        
        response_data = return_value_ugsync_groupusers(payload, self.ranger_admin_config, self.base_url, self.headers)
        data = response.json()
        assert data == response_data, f"Expected response to be {response_data} for test case: {testcase}, got {data}"            


    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("auth, testcase", [
        ("ranger_key_admin_config", "unauthorized access by key admin"),
        ("ranger_auditor_config", "unauthorized access by auditor"),
        ("ranger_user_config", "unauthorized access by user"),
        ("ranger_admin_config", "missing fields in payload")
    ])
    def test_create_ugsync_auditinfo_unauthorized(self, auth, testcase):
        auth = getattr(self, auth)

        payload = {
            "syncSource":  "LDAP",
            "ldapSyncSourceInfo": {},
            "noOfNewUsers": 10,
            "noOfNewGroups": 5,
            "noOfModifiedUsers": 3,
            "noOfModifiedGroups": 2,
        }

        if testcase == "missing fields in payload":
            mandatory_fields = ["syncSource", "noOfNewUsers", "noOfNewGroups", "noOfModifiedUsers", "noOfModifiedGroups"]
            if "ldapSyncSourceInfo" in payload:
                mandatory_fields.append("ldapSyncSourceInfo")
            if "unixSyncSourceInfo" in payload:
                mandatory_fields.append("unixSyncSourceInfo")
            if "fileSyncSourceInfo" in payload:
                mandatory_fields.append("fileSyncSourceInfo")
            #field_to_remove = random.choice(mandatory_fields)
            field_to_remove = "syncSource"
            print(f"Removing field {field_to_remove} from payload for test case: {testcase}")
            del payload[field_to_remove]
            expected_status = 404 
        else:
            expected_status = 404 # spring silent failure for unauthorized access to this API is returning 404 instead of 403 which is not expected but we are asserting based on actual response due to this reason               

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/auditinfo",
            auth = auth,
            json = payload,
            headers = self.headers
        )

        assert_response(response, expected_status, f"Expected status code {expected_status} for test case: {testcase}, got {response.status_code}")


    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("auth, testcase", [
        ("ranger_key_admin_config", "unauthorized access by key admin"),
        ("ranger_auditor_config", "unauthorized access by auditor"),
        ("ranger_user_config", "unauthorized access by user"),
        ("ranger_admin_config", "invalid payload in Vxgroups"),
        ("ranger_admin_config", "invalid group name in payload")
    ])
    def test_create_ugsync_group_negative(self, auth, testcase):
        auth = getattr(self, auth)

        payload = {
            "vXGroups":[
                {
                    "name": "testgroupugsync"+str(random.randint(1000,9999)),
                    "description": "test description for group created via ugsync"
                }
            ]
        }

        if testcase == "invalid payload in Vxgroups":
            payload["vXGroups"][0]["isVisible"] = "invalidValue_expected bool"
            expected_status = 400
        elif testcase == "invalid group name in payload":
            payload["vXGroups"][0]["name"] = ""
            expected_status = 200 # silent failure but group should not be created
        else:
            expected_status = 404 # spring silent failure for unauthorized access to this API is returning 404 instead of 403 which is not expected but we are asserting based on actual response due to this reason

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/groups",
            auth = auth,
            json = payload,
            headers = self.headers
        )

        assert_response(response, expected_status, f"Expected status code {expected_status} for test case: {testcase}, got {response.status_code}")


    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("auth, testcase", [
        ("ranger_key_admin_config", "unauthorized access by key admin"),
        ("ranger_auditor_config", "unauthorized access by auditor"),
        ("ranger_user_config", "unauthorized access by user"),
        ("ranger_admin_config", "invalid group name in payload - repetition in list"),
        ("ranger_admin_config", "invalid group name in payload - non existing group"),
    ])
    def test_create_ugsync_group_visibility_negative(self, auth, testcase):
        auth = getattr(self, auth)

        payload = [self.group1["name"]]

        if testcase == "invalid group name in payload - repetition in list":
            payload.append(self.group1["name"])
            expected_status = 200 # silent failure but visibility should be updated for the group
        elif testcase == "invalid group name in payload - non existing group":
            payload[0] = "non_existing_group"+str(random.randint(1000,9999))
            expected_status = 200 # silent failure but no group should be created
        else:
            expected_status = 404 # spring silent failure for unauthorized access to this API is returning 404 instead of 403 which is not expected but we are asserting based on actual response due to this reason

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/groups/visibility",
            auth = auth,
            json = payload,
            headers = self.headers
        )

        assert_response(response, expected_status, f"Expected status code {expected_status} for test case: {testcase}, got {response.status_code}")
        
        if testcase.startswith("unauthorized access"):
            return
        return_value = len(set(payload))
        assert return_value == response.json(), f"Expected response to be {return_value} for test case: {testcase}, got {response.json()}"

    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("auth, testcase", [
        ("ranger_key_admin_config", "unauthorized access by key admin"),
        ("ranger_auditor_config", "unauthorized access by auditor"),
        ("ranger_user_config", "unauthorized access by user"),
        ("ranger_admin_config", "invalid payload in Vxusers - missing mandatory field"),
        ("ranger_admin_config", "invalid payload in Vxusers - missing mandatory field - userRoleList"),
        ("ranger_admin_config", "invalid payload in Vxusers - keyadmin role in userRoleList"),
        ("ranger_admin_config", "invalid payload in Vxusers - empty user name"),
        ("ranger_admin_config", "invalid payload in Vxusers - empty first name"),
        ("ranger_admin_config", "semi failure - invalid and valid users in payload")
    ])
    def test_create_ugsync_users_negative(self, auth, testcase):
        auth = getattr(self, auth)

        payload = {
            "vXUsers":[
                {
                    "name": "testuserugsync"+str(random.randint(1000,9999)),
                    "firstName": "test",
                    "userRoleList": ["ROLE_USER"],
                }
            ]
        }

        return_value = 0

        if testcase == "invalid payload in Vxusers - missing mandatory field":
            del payload["vXUsers"][0]["firstName"]
            expected_status = 200 # silent failure but user should not be created
            return_value -=1
        
        elif testcase == "invalid payload in Vxusers - missing mandatory field - userRoleList":
            del payload["vXUsers"][0]["userRoleList"]
            expected_status = 403 # userRoleList is mandatory field for user creation via ugsync and should throw 403 if missing in payload

        elif testcase == "invalid payload in Vxusers - keyadmin role in userRoleList":
            payload["vXUsers"][0]["userRoleList"] = ["ROLE_KEY_ADMIN"]
            expected_status = 403 # cant assign key admin role to user via ugsync

        elif testcase == "invalid payload in Vxusers - empty user name":
            payload["vXUsers"][0]["name"] = ""
            expected_status = 200 # silent failure but user should not be created
            return_value -=1

        elif testcase == "invalid payload in Vxusers - empty first name":
            payload["vXUsers"][0]["firstName"] = ""
            expected_status = 200 # silent failure but user should not be created
            return_value -=1

        elif testcase == "semi failure - invalid and valid users in payload":
            payload = {
                "vXUsers":[
                    {
                        "name": "testuserugsync"+str(random.randint(1000,9999)),
                        "firstName": "test",
                        "userRoleList": ["ROLE_USER"],
                    },
                    {
                        "name": "", # invalid user with empty name
                        "firstName": "test",
                        "userRoleList": ["ROLE_USER"],
                    }
                ]
            }
            expected_status = 200 # silent failure but user should not be created
            return_value -= 1 # only the valid user in payload should be created

        else:
            expected_status = 404 # spring silent failure for unauthorized access to this API is returning 404 instead of 403 which is not expected but we are asserting based on actual response due to this reason

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/users",
            auth = auth,
            json = payload,
            headers = self.headers
        )

        assert_response(response, expected_status, f"Expected status code {expected_status} for test case: {testcase}, got {response.status_code}")

        if testcase.startswith("unauthorized access") or testcase.endswith("userRoleList"):
            return
        
        return_value += len(payload["vXUsers"])

        assert response.json() == return_value , f"Expected response to be {return_value} for test case: {testcase}, got {response.json()}"

    
    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize("auth, testcase", [
        ("ranger_key_admin_config", "unauthorized access by key admin"),
        ("ranger_auditor_config", "unauthorized access by auditor"),
        ("ranger_user_config", "unauthorized access by user"),
        ("ranger_admin_config", "invalid user name in payload - repetition in list"),
        ("ranger_admin_config", "invalid user name in payload - non existing user"),
    ])
    def test_create_ugsync_users_visibility_negative(self, auth, testcase):
        auth = getattr(self, auth)

        payload = [self.group1["name"]]

        if testcase == "invalid user name in payload - repetition in list":
            payload.append(self.group1["name"])
            expected_status = 200 # silent failure but visibility should be updated for the user if it exists
        elif testcase == "invalid user name in payload - non existing user":
            payload[0] = "non_existing_user"+str(random.randint(1000,9999))
            expected_status = 200 # silent failure but no user should be created
        else:
            expected_status = 404 # spring silent failure for unauthorized access to this API is returning 404 instead of 403 which is not expected but we are asserting based on actual response due to this reason

        response = requests.post(
            f"{self.base_url}/xusers/ugsync/users/visibility",
            auth = auth,
            json = payload,
            headers = self.headers
        )

        assert_response(response, expected_status, f"Expected status code {expected_status} for test case: {testcase}, got {response.status_code}")
        
        if testcase.startswith("unauthorized access"):
            return
        return_value = len(set(payload))
        assert return_value == response.json(), f"Expected response to be {return_value} for test case: {testcase}, got {response.json()}"
