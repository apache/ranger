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

from utility.utils import *
import uuid
import pytest
import requests
from datetime import datetime
#from utility.utils import fetch_logs
import random

@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestPermissions:

    SERVICE_NAME = "admin"


    
    @pytest.fixture(autouse=True, scope="class")
    def _setup(
        self,
        request,
        temp_secure_user,
        temp_keyadmin_user,
        temp_permission_module,
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


        # ---------- permissions setup ----------
        cls.permission_module, cls.permission_module_id = temp_permission_module()
        cls.permission_module1, cls.permission_module_id1 = temp_permission_module()

        cls.permission_module_name = cls.permission_module["module"]
        cls.permission_module_name1 = cls.permission_module1["module"]

        # ---------- common ----------
        cls.headers = default_headers
        cls.base_url = ranger_config["base_url"]

        init_configs(
            cls.ranger_admin_config,
            cls.ranger_key_admin_config,
            cls.ranger_auditor_config,
            cls.ranger_user_config,
        )


    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "params, test_case",
        [
            ({}, "default_request"),
            ({"startIndex": 0, "pageSize": 10}, "pagination_basic"),
            ({"startIndex": 5, "pageSize": 5}, "pagination_offset"),
            ({"module": "admin"}, "filter_module"),
            ({"userName": "admin"}, "filter_user"),
            ({"groupName": "public"}, "filter_group"),
            ({"module": "admin", "userName": "admin"}, "filter_combined"),
            ({"sortBy": "id", "sortType": "asc"}, "sorting_asc"),
            ({"sortBy": "id", "sortType": "desc"}, "sorting_desc"),
            ({"getCount": "false"}, "get_count_false"),
        ],
        ids=[
            "default",
            "pagination-basic",
            "pagination-offset",
            "filter-module",
            "filter-user",
            "filter-group",
            "filter-combined",
            "sorting-asc",
            "sorting-desc",
            "getcount-false",
        ],
    )
    @pytest.mark.parametrize(
    "role, auth",
    [
        ("admin", "ranger_admin_config"),
        ("auditor", "ranger_auditor_config"),
    ],
    ids=["admin", "auditor"],
    )
    def test_get_permission(self, params, test_case, role, auth):
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permission",
            params=params,
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, f"Failed for case: {test_case}")
        
        data = response.json()
    
        assert isinstance(data, dict), f"Invalid response format in {test_case}"

        assert "vXModuleDef" in data, f"'vXModuleDef' not found in {test_case}"
        assert isinstance(data["vXModuleDef"], list)

        modules = data["vXModuleDef"]

        if not modules:
            assert data.get("totalCount", 0) == 0
            assert data.get("resultSize", 0) == 0
            return
        
        module = modules[0]
        

        if module["userPermList"]:
            assert "id" in module["userPermList"][0]
        # group permissions (optional — API dependent)
        if "groupPermList" in module:
            assert isinstance(module["groupPermList"], list)

        if "resultSize" in data:
            assert isinstance(data["resultSize"], int)

        if "listSize" in data:
            assert isinstance(data["listSize"], int)



    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "params, test_case",
        [
            ({}, "default_request"),
            ({"startIndex": 0, "pageSize": 10}, "pagination_basic"),
            ({"startIndex": 5, "pageSize": 5}, "pagination_offset"),
            ({"module": "admin"}, "filter_module"),
            ({"userName": "admin"}, "filter_user"),
            ({"groupName": "public"}, "filter_group"),
            ({"module": "admin", "userName": "admin"}, "filter_combined"),
            ({"sortBy": "id", "sortType": "asc"}, "sorting_asc"),
            ({"sortBy": "id", "sortType": "desc"}, "sorting_desc"),
            ({"getCount": "false"}, "get_count_false"),
        ],
        ids=[
            "default",
            "pagination-basic",
            "pagination-offset",
            "filter-module",
            "filter-user",
            "filter-group",
            "filter-combined",
            "sorting-asc",
            "sorting-desc",
            "getcount-false",
        ],
    )
    @pytest.mark.parametrize(
    "role, auth",
    [
        ("admin", "ranger_admin_config"),
        ("auditor", "ranger_auditor_config"),
    ],
    ids=["admin", "auditor"],
    )
    def test_get_permissionlist(self, params, test_case, role, auth):
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permissionlist",
            params=params,
            auth=auth,
            headers=self.headers
        )

        
        assert_response(response, 200, f"Failed for case: {test_case}")
        
        data = response.json()
    
        assert isinstance(data, dict), f"Invalid response format in {test_case}"


        assert "vXModulePermissionList" in data, f"'vXModulePermissionList' not found in {test_case}"
        assert isinstance(data["vXModulePermissionList"], list)

        modules = data["vXModulePermissionList"]

        if not modules:
            assert data.get("totalCount", 0) == 0
            assert data.get("resultSize", 0) == 0
            return
        
        assert "userNameList" in modules[0], f"'userNameList' not found in {test_case}"
        assert isinstance(modules[0]["userNameList"], list)
        
        if len(modules[0]["userNameList"]) > 0:
            assert isinstance(modules[0]["userNameList"][0], str)
        if "groupPermList" in modules[0]:
            assert isinstance(modules[0]["groupPermList"], list)

        if "resultSize" in data:
            assert isinstance(data["resultSize"], int)

        if "listSize" in data:
            assert isinstance(data["listSize"], int)


    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
    "role, auth",
    [
        ("admin", "ranger_admin_config"),
        ("auditor", "ranger_auditor_config")
    ],
    ids=["admin", "auditor"],
    )
    def test_get_permission_count(self, role, auth):
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permission/count",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200)
        
        data = response.json()

        assert isinstance(data["value"], int), "Invalid response format"

    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
    "role, auth",
    [
        ("admin", "ranger_admin_config"),
        ("auditor", "ranger_auditor_config")
    ],
    ids=["admin", "auditor"],
    )
    def test_get_permission_by_id(self, role, auth):
        auth = getattr(self, auth)

        permission_id = 1 # Assuming ID 1 exists, ideally we should create a permission first and then fetch by that ID to ensure test reliability

        # Now fetch permission by ID
        response = requests.get(
            f"{self.base_url}/xusers/permission/{permission_id}",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 200, "Failed to fetch permission by ID")
    
    @pytest.mark.post
    @pytest.mark.positive
    @pytest.mark.parametrize(
    "role, auth",
    [
        ("admin", "ranger_admin_config"),
    ],
    ids=["admin"],
    )
    def test_create_permissions(self, role, auth):
        auth = getattr(self, auth)

        module_name = f"pytest_permission_{uuid.uuid4().hex[:8]}"
        payload = {
            "module": module_name,
        }

        if permission_module_exists(module_name, self.ranger_admin_config, self.base_url, self.headers):
            assert False, f"Module name {module_name} should not exist so, cannot assign permissions for existing module"

        # Fetch existing permissions to get a valid module and permission ID
        response = requests.post(
            f"{self.base_url}/xusers/permission",
            json=payload,
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 200, "Failed to fetch permissions for assignment")

    @pytest.mark.put
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("admin", "ranger_admin_config"),
        ],
        ids=["admin"],
    )
    @pytest.mark.parametrize(
        "test_case",
        [
            "existing_user_different_data",
            "existing_user_same_data",
            "new_user_permission",
            "existing_group_different_data",
            "existing_group_same_data",
            "new_group_permission",
            "user_and_group_update_together",
            "empty_permission_lists",
        ]
    )
    def test_update_permissions(self, role, auth, test_case):

        auth_config = getattr(self, auth)
        perm_id = self.permission_module_id

        resp = requests.get(
            f"{self.base_url}/xusers/groups",
            auth=auth_config,
            headers=self.headers
        )
        assert_response(resp, 200, f"Unable to fetch groups: {resp.text}")

        groups = resp.json().get("vXGroups", [])
        assert groups, "No groups found in the system to perform the test."

        existing_user_id = self.ranger_user_id1
        new_user_id = self.ranger_user_id
        existing_group_id = groups[0]["id"]
        new_group_id = groups[1]["id"] if len(groups) > 1 else groups[0]["id"]

        test_data_map = {
            "existing_user_different_data": {
                "userPermList": [build_user_permission(existing_user_id, perm_id, 0)],
                "groupPermList": []
            },
            "existing_user_same_data": {
                "userPermList": [build_user_permission(existing_user_id, perm_id, 1)],
                "groupPermList": []
            },
            "new_user_permission": {
                "userPermList": [build_user_permission(new_user_id, perm_id, 1)],
                "groupPermList": []
            },
            "existing_group_different_data": {
                "userPermList": [],
                "groupPermList": [build_group_permission(existing_group_id, perm_id, 0)]
            },
            "existing_group_same_data": {
                "userPermList": [],
                "groupPermList": [build_group_permission(existing_group_id, perm_id, 1)]
            },
            "new_group_permission": {
                "userPermList": [],
                "groupPermList": [build_group_permission(new_group_id, perm_id, 1)]
            },
            "user_and_group_update_together": {
                "userPermList": [build_user_permission(existing_user_id, perm_id, 0)],
                "groupPermList": [build_group_permission(existing_group_id, perm_id, 0)]
            },
            "empty_permission_lists": {
                "userPermList": [],
                "groupPermList": []
            }
        }

        payload = build_permission_payload(
            module_id=perm_id,
            module_name=self.permission_module_name,
            user_perm_list=test_data_map[test_case]["userPermList"],
            group_perm_list=test_data_map[test_case]["groupPermList"]
        )

        response = requests.put(
            f"{self.base_url}/xusers/permission/{perm_id}",
            json=payload,
            auth=auth_config,
            headers=self.headers
        )

        assert_response(
            response,
            200,
            f"Permission update failed for {test_case}: {response.text}"
        )

        data = response.json()
        assert data["id"] == perm_id
        assert data["module"] == self.permission_module_name

        if payload["userPermList"]:
            assert "userPermList" in data
            assert isinstance(data["userPermList"], list)

        if payload["groupPermList"]:
            assert "groupPermList" in data
            assert isinstance(data["groupPermList"], list)


    @pytest.mark.delete
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("admin", "ranger_admin_config"),
        ],
        ids=["admin"],)
    def test_delete_permissions(self, role, auth):

        auth_config = getattr(self, auth)
        # First create a permission to ensure we have a valid ID to delete
        module_name = f"pytest_permission_delete_{uuid.uuid4().hex[:8]}"
        payload = {
            "module": module_name,
        }   
        response = requests.post(
            f"{self.base_url}/xusers/permission",
            json=payload,
            auth=auth_config,
            headers=self.headers
        )
        assert_response(response, 200, "Failed to create permission for deletion test")
        perm_id = response.json().get("id")
        assert perm_id, "No permission ID returned after creation, cannot proceed with deletion test"

        response = requests.delete(
            f"{self.base_url}/xusers/permission/{perm_id}",
            auth=auth_config,
            headers=self.headers
        )

        assert_response(
            response,
            204,
            f"Permission deletion failed: {response.text}"
        )
    

    # NEGATIVE TESTS

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("user", "ranger_user_config"),
            ("key_admin", "ranger_key_admin_config"),
        ],
        ids=["user", "key_admin"],
    )
    def test_get_permission_with_invalid_role(self, role, auth):
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permission",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 403, f"{role} should not have permission to view permissions, but got {response.status_code}")

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("user", "ranger_user_config"),
            ("key_admin", "ranger_key_admin_config"),
        ],
        ids=["user", "key_admin"],
    )
    def test_get_permissionlist_with_invalid_role(self, role, auth):
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permissionlist",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 403, f"{role} should not have permission to view permission list, but got {response.status_code}")


    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
    "role, auth",
    [
        ("keyadmin", "ranger_key_admin_config"),
        ("user", "ranger_user_config"),
    ],
    ids=["keyadmin", "user"],
    )
    def test_get_permission_count_with_invalid_role(self, role, auth):
        auth = getattr(self, auth)
       
        response = requests.get(
            f"{self.base_url}/xusers/permission/count",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 403, f"{role} should not have permission to view permission count, but got {response.status_code}")

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case",
        [
            "invalid_id",
            ("keyadmin", "ranger_key_admin_config"),
            ("user", "ranger_user_config"),
        ]
    )
    def test_invalid_get_permission_by_id(self, test_case):

        if test_case == "invalid_id":
            permission_id = -93402  # Assuming this ID does not exist
            auth = self.ranger_admin_config
            expected_status = 404
        
        else:
            #permission_id = 1 # Assuming ID 1 exists, ideally we should create a permission first and then fetch by that ID to ensure test reliability
            permission_id = self.permission_module_id
            auth = getattr(self, test_case[1])  
            expected_status = 403
        
        response = requests.get(
            f"{self.base_url}/xusers/permission/{permission_id}",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, expected_status, "Failed to fetch permission by ID")



    @pytest.mark.post
    @pytest.mark.negative
    @pytest.mark.parametrize(
    "role, auth",
    [
        ("auditor", "ranger_auditor_config"),
        ("keyadmin", "ranger_key_admin_config"),
        ("user", "ranger_user_config"),
    ],
    ids=["auditor", "keyadmin", "user"],
    )
    def test_create_permissions_with_invalid_role(self, role, auth):
        auth = getattr(self, auth)

        module_name = f"pytest_permission_{uuid.uuid4().hex[:8]}"
        payload = {
            "module": module_name,
        }

        if permission_module_exists(module_name, self.ranger_admin_config, self.base_url, self.headers):
            assert False, f"Module name {module_name} should not exist so, cannot assign permissions for existing module"

        response = requests.post(
            f"{self.base_url}/xusers/permission",
            json=payload,
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 403, f"{role} should not have permission to assign permissions, but got {response.status_code}")

    @pytest.mark.post
    @pytest.mark.negative
    def test_create_permissions_with_nonexistent_module(self):

        auth = self.ranger_admin_config
        module_name = "Tag Based Policies" # using an existing default module assuming we did not delete it

        if not permission_module_exists(module_name, self.ranger_admin_config, self.base_url, self.headers):
            assert False, f"Module name {module_name} should exist for this test to be valid"

        payload = {
            "module": module_name,
        }

        response = requests.post(
            f"{self.base_url}/xusers/permission",
            json=payload,
            auth=auth,
            headers=self.headers
        )
       

        assert_response(response, 400, f"Should not be able to assign permissions for nonexistent module, but got {response.status_code}")


    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("keyadmin", "ranger_key_admin_config"),
            ("user", "ranger_user_config"),
        ],
        ids=["keyadmin", "user"],
    )
    
    def test_update_permissions_using_invalid_role(self, role, auth):

        auth_config = getattr(self, auth)
        perm_id = self.permission_module_id

        payload = build_permission_payload(
            module_id=perm_id,
            module_name=self.permission_module_name,
            user_perm_list=[],
            group_perm_list=[]
        )

        response = requests.put(
            f"{self.base_url}/xusers/permission/{perm_id}",
            json=payload,
            auth=auth_config,
            headers=self.headers
        )

        assert_response(
            response,
            403,
            f"Permission update failed for: {response.text}"
        )

    @pytest.mark.put
    @pytest.mark.negative
    def test_update_permissions_with_nonexistent_id(self):
        
        auth_config = self.ranger_admin_config
        perm_id = -93402  # Assuming this ID does not exist

        payload = build_permission_payload(
            module_id=perm_id,
            module_name=self.permission_module_name,
            user_perm_list=[],
            group_perm_list=[]
        )

        response = requests.put(
            f"{self.base_url}/xusers/permission/{perm_id}",
            json=payload,
            auth=auth_config,
            headers=self.headers
        )

        assert_response(
            response,
            404,
            f"Permission update should fail for nonexistent ID: {response.text}"
        )
    
    @pytest.mark.delete
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("keyadmin", "ranger_key_admin_config"),
            ("auditor", "ranger_auditor_config"),
            ("user", "ranger_user_config"),
        ],
        ids=[ "keyadmin", "auditor", "user"],)
    def test_delete_permissions_with_invalid_roles(self, role, auth):

        auth_config = getattr(self, auth)
        

        perm_id = self.permission_module_id

        response = requests.delete(
            f"{self.base_url}/xusers/permission/{perm_id}",
            auth=auth_config,
            headers=self.headers
        )

        assert_response(
            response,
            403,
            f"Permission deletion failed: {response.text}"
        )

    