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
        temp_permission_group,
        temp_group,
        temp_permission_user,
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

        # create group 
        cls.group, cls.group_id = temp_group()

        cls.permission_group, cls.permission_group_id = temp_permission_group(
            cls.group_id,
            cls.permission_module_id
            )
        
        # create user
        cls.permission_user, cls.permission_user_id = temp_permission_user(
            cls.ranger_user_id, cls.permission_module_id
        )
        cls.permission_user1, cls.permission_user_id1 = temp_permission_user(
            cls.ranger_user_id1, cls.permission_module_id
        )
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
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )
    def test_get_permission(self, params, test_case, role, auth):
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permissions, but got access in {test_case}"

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
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )
    def test_get_permissionlist(self, params, test_case, role, auth):
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permission list, but got access in {test_case}"
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
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )
    def test_get_permission_count(self, role, auth):
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permission count, but got access"
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
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )
    def test_get_permission_by_id(self, role, auth):
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permission by ID, but got access"
        auth = getattr(self, auth)

        permission_id = self.permission_module_id 

        # Now fetch permission by ID
        response = requests.get(
            f"{self.base_url}/xusers/permission/{permission_id}",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 200, "Failed to fetch permission by ID")
    

    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )
    def test_get_permission_group(self, role, auth): # since the same searchUtil.extractCommonCriterias already checked test_get_permission with the  for sortfields so we are not checking them again.
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permission group, but got access"

        auth = getattr(self, auth)

        resp = requests.get(
            f"{self.base_url}/xusers/permission/group",
            auth=auth,
            headers=self.headers
        )
        assert_response(resp, 200, "Unable to fetch groups")

        data = resp.json()
        print(data)
        group_permission_schema(data)


    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )
    def test_get_permission_group_count(self, role, auth):
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permission group count, but got access"
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permission/group/count",
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
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )   
    def test_get_permission_group_by_id(self, role, auth):
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permission group by ID, but got access"

        auth = getattr(self, auth)

        group_permission_id = self.permission_group_id 

        response = requests.get(
            f"{self.base_url}/xusers/permission/group/{group_permission_id}",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, "Failed to fetch permission group by ID")
        data = response.json()
        assert data["id"] == group_permission_id
        assert data["moduleId"] == self.permission_module_id
        assert "groupId" in data   

    
    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )
    def test_get_permission_user(self, role, auth):   # since the same searchUtil.extractCommonCriterias already checked test_get_permission with the  for sortfields so we are not checking them again.
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permission user, but got access"
        
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permission/user",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, "Failed to fetch permission user")
        
        data = response.json()
    
        assert isinstance(data, dict), "Invalid response format"    
        assert "vXUserPermission" in data, "'vXUserPermission' not found in response"
        assert isinstance(data["vXUserPermission"], list)

        modules = data["vXUserPermission"]

        if not modules:
            assert data.get("totalCount", 0) == 0
            assert data.get("resultSize", 0) == 0
            return
        mod = modules[0]
        assert "userName" in mod, "'userName' not found in response"
        assert isinstance(mod["userName"], str)
        assert isinstance(mod['id'], int)

        if "resultSize" in data:
            assert isinstance(data["resultSize"], int)

        if "listSize" in data:
            assert isinstance(data["listSize"], int)

        
    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )
    def test_get_permission_user_count(self, role, auth):
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permission user count"

        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permission/user/count",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, "Failed to fetch permission user count")
        data = response.json()
        assert isinstance(data.get("value"), int), "Invalid response format"

    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"), ("auditor", "ranger_auditor_config")],
        ids=["admin", "auditor"],
    )
    def test_get_permission_user_by_id(self, role, auth, request):
        if role not in ["admin", "auditor"]:
            assert False, f"Role {role} should not have access to permission user details"
        auth = getattr(self, auth)
        module_id = self.permission_module_id
        permission_user_id = self.permission_user_id
        response = requests.get(
            f"{self.base_url}/xusers/permission/user/{permission_user_id}",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 200, "Failed to fetch permission user by ID")
        data = response.json()
        assert data["id"] == permission_user_id
        assert data["userId"] == self.ranger_user_id
        assert data["moduleId"] == module_id


    @pytest.mark.post
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config")],
        ids=["admin"],
    )
    def test_create_permissions(self, role, auth):
        if role != "admin":
            assert False, f"Role {role} should not have access to create permissions"
        
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



    @pytest.mark.post
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config")],
        ids=["admin"],
    )
    def test_create_permissions_group(self, role, auth, request):
        if role != "admin":
            assert False, f"Role {role} should not have access to create permissions group"
        auth = getattr(self, auth)

        module, module_id = request.getfixturevalue("temp_permission_module")()  
        module_check = requests.get(
            f"{self.base_url}/xusers/permission/{module_id}",
            auth=auth,
            headers=self.headers
        )
        assert_response(module_check, 200, "Failed to fetch module for group permission assignment")

        group, group_id = request.getfixturevalue("temp_group")()
        group_check = requests.get(
            f"{self.base_url}/xusers/groups/{group_id}",
            auth=auth,
            headers=self.headers
        )
        assert_response(group_check, 200, "Failed to fetch group for permission assignment")

        payload = {
            "groupId": group_id,
            "moduleId": module_id,
            "isAllowed": 1
        }

        response = requests.post(
            f"{self.base_url}/xusers/permission/group",
            json=payload,
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, "Failed to create permissions for assignment")


    @pytest.mark.post
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config")],
        ids=["admin"],
    )
    def test_create_permissions_user(self, role, auth, request):
        if role != "admin":
            assert False, f"Role {role} should not have access to create permission user"
        auth = getattr(self, auth)

        module, module_id = request.getfixturevalue("temp_permission_module")()
        user, user_id = request.getfixturevalue("temp_secure_user")(["user"])
        params = {
            "userId": user_id,
            "moduleId": module_id,
            "isAllowed": 1
        }

        assert not user_permission_exists(user_id, module_id, self.ranger_admin_config, self.base_url, self.headers), "Permission user should not exist before creation"
        module_check = requests.post(
            f"{self.base_url}/xusers/permission/user",
            json=params,
            auth=auth,
            headers=self.headers
        )
        
        assert_response(module_check, 200, "Failed to create permission for user")
        assert user_permission_exists(user_id, module_id, self.ranger_admin_config, self.base_url, self.headers), "Permission user should exist after creation"
        module_check_data = module_check.json()
        assert module_check_data["userId"] == user_id, "Created permission user ID does not match expected user ID"
        assert module_check_data["moduleId"] == module_id, "Created permission module ID does not match expected module ID"

    @pytest.mark.put
    @pytest.mark.positive
    @pytest.mark.parametrize(
    "role, auth, test_case",
    [
        ("admin", "ranger_admin_config", "existing_user_different_data"),
        ("admin", "ranger_admin_config", "existing_user_same_data"),
        ("admin", "ranger_admin_config", "new_user_permission"),
        ("admin", "ranger_admin_config", "existing_group_different_data"),
        ("admin", "ranger_admin_config", "existing_group_same_data"),
        ("admin", "ranger_admin_config", "new_group_permission"),
        ("admin", "ranger_admin_config", "user_and_group_update_together"),
        ("admin", "ranger_admin_config", "empty_permission_lists"),
    ])
    def test_update_permissions(self, role, auth, test_case):
        if role != "admin":
            assert False, f"Role {role} should not have access to update permissions"
        
        auth_config = getattr(self, auth)
        perm_id = self.permission_module_id

        # Fetch groups
        resp = requests.get(f"{self.base_url}/xusers/groups", auth=auth_config, headers=self.headers)
        assert_response(resp, 200, f"Unable to fetch groups: {resp.text}")
        groups = resp.json().get("vXGroups", [])
        assert groups, "No groups found in the system."

        u_exist, u_new = self.ranger_user_id1, self.ranger_user_id
        g_exist, g_new = groups[0]["id"], groups[1]["id"] if len(groups) > 1 else groups[0]["id"]

        # Helper functions
        u_perm = lambda uid, allow: build_user_permission(uid, perm_id, allow)
        g_perm = lambda gid, allow: build_group_permission(gid, perm_id, allow)

        # Map test cases to tuple: (userPermList, groupPermList)
        cases = {
            "existing_user_different_data":   ([u_perm(u_exist, 0)], []),
            "existing_user_same_data":        ([u_perm(u_exist, 1)], []),
            "new_user_permission":            ([u_perm(u_new, 1)], []),
            "existing_group_different_data":  ([], [g_perm(g_exist, 0)]),
            "existing_group_same_data":       ([], [g_perm(g_exist, 1)]),
            "new_group_permission":           ([], [g_perm(g_new, 1)]),
            "user_and_group_update_together": ([u_perm(u_exist, 0)], [g_perm(g_exist, 0)]),
            "empty_permission_lists":         ([], [])
        }

        user_list, group_list = cases[test_case]
        
        payload = build_permission_payload(
            module_id=perm_id,
            module_name=self.permission_module_name,
            user_perm_list=user_list,
            group_perm_list=group_list
        )

        response = requests.put(
            f"{self.base_url}/xusers/permission/{perm_id}",
            json=payload,
            auth=auth_config,
            headers=self.headers
        )

        assert_response(response, 200, f"Permission update failed for {test_case}: {response.text}")

        data = response.json()
        assert data["id"] == perm_id
        assert data["module"] == self.permission_module_name

        if payload["userPermList"]:
            assert "userPermList" in data
            assert isinstance(data["userPermList"], list)

        if payload["groupPermList"]:
            assert "groupPermList" in data
            assert isinstance(data["groupPermList"], list)


    @pytest.mark.put
    @pytest.mark.positive
    @pytest.mark.parametrize(
    "role, auth, test_case",
    [
        ("admin", "ranger_admin_config", "with payload id"),
        ("admin", "ranger_admin_config", "without payload id"),
    ],
    ids=["admin-with_id", "admin-without_id"]
    )
    def test_update_permissions_group(self, request, role, auth, test_case):
        if role != "admin":
            assert False, f"Role {role} should not have access to update permission group"

        auth_config = getattr(self, auth)
        module, module_id = request.getfixturevalue("temp_permission_module")()
        grp, grp_id = request.getfixturevalue("temp_group")()

        perm_grp, perm_id = request.getfixturevalue("temp_permission_group")(grp_id, module_id)

        if test_case == "without payload id":
            payload ={
                "groupId": self.group_id,
                "moduleId": module_id,
                "isAllowed": 1
            }
            assert "id" not in payload, "Payload should not contain 'id' for this test case"

        elif test_case == "with payload id":
        
            payload ={
            "id": perm_id,
            "groupId": self.group_id,
            "moduleId": module_id,
            "isAllowed": 1
            
            }
            assert payload["id"] == perm_id, "Payload groupId does not match expected group ID for update test"


        assert grp_id != self.group_id, "Precondition failed: Need a different group ID to test update of group permission"
        assert perm_grp["groupId"] == grp_id, "Precondition failed: Permission group is not linked to the expected group"

        
        response = requests.put(
            f"{self.base_url}/xusers/permission/group/{perm_id}",
            json=payload,
            auth=auth_config,
            headers=self.headers
        )

        assert_response(response,200,f"Permission update failed for empty lists: {response.text}")
        response_data = response.json()
        assert response_data["groupId"] == self.group_id, "Updated groupId does not match expected group ID"

    @pytest.mark.put
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config")],
        ids=["admin"]
    )
    def test_update_permissions_user(self, request, role, auth):
        if role != "admin":
            assert False, f"Role {role} should not have access to update permission user"

        auth_config = getattr(self, auth)
        module, module_id = request.getfixturevalue("temp_permission_module")()
        user, user_id = request.getfixturevalue("temp_secure_user")(["user"])

        perm_user, perm_user_id = request.getfixturevalue("temp_permission_user")(user_id, module_id) 

        
        payload = {
            "id": perm_user_id,
            "userId": user_id,
            "moduleId": module_id,
            "isAllowed": 0  # only we can change this field in update
        }


        assert perm_user["userId"] == user_id, "Precondition failed: Permission user is not linked to the expected user"
        assert perm_user["moduleId"] == module_id, "Precondition failed: Permission user is not linked to the expected module"
        assert user_permission_exists(user_id, module_id, self.ranger_admin_config, self.base_url, self.headers, user_perm_id = perm_user_id), "Permission user should exist before update"
        assert payload["isAllowed"] != perm_user["isAllowed"], "Precondition failed: Need different isAllowed value to test update of user permission"


        response = requests.put(
            f"{self.base_url}/xusers/permission/user/{perm_user_id}",
            json=payload,
            auth=auth_config,
            headers=self.headers
        )

        assert_response(response, 200, f"Permission user update failed: {response.text}")
        response_data = response.json()
        assert response_data["isAllowed"] == 0, "Updated isAllowed does not match expected value"
    
    @pytest.mark.delete
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config")],
        ids=["admin"]
    )
    def test_delete_permissions(self, role, auth):
        if role != "admin":
            assert False, f"Role {role} should not have access to delete permissions"
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

    @pytest.mark.delete
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config")],
        ids=["admin"]
    )
    def test_delete_permission_group(self, role, auth, request):
        if role != "admin":
            assert False, f"Role {role} should not have access to delete permission group"
        auth_config = getattr(self, auth)
        module, module_id = request.getfixturevalue("temp_permission_module")()
        grp, grp_id = request.getfixturevalue("temp_group")()

        perm_grp, perm_id = request.getfixturevalue("temp_permission_group")(grp_id, module_id)

        response = requests.delete(
            f"{self.base_url}/xusers/permission/group/{perm_id}",
            auth=auth_config,
            headers=self.headers
        )

        assert_response(
            response,
            204,
            f"Permission group deletion failed: {response.text}"
        )
    

    @pytest.mark.delete
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config")],
        ids=["admin"]
    )
    def test_delete_permission_user(self, role, auth, request):
        if role != "admin":
            assert False, f"Role {role} should not have access to delete permission user"
        auth_config = getattr(self, auth)
        module, module_id = request.getfixturevalue("temp_permission_module")()
        user, user_id = request.getfixturevalue("temp_secure_user")(["user"])

        perm_user, perm_user_id = request.getfixturevalue("temp_permission_user")(user_id, module_id) 

        response = requests.delete(
            f"{self.base_url}/xusers/permission/user/{perm_user_id}",
            auth=auth_config,
            headers=self.headers
        )

        assert_response(
            response,
            204,
            f"Permission user deletion failed: {response.text}"
        )


    # NEGATIVE TESTS

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [("user", "ranger_user_config"), ("key_admin", "ranger_key_admin_config")],
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
        [("user", "ranger_user_config"), ("key_admin", "ranger_key_admin_config")],
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
        [("keyadmin", "ranger_key_admin_config"), ("user", "ranger_user_config")],
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
        ["invalid_id", ("keyadmin", "ranger_key_admin_config"), ("user", "ranger_user_config")],
        ids=["invalid_id", "invalid_role_keyadmin", "invalid_role_user"],
    )
    def test_invalid_get_permission_by_id(self, test_case):

        if test_case == "invalid_id":
            permission_id = -93402  # Assuming this ID does not exist
            auth = self.ranger_admin_config
            expected_status = 404
        
        else:
            permission_id = self.permission_module_id
            auth = getattr(self, test_case[1])  
            expected_status = 403
        
        response = requests.get(
            f"{self.base_url}/xusers/permission/{permission_id}",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, expected_status, "Failed to fetch permission by ID")

    @pytest.mark.get
    @pytest.mark.negative 
    @pytest.mark.parametrize(
        "role, auth",
        [("user", "ranger_user_config"), ("key_admin", "ranger_key_admin_config")],
        ids=["user", "key_admin"],
    )    
    def test_get_permission_group_with_invalid_role(self, role, auth):

        auth = getattr(self, auth)

        resp = requests.get(
            f"{self.base_url}/xusers/permission/group",
            auth=auth,
            headers=self.headers
        )
        assert_response(resp, 403, f"{role} should not have permission to view permission groups, but got {resp.status_code}")


    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [("user", "ranger_user_config"), ("key_admin", "ranger_key_admin_config")],
        ids=["user", "key_admin"],
    )    
    def test_get_permission_group_count_with_invalid_role(self, role, auth):

        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permission/group/count",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 403, f"{role} should not have permission to view permission group count, but got {response.status_code}")

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [("user", "ranger_user_config"), ("key_admin", "ranger_key_admin_config")],
        ids=["user", "key_admin"],
    )    
    def test_get_permission_group_by_id_with_invalid_role(self, role, auth):
        auth = getattr(self, auth)

        group_permission_id = self.permission_group_id 

        response = requests.get(
            f"{self.base_url}/xusers/permission/group/{group_permission_id}",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 403, f"{role} should not have permission to view permission group by ID, but got {response.status_code}")

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [("user", "ranger_user_config"), ("key_admin", "ranger_key_admin_config")],
        ids=["user", "key_admin"],
    ) 
    def test_get_permission_user_with_invalid_role(self, role, auth):
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permission/user",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 403, f"{role} should not have permission to view permission users, but got {response.status_code}")

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [("user", "ranger_user_config"), ("key_admin", "ranger_key_admin_config")],
        ids=["user", "key_admin"],
    ) 
    def test_get_permission_user_count_with_invalid_role(self, role, auth):
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/permission/user/count",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 403, f"{role} should not have permission to view permission user count, but got {response.status_code}")


    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "Test_case",
        ["invalid_id", "invalid_role_keyadmin", "invalid_role_user"],
        ids=["invalid_id", "invalid_role_keyadmin", "invalid_role_user"],
    )
    def test_get_permission_user_by_id_with_invalid_test_case(self, Test_case, request):
        if Test_case == "invalid_id":
            permission_user_id = -93402  # Assuming this ID does not exist
            auth = self.ranger_admin_config
            expected_status = 404
        elif Test_case == "invalid_role_keyadmin":
            permission_user_id = self.permission_user_id 
            auth = self.ranger_key_admin_config  
            expected_status = 403
        elif Test_case == "invalid_role_user":
            permission_user_id = self.permission_user_id 
            auth = self.ranger_user_config  
            expected_status = 403

        response = requests.get(
            f"{self.base_url}/xusers/permission/user/{permission_user_id}",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, expected_status, f"Invalid test case {Test_case}: Expected status {expected_status}, got {response.status_code}")

    @pytest.mark.post
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth, test_case",
        [("admin", "ranger_admin_config", "creating permission with existing module name"), ("auditor", "ranger_auditor_config", "invalid auth by auditor"), ("keyadmin", "ranger_key_admin_config", "invalid auth by keyadmin"), ("user", "ranger_user_config", "invalid auth by user")],
        ids=["admin-existing_module", "auditor-invalid_auth", "keyadmin-invalid_auth", "user-invalid_auth"],
    )
    def test_create_permissions_with_negative_testcases(self, role, auth, test_case):

        auth = getattr(self, auth)
        module_name = self.permission_module_name 

        if test_case == "creating permission with existing module name":
            assert permission_module_exists(module_name, self.ranger_admin_config, self.base_url, self.headers), f"Module name {module_name} should exist for this test case to be valid"
            expected_status = 400
        else:
            expected_status = 403
        payload = {
            "module": module_name,
        }

        response = requests.post(
            f"{self.base_url}/xusers/permission",
            json=payload,
            auth=auth,
            headers=self.headers
        )
        assert_response(response, expected_status, f"{test_case}: Expected status {expected_status}, got {response.status_code}")


    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [("keyadmin", "ranger_key_admin_config"), ("user", "ranger_user_config")],
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
    
    @pytest.mark.post
    @pytest.mark.negative
    @pytest.mark.parametrize("test_case", [
        "nonexistent_module", "nonexistent_group_id", "preexisting_permission_group_payload",
        "invalid_auth_by_role", "invalid_payload_missing_fields"
    ])
    def test_create_permissions_group_with_invalid_cases(self, test_case, request):

        get_g = lambda: request.getfixturevalue("temp_group")()[1]
        get_m = lambda: request.getfixturevalue("temp_permission_module")()[1]

        cases = {
            "nonexistent_module": ({"groupId": get_g(), "moduleId": -99999, "isAllowed": 1}, self.ranger_admin_config, 404),
            "nonexistent_group_id": ({"groupId": -99999, "moduleId": get_m(), "isAllowed": 1}, self.ranger_admin_config, 404),
            "preexisting_permission_group_payload": ({"groupId": get_g(), "moduleId": get_m(), "isAllowed": 1}, self.ranger_admin_config, 400),
            "invalid_auth_by_role": ({"groupId": get_g(), "moduleId": get_m(), "isAllowed": 1}, self.ranger_user_config, 403),
            "invalid_payload_missing_fields": ({"moduleId": self.permission_module_id, "isAllowed": 1}, self.ranger_admin_config, 400),
        }

        payload, auth, expected_status = cases[test_case]

        if test_case == "preexisting_permission_group_payload":
            requests.post(f"{self.base_url}/xusers/permission/group", json=payload, auth=auth, headers=self.headers)

        response = requests.post(f"{self.base_url}/xusers/permission/group", json=payload, auth=auth, headers=self.headers)
        assert_response(response, expected_status, f"Case {test_case} failed: {response.status_code}")


    @pytest.mark.post
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth_name, test_case, expected_status",
        [
            ("admin", "ranger_admin_config", "nonexistent_module", 404),
            ("admin", "ranger_admin_config", "nonexistent_user", 400),
            ("admin", "ranger_admin_config", "preexisting_permission_user_payload", 400),
            ("user", "ranger_user_config", "invalid_auth_by_role_user", 403),
            ("keyadmin", "ranger_key_admin_config", "invalid_auth_by_role_keyadmin", 403),
            ("auditor", "ranger_auditor_config", "invalid_auth_by_role_auditor", 403),
            ("admin", "ranger_admin_config", "invalid_payload_missing_fields", 400),
        ],
        ids=lambda x: x # Automatically uses the test_case name for the ID
    )
    def test_create_permissions_user_with_invalid_roless(self, role, auth_name, test_case, expected_status, request):
        auth = getattr(self, auth_name)
        
        payload = {
            "userId": self.ranger_user_id,
            "moduleId": self.permission_module_id,
            "isAllowed": 1
        }

        # Apply specific overrides for unique cases
        overrides = {
            "nonexistent_module": {"moduleId": -99999},
            "nonexistent_user": {"userId": -99999},
            "invalid_payload_missing_fields": {"isAllowed": None} # This will trigger the pop below
        }

        if test_case in overrides:
            payload.update(overrides[test_case])
            if payload.get("isAllowed") is None: payload.pop("isAllowed")

        if test_case == "preexisting_permission_user_payload":
            assert user_permission_exists(self.ranger_user_id, self.permission_module_id, self.ranger_admin_config, self.base_url, self.headers), "Precondition failed"

        response = requests.post(
            f"{self.base_url}/xusers/permission/user",
            json=payload,
            auth=auth,
            headers=self.headers
        )
        
        assert_response(response, expected_status, f"Failed {test_case}: {response.text}")

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


    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, auth_name, expected_status",
        [
            ("non_matching_payload_id", "ranger_admin_config", 400),
            ("missing_mandatory_fields", "ranger_admin_config", 400),
            ("invalid_auth_by_user", "ranger_user_config", 403),
            ("invalid_auth_by_keyadmin", "ranger_key_admin_config", 403),
        ],
        ids=lambda x: str(x)
    )
    def test_update_permissions_group(self, request, test_case, auth_name, expected_status):
        
        module, module_id = request.getfixturevalue("temp_permission_module")()
        grp, grp_id = request.getfixturevalue("temp_group")()

        perm_grp, perm_id = request.getfixturevalue("temp_permission_group")(grp_id, module_id)

        auth_config = getattr(self, auth_name)

        payload = {
            "id" : perm_id,
            "groupId": self.group_id,
            "moduleId": module_id,
            "isAllowed": 1
        }

        if test_case == "non_matching_payload_id":
            payload["id"] = self.permission_group_id  # using a different existing permission group ID to cause mismatch
            assert payload["id"] != perm_id, "Payload ID should not match the permission group ID for this test case"
        
        elif test_case == "missing_mandatory_fields":
            del payload["groupId"]  
        
        response = requests.put(
            f"{self.base_url}/xusers/permission/group/{perm_id}",
            json=payload,
            auth=auth_config,
            headers=self.headers
        )
        assert_response(response, expected_status, f"Permission update should fail for test case '{test_case}': but got {response.text}")
 
        del_response = requests.delete(
            f"{self.base_url}/xusers/permission/group/{perm_id}",
            auth=self.ranger_admin_config,
            headers=self.headers
        )
        assert_response(del_response, 204, f"Cleanup failed for permission group ID {perm_id}: {del_response.text}")

    
    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth, test_case",
        [
            ("admin", "ranger_admin_config", "invalid payload missing id"),
            ("admin", "ranger_admin_config", "invalid payload missing userId"),
            ("admin", "ranger_admin_config", "invalid payload missing isAllowed"),
            ("admin", "ranger_admin_config", "invalid payload editing non editable fields - userId"),
            ("admin", "ranger_admin_config", "invalid payload editing non editable fields - moduleId"),
            ("user", "ranger_user_config", "invalid auth by user"),
            ("key_admin", "ranger_key_admin_config", "invalid auth by keyadmin"),
            ("auditor", "ranger_auditor_config", "invalid auth by auditor"),
        ],
        ids=["invalid payload missing id", "invalid payload missing userId", "invalid payload missing isAllowed", "invalid payload (editing non editable fields - userId)", "invalid payload (editing non editable fields - moduleId)", "invalid auth by user", "invalid auth by keyadmin", "invalid auth by auditor"],
    )
    def test_update_permissions_user_with_invalid_testcases(self, request, role, auth, test_case):
        
        if role == "admin":
            auth_config = self.ranger_admin_config
            module, module_id = request.getfixturevalue("temp_permission_module")()
            user, user_id = request.getfixturevalue("temp_secure_user")(["user"])
            perm_user, perm_user_id = request.getfixturevalue("temp_permission_user")(user_id, module_id) 
            payload = {"id": perm_user_id, "userId": user_id, "moduleId": module_id, "isAllowed": 0}
        else:
            perm_user_id = self.permission_user_id

        if test_case.startswith("invalid payload missing"):
            field = test_case.split("missing ")[1]
            payload.pop(field, None)
            
            status_map = {"id": 400, "userId": 400, "moduleId": 400, "isAllowed": 404}
            expected_status = status_map.get(field, 400)
            

        elif test_case.startswith("invalid payload editing non editable fields"):
            field = test_case.split("- ")[1]
            payload[field] = random.randint(10000, 99999)
            expected_status = 400 if field == "userId" else 404

        elif test_case.startswith("invalid auth by "):
            payload = {
                "id": perm_user_id,
                "userId": self.ranger_user_id,
                "moduleId": self.permission_module_id,
                "isAllowed": 0
            }
            auth_config = getattr(self, f"ranger_{role}_config")
            expected_status = 403
            
        response = requests.put(
            f"{self.base_url}/xusers/permission/user/{perm_user_id}",
            json=payload,
            auth=auth_config,
            headers=self.headers
        )

        assert_response(response, expected_status, f"Permission user update failed: {response.text}")
        
    @pytest.mark.delete
    @pytest.mark.negative
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

    @pytest.mark.delete
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        (["keyadmin", "ranger_key_admin_config"], ["auditor", "ranger_auditor_config"],["user", "ranger_user_config"]),
        ids=["keyadmin", "auditor", "user"],)
    def test_delete_permission_group_with_invalid_roles(self, role, auth, request):
        auth_config = getattr(self, auth)
        module, module_id = request.getfixturevalue("temp_permission_module")()
        grp, grp_id = request.getfixturevalue("temp_group")()

        perm_grp, perm_id = request.getfixturevalue("temp_permission_group")(grp_id, module_id)

        response = requests.delete(
            f"{self.base_url}/xusers/permission/group/{perm_id}",
            auth=auth_config,
            headers=self.headers
        )

        assert_response(
            response,
            403,
            f"Permission group deletion should fail for role {role} but got {response.status_code}"
        )
    
    @pytest.mark.delete
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        (["keyadmin", "ranger_key_admin_config"], ["auditor", "ranger_auditor_config"], ["user", "ranger_user_config"]),
        ids=["keyadmin", "auditor", "user"],
    )
    def test_delete_permission_user(self, role, auth, request):
        
        auth_config = getattr(self, auth)
        module_id = self.permission_module_id
        user_id = self.ranger_user_id

        perm_user_id = self.permission_user_id

        response = requests.delete(
            f"{self.base_url}/xusers/permission/user/{perm_user_id}",
            auth=auth_config,
            headers=self.headers
        )

        assert_response(
            response,
            403,
            f"Permission user deletion should fail for role {role} but got {response.status_code}"
        )