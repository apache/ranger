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
from rolerest.utility.utils import *
from rolerest.utility.utils import assert_response
from xuserrest.utility.utils import *
import uuid
import string
import pytest
import requests
from datetime import datetime
import random

@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.rolerest
class TestRoleCRUD:
    SERVICE_NAME = "admin"

    @pytest.fixture(autouse=True, scope="class")
    def _setup(
        self,
        request,
        temp_secure_user,
        temp_keyadmin_user,
        temp_group,
        temp_role,
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

        # ---------- role details ----------
        cls.role, cls.role_id = temp_role()
        cls.role1, cls.role_id1 = temp_role()

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
        "role, auth",
        [("admin", "ranger_admin_config")],
    )
    @pytest.mark.parametrize(
        "params, test_case",
        [
            ({}, "default_request"),
            ({"startIndex": 0, "pageSize": 10}, "pagination_basic"),
            ({"roleName": "admin"}, "filter_role_name"),
            ({"roleId": 1}, "filter_role_id"),
            ({"sortBy": "id", "sortType": "asc"}, "sorting_asc"),
            ({"sortBy": "id", "sortType": "desc"}, "sorting_desc"),
            ({"sortBy": "name", "sortType": "asc"}, "sorting_name_asc"),
            ({"startIndex": 0, "pageSize": 10, "sortBy": "id", "sortType": "asc"}, "pagination_with_sort"),
            ({"getCount": "false"}, "get_count_false"),
        ],
        ids=[
            "default",
            "pagination-basic",
            "filter-role-name",
            "filter-role-id",
            "sorting-asc",
            "sorting-desc",
            "sorting-name-asc",
            "pagination-with-sort",
            "getcount-false",
        ],
    )
    def test_get_all_roles(self, params, test_case, role, auth):
        if role != "admin":
            pytest.fail("Admin privileges required to get all roles")

        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/roles/roles",
            headers=self.headers,
            auth=auth,
            params=params
        )
        assert_response(response, 200, f"Failed to get all roles and got response code {response.status_code}")

        if "pageSize" in params:
            assert len(response.json().get("roles", [])) <= params["pageSize"]

        if "roleName" in params:
            roles = response.json().get("roles", [])
            for r in roles:
                assert params["name"].lower() in r["name"].lower(), \
                    f"Role {r['name']} doesn't match filter {params['roleName']}"
                
    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"), ("keyadmin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")],
    )
    def test_get_role_by_id(self, role, auth):

        auth = getattr(self, auth)

        role_id = self.role_id

        response = requests.get(
            f"{self.base_url}/roles/roles/{role_id}",
            headers=self.headers,
            auth=auth
        )
        assert_response(response, 200, f"Failed to get role by ID and got response code {response.status_code}")
        data = response.json()
        assert data["id"] == role_id, f"Expected role ID {role_id} but got {data['id']}"

    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "login_test_case", [
            ("login_user is admin"),
            ("login_user is service admin"),
            ("login_user is in service admin groups in grouplist"),
            #("login_user is service user"), 
        ]
    )
    @pytest.mark.parametrize(
        "query_param_user, effective_user, additional_conditions",
        [   
            ("not exists", "login_user", "login user should not be service user"),
            ("exists", "query param userName", "query user is admin"),
            ("exists", "query param userName", "query user is service admin"),
            ("exists", "query param userName", "query user is in service admin groups in grouplist"),
        ])
    def test_get_all_roles_by_name(self, login_test_case, additional_conditions, query_param_user, effective_user, request):
        params = {}
        if login_test_case == "login_user is admin":
            auth = self.ranger_admin_config
            del_list = []
        elif login_test_case == "login_user is service admin":
            temp_service, temp_service_id = create_service()
            temp_s_admin, temp_s_admin_id = request.getfixturevalue("temp_secure_user")("auditor")
            assign_service_admin(temp_service_id, temp_service, temp_s_admin['name'])
            auth = (temp_s_admin["name"], "Test@123")
            params["serviceName"] = temp_service["name"]
            del_list = [temp_service_id]
        else:
            temp_service, temp_service_id = create_service()
            temp_s_admin, temp_s_admin_id = request.getfixturevalue("temp_secure_user")("auditor")
            assign_service_admin(temp_service_id, temp_service, temp_s_admin['name'])
            group, group_id = request.getfixturevalue("temp_group")()
            assign_groups_to_user(temp_s_admin["name"], [group["name"]], self.ranger_admin_config, self.base_url, self.headers)
            assign_service_admin_group(temp_service_id, temp_service, group["name"])
            auth = (temp_s_admin["name"], "Test@123")
            params["serviceName"] = temp_service["name"]
            del_list = [temp_service_id]

        if query_param_user == "not exists" and login_test_case != "login_user is service user":
            pass
        else:
            if "admin" in additional_conditions:
                params["execUser"] = self.admin1["name"]
            elif "service admin" in additional_conditions:
                if login_test_case == "login_user is service admin":
                    temp_service, temp_service_id = create_service()
                    del_list.append(temp_service_id)
                s_admin, s_admin_id = request.getfixturevalue("temp_secure_user")("auditor")
                assign_service_admin(temp_service_id, temp_service, s_admin['name'])
                params["execUser"] = s_admin["name"]
            elif "service admin groups" in additional_conditions:
                if login_test_case == "login_user is in service admin":
                    temp_service, temp_service_id = create_service()
                    del_list.append(temp_service_id)
                s_admin, s_admin_id = request.getfixturevalue("temp_secure_user")("auditor")
                assign_service_admin(temp_service_id, temp_service, s_admin['name'])
                group, group_id = request.getfixturevalue("temp_group")()
                assign_groups_to_user(s_admin["name"], [group["name"]], self.ranger_admin_config, self.base_url, self.headers)
                assign_service_admin_group(temp_service_id, temp_service, group["name"])
                params["execUser"] = s_admin["name"]
        
        response = requests.get(
            f"{self.base_url}/roles/roles/names",
            auth=auth,
            headers=self.headers,
            params=params
        )

        assert_response(response, 200, f"Failed to get role names with different login creds and got response code {response.status_code} with \n response text: {response.text}")
        data = response.json()
        assert isinstance(data, list), f"Expected response to be a list of role names but got {type(data)} for test case {login_test_case} with additional conditions: {additional_conditions}"
        # Cleanup
        for item in del_list:
            delete_service(item)

    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "test_case", [
            ("login_user is not (admin not service admin/group) & role-membership via  user"),
            ("login_user is not (admin not service admin/group) & role-membership via  group"),
            ("login_user is not (admin not service admin/group) & role-membership via  role-user"),
            ("login_user is not (admin not service admin/group) & role-membership via  role-group"),
            ("login_user is admin"),
            ("login_user is service admin"),
            ("login_user is in service admin groups in grouplist"),
        ]
    )
    def test_get_role_by_name_same_creds(self, test_case, request):   # here always the login_user = queryparam user
        
        condition = test_case.removeprefix("login_user is ")
        auth, params, role, clean_up_items = ensureRoleAccess(condition, request)
        
        response = requests.get(
            f"{self.base_url}/roles/roles/name/{role['name']}",
            auth=auth,
            headers=self.headers,
            params=params
        )

        assert_response(response, 200, f"Failed to get role by name with same creds and got response code {response.status_code} with \n response text: {response.text}")

        data = response.json()
        assert data["id"] == role["id"], f"Expected role ID {role['id']} but got {data['id']} for test case {test_case}"
       
        # Cleanup 
        for item in clean_up_items["role_list"]:
            delete_role(item)
        for item in clean_up_items["service_list"]:
            delete_service(item)


    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "test_case", [
            "login user is admin",          
            "login user is service admin",
            "login user has service admin groups",
        ]
    )
    @pytest.mark.parametrize(
        "query_param_user, effective_user, additional_conditions", 
        [
            ("exists", "query param userName", "query user is admin"),
            ("exists", "query param userName", "query user is service admin"),
            ("exists", "query param userName", "query user is in service admin groups in grouplist"),
            ("exists", "query param userName", "query user is not admin or service admin/group but has role membership via user"),
            ("exists", "query param userName", "query user is not admin or service admin/group but has role membership via group"),
            ("exists", "query param userName", "query user is not admin or service admin/group but has role membership via role-user"),
            ("exists", "query param userName", "query user is not admin or service admin/group but has role membership via role-group"),
            ("not_exists", "login userName", "None"),
        ],
    )
    def test_get_role_by_name_with_diff_creds(self, query_param_user, effective_user, test_case, additional_conditions, request):
        param = {}
        service = None
        service_id = None


        if test_case == "login user is admin":           
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["admin"])
            auth = (temp_user["name"], "Test@123")

        elif test_case == "login user is service admin": 
            service, service_id = create_service()
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["auditor"])
            resp = requests.get(
                f"{self.base_url}/xusers/users/userName/{temp_user['name']}",
                headers=self.headers,
                auth=self.ranger_admin_config
            )
            print(f"\n before User details for {temp_user['name']} after group assignment and service admin group assignment:\n {resp.json()} \n")

            assign_service_admin(service_id, service, temp_user['name'])
            auth = (temp_user["name"], "Test@123")


        elif test_case == "login user has service admin groups":  
            
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["auditor"])
            group, group_id = request.getfixturevalue("temp_group")()
            assign_groups_to_user(temp_user["name"], [group["name"]], self.ranger_admin_config, self.base_url, self.headers)
            service, service_id = create_service()
            assign_service_admin_group(service_id, service, group["name"])
        


        auth = (temp_user["name"], "Test@123")


        if query_param_user == "not_exists":
            role, role_id = request.getfixturevalue("temp_role")()
            params = {}
            if service:
                params["serviceName"] = service["name"]
            clean_up_items = {"role_list": [role_id], "service_list": [service_id] if service_id else []}
        else:
            condition = additional_conditions.removeprefix("query user is ")
            auth_user, params, role, clean_up_items = ensureRoleAccess(
                condition, request,
                existing_service=[service] if service and "service admin" in test_case else None
            )

            if service:
                params["serviceName"] = service["name"]


        print("\n Auth used: ", auth)
        print("\n Query params used: ", params)
        response = requests.get(
            f"{self.base_url}/roles/roles/name/{role['name']}",
            auth=auth,  # auth is admin but execUser in query param has service admin group permissions
            headers=self.headers,
            params=params
        )

        assert_response(response, 200, f"Failed to get role by name with different creds and got response code {response.status_code} with \n response text: {response.text}")


        data = response.json()
        assert data["id"] == role["id"], f"Expected role ID {role['id']} but got {data['id']} for test case {test_case}"
        print(f"Response data: {data} for test case {test_case}")
        # Cleanup
        for item in clean_up_items["role_list"]:
            delete_role(item)
        for item in clean_up_items["service_list"]:
            delete_service(item)
        # Cleanup login user's service if not already cleaned up
        if service_id and service_id not in clean_up_items["service_list"]:
            delete_service(service_id)

    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "u_role", ["admin", "user", "key_admin", "auditor"],)
    @pytest.mark.parametrize(
        "roles, auth",
        [("admin", "ranger_admin_config"),("user", "ranger_user_config"), ("key_admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config")],)
    @pytest.mark.parametrize(
        "test_case", [
            "user directly assigned to roles",
            "user assigned to roles via group membership",
        ])
    def test_get_roles_for_user_by_userName(self, test_case, roles, auth, u_role, request):
        

        if u_role == "key_admin":
            test_user, test_user_id = request.getfixturevalue("temp_keyadmin_user")()
            auth = getattr(self, auth)
        
        else:
            test_user, test_user_id = request.getfixturevalue("temp_secure_user")([u_role])
            auth = getattr(self, auth)
        if test_case == "user directly assigned to roles":
            role, role_id = request.getfixturevalue("temp_role")(user_list=[{"name": test_user["name"], "isAdmin": True}])
        elif test_case == "user assigned to roles via group membership":
            group, group_id = request.getfixturevalue("temp_group")()
            assign_groups_to_user(test_user["name"], [group["name"]], self.ranger_admin_config, self.base_url, self.headers)
            role, role_id = request.getfixturevalue("temp_role")(group_list=[{"name": group["name"], "isAdmin": True}])      
        response = requests.get(
            f"{self.base_url}/roles/roles/user/{test_user['name']}",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 200, f"Failed to get roles for user by userName and got response code {response.status_code} with \n response text: {response.text}")
        data = response.json()
        assert isinstance(data, list), f"Expected response to be a list of roles but got {type(data)} for test case {test_case}"
        if test_case == "user assigned to roles via group membership":
            if roles == "user":
                assert data == [], f"1. expected silent failure and groups should not be returned for user role but got {data} for test case {test_case}"
            elif u_role in ["admin", "auditor"] and roles == "keyadmin":
                assert data == [], f"2. expected silent failure and groups should not be returned for non key admin role but got {data} for test case {test_case}"
            elif u_role == "key_admin" and roles in ["admin", "auditor"]:
                assert data == [], f"3. expected silent failure and groups should not be returned for non key admin role but got {data} for user role but got {data} for test case {test_case}"
        else:
            assert role['name'] in data, f"Expected role {role['name']} to be in the response but got {data} for test case {test_case}"

    @pytest.mark.get
    @pytest.mark.positive
    def test_get_roles_for_user_by_userName_special_case(self, request):
        temp_user, temp_user_id = self.user, self.ranger_user_id
        group, group_id = self.group, self.group_id
        
        assign_groups_to_user(temp_user["name"], [group["name"]], self.ranger_admin_config, self.base_url, self.headers)
        
        role, role_id = request.getfixturevalue("temp_role")(group_list=[{"name": group["name"], "isAdmin": True}])
        
        auth = (temp_user["name"], "Test@123")
        response = requests.get(
            f"{self.base_url}/roles/roles/user/{temp_user['name']}",
            auth=auth,
            headers=self.headers
        )
        
        assert_response(response, 200, f"Failed to get roles for user by userName in special case and got response code {response.status_code} with \n response text: {response.text}")
        
        data = response.json()
        assert isinstance(data, list), f"Expected response to be a list of roles but got {type(data)} for special test case"
        assert role['name'] in data, f"Expected role {role['name']} to be in the response but got {data} for special test case"

            
    @pytest.mark.post
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth, test_case",
        [("admin", "ranger_admin_config", "minimal_request"),
         ("admin", "ranger_admin_config", "with_users_groups"), 
         ("admin", "ranger_admin_config", "with_create_non_exist_user_group"),
         ("service_admin","","with_service_name")
        ],
    )
    def test_create_role(self, role, auth, test_case, request):
        if role == "service_admin":
            service, service_id = create_service()
            s_admin, s_admin_id = request.getfixturevalue("temp_secure_user")("auditor")
            assign_service_admin(service_id, service, s_admin['name'])
            auth = (s_admin['name'], "Test@123")
        elif role == "admin":
            auth = getattr(self, auth)
        else:
            pytest.fail("Invalid role for this test")
        
        
        param = {}

        if test_case == "minimal_request":
            payload = {
                "name": f"test-role-{uuid.uuid4()}"
            }
        elif test_case == "with_users_groups":
            payload = {
                "name": f"test-role-{uuid.uuid4()}",
                "users": [{"name": self.admin["name"], "isAdmin": True}],
                "groups": [{"name": self.group["name"], "isAdmin": False}]
            }
        elif test_case == "with_create_non_exist_user_group":
            param = {"createNonExistUserGroup": "true"}
            payload = {
                "name": f"test-role-{uuid.uuid4()}",
                "users": [{"name": f"newuser_{uuid.uuid4().hex[:6]}", "isAdmin": False}],
                "groups": [{"name": f"newgroup_{uuid.uuid4().hex[:6]}", "isAdmin": False}]
            }

        elif test_case == "with_service_name" :
            param = {"serviceName": service["name"]}
            payload = {
                "name": f"test-role-{uuid.uuid4()}"
            }
        response = requests.post(
            f"{self.base_url}/roles/roles",
            headers=self.headers,
            auth=auth,
            json=payload,
            params=param
        )

        assert_response(response, 200, f"Expected 200 for {test_case} but got {response.status_code}")

        response_data = response.json()
        role_id = response_data["id"]
        print(f"[+] Role created successfully with id={role_id} for test case {test_case}")
        print(f" /n /n Response data: {response_data}.   /n /n")
        delete_role(role_id)

        if test_case == "with_service_name":
            # Cleanup the created service as well
            delete_service(service_id)

        if test_case == "with_create_non_exist_user_group":
            # Cleanup the created user and group
            new_user = payload["users"][0]["name"]
            new_group = payload["groups"][0]["name"]


            requests.delete(
                f"{self.base_url}/xusers/users/userName/{new_user}",
                params={"forceDelete": "true"},
                auth=self.ranger_admin_config,
                headers={**self.headers, "X-Requested-By": "ranger"}
            )
            assert_response(response, [200, 204], f"Expected 200 for user deletion but got {response.status_code}")


            requests.delete(
                f"{self.base_url}/xusers/groups/groupName/{new_group}",
                params={"forceDelete": "true"},
                auth=self.ranger_admin_config,
                headers={**self.headers, "X-Requested-By": "ranger"}
            )
            assert_response(response, [200, 204], f"Expected 200 for group deletion but got {response.status_code}")
    
    @pytest.mark.put
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role", ["admin", "auditor", "keyadmin", "user", "any admin"],
    )
    @pytest.mark.parametrize(
        "test_case",
        ["update via user membership", "update via group membership", "update via role membership - user", "update via role membership - group"]
    )
    def test_update_role(self, request, role, test_case):

        if role == "any admin":
            user, user_id = request.getfixturevalue("temp_secure_user")("admin") 
            auth= self.ranger_admin_config
        else:
            user, user_id = request.getfixturevalue("temp_secure_user")(role)
            auth=(user["name"], "Test@123")

        if test_case == "update via user membership":
            role, r_id = request.getfixturevalue("temp_role")(user_list=[{"name": user["name"], "isAdmin": True}])

        elif test_case == "update via group membership":
            group, group_id = request.getfixturevalue("temp_group")()
            group_list=[{"name": group["name"], "isAdmin": True}]
            assert group_list[0]["isAdmin"] == True, "Group should be admin in this test case"
            role, r_id = request.getfixturevalue("temp_role")(group_list=group_list)
            assign_groups_to_user(user["name"], [group["name"]], self.ranger_admin_config, self.base_url, self.headers)

        elif test_case == "update via role membership - user":
            c_role, c_id = request.getfixturevalue("temp_role")(user_list=[{"name": user["name"], "isAdmin": True}])
            role_list = [{"name": c_role["name"], "isAdmin": True}]
            assert role_list[0]["isAdmin"] == True, "Role should be admin in this test case"
            role, r_id = request.getfixturevalue("temp_role")(role_list=role_list)

        elif test_case == "update via role membership - group":
            group, group_id = request.getfixturevalue("temp_group")()
            c_role, c_id = request.getfixturevalue("temp_role")(group_list=[{"name": group["name"], "isAdmin": True}])
            role_list = [{"name": c_role["name"], "isAdmin": True}]
            assert role_list[0]["isAdmin"] == True, "Role should be admin in this test case"
            role, r_id = request.getfixturevalue("temp_role")(role_list=role_list)
            assign_groups_to_user(user["name"], [group["name"]], self.ranger_admin_config, self.base_url, self.headers)

        
        payload = {
            "id": r_id,
            "name": role["name"],
            "description": f"Updated description for {test_case}",
            "users": [{"name": user["name"], "isAdmin": "false"}],
        }

        response = requests.put(
            f"{self.base_url}/roles/roles/{r_id}",
            headers=self.headers,
            auth=auth,
            json=payload
        )
        assert_response(response, 200, f"Expected 200 for {test_case} but got {response.status_code}")

        data = response.json()
        assert data["description"] == payload["description"], f"Description not updated for {test_case}"
        # Cleanup should be done after the role dependency is removed
        delete_role(r_id)
        if test_case.startswith("update via role membership"):
            delete_role(c_id) 

        if "group" in test_case:
            delete_group(group_id, self.ranger_admin_config, self.base_url, self.headers) 
        
        delete_user(user_id, self.ranger_admin_config, self.base_url, self.headers)


    @pytest.mark.delete
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"),])
    def test_delete_role(self, role, auth, request):

        if role != "admin":
            pytest.fail("Deletion of role requires admin priviliges")
        
        auth = getattr(self, auth)

        role, r_id = request.getfixturevalue("temp_role")()

        response = requests.delete(
            f'{self.base_url}/roles/roles/{r_id}',
            auth = auth,
            headers= self.headers
        )

        assert_response(response, 204, f'Expected the test_case to be returing 204, but got {response.status_code}')

    @pytest.mark.delete
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "test_case", [
            "login user is admin",          
            "login user is service admin",
            "login user has service admin groups",
        ]
    )
    @pytest.mark.parametrize(
        "query_param_user, effective_user, additional_conditions", 
        [
            ("exists", "query param userName", "query user is admin"),
            ("exists", "query param userName", "query user is service admin"),
            ("exists", "query param userName", "query user is in service admin groups in grouplist"),
            ("not_exists", "login userName", "None"),
        ],
    )
    def test_delete_role_by_name(self, test_case, query_param_user, effective_user, additional_conditions, request):
        params = {}
        if test_case == "login user is admin":           
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["admin"])
            auth = (temp_user["name"], "Test@123")
        elif test_case == "login user is service admin":
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["auditor"])
            service, service_id = create_service()
            assign_service_admin(service_id, service, temp_user['name'])
            auth = (temp_user["name"], "Test@123")
            params["serviceName"] = service["name"]
        elif test_case == "login user has service admin groups":
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["auditor"])
            group, group_id = request.getfixturevalue("temp_group")()
            assign_groups_to_user(temp_user["name"], [group["name"]], self.ranger_admin_config, self.base_url, self.headers)
            service, service_id = create_service()
            assign_service_admin_group(service_id, service, group["name"])
            auth = (temp_user["name"], "Test@123")
            params["serviceName"] = service["name"]
        
        if query_param_user == "not_exists":
            pass
        else:
            if "admin" in additional_conditions:
                params["execUser"] = self.admin1["name"]
            elif "service admin" in additional_conditions:
                s_admin, s_admin_id = request.getfixturevalue("temp_secure_user")("auditor")
                # if service admin exists in params use same else create
                if "serviceName" not in params:
                    service, service_id = create_service()
                    params["serviceName"] = service["name"]
                assign_service_admin(service_id, service, s_admin['name'])
                params["execUser"] = s_admin["name"] 
            elif "service admin groups" in additional_conditions:
                s_admin, s_admin_id = request.getfixturevalue("temp_secure_user")("auditor")
                if "serviceName" not in params:
                    service, service_id = create_service()
                    params["serviceName"] = service["name"]
                group, group_id = request.getfixturevalue("temp_group")()
                assign_groups_to_user(s_admin["name"], [group["name"]], self.ranger_admin_config, self.base_url, self.headers)
                assign_service_admin_group(service_id, service, group["name"])
                params["execUser"] = s_admin["name"]      

        role, r_id = request.getfixturevalue("temp_role")()
        response = requests.delete(
            f'{self.base_url}/roles/roles/name/{role["name"]}',
            auth = auth,
            headers= self.headers,
            params=params
        )
        assert_response(response, 204, f'Expected the test_case to be returing 204, but got {response.status_code} with \n response text: {response.text}')
        
        if params.get("serviceName"):
            delete_service(service_id)

    # NEGATIVE TESTS

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "role, auth",
        [("user", "ranger_user_config"), ("auditor", "ranger_auditor_config"), ("keyadmin", "ranger_key_admin_config")],
    )
    def test_get_all_roles_negative(self, role, auth):
        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/roles/roles",
            headers=self.headers,
            auth=auth
        )
        assert response.status_code == 400, f"Expected 400 for {role} but got {response.status_code}"

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case", ["invalid_role_id"])
    def test_get_role_by_id_negative(self, test_case):
        invalid_role_id = 9999999  # Assuming this role ID doesn't exist
        response = requests.get(
            f"{self.base_url}/roles/roles/{invalid_role_id}",
            headers=self.headers,
            auth=self.ranger_admin_config
        )
        assert response.status_code == 400, f"Expected 400 for {test_case} but got {response.status_code}"

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case", [
            "login user is not (admin not service admin/group) - user creds",
            "login user is not (admin not service admin/group) - auditor creds",
            "login user is not (admin not service admin/group) - key admin creds",
            "login user passes but query param user is not admin or service admin/group - user creds",
            "login user passes but query param user is not admin or service admin/group - auditor creds",
            "login user passes but query param user is not admin or service admin/group - key admin creds",
        ])
    def test_get_all_roles_by_name_negative(self, test_case, request):
        params = {}
        if "login user is not (admin not service admin/group)" in test_case:
            if "user creds" in test_case:
                auth = self.ranger_user_config
            elif "auditor creds" in test_case:
                auth = self.ranger_auditor_config
            elif "key admin creds" in test_case:
                auth = self.ranger_key_admin_config
        else:
            auth = self.ranger_admin_config
            if "user creds" in test_case:
                params["execUser"] = self.ranger_user_config[0]
            elif "auditor creds" in test_case:
                params["execUser"] = self.ranger_auditor_config[0]
            elif "key admin creds" in test_case:
                params["execUser"] = self.ranger_key_admin_config[0]
        response = requests.get(
            f"{self.base_url}/roles/roles/names",
            headers=self.headers,
            auth=auth,
            params=params
        )
        assert_response(response, 400, f"Expected 400 for {test_case} but got {response.status_code} with \n response text: {response.text}")

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case", [
            ("unauthorized by non service admin/group - user access"),
            ("unauthorized by non service admin/group - auditor access"),
            ("unauthorized by non service admin/group - key admin access"),
            ("invalid payload - wrong service name in params"),
            ("invalid payload - wrong param user(non exist user)"),
        ]
    )
    def test_get_role_by_name_same_creds_negative(self, test_case, request):

        auth = self.ranger_admin_config  # Default to admin auth, will be overridden for specific negative cases
        if test_case.startswith("unauthorized by non service admin/group"):

            if "user" in test_case:
                auth = self.ranger_user_config
            elif "auditor" in test_case:
                auth = self.ranger_auditor_config
            elif "key admin" in test_case:
                auth = self.ranger_key_admin_config

        name = self.role["name"]
        params = {"execUser": self.admin["name"]}  # Default param for all test cases
        if test_case == "invalid payload - wrong service name in params":
            params= {"serviceName": "invalid_service_name", "execUser": self.admin["name"]}

        response = requests.get(
            f"{self.base_url}/roles/roles/name/{name}",
            auth=auth,
            headers=self.headers,
            params=params
        )
        if "invalid payload" in test_case:
            assert_response(response, 200, "dd")
            data = response.json()
            print(f"Response data for invalid payload case: {data}")
            return
            

        assert_response(response, 400, f"Expected 400 for {test_case} but got {response.status_code} with \n response text: {response.text}")

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "login_test_case", [
            "login user is neither admin nor service admin/group - user creds",
            "login user is neither admin nor service admin/group - auditor creds",
            "login user is neither admin nor service admin/group - key admin creds",
        ]
    )
    def test_get_role_by_name_with_diff_creds_negative_login(self, login_test_case, request):
        auth = self.ranger_admin_config  # Default to admin auth, will be overridden for specific negative cases
        if "user" in login_test_case:
            auth = self.ranger_user_config
        elif "auditor" in login_test_case:
            auth = self.ranger_auditor_config
        elif "key admin" in login_test_case:
            auth = self.ranger_key_admin_config

        role, role_id = request.getfixturevalue("temp_role")()
        params = {"execUser": self.admin["name"]}  # Using admin as execUser in query param

        response = requests.get(
            f"{self.base_url}/roles/roles/name/{role['name']}",
            auth=auth,
            headers=self.headers,
            params=params
        )

        assert_response(response, 400, f"Expected 400 for {login_test_case} but got {response.status_code} with \n response text: {response.text}")


    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "query_user, test_case", [
            #("non exists", "login user exists and it becomes effective user but is service user"),
            ("not exists", "login user passes but query param user is non exist user"),
            ("exists", "login user is admin but query param user is non admin or service admin and has no role membership - user creds"),
            ("exists", "login user is admin but query param user is non admin or service admin and has no role membership - auditor creds"),
            ("exists", "login user is admin but query param user is non admin or service admin and has no role membership - key admin creds"),
        ]
    )
    def test_get_role_by_name_with_diff_creds_negative_query_user(self, query_user, test_case, request):
        auth = self.ranger_admin_config  # Using admin for authentication in these negative test cases
        params = {}
        role, role_id = request.getfixturevalue("temp_role")()

        if query_user == "not exists":
            params["execUser"] = f"nonexistuser_{uuid.uuid4().hex[:6]}"
        else:
            if "user creds" in test_case:
                params["execUser"] = self.ranger_user_config
            elif "auditor creds" in test_case:
                params["execUser"] = self.ranger_auditor_config
            elif "key admin creds" in test_case:
                params["execUser"] = self.ranger_key_admin_config
        
        response = requests.get(
            f"{self.base_url}/roles/roles/name/{role['name']}",
            auth=auth,
            headers=self.headers,
            params=params
        )

        assert_response(response, 400, f"Expected 400 for {test_case} but got {response.status_code} with \n response text: {response.text}")


    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case", [
            "invalid user_name",
        ])
    def test_get_roles_for_user_by_userName_negative(self, test_case):
        auth = self.ranger_admin_config
        invalid_user_name = f"nonexistuser_{uuid.uuid4().hex[:6]}"
        response = requests.get(
            f"{self.base_url}/roles/roles/user/{invalid_user_name}",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 400, f"Expected 400 for {test_case} but got {response.status_code} with \n response text: {response.text}")

    @pytest.mark.post
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, auth_name, expected_status",
        [
            ("duplicate_role_name", "ranger_admin_config", 400),
            ("invalid_member_owner", "ranger_admin_config", 400),
            ("null_role_name", "ranger_admin_config", 400),
            ("blank_role_name", "ranger_admin_config", 400),
            ("no_create_non_exist_user_with_new_user", "ranger_admin_config", 400),
            ("empty_body", "ranger_admin_config", 400),
            ("unauthorized_user", "ranger_user_config", 400),
            ("unauthorized_auditor", "ranger_auditor_config", 400),
            ("unauthorized_key_admin", "ranger_key_admin_config", 400),
            ("wrong_service_name", "ranger_key_admin_config", 400),

        ],
    )
    def test_create_role_negative(self, test_case, auth_name, expected_status, request):
        param = {}
        payload = {}
        auth = getattr(self, auth_name)

        if test_case == "duplicate_role_name":
            temp_role_name = f"test-role-{uuid.uuid4()}"
            requests.post(
                f"{self.base_url}/roles/roles",
                headers=self.headers,
                auth=auth,
                json={"name": temp_role_name}
            )
            payload = {"name": temp_role_name}
            role_to_delete = temp_role_name

        elif test_case == "invalid_member_owner":
            payload = {
                "name": f"test-role-{uuid.uuid4()}",
                "users": [{"name": "{OWNER}", "isAdmin": True}]
            }

        elif test_case == "null_role_name":
            payload = {"name": None}

        elif test_case == "blank_role_name":
            payload = {"name": "   "}

        elif test_case == "no_create_non_exist_user_with_new_user":
            param = {"createNonExistUserGroup": "false"}
            payload = {
                "name": f"test-role-{uuid.uuid4()}",
                "users": [{"name": f"newuser_{uuid.uuid4().hex[:6]}", "isAdmin": False}]
            }

        elif test_case == "empty_body":
            payload = {}

        elif test_case.startswith("unauthorized_user"):
            payload = {"name": f"test-role-{uuid.uuid4()}"}

        elif test_case == "wrong_service_name":
            service, service_id = create_service()
            other_service, _ = create_service()
            s_admin, _ = request.getfixturevalue("temp_secure_user")("auditor")
            assign_service_admin(service_id, service, s_admin['name'])
            auth = (s_admin['name'], "Test@123")
            param = {"serviceName": other_service["name"]}
            payload = {"name": f"test-role-{uuid.uuid4()}"}

        response = requests.post(
            f"{self.base_url}/roles/roles",
            headers=self.headers,
            auth=auth,
            json=payload,
            params=param
        )

        assert_response(response, expected_status, f"Expected {expected_status} for {test_case} but got {response.status_code}")

        if test_case == "wrong_service_name":
            delete_service(service_id)
            delete_service(other_service["id"])
        if test_case == "duplicate_role_name":
            # Cleanup the created role
            roles_resp = requests.get(
                f"{self.base_url}/roles/roles",
                headers=self.headers,
                auth=auth,
                params={"roleName": role_to_delete}
            )
            if roles_resp.status_code == 200 and roles_resp.json().get("roles"):
                role_id = roles_resp.json()["roles"][0]["id"]
                delete_role(role_id)

    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, expected_status", [
            ("update_non_exist_role", 400),
            ("update_via_id_mismatch", 400),
            ("update_via_non_member_global_user", 400),
            ("update_via_non_member_global_auditor", 400),
            ("update_via_non_member_global_keyadmin", 400),
            ("update via invalid payload with out required membership fields", 400),
            ("invalid update of role name when role is linked with other role", 400)
        ]
    )
    def test_update_role_negative(self, test_case, request, expected_status):
        # For all negative test cases we will use admin user to avoid 403 and focus on the specific negative scenario
        auth = self.ranger_admin_config

        if test_case == "update_non_exist_role":
            r_id = 999999  # Assuming this role ID doesn't exist
            payload = {
                "id": r_id,
                "name": f"test-role-{uuid.uuid4()}",
                "description": "Trying to update non-existent role"
            }

        elif test_case == "update_via_id_mismatch":
            role, r_id = request.getfixturevalue("temp_role")()
            payload = {
                "id": r_id + 1,  # Mismatching ID
                "name": role["name"],
                "description": "Trying to update with ID mismatch"
            }

        elif test_case == "update_via_non_member_global_user":
            role, r_id = request.getfixturevalue("temp_role")()
            payload = {
                "id": r_id,
                "name": role["name"],
                "description": "Trying to update via non-member global user"
            }
            auth = self.ranger_user_config  # Non-member global user

        elif test_case == "update_via_non_member_global_auditor":
            role, r_id = request.getfixturevalue("temp_role")()
            payload = {
                "id": r_id,
                "name": role["name"],
                "description": "Trying to update via non-member global auditor"
            }
            auth = self.ranger_auditor_config  # Non-member global auditor

        elif test_case == "update_via_non_member_global_keyadmin":
            role, r_id = request.getfixturevalue("temp_role")()
            payload = {
                "id": r_id,
                "name": role["name"],
                "description": "Trying to update via non-member global keyadmin"
            }
            auth = self.ranger_key_admin_config  # Non-member global keyadmin
        
        elif test_case == "update via invalid payload with out required membership fields":
            role, r_id = request.getfixturevalue("temp_role")()
            payload = {
                "id": r_id,
                # "name is required for update, so we will not provide it to create invalid payload"
                "description": "Trying to update with invalid payload"
            }
        
        elif test_case == "invalid update of role name when role is linked with other role":
            # Create a parent role
            parent_role, parent_role_id = request.getfixturevalue("temp_role")()
            # Create a child role linked to parent role
            child_role, child_role_id = request.getfixturevalue("temp_role")(role_list=[{"name": parent_role["name"], "isAdmin": True}])
            payload = {
                "id": parent_role_id,
                "name": child_role["name"],  # Trying to update parent role name to child role name which should fail due to name conflict
                "description": "Trying to update role name to an existing linked role name"
            }
            r_id = parent_role_id
        
        response = requests.put(
            f"{self.base_url}/roles/roles/{r_id}",
            headers=self.headers,
            auth=auth,
            json=payload
        )
        assert_response(response, expected_status, f"Expected {expected_status} for {test_case}")

        if test_case == "invalid update of role name when role is linked with other role":
            # Cleanup the created roles
            delete_role(child_role_id)
            delete_role(parent_role_id)
        elif test_case != "update_non_exist_role":
            delete_role(r_id)

    @pytest.mark.delete
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, auth_name, expected_status",
        [
            ("delete_non_exist_role", "ranger_admin_config", 400),
            ("unauthorized_user", "ranger_user_config", 400),
            ("unauthorized_auditor", "ranger_auditor_config", 400),
            ("unauthorized_key_admin", "ranger_key_admin_config", 400),
            ("delete role linked with other role and has role membership which should prevent deletion", "ranger_admin_config", 400)
        ]
    )
    def test_delete_role_negative(self, test_case, auth_name, expected_status, request):
        auth = getattr(self, auth_name)

        if test_case == "delete_non_exist_role":
            r_id = 9999999  # Assuming this role ID doesn't exist

        elif test_case.startswith("unauthorized"):
            role, r_id = request.getfixturevalue("temp_role")()

        elif test_case == "delete role linked with other role and has role membership which should prevent deletion":
            # Create a parent role
            parent_role, parent_role_id = request.getfixturevalue("temp_role")()
            # Create a child role linked to parent role
            child_role, child_role_id = request.getfixturevalue("temp_role")(role_list=[{"name": parent_role["name"], "isAdmin": True}])
            r_id = parent_role_id
        response = requests.delete(
            f'{self.base_url}/roles/roles/{r_id}',
            auth = auth,
            headers= self.headers
        )

        assert_response(response, expected_status, f'Expected {expected_status} for {test_case} but got {response.status_code}')

        if test_case == "delete role linked with other role and has role membership which should prevent deletion":
            # Cleanup the created roles
            delete_role(child_role_id)
        if test_case.startswith("unauthorized"):
            # Cleanup the created role
            delete_role(r_id)

    @pytest.mark.delete
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case", [
            "login user is not (admin not service admin/group) - user creds",
            "login user is not (admin not service admin/group) - auditor creds",
            "login user is not (admin not service admin/group) - key admin creds",
            "login user passes but query param user is not admin or service admin/group - user creds",
            "login user passes but query param user is not admin or service admin/group - auditor creds",
            "login user passes but query param user is not admin or service admin/group - key admin creds",
            "all corect but the role is linked with other role and has role membership which should prevent deletion"
        ]
    )
    def test_delete_role_by_name_negative(self, test_case, request):
        

        params = {}
        if "login user is not (admin not service admin/group)" in test_case:
            if "user creds" in test_case:
                auth = self.ranger_user_config
            elif "auditor creds" in test_case:
                auth = self.ranger_auditor_config
            elif "key admin creds" in test_case:
                auth = self.ranger_key_admin_config
        else:
            auth = self.ranger_admin_config
            if "user creds" in test_case:
                params["execUser"] = self.ranger_user_config[0]
            elif "auditor creds" in test_case:
                params["execUser"] = self.ranger_auditor_config[0]
            elif "key admin creds" in test_case:
                params["execUser"] = self.ranger_key_admin_config[0]
        if test_case == "all corect but the role is linked with other role and has role membership which should prevent deletion":
            auth = self.ranger_admin_config
            # parent role
            role, role_id = request.getfixturevalue("temp_role")()
            # parent role linked with child role
            child, child_id = request.getfixturevalue("temp_role")(role_list=[{"name": role["name"], "isAdmin": True}])
        else:  
            role, role_id = request.getfixturevalue("temp_role")()

        response = requests.delete(
            f'{self.base_url}/roles/roles/name/{role["name"]}',
            auth = auth,
            headers= self.headers,
            params=params
        )
        assert_response(response, 400, f"Expected 400 for {test_case} but got {response.status_code} with \n response text: {response.text}")
        
        if test_case == "all corect but the role is linked with other role and has role membership which should prevent deletion":
            # Cleanup the created roles
            delete_role(child_id)
  
        delete_role(role_id)