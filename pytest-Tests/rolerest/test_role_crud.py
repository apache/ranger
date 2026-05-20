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
            ({"startIndex": 5, "pageSize": 5}, "pagination_offset"),
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
            "pagination-offset",
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
        assert response.status_code == 200, f"Failed to get all roles and got response code {response.status_code}"


        if "pageSize" in params:
            assert len(response.json().get("roles", [])) <= params["pageSize"]

        if "roleName" in params:
            roles = response.json().get("roles", [])
            for r in roles:
                assert params["name"].lower() in r["name"].lower(), \
                    f"Role {r['name']} doesn't match filter {params['roleName']}"
                

    @pytest.mark.post
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth, test_case",
        [("admin", "ranger_admin_config", "minimal_request"),
         ("admin", "ranger_admin_config", "with_users_groups"), 
         ("admin", "ranger_admin_config", "with_create_non_exist_user_group"),
         ("service_admin","","with_service_name")],
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
        role_id = response_data.get("id")
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

