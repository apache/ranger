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
import io
import os
import json
import uuid
import string
import pytest
import requests
from datetime import datetime
import random


@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.rolerest
class TestRoleUtilityFun:
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
        "auth", ["ranger_admin_config"])
    def test_get_rokes_exportJson(self, auth):
        auth_creds = getattr(self, auth)
        export_filename = "roles_export.json"
        
        response = requests.get(
            f"{self.base_url}/roles/roles/exportJson",
            headers=self.headers,
            auth=auth_creds
        )
        
        assert_response(response, [200, 204], f"Expected 200 or 204 for exporting roles but got {response.status_code}")
        
        # Save the response to a file, mimicking the curl -o behavior
        with open(export_filename, "wb") as f:
            f.write(response.content)
            
        try:
            if response.status_code == 200:
                with open(export_filename, "r") as f:
                    data = json.load(f)
                    assert "roles" in data, "Exported JSON should contain 'roles' key"
                assert isinstance(data["roles"], list), "'roles' should be a list in the exported file"
            
            elif response.status_code == 204:
                # No roles exist in the system, and we get a 204. The downloaded file should be exactly 0 bytes.
                assert os.path.getsize(export_filename) == 0, "Expected empty file for 204 status code"
        
        finally:
            if os.path.exists(export_filename):
                os.remove(export_filename)


    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth, service_name",
        [
            ("admin", "ranger_admin_config", "dev_hdfs"),
            ("key_admin", "ranger_key_admin_config", "dev_kms"),
        ])
    @pytest.mark.parametrize(
        "test_case, last_known_version, expected_status",
        [
            ("valid-service-no-change", -1, 304),
            ("valid-service-with-update", 0, 200),
            ("valid-service-old-version", 1, 200),
        ])
    def test_get_secure_roles_download_by_serviceName(self, test_case, auth, service_name, last_known_version, expected_status, role):

        auth = getattr(self, auth)

        base_params = {
            "lastActivationTime": 0,
            "clusterName": "mycluster",
            "pluginId": "hdfs@host1"
        }

        if test_case == "valid-service-no-change":
            # Fetch the current role version to simulate a "no-change" 304 scenario
            seed_response = requests.get(
                f"{self.base_url}/roles/secure/download/{service_name}",
                params={
                    **base_params,
                    "lastKnownRoleVersion": -1
                },
                auth=auth,
                headers=self.headers
            )

            assert seed_response.status_code == 200, \
                f"Seed request failed: {seed_response.status_code}"

            # Extract roleVersion from RangerRoles object
            last_known_version = seed_response.json().get("roleVersion")

        response = requests.get(
            f"{self.base_url}/roles/secure/download/{service_name}",
            params={
                **base_params,
                "lastKnownRoleVersion": last_known_version
            },
            auth=auth,
            headers=self.headers
        )

        assert_response(response, expected_status, test_case)

        if expected_status == 200:
            data = response.json()

            assert "roleVersion" in data, "Response missing 'roleVersion'"
            assert "rangerRoles" in data, "Response missing 'rangerRoles'"
            
            assert data.get("serviceName") == service_name, f"Expected serviceName {service_name}"



    # @pytest.mark.get
    # @pytest.mark.positive
    # @pytest.mark.parametrize(
    #     "role, auth",
    #     [
    #         ("admin", "ranger_admin_config"),
    #         ("key_admin", "ranger_key_admin_config"),
    #         ("auditor", "ranger_auditor_config"),
    #         ("user", "ranger_user_config"),
    #     ]
    # )
    # @pytest.mark.parametrize(
    #     "test_case, last_known_version, expected_status",
    #     [
    #         ("valid-service-no-change", -1, 304), # will be updated in future
    #         ("valid-service-with-update", 0, 200),
    #         ("valid-service-old-version", 1, 200),
    #     ])
    # def test_get_download_by_serviceName(self, test_case, auth, last_known_version, expected_status, role):
    #     auth = getattr(self, auth)
    #     service_name = "dev_hdfs"  # valid existing service

    #     if test_case == "valid-service-no-change":
    #         seed_response = requests.get(
    #             f"{self.base_url}/roles/download/{service_name}",
    #             params={"lastKnownUserStoreVersion": -1, "lastActivationTime": 0,
    #                 "clusterName": "mycluster", "pluginId": "hdfs@host1"},
    #             auth=auth,
    #             headers=self.headers
    #         )
    #         assert seed_response.status_code == 200, f"Seed request failed: {seed_response.status_code}"
    #         current_version = seed_response.json().get("userStoreVersion")
    #         last_known_version = current_version  # now use the actual version → should get 304

    #     params = {
    #         "lastKnownUserStoreVersion": last_known_version,
    #         "lastActivationTime": 0,
    #         "clusterName": "mycluster",
    #         "pluginId": f"hdfs@host1"
    #     }

    #     response = requests.get(
    #         f"{self.base_url}/roles/download/{service_name}",
    #         params=params,
    #         auth=auth,
    #         headers=self.headers
    #     )
    #     assert_response(response, expected_status, test_case)
    #     if expected_status == 200:
    #         data = response.json()
    #         assert "userStoreVersion" in data, f"Response missing 'userStoreVersion' for {test_case}"
    #         assert "userGroupMapping" in data or "users" in data, f"Response missing user/group data for {test_case}"
        


    @pytest.mark.post
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "auth", ["ranger_admin_config"]
    )
    @pytest.mark.parametrize(
        "test_case",
        [
            "create new role",
            "update existing role - updateIfExists=true",
            "skip existing role modified - updateIfExists=false",
            "skip existing role identical - updateIfExists=true",
        ]
    )
    def test_post_import_roles_from_file(self, request, auth, test_case):
        auth_creds = getattr(self, auth)
        r_id = None
        new_role_name = "test_imported_role_new_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
        flg, _ = get_role_by_name(new_role_name, auth_creds) 
        assert not flg, f"Role with name {new_role_name} already exists, cannot proceed with the test case for creating new role."
        
        import_payload = {
            "metaDataInfo": {
                "Host name": "test-host",
                "Exported by": "test-admin"
            },
            "roles": []
        }

        params = {
            "createNonExistUserGroupRole": "true"
        }

        if test_case == "create new role":
            params["updateIfExists"] = "false"
            import_payload["roles"].append({
                "name": new_role_name,
                "users": [{"name": "admin", "isAdmin": False}],
                "groups": [],
                "roles": []
            })

        elif "existing role" in test_case:
            role, r_id = request.getfixturevalue("temp_role")(
                user_list=[{"name": "admin", "isAdmin": False}]
            )
            
            # FIX: Copy the full role object so Java's .equals() matches correctly
            role_payload = role.copy()
            role_payload["id"] = r_id

            if "identical" not in test_case:
                # Safely append user if we want to intentionally trigger an update
                if "users" not in role_payload:
                    role_payload["users"] = []
                role_payload["users"].append({"name": "test_user_new", "isAdmin": True})

            import_payload["roles"].append(role_payload)

            if "updateIfExists=true" in test_case:
                params["updateIfExists"] = "true"
            else:
                params["updateIfExists"] = "false"

        req_headers = self.headers.copy()

        # Cleaned up header manipulation
        if "Content-Type" in req_headers:
            del req_headers["Content-Type"]
            
        req_headers["Accept"] = "application/json"

        json_str = json.dumps(import_payload)
        file_obj = io.BytesIO(json_str.encode('utf-8'))

        files = {
            'file': ('roles.json', file_obj, 'application/json')
        }

        response = requests.post(
            f"{self.base_url}/roles/roles/importRolesFromFile",
            headers=req_headers,
            auth=auth_creds,
            params=params,
            files=files
        )
        
        assert_response(response, 200, f"Expected 200 for {test_case} but got {response.status_code}")

        data_n = response.json()
        print(data_n)

        # FIX: Parse the msgDesc safely by checking the keys, avoiding index assumption errors
        data = {'Total Role Created': 0, 'Total Role Updated': 0, 'Total Role Unchanged': 0}
        msg_parts = data_n.get('msgDesc', '').split(',')
        
        for part in msg_parts:
            if 'Total Role Created' in part:
                data['Total Role Created'] = int(part.split('=')[1].strip())
            elif 'Total Role Updated' in part:
                data['Total Role Updated'] = int(part.split('=')[1].strip())
            elif 'Total Role Unchanged' in part:
                data['Total Role Unchanged'] = int(part.split('=')[1].strip())

        if test_case == "create new role":
            assert data['Total Role Created'] == 1, f"Expected 1 role to be created but got {data['Total Role Created']}"
            get_role_by_name_flg, role_data = get_role_by_name(new_role_name, auth_creds)
            assert get_role_by_name_flg, "Newly created role not found when fetched by name."
            assert role_data["name"] == new_role_name, f"Expected role name to be {new_role_name} but got {role_data['name']}"
            delete_role(role_data["id"])
        
        elif "update existing role - updateIfExists=true" in test_case:
            assert data['Total Role Updated'] == 1, f"Expected 1 role to be updated but got {data['Total Role Updated']}"
            get_role_by_name_flg, role_data = get_role_by_name(role["name"], auth_creds)
            assert get_role_by_name_flg, "Updated role not found when fetched by name."
            returned_users = {u["name"]: u.get("isAdmin", False) for u in role_data.get("users", [])}
            assert "test_user_new" in returned_users, "New user added in the update payload is not present in the updated role."
            assert returned_users["test_user_new"] is True, "Admin privilege for the new user should be true as per the update payload."
        
        elif "skip existing role modified - updateIfExists=false" in test_case:
            assert data['Total Role Updated'] == 0, f"Expected 0 roles to be updated but got {data['Total Role Updated']}"
            get_role_by_name_flg, role_data = get_role_by_name(role["name"], auth_creds)
            assert get_role_by_name_flg, "Existing role not found when fetched by name."
            returned_users = {u["name"]: u.get("isAdmin", False) for u in role_data.get("users", [])}
            assert "test_user_new" not in returned_users, "User that was supposed to be added in the update payload is present even though updateIfExists was false."
        
        elif "skip existing role identical - updateIfExists=true" in test_case:
            assert data['Total Role Updated'] == 0, f"Expected 0 roles to be updated but got {data['Total Role Updated']}"


    @pytest.mark.put
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "auth", ["ranger_admin_config"]
    )
    @pytest.mark.parametrize(
        "isAdmin", ["true", "false"]
    )
    @pytest.mark.parametrize(
        "test_case",
        [
            "existing users are admin",
            "existing groups are admin",
            "existing users are not admin",
            "existing groups are not admin",
        ]
    )
    def test_put_role_add_users_and_groups(self, request, auth, isAdmin, test_case):
        auth_creds = getattr(self, auth)
        user_id = None
        group_id = None
        param_user_id = None
        param_group_id = None
        params = {"isAdmin": isAdmin}

        if "user" in test_case:
            user, user_id = request.getfixturevalue("temp_secure_user")("user")
            is_initial_admin = False if "not admin" in test_case else True

            role, r_id = request.getfixturevalue("temp_role")(
                user_list=[{"name": user["name"], "isAdmin": is_initial_admin}]
            )
            param_user, param_user_id = request.getfixturevalue("temp_secure_user")("user")
            params["users"] = [param_user["name"], user["name"]]
            
        elif "group" in test_case:
            group, group_id = request.getfixturevalue("temp_group")()
            is_initial_admin = False if "not admin" in test_case else True
                
            role, r_id = request.getfixturevalue("temp_role")(
                group_list=[{"name": group["name"], "isAdmin": is_initial_admin}]
            )
            param_group, param_group_id = request.getfixturevalue("temp_group")()
            params["groups"] = [param_group["name"], group["name"]]

        response = requests.put(
            f"{self.base_url}/roles/roles/{r_id}/addUsersAndGroups",
            headers=self.headers,
            auth=auth_creds,
            params=params,
            json={} 
        )
        
        assert_response(response, 200, f"Expected 200 for {test_case} but got {response.status_code}")

        data = response.json()
        returned_users = [u["name"] for u in data.get("users", [])]
        returned_groups = [g["name"] for g in data.get("groups", [])]
        
        if "user" in test_case:
            if isAdmin == 'false':
                # drops the existing user because grantAdmin is false, 
                # and skips adding them again because they are in existingUsernames.
                assert len(returned_users) == 1, \
                    f"Expected only 1 user in the updated role but got {len(returned_users)}"
                assert param_user["name"] in returned_users, "New user was not added."
                assert user["name"] not in returned_users, "Existing user should have been dropped."
            else:
                # retains the existing user and adds the new user.
                assert len(returned_users) == 2, \
                    f"Expected 2 users in the updated role but got {len(returned_users)}"
                assert param_user["name"] in returned_users and user["name"] in returned_users, \
                    "Expected both the new and existing user to be present."

        if "group" in test_case:
            # for groups simply loops through all effectiveGroups and adds them unconditionally.
            # Since both groups are passed in params, both will always be returned.
            assert len(returned_groups) == 2, \
                f"Expected 2 groups in the updated role but got {len(returned_groups)}"
            assert param_group["name"] in returned_groups and group["name"] in returned_groups, \
                "Expected both the new and existing group to be present."
            
    
    @pytest.mark.put
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "auth", ["ranger_admin_config"]
    )
    @pytest.mark.parametrize(
        "test_case",
        [
            "remove existing user",
            "remove existing group",
            "remove non-existent user",
            "remove non-existent group"
        ]
    )
    def test_put_role_remove_users_and_groups(self, request, auth, test_case):
        auth_creds = getattr(self, auth)
        params = {}

        if "user" in test_case:
            user, _ = request.getfixturevalue("temp_secure_user")("user")

            role, r_id = request.getfixturevalue("temp_role")(
                user_list=[{"name": user["name"], "isAdmin": True}]
            )
            if "non-existent" in test_case:
                params["users"] = ["some_fake_user_name"]
            else:
                params["users"] = [user["name"]]
            
        elif "group" in test_case:
            group, _ = request.getfixturevalue("temp_group")()
                
            role, r_id = request.getfixturevalue("temp_role")(
                group_list=[{"name": group["name"], "isAdmin": True}]
            )
            
            if "non-existent" in test_case:
                params["groups"] = ["some_fake_group_name"]
            else:
                params["groups"] = [group["name"]]

        response = requests.put(
            f"{self.base_url}/roles/roles/{r_id}/removeUsersAndGroups",
            headers=self.headers,
            auth=auth_creds,
            params=params,
            json={} 
        )
        
        assert_response(response, 200, f"Expected 200 for {test_case} but got {response.status_code}")

        data = response.json()
        returned_users = [u["name"] for u in data.get("users", [])]
        returned_groups = [g["name"] for g in data.get("groups", [])]
        
        if "user" in test_case:
            if "non-existent" in test_case:
                assert user["name"] in returned_users, "Original user should remain if a different user was removed."
            else:
                assert user["name"] not in returned_users, "Existing user should have been removed from the role."

        if "group" in test_case:
            if "non-existent" in test_case:
                assert group["name"] in returned_groups, "Original group should remain if a different group was removed."
            else:
                assert group["name"] not in returned_groups, "Existing group should have been removed from the role."


    @pytest.mark.put
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "auth", ["ranger_admin_config"]
    )
    @pytest.mark.parametrize(
        "test_case",
        [
            "existing user is admin",
            "existing user is not admin",
            "existing group is admin",
            "existing group is not admin",
        ]
    )
    def test_put_role_remove_admin_from_users_and_groups(self, request, auth, test_case):
        auth_creds = getattr(self, auth)
        params = {}
        if "user" in test_case:
            user, _ = request.getfixturevalue("temp_secure_user")("user")
            is_initial_admin = False if "not admin" in test_case else True

            role, r_id = request.getfixturevalue("temp_role")(
                user_list=[{"name": user["name"], "isAdmin": is_initial_admin}]
            )
            params["users"] = [user["name"]]
            
        elif "group" in test_case:
            group, _ = request.getfixturevalue("temp_group")()
            is_initial_admin = False if "not admin" in test_case else True
                
            role, r_id = request.getfixturevalue("temp_role")(
                group_list=[{"name": group["name"], "isAdmin": is_initial_admin}]
            )
            params["groups"] = [group["name"]]

        response = requests.put(
            f"{self.base_url}/roles/roles/{r_id}/removeAdminFromUsersAndGroups",
            headers=self.headers,
            auth=auth_creds,
            params=params,
            json={} 
        )
        
        assert_response(response, 200, f"Expected 200 for {test_case} but got {response.status_code}")
        data = response.json()
        
        returned_users = {u["name"]: u.get("isAdmin", False) for u in data.get("users", [])}
        returned_groups = {g["name"]: g.get("isAdmin", False) for g in data.get("groups", [])}
        
        if "user" in test_case:
            assert user["name"] in returned_users, "User should not be removed from the role, only modified."
            assert returned_users[user["name"]] is False, "Admin privilege should be false after removal."

        if "group" in test_case:
            assert group["name"] in returned_groups, "Group should not be removed from the role, only modified."
            assert returned_groups[group["name"]] is False, "Admin privilege should be false after removal."

    @pytest.mark.put
    @pytest.mark.positive
    # @pytest.mark.parametrize( # since all thses are just variations of login user we are not testing all
    #     "test_case", [
    #         "login user is admin",          
    #         "login user is service admin",
    #         "login user has service admin groups",
    #     ]
    # )
    # @pytest.mark.parametrize(
    #     "grantOption", ["true", "false"] # to reduce the number of combinations randomly assigned 
    # )
    # @pytest.mark.parametrize(
    #     "payload_test_case", 
    #     ["users adition", "groups addition", "roles addition"],  # to cover all 3 possibilities of adding users/groups/roles in the payload in different test runs without combinatorial explosion of test cases
    # )
    @pytest.mark.parametrize(
        "test_case, query_param_user, effective_user, additional_conditions, grantOption, payload_test_case",  
        [
            ("login user is service admin", "exists", "query param userName", "query user is admin", "true", "users adition"),
            ("login user has service admin groups", "exists", "query param userName", "query user is admin", "true", "groups addition"),            
            ("login user is admin","exists", "query param userName", "query user is admin", "true", "roles addition"),
            ("login user is service admin", "exists", "query param userName", "query user is service admin", "true", "users adition"),
            ("login user has service admin groups","exists", "query param userName", "query user is in service admin groups in grouplist", "true", "groups addition"),
            ("login user is admin","exists", "query param userName", "query user is not admin or service admin/group but has role membership via user", "true", "roles addition"),
            ("login user is admin","exists", "query param userName", "query user is not admin or service admin/group but has role membership via group", "true", "groups addition"),
            ("login user is admin","exists", "query param userName", "query user is not admin or service admin/group but has role membership via role-user", "true", "roles addition"),
            ("login user is admin","exists", "query param userName", "query user is not admin or service admin/group but has role membership via role-group", "true", "groups addition"),
        ],
    )
    def test_put_role_grant(self, request, test_case, query_param_user, effective_user, additional_conditions, grantOption, payload_test_case):
        param = {}
        service = None
        service_id = None


        if test_case == "login user is admin":           
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["admin"])
            auth = (temp_user["name"], "Test@123")

        elif test_case == "login user is service admin": 

            service, service_id = create_service()
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["auditor"])
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

        if 'serviceName' in params:
            path_param = params['serviceName']
        else:
            path_param = 'nonexistentService'
        
        if 'execUser' in params:
            payload = {
                'grantor' : params['execUser'],
            }      
        if payload_test_case == "users adition":
            payload_user, payload_user_id = request.getfixturevalue("temp_secure_user")()
            payload["users"] = [payload_user["name"]]
            clean_up_items["user_list"] = [payload_user_id]
        elif payload_test_case == "groups addition":
            payload_group, payload_group_id = request.getfixturevalue("temp_group")()
            payload["groups"] = [payload_group["name"]]
            clean_up_items["group_list"] = [payload_group_id]
        elif payload_test_case == "roles addition":
            payload_role, payload_role_id = request.getfixturevalue("temp_role")()
            payload["roles"] = [payload_role["name"]]
            #clean_up_items["role_list"] = [payload_role_id]
        
        payload["grantOption"] = grantOption
        payload["targetRoles"] = [role["name"]]

        print("\n Auth used: ", auth)
        print("\n payload used: ", payload)
        response = requests.put(
            f"{self.base_url}/roles/roles/grant/{path_param}",
            auth=auth,  # auth is admin but execUser in query param has service admin group permissions
            headers=self.headers,
            json = payload
        )

        assert_response(response, 200, f"Failed to update roles and got response code {response.status_code} with \n response text: {response.text}")


        if payload_test_case == "users adition":
            get_role_by_name_flg, role_data = get_role_by_name(role["name"])
            assert get_role_by_name_flg, "Role not found when fetched by name after granting role to user."
            returned_users = {u["name"]: u.get("isAdmin", False) for u in role_data.get("users", [])}
            assert payload_user["name"] in returned_users, "New user added in the grant payload is not present in the updated role."
        elif payload_test_case == "groups addition":
            get_role_by_name_flg, role_data = get_role_by_name(role["name"])
            assert get_role_by_name_flg, "Role not found when fetched by name after granting role to group."
            returned_groups = {g["name"]: g.get("isAdmin", False) for g in role_data.get("groups", [])}
            assert payload_group["name"] in returned_groups, "New group added in the grant payload is not present in the updated role."
        elif payload_test_case == "roles addition":
            get_role_by_name_flg, role_data = get_role_by_name(role["name"])
            assert get_role_by_name_flg, "Role not found when fetched by name after granting role to role."
            returned_roles = [r["name"] for r in role_data.get("roles", [])]
            assert payload_role["name"] in returned_roles, "New role added in the grant payload is not present in the updated role."

        
        # # Cleanup
        if service_id is not None and service_id not in clean_up_items.get("service_list", []):
            clean_up_items["service_list"].append(service_id)

        # 1. Delete the target roles first (removes the references)
        for item in clean_up_items.get("role_list", []):
            delete_role(item)
            
        # 2. Delete the payload role now that it's no longer referenced
        if payload_test_case == "roles addition":
            delete_role(payload_role_id)
            
        # 3. Clean up services
        for item in clean_up_items.get("service_list", []):
            delete_service(item)

    @pytest.mark.put
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "test_case, query_param_user, effective_user, additional_conditions, grantOption, payload_test_case",  
        [
            ("login user is service admin", "exists", "query param userName", "query user is admin", "true", "users adition"),
            ("login user has service admin groups", "exists", "query param userName", "query user is admin", "true", "groups addition"),            
            ("login user is admin","exists", "query param userName", "query user is admin", "true", "roles addition"),
            ("login user is service admin", "exists", "query param userName", "query user is service admin", "false", "users adition"), # Switched to false to test complete removal
            ("login user has service admin groups","exists", "query param userName", "query user is in service admin groups in grouplist", "false", "groups addition"),
            ("login user is admin","exists", "query param userName", "query user is not admin or service admin/group but has role membership via user", "false", "roles addition"),
            ("login user is admin","exists", "query param userName", "query user is not admin or service admin/group but has role membership via group", "true", "groups addition"),
            ("login user is admin","exists", "query param userName", "query user is not admin or service admin/group but has role membership via role-user", "true", "roles addition"),
            ("login user is admin","exists", "query param userName", "query user is not admin or service admin/group but has role membership via role-group", "true", "groups addition"),
        ],
    )
    def test_put_role_revoke(self, request, test_case, query_param_user, effective_user, additional_conditions, grantOption, payload_test_case):
        param = {}
        service = None
        service_id = None

        if test_case == "login user is admin":           
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["admin"])
            auth = (temp_user["name"], "Test@123")

        elif test_case == "login user is service admin": 
            service, service_id = create_service()
            temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")(["auditor"])
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

        if 'serviceName' in params:
            path_param = params['serviceName']
        else:
            path_param = 'nonexistentService'
        
        if 'execUser' in params:
            payload = {
                'grantor' : params['execUser'],
            }      
            
        if payload_test_case == "users adition":
            payload_user, payload_user_id = request.getfixturevalue("temp_secure_user")()
            payload["users"] = [payload_user["name"]]
            clean_up_items["user_list"] = [payload_user_id]
        elif payload_test_case == "groups addition":
            payload_group, payload_group_id = request.getfixturevalue("temp_group")()
            payload["groups"] = [payload_group["name"]]
            clean_up_items["group_list"] = [payload_group_id]
        elif payload_test_case == "roles addition":
            payload_role, payload_role_id = request.getfixturevalue("temp_role")()
            payload["roles"] = [payload_role["name"]]
        
        payload["targetRoles"] = [role["name"]]

        pre_grant_payload = payload.copy()
        pre_grant_payload["grantOption"] = "true" 
        
        requests.put(
            f"{self.base_url}/roles/roles/grant/{path_param}",
            auth=auth,
            headers=self.headers,
            json=pre_grant_payload
        )

        #  REVOKE STEP 
        payload["grantOption"] = grantOption

        print("\n Auth used: ", auth)
        print("\n payload used: ", payload)
        response = requests.put(
            f"{self.base_url}/roles/roles/revoke/{path_param}",
            auth=auth,  
            headers=self.headers,
            json=payload
        )

        assert_response(response, 200, f"Failed to revoke roles and got response code {response.status_code} with \n response text: {response.text}")

 
        get_role_by_name_flg, role_data = get_role_by_name(role["name"])
        assert get_role_by_name_flg, "Role not found when fetched by name after revoking role."
        
        is_grant_option_true = grantOption.lower() == "true"

        if payload_test_case == "users adition":
            returned_users = {u["name"]: u.get("isAdmin", False) for u in role_data.get("users", [])}
            if is_grant_option_true:
                assert payload_user["name"] in returned_users, "User should remain in role, but have isAdmin revoked."
                assert not returned_users[payload_user["name"]], "User's isAdmin flag should be False after revoke."
            else:
                assert payload_user["name"] not in returned_users, "User should be completely removed from the role."

        elif payload_test_case == "groups addition":
            returned_groups = {g["name"]: g.get("isAdmin", False) for g in role_data.get("groups", [])}
            if is_grant_option_true:
                assert payload_group["name"] in returned_groups, "Group should remain in role, but have isAdmin revoked."
                assert not returned_groups[payload_group["name"]], "Group's isAdmin flag should be False after revoke."
            else:
                assert payload_group["name"] not in returned_groups, "Group should be completely removed from the role."

        elif payload_test_case == "roles addition":
            returned_roles = {r["name"]: r.get("isAdmin", False) for r in role_data.get("roles", [])}
            if is_grant_option_true:
                assert payload_role["name"] in returned_roles, "Role should remain in target role, but have isAdmin revoked."
                assert not returned_roles[payload_role["name"]], "Role's isAdmin flag should be False after revoke."
            else:
                assert payload_role["name"] not in returned_roles, "Role should be completely removed from the target role."

        if service_id is not None and service_id not in clean_up_items.get("service_list", []):
            clean_up_items["service_list"].append(service_id)

        for item in clean_up_items.get("role_list", []):
            delete_role(item)
            
        if payload_test_case == "roles addition":
            delete_role(payload_role_id)
            
        for item in clean_up_items.get("service_list", []):
            delete_service(item)       
        
        
            
    # NEGATIVE TEST CASES

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "auth", ["ranger_key_admin_config", "ranger_auditor_config", "ranger_user_config"]
    )
    def test_get_rokes_exportJson_unauthorized(self, auth): 
        auth_creds = getattr(self, auth)
        
        response = requests.get(
            f"{self.base_url}/roles/roles/exportJson",
            headers=self.headers,
            auth=auth_creds
        )
        
        assert_response(response, 403, f"Expected 403 for unauthorized user but got {response.status_code}")


    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, auth, service_name, expected_status",
        [
            ("invalid-service-name", "ranger_admin_config", "nonExistentService_XYZ", 404),
            ("empty-service-name", "ranger_admin_config", "", 404),
            ("admin-accessing-kms", "ranger_admin_config", "dev_kms", 403),
            ("unauthorized-auditor-access", "ranger_auditor_config", "dev_kms", 403),
            ("unauthorized-user-access", "ranger_user_config", "dev_kms", 400),
            ("keyadmin-nonkms-access", "ranger_key_admin_config", "dev_hdfs", 400),
        ]
    )
    def test_get_secure_roles_download_by_serviceName_negative(self, test_case, auth, service_name, expected_status):

        auth_credentials = getattr(self, auth)

        params = {
            "lastKnownUserStoreVersion": -1,
            "lastActivationTime": 0,
            "clusterName": "mycluster",
        }

        if test_case != "missing-plugin-id":
            params["pluginId"] = "hdfs@host1"

        response = requests.get(
            f"{self.base_url}/roles/secure/download/{service_name}",
            params=params,
            auth=auth_credentials,
            headers=self.headers
        )

        assert_response(response, expected_status, test_case)

    # @pytest.mark.get
    # @pytest.mark.negative
    # @pytest.mark.parametrize(
    #     "test_case, auth, expected_status",
    #     [
    #         #("unauthorized-access-user", "ranger_user_config", 403),
    #         ("invalid-service-name", "ranger_admin_config", 404),
    #         ("empty-service-name", "ranger_admin_config", 404),
    #         ("missing-plugin-id", "ranger_admin_config", 200),
    #     ]
    # )
    # def test_get_download_by_serviceName_negative(self, test_case, auth, expected_status):
    #     auth_credentials = getattr(self, auth)

    #     if test_case == "invalid-service-name":
    #         service_name = "nonExistentService_XYZ"
    #     elif test_case == "empty-service-name":
    #         service_name = ""
    #     else:
    #         service_name = "dev_hdfs"  # Assuming this is your valid seed service


    #     params = {
    #         "lastKnownUserStoreVersion": -1,
    #         "lastActivationTime": 0,
    #         "clusterName": "mycluster",
    #     }

    #     if test_case != "missing-plugin-id":
    #         params["pluginId"] = "hdfs@host1"

    #     response = requests.get(
    #         f"{self.base_url}/xusers/download/{service_name}",
    #         params=params,
    #         auth=auth_credentials,
    #         headers=self.headers
    #     )

    #     assert_response(response, expected_status, test_case)


    @pytest.mark.post
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "auth", ["ranger_key_admin_config", "ranger_auditor_config", "ranger_user_config"]
    )
    def test_post_import_roles_from_file_unauthorized(self, auth):
        auth_creds = getattr(self, auth)
        new_role_name = "test_imported_role_new"
        
        import_payload = {
            "metaDataInfo": {
                "Host name": "test-host",
                "Exported by": "test-admin"
            },
            "roles": [{
                "name": new_role_name,
                "users": [{"name": "admin", "isAdmin": False}],
                "groups": [],
                "roles": []
            }]
        }

        req_headers = self.headers.copy()
        
        if "Content-Type" in req_headers:
            del req_headers["Content-Type"]
            
        req_headers["Accept"] = "application/json"

        json_str = json.dumps(import_payload)
        file_obj = io.BytesIO(json_str.encode('utf-8'))

        files = {
            'file': ('roles.json', file_obj, 'application/json')
        }

        response = requests.post(
            f"{self.base_url}/roles/roles/importRolesFromFile",
            headers=req_headers,
            auth=auth_creds,
            params={"createNonExistUserGroupRole": "true", "updateIfExists": "false"},
            files=files
        )
        
        assert_response(response, 403, f"Expected 403 for unauthorized user but got {response.status_code}")

    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "auth", ["ranger_key_admin_config", "ranger_auditor_config", "ranger_user_config"]
    )
    def test_put_role_add_users_and_groups_unauthorized(self, auth):
        auth_creds = getattr(self, auth)
        role, r_id = self.role, self.role_id

        response = requests.put(
            f"{self.base_url}/roles/roles/{r_id}/addUsersAndGroups",
            headers=self.headers,
            auth=auth_creds,
            params={"isAdmin": "true", "users": [self.user["name"]], "groups": [self.group["name"]]},
            json={}
        )
        
        assert_response(response, 400, f"Expected 400 for unauthorized user but got {response.status_code}")
    
    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "auth", ["ranger_key_admin_config", "ranger_auditor_config", "ranger_user_config"]
    )
    def test_put_role_remove_users_and_groups_unauthorized(self, auth):
        auth_creds = getattr(self, auth)
        r_id = self.role_id

        response = requests.put(
            f"{self.base_url}/roles/roles/{r_id}/removeUsersAndGroups",
            headers=self.headers,
            auth=auth_creds,
            params={"users": [self.user["name"]], "groups": [self.group["name"]]},
            json={}
        )
        
        assert_response(response, 400, f"Expected 400 for unauthorized user but got {response.status_code}")

    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "login_test_case", [
            "login user is neither admin nor service admin/group - user creds",
            "login user is neither admin nor service admin/group - auditor creds",
            "login user is neither admin nor service admin/group - key admin creds",
        ]
    )
    def test_put_role_grant_with_diff_creds_negative_login(self, login_test_case, request):
        auth = self.ranger_admin_config  # Default to admin auth, will be overridden for specific negative cases
        if "user" in login_test_case:
            auth = self.ranger_user_config
        elif "auditor" in login_test_case:
            auth = self.ranger_auditor_config
        elif "key admin" in login_test_case:
            auth = self.ranger_key_admin_config

        role, role_id = request.getfixturevalue("temp_role")()
        params = {"execUser": self.admin["name"]}  # Using admin as execUser in query param

        payload = {
            "grantor": self.admin["name"],
            "users": [self.user["name"]],
            "groups": [self.group["name"]],
            "roles": [],
            "grantOption": "true",
            "targetRoles": [role["name"]]
        }

        try:
            response = requests.put(
                f"{self.base_url}/roles/roles/grant/nonexistentService",
                auth=auth,
                headers=self.headers,
                params=params, 
                json=payload
            )

            assert_response(response, 400, f"Expected 400 for {login_test_case} but got {response.status_code} with \n response text: {response.text}")
        finally:
            delete_role(role_id) 


    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "query_user, test_case", [
            ("non exists", "login user exists and it becomes effective user but is service user"),
            ("not exists", "login user is admin but query param user is non exist user"),
            ("exists", "login user is admin but query param user is non admin or service admin and has no role membership - user creds"),
            ("exists", "login user is admin but query param user is non admin or service admin and has no role membership - auditor creds"),
            ("exists", "login user is admin but query param user is non admin or service admin and has no role membership - key admin creds"),
        ])
    def test_put_role_grant_with_diff_creds_negative_query_user(self, query_user, test_case, request):  
        auth = self.ranger_admin_config  
        params = {}
        role, role_id = request.getfixturevalue("temp_role")()

        if query_user in ["non exists", "not exists"]:
            params["execUser"] = f"nonexistuser_{uuid.uuid4().hex[:6]}"
        else:
            if "user creds" in test_case:
                params["execUser"] = self.ranger_user_config[0] 
            elif "auditor creds" in test_case:
                params["execUser"] = self.ranger_auditor_config[0]
            elif "key admin creds" in test_case:
                params["execUser"] = self.ranger_key_admin_config[0]
        
        payload = {
            "grantor": params.get("execUser", ""), 
            "users": [self.user["name"]],
            "groups": [self.group["name"]],
            "roles": [],
            "grantOption": "true",
            "targetRoles": [role["name"]]
        }

        try:
            response = requests.put(
                f"{self.base_url}/roles/roles/grant/nonexistentService",
                auth=auth,
                headers=self.headers,
                params=params,
                json=payload
            )

            assert_response(response, 400, f"Expected 400 for {test_case} but got {response.status_code} with \n response text: {response.text}")
        finally:
            delete_role(role_id)


    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case", [
            "adding non existent user to role",
            "adding non existent group to role",
            "adding non existent role to role",
        ])
    def test_put_role_grant_with_diff_creds_negative_payload(self, test_case, request):
        auth = self.ranger_admin_config  
        role, role_id = request.getfixturevalue("temp_role")()
        params = {"execUser": self.admin["name"]} 

        payload = {
            "grantor": self.admin["name"],
            "users": [],
            "groups": [],
            "roles": [],
            "grantOption": "true",
            "targetRoles": [role["name"]]
        }

        if test_case == "adding non existent user to role":
            payload["users"] = [f"nonexistentuser_{uuid.uuid4().hex[:6]}"]
        elif test_case == "adding non existent group to role":
            payload["groups"] = [f"nonexistentgroup_{uuid.uuid4().hex[:6]}"]
        elif test_case == "adding non existent role to role":
            payload["roles"] = [f"nonexistentrole_{uuid.uuid4().hex[:6]}"]

        try:
            response = requests.put(
                f"{self.base_url}/roles/roles/grant/nonexistentService",
                auth=auth,
                headers=self.headers,
                params=params,
                json=payload
            )

            assert_response(response, 400, f"Expected 400 for {test_case} but got {response.status_code} with \n response text: {response.text}")
        finally:
            delete_role(role_id)
    
    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "login_test_case", [
            "login user is neither admin nor service admin/group - user creds",
            "login user is neither admin nor service admin/group - auditor creds",
            "login user is neither admin nor service admin/group - key admin creds",
        ]
    )
    def test_put_role_revoke_with_diff_creds_negative_login(self, login_test_case, request):
        auth = self.ranger_admin_config  # Default to admin auth, will be overridden for specific negative cases
        if "user" in login_test_case:
            auth = self.ranger_user_config
        elif "auditor" in login_test_case:
            auth = self.ranger_auditor_config
        elif "key admin" in login_test_case:
            auth = self.ranger_key_admin_config

        role, role_id = request.getfixturevalue("temp_role")()
        params = {"execUser": self.admin["name"]}  # Using admin as execUser in query param

        payload = {
            "grantor": self.admin["name"],
            "users": [self.user["name"]],
            "groups": [self.group["name"]],
            "roles": [],
            "grantOption": "true",
            "targetRoles": [role["name"]]
        }

        try:
            response = requests.put(
                f"{self.base_url}/roles/roles/revoke/nonexistentService",
                auth=auth,
                headers=self.headers,
                params=params, 
                json=payload
            )

            assert_response(response, 400, f"Expected 400 for {login_test_case} but got {response.status_code} with \n response text: {response.text}")
        finally:
            delete_role(role_id)

    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "query_user, test_case", [  
            ("non exists", "login user exists and it becomes effective user but is service user"),
            ("not exists", "login user is admin but query param user is non exist user"),
            ("exists", "login user is admin but query param user is non admin or service admin and has no role membership - user creds"),
            ("exists", "login user is admin but query param user is non admin or service admin and has no role membership - auditor creds"),
            ("exists", "login user is admin but query param user is non admin or service admin and has no role membership - key admin creds"),
        ])
    def test_put_role_revoke_with_diff_creds_negative_query_user(self, query_user, test_case, request):  
        auth = self.ranger_admin_config  
        params = {}
        role, role_id = request.getfixturevalue("temp_role")()

        if query_user in ["non exists", "not exists"]:
            params["execUser"] = f"nonexistuser_{uuid.uuid4().hex[:6]}"
        else:
            if "user creds" in test_case:
                params["execUser"] = self.ranger_user_config[0] 
            elif "auditor creds" in test_case:
                params["execUser"] = self.ranger_auditor_config[0]
            elif "key admin creds" in test_case:
                params["execUser"] = self.ranger_key_admin_config[0]
        
        payload = {
            "grantor": params.get("execUser", ""), 
            "users": [self.user["name"]],
            "groups": [self.group["name"]],
            "roles": [],
            "grantOption": "true",
            "targetRoles": [role["name"]]
        }

        try:
            response = requests.put(
                f"{self.base_url}/roles/roles/revoke/nonexistentService",
                auth=auth,
                headers=self.headers,
                params=params,
                json=payload
            )

            assert_response(response, 400, f"Expected 400 for {test_case} but got {response.status_code} with \n response text: {response.text}")
        finally:
            delete_role(role_id)

    @pytest.mark.put
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case", [
            "removing non existent user from role",
            "removing non existent group from role",
            "removing non existent role from role",
        ])
    def test_put_role_revoke_with_diff_creds_negative_payload(self, test_case, request):
        auth = self.ranger_admin_config  
        role, role_id = request.getfixturevalue("temp_role")()
        params = {"execUser": self.admin["name"]} 

        payload = {
            "grantor": self.admin["name"],
            "users": [],
            "groups": [],
            "roles": [],
            "grantOption": "true",
            "targetRoles": [role["name"]]
        }

        if test_case == "removing non existent user from role":
            payload["users"] = [f"nonexistentuser_{uuid.uuid4().hex[:6]}"]
        elif test_case == "removing non existent group from role":
            payload["groups"] = [f"nonexistentgroup_{uuid.uuid4().hex[:6]}"]
        elif test_case == "removing non existent role from role":
            payload["roles"] = [f"nonexistentrole_{uuid.uuid4().hex[:6]}"]

        try:
            response = requests.put(
                f"{self.base_url}/roles/roles/revoke/nonexistentService",
                auth=auth,
                headers=self.headers,
                params=params,
                json=payload
            )

            assert_response(response, 200, f"Expected 200 for {test_case} but got {response.status_code} with \n response text: {response.text}")
            
            get_role_by_name_flg, role_data = get_role_by_name(role["name"])
            assert get_role_by_name_flg, "Role not found when fetched by name after revoke with non-existent user/group/role in payload."
            assert role_data.get("users", []) == [], "Users list should be empty as the only user in revoke payload was non-existent."
            assert role_data.get("groups", []) == [], "Groups list should be empty as the only group in revoke payload was non-existent."
            assert role_data.get("roles", []) == [], "Roles list should be empty as the only role in revoke payload was non-existent."
        finally:
            delete_role(role_id)