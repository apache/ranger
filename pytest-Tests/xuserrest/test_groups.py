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
from xuserrest.utility.utils import *
import uuid
import string
import pytest
import requests
from datetime import datetime
import random

@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestSecureGroups:
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
    @pytest.mark.parametrize(
        "role, auth", 
        [("admin", "ranger_admin_config"), ("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")])
    def test_get_groups_by_id(self, role, auth):
        auth = getattr(self, auth)

        if role == "user":
            assign_groups_to_user(self.user["name"], [self.group["name"]], self.ranger_admin_config, self.base_url, self.headers)  
            data = fetch_groups_for_user_id(self.user["id"], self.ranger_admin_config, self.base_url, self.headers)
            assert check_group_in_user_groups(self.group_id, data), f"Group with id {self.group_id} is not assigned to user with id {self.user['id']}"

        group_exists(self.group_id, self.ranger_admin_config, self.base_url, self.headers)

        response = requests.get(
            f"{self.base_url}/xusers/secure/groups/{self.group_id}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, 200, f"Get group by id failed and got {response.status_code} instead of 200 for role {role}")
        response_data = response.json()
        validate_xgroup_schema(response_data)
        assert response_data["id"] == self.group_id
        assert response_data["name"] == self.group["name"]

        

    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])

    def test_create_group(self, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to create group")
        auth = getattr(self, auth)
        group_name = "test_group_" + str(uuid.uuid4())[:8]

        mandatory_fields = ["name"]
        
        payload ={
            "name": group_name,
            "groupSource": 0
            }
        
        mandatory_field_check_in_response(payload, mandatory_fields)
        
        
        response = requests.post(
            f"{self.base_url}/xusers/secure/groups",
            auth = auth,
            json=payload,
            headers=self.headers,
        )
        assert_response(response, 200, f"Group creation failed and got {response.status_code} instead of 200")
        response_data = response.json()
        validate_xgroup_schema(response_data)
        if 'description' in payload:
            assert response_data["description"] == payload["description"]
        else:
            assert response_data["description"] == payload["name"]
        
        delete_group(response_data["id"], auth, self.base_url, self.headers)
        print(f"Group with name {group_name} created successfully with id {response_data['id']}")


    @pytest.mark.positive
    @pytest.mark.put
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_update_group(self, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to update group")
        auth = getattr(self, auth)

        mandatory_fields = ["id","name"]
        payload ={
            "name": self.group["name"],
            "id": self.group_id,
            "groupSource": 0
            }
        
        assert mandatory_field_check_in_response(payload, mandatory_fields), f"Mandatory fields {mandatory_fields} are not present in payload for update group"
        assert payload["name"] == self.group["name"], f"Name field in payload is not same as existing group name for update group"

        group_exists(self.group_id, self.ranger_admin_config, self.base_url, self.headers)

        response = requests.put(
            f"{self.base_url}/xusers/secure/groups/{self.group_id}",
            auth = auth,
            json=payload,
            headers=self.headers,
        )
        assert_response(response, 200, f"Group update failed and got {response.status_code} instead of 200")
        response_data = response.json()
        validate_xgroup_schema(response_data)
        assert response_data["groupSource"] == 0, f"Group source is not same as expected after update group"


    @pytest.mark.positive
    @pytest.mark.put
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_update_group_visibility(self, role, auth, request):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to update group")
        auth = getattr(self, auth)

        temp_group, temp_group_id = request.getfixturevalue("temp_group")()
        temp_group1, temp_group_id1 = request.getfixturevalue("temp_group")()
        payload = {
            str(temp_group_id): 0,
            str(temp_group_id1): 0
        }
        for group_id in payload.keys():
            flag, data = group_exists(int(group_id), self.ranger_admin_config, self.base_url, self.headers)
            assert flag, f"Group with id {group_id} does not exist before update visibility for group which is expected for test case: {role}"
            assert data["isVisible"] != 0, f"Group with id {group_id} is not visible before update visibility for group which is expected for test case: {role}"

        response = requests.put(
            f"{self.base_url}/xusers/secure/groups/visibility",
            auth = auth,
            json=payload,
            headers=self.headers,
        )
        assert_response(response, 204, f"Group update failed and got {response.status_code} instead of 204")
        
        # checking if groups are updated to invisible after update visibility for group
        flag, data = group_exists(temp_group_id, self.ranger_admin_config, self.base_url, self.headers)
        assert data["isVisible"] == 0, f"Group with id {temp_group_id} is not updated to invisible after update visibility for group which is expected for test case"


    @pytest.mark.positive
    @pytest.mark.delete
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_delete_group_by_id(self, role, auth, request):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to delete group")
        auth = getattr(self, auth)

        temp_group, temp_group_id = request.getfixturevalue("temp_group")()
        response = requests.delete(
            f"{self.base_url}/xusers/secure/groups/id/{temp_group_id}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, 204, f"Group deletion failed and got {response.status_code} instead of 204 for role {role}")
        flag, data = group_exists(temp_group_id, self.ranger_admin_config, self.base_url, self.headers)
        assert not flag, f"Group with id {temp_group_id} still exists after deletion which is not expected for test case: {role}"
    

    @pytest.mark.positive
    @pytest.mark.delete
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_delete_group_by_userName(self, role, auth, request):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to delete group")
        auth = getattr(self, auth)

        temp_group, temp_group_id = request.getfixturevalue("temp_group")()
        flag = group_exists_by_name(temp_group["name"], self.ranger_admin_config, self.base_url, self.headers)
        assert flag, f"Group with name {temp_group['name']} does not exist before deletion which is expected for test case: {role}"
        response = requests.delete(
            f"{self.base_url}/xusers/secure/groups/{temp_group['name']}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, 204, f"Group deletion failed and got {response.status_code} instead of 204 for role {role}")
        flag = group_exists_by_name(temp_group["name"], self.ranger_admin_config, self.base_url, self.headers)
        assert not flag, f"Group with name {temp_group['name']} still exists after deletion which is not expected for test case: {role}"
    
    
    @pytest.mark.positive
    @pytest.mark.delete
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_delete_group_by_userName_bulk(self, role, auth, request):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to delete group")
        auth = getattr(self, auth)

        temp_group, temp_group_id = request.getfixturevalue("temp_group")()
        temp_group1, temp_group_id1 = request.getfixturevalue("temp_group")()

        names = [temp_group["name"], temp_group1["name"]]

        for name in names:
            flag = group_exists_by_name(name, self.ranger_admin_config, self.base_url, self.headers)
            assert flag, f"Group with name {name} does not exist before deletion which is expected for test case: {role}"

        payload = {
            "vXStrings": [{"value": name} for name in names]
        }

        response = requests.delete(
            f"{self.base_url}/xusers/secure/groups/delete?forceDelete=true",
            auth=auth,
            json=payload,
            headers=self.headers,
        )
        assert_response(response, 204, f"Group deletion failed and got {response.status_code} instead of 204 for role {role}")

        for name in names:
            flag = group_exists_by_name(name, self.ranger_admin_config, self.base_url, self.headers)
            assert not flag, f"Group with name {name} still exists after deletion which is not expected for test case: {role}"

    

    # NEGATIVE TEST CASES

    @pytest.mark.negative
    @pytest.mark.get
    @pytest.mark.parametrize(
        "test_case",[("invalid access by user with out group membership")])
    def test_get_group_by_id_negative(self, test_case, request):
        temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")()  
        auth = (temp_user["name"], "Test@123")
        data = fetch_groups_for_user_id(temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        assert not check_group_in_user_groups(self.group_id, data), f"Group with id {self.group_id} is assigned to user with id {temp_user_id} which is not expected for test case: {test_case}"
        if test_case == "invalid access by user with out group membership":
            response = requests.get(
                f"{self.base_url}/xusers/secure/groups/{self.group_id}",
                auth = auth,
                headers=self.headers,
            )
            assert_response(response, 403, f"Get group by id should have failed with 403 but got {response.status_code} instead for test case: {test_case}")


    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize(
        "test_case, auth", 
        [("invalid auth by keyadmin", "ranger_key_admin_config"), ("invalid auth by auditor", "ranger_auditor_config"), ("invalid auth by user", "ranger_user_config"),
         ("missing mandatory field name", "ranger_admin_config"), ("malformed format no name value(empty string)", "ranger_admin_config"), ("invalid format for mandatory field", "ranger_admin_config")])
    def test_create_group_negative(self, test_case, auth):
        auth = getattr(self, auth)
        group_name = "test_group_" + str(uuid.uuid4())[:8]
        payload ={
            "name": group_name,
            "groupSource": 0
            }
        response_code = 404 # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.
        mandatory_fields = ["name"]

        if test_case == "missing mandatory field name":
            del payload["name"]
            response_code = 404

        elif test_case == "malformed format no name value(empty string)":
            payload = r'{"name": "" ,"groupSource": 0}'
            response_code = 400

        elif test_case == "invalid format for mandatory field":
            payload = r'{"name": RANDOM ,"groupSource": 0}'
            response_code = 400

        response = requests.post(
            f"{self.base_url}/xusers/secure/groups",
            auth = auth,
            json=payload,
            headers=self.headers,
        )

        assert_response(response, response_code, f"Group creation should have failed with {response_code} but got {response.status_code} instead for test case: {test_case}")

    @pytest.mark.negative
    @pytest.mark.put
    @pytest.mark.parametrize(
        "test_case, auth", 
        [("invalid auth by keyadmin", "ranger_key_admin_config"), ("invalid auth by auditor", "ranger_auditor_config"), ("invalid auth by user", "ranger_user_config"), 
         ("missing mandatory field name", "ranger_admin_config"), ("missing mandatory field id", "ranger_admin_config"),("updating immutable field username", "ranger_admin_config")])
    def test_update_group_negative(self, test_case, auth):
        auth = getattr(self, auth)
        payload ={
            "name": self.group["name"],
            "id": self.group_id,
            "groupSource": 0
            }
        response_code = 403
        mandatory_fields = ["id","name"]

        if test_case == "missing mandatory field name":
            del payload["name"]
            response_code = 404

        elif test_case == "missing mandatory field id":
            del payload["id"]
            response_code = 400

        elif test_case == "updating immutable field username":
            payload["name"] = "new_username"
            assert payload["name"] != self.group["name"], f"Name field in payload is same as existing group name for update group which is not expected for test case: {test_case}"
            response_code = 400

        response = requests.put(
            f"{self.base_url}/xusers/secure/groups/{self.group_id}",
            auth = auth,
            json=payload,
            headers=self.headers,
        )
        assert_response(response, response_code, f"Group update should have failed with {response_code} but got {response.status_code} instead for test case: {test_case}")

    @pytest.mark.negative
    @pytest.mark.put
    @pytest.mark.parametrize(
        "test_case, auth", 
        [("invalid auth by keyadmin", "ranger_key_admin_config"), ("invalid auth by auditor", "ranger_auditor_config"), ("invalid auth by user", "ranger_user_config"),
         ("updating visibility with invalid group id", "ranger_admin_config")])
    def test_update_group_visibility_negative(self, test_case, auth):
        auth = getattr(self, auth)

        if test_case == "updating visibility with invalid group id":
            rand_id = random.randint(999999, 9999999)
            flg, data = group_exists(rand_id, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group with id {rand_id} exists which is not expected for test case: {test_case}"
            group_id = rand_id
            response_code = 404

        else:
            group_id = self.group_id
            response_code = 403
       
        payload = {
            str(group_id): 0
        }

        response = requests.put(
            f"{self.base_url}/xusers/secure/groups/visibility",
            auth = auth,
            json=payload,
            headers=self.headers,
        )
        assert_response(response, response_code, f"Group update should have failed with {response_code} but got {response.status_code} instead for test case: {test_case}")

    @pytest.mark.negative
    @pytest.mark.delete
    @pytest.mark.parametrize("test_case, auth", [("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config"), ("non existent id", "ranger_admin_config")])
    def test_delete_group_by_id_negative(self, test_case, auth, request):
        auth = getattr(self, auth)
        if test_case == "non existent id":
            rand_id = random.randint(999999, 9999999)
            flg, data = group_exists(rand_id, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group with id {rand_id} exists which is not expected for test case: {test_case}"
            temp_group_id = rand_id
            expected_response_code = 404
        else:
            temp_group_id = self.group_id
            expected_response_code = 404  # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.
        response = requests.delete(
            f"{self.base_url}/xusers/secure/groups/id/{temp_group_id}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, expected_response_code, f"Group deletion failed and got {response.status_code} instead of {expected_response_code} for test case {test_case}")
    
    @pytest.mark.negative
    @pytest.mark.delete
    @pytest.mark.parametrize("test_case, auth", [("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config"), ("non existent name", "ranger_admin_config")])
    def test_delete_group_by_userName_negative(self, test_case, auth, request):
        auth = getattr(self, auth)
        if test_case == "non existent name":
            name = "non_existent_group_name_" + str(uuid.uuid4())[:8]
            flg = group_exists_by_name(name, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group with name {name} exists which is not expected for test case: {test_case}"
            expected_response_code = 400
        else:
            temp_group = self.group
            name = temp_group["name"]
            expected_response_code = 404 # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.
        
        response = requests.delete(
            f"{self.base_url}/xusers/secure/groups/{name}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, expected_response_code, f"Group deletion failed and got {response.status_code} instead of {expected_response_code} for test case {test_case}")
        
    @pytest.mark.negative
    @pytest.mark.delete
    @pytest.mark.parametrize("test_case, auth", [("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config"), 
                                                 ("non existent name", "ranger_admin_config")])
    def test_delete_group_by_userName_bulk_negative(self, test_case, auth, request):
        auth = getattr(self, auth)

        temp_group, temp_group_id = request.getfixturevalue("temp_group")()
        temp_group1, temp_group_id1 = request.getfixturevalue("temp_group")()
        if test_case == "non existent name":
            name = "non_existent_group_name_" + str(uuid.uuid4())[:8]
            flg = group_exists_by_name(name, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group with name {name} exists which is not expected for test case: {test_case}"
            names = [name] 
            expexted_response_code = 400

        else:
            names = [temp_group["name"], temp_group1["name"]]

            for name in names:
                flag = group_exists_by_name(name, self.ranger_admin_config, self.base_url, self.headers)
                assert flag, f"Group with name {name} does not exist before deletion which is expected for test case: {test_case}"
            expexted_response_code = 404 # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.
        payload = {
            "vXStrings": [{"value": name} for name in names]
        }

        response = requests.delete(
            f"{self.base_url}/xusers/secure/groups/delete?forceDelete=true",
            auth=auth,
            json=payload,
            headers=self.headers,
        )
        assert_response(response, expexted_response_code, f"Group deletion failed and got {response.status_code} instead of {expexted_response_code} for test case {test_case}")



@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestGroups:
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
    @pytest.mark.parametrize(
        "role, auth", 
        [("admin", "ranger_admin_config"), ("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")])
    def test_get_groups_by_id(self, role, auth):
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        group_exists(temp_group_id, self.ranger_admin_config, self.base_url, self.headers)

        if role == "user":
            assign_groups_to_user(self.user["name"], [temp_group["name"]], self.ranger_admin_config, self.base_url, self.headers)  
            data = fetch_groups_for_user_id(self.user["id"], self.ranger_admin_config, self.base_url, self.headers)
            assert check_group_in_user_groups(temp_group_id, data), f"Group with id {temp_group_id} is not assigned to user with id {self.user['id']} which is not expected for test case: {role}"
        
        response = requests.get(
            f"{self.base_url}/xusers/groups/{temp_group_id}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, 200, f"Get group by id failed and got {response.status_code} instead of 200")
        response_data = response.json()
        validate_xgroup_schema(response_data)
        assert response_data["id"] == temp_group_id
        assert response_data["name"] == temp_group["name"]
        if role == "user":
            assert response_data["updatedBy"] == "*****", f"Group with id {temp_group_id} should be masked for user role but it did not, which is not expected for test case: {role}"
        
    @pytest.mark.positive
    @pytest.mark.parametrize(
    "params, test_case",
    [
        ({}, "default_request"),
        ({"startIndex": 0, "pageSize": 10}, "pagination_basic"),
        ({"name": "public"}, "filter_name"),
        ({"isVisible": 1}, "filter_is_visible"),
        ({"groupSource": 0}, "filter_group_source_internal"),
        ({"groupSource": 1}, "filter_group_source_external"),
        ({"syncSource": "Unix"}, "filter_sync_source"),
        ({"name": "public", "isVisible": 1}, "filter_combined"),
        ({"sortBy": "name", "sortType": "asc"}, "sorting_asc"),
        ({"sortBy": "name", "sortType": "desc"}, "sorting_desc"),
        ({"startIndex": 0, "pageSize": 5, "name": "public"}, "pagination_with_filter"),
    ],
    ids=[
        "default",
        "pagination-basic",
        "filter-name",
        "filter-isVisible",
        "filter-groupSource-internal",
        "filter-groupSource-external",
        "filter-syncSource",
        "filter-combined",
        "sorting-asc",
        "sorting-desc",
        "pagination-with-filter",
    ],
    )
    @pytest.mark.parametrize(
        "role, auth",
        [("admin", "ranger_admin_config"), ("key admin", "ranger_key_admin_config"),("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")],
        ids=["admin", "key admin", "auditor", "user"],
    )
    def test_get_group(self, params, test_case, role, auth):
        
        auth = getattr(self, auth)

        response = requests.get(
            f"{self.base_url}/xusers/groups",
            params=params,
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, f"Failed for case: {test_case}")
        
        data = response.json()

        assert "startIndex" in data and isinstance(data["startIndex"], int)
        assert "pageSize" in data and isinstance(data["pageSize"], int)
        assert "totalCount" in data and isinstance(data["totalCount"], int)
        assert "resultSize" in data and isinstance(data["resultSize"], int)
        assert isinstance(data, dict), f"Invalid response format in {test_case}"

        assert "vXGroups" in data, f"'vXGroups' not found in {test_case}"
        assert isinstance(data["vXGroups"], list)

        groups = data["vXGroups"]

        if not groups:
            assert data.get("totalCount", 0) == 0
            assert data.get("resultSize", 0) == 0
            return
        

        validate_xgroup_schema(groups[0]) # checking for first group in the list, as all groups in the response should have same schema
        

    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config"), ("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")])
    def test_get_group_count(self, role, auth):
        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/xusers/groups/count",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 200, f"Failed to get group count and got {response.status_code} instead of 200 for role {role}")
        data = response.json()
        assert "value" in data and isinstance(data["value"], int), f"Invalid response format for group count for role {role}"

    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize(
        "role, auth", 
        [("admin", "ranger_admin_config"), ("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")])
    def test_get_groups_by_groupName(self, role, auth):
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        group_exists(temp_group_id, self.ranger_admin_config, self.base_url, self.headers)

        if role == "user":
            assign_groups_to_user(self.user["name"], [temp_group["name"]], self.ranger_admin_config, self.base_url, self.headers)  
            data = fetch_groups_for_user_id(self.user["id"], self.ranger_admin_config, self.base_url, self.headers)
            assert check_group_in_user_groups(temp_group_id, data), f"Group with id {temp_group_id} is not assigned to user with id {self.user['id']} which is not expected for test case: {role}"
        
        response = requests.get(
            f"{self.base_url}/xusers/groups/groupName/{temp_group['name']}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, 200, f"Get group by name failed and got {response.status_code} instead of 200")
        response_data = response.json()
        validate_xgroup_schema(response_data)
        assert response_data["id"] == temp_group_id
        assert response_data["name"] == temp_group["name"]
        # if role == "user":
        #     assert response_data["updatedBy"] == "*****", f"Group with id {temp_group_id} should be masked for user role but it did not, which is not expected for test case: {role}"
        
        
    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize(
        "testcase, ,role, auth",
        [("non existing group name", "admin", "ranger_admin_config"), ("existing group name", "admin", "ranger_admin_config")],)
    def test_create_group_by_name_positive(self, testcase, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to create group")
        auth = getattr(self, auth)
        if testcase == "non existing group name":
            group_name = "non_existing_group_name_" + str(uuid.uuid4())[:8]
            flg = group_exists_by_name(group_name, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group with name {group_name} exists which is not expected for test case: {testcase}"
        else:
            group_name = self.group["name"]
            flg= group_exists_by_name(group_name, self.ranger_admin_config, self.base_url, self.headers)
            assert flg, f"Group with name {group_name} does not exist which is not expected for test case: {testcase}"
            
        payload = {
            "name": group_name,
            "description": f"Description for {group_name} via pytest fun()" # since description changes we can validate that it is updating
        }
        
        response = requests.post(
            f"{self.base_url}/xusers/groups",
            auth=auth,
            json=payload,
            headers=self.headers
        )
        assert_response(response, 200, f"Failed to get group by name and got {response.status_code} instead of 200 for test case: {testcase}")
        response_data = response.json()
        validate_xgroup_schema(response_data)
        assert response_data["name"] == group_name, f"Group name in response is not same as requested group name for test case: {testcase}"
        assert response_data["description"] == payload["description"], f"Group description in response is not same as requested group description for test case: {testcase}"
        if testcase == "non existing group name":
            delete_group(response_data["id"], auth, self.base_url, self.headers)
            print(f"Group with name {group_name} created successfully with id {response_data['id']} and deleted successfully for test case: {testcase}")
        else:
            assert response_data["id"] == self.group_id, f"Group id in response is not same as existing group id for test case: {testcase}"



    @pytest.mark.positive
    @pytest.mark.put 
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_update_group(self, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to update group")
        auth = getattr(self, auth)

        mandatory_fields = ["id","name"]
        payload ={
            "name": self.group["name"],
            "id": self.group_id,
            "groupSource": 0
            }
        
        assert mandatory_field_check_in_response(payload, mandatory_fields), f"Mandatory fields {mandatory_fields} are not present in payload for update group"
        assert payload["name"] == self.group["name"], f"Name field in payload is not same as existing group name for update group"

        group_exists(self.group_id, self.ranger_admin_config, self.base_url, self.headers)

        response = requests.put(
            f"{self.base_url}/xusers/groups",
            auth = auth,
            json=payload,
            headers=self.headers,
        )
        assert_response(response, 200, f"Group update failed and got {response.status_code} instead of 200")
        response_data = response.json()
        validate_xgroup_schema(response_data)
        assert response_data["groupSource"] == 0, f"Group source is not same as expected after update group"

    @pytest.mark.positive
    @pytest.mark.delete
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_delete_group_by_id(self, role, auth, request):
        grp, grp_id = request.getfixturevalue("temp_group")()
        auth = getattr(self, auth)
        
        response = requests.delete(
            f"{self.base_url}/xusers/groups/{grp_id}",
            auth = auth,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        assert_response(response, 204, f"Group deletion failed and got {response.status_code} instead of 204 for role {role}")
        flg, dat = group_exists(grp_id, self.ranger_admin_config, self.base_url, self.headers)
        assert not flg, f"Group with id {grp_id} still exists after deletion which is not expected for test case: {role}"


    @pytest.mark.positive
    @pytest.mark.delete
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_delete_group_by_groupName(self, role, auth, request):
        grp, grp_id = request.getfixturevalue("temp_group")()
        auth = getattr(self, auth)
        name = grp["name"]
        flag = group_exists_by_name(name, self.ranger_admin_config, self.base_url, self.headers)
        assert flag, f"Group with name {name} does not exist before deletion which is expected for test case: {role}"
        response = requests.delete(
            f"{self.base_url}/xusers/groups/groupName/{name}",
            auth = auth,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        assert_response(response, 204, f"Group deletion failed and got {response.status_code} instead of 204 for role {role}")
        flag = group_exists_by_name(name, self.ranger_admin_config, self.base_url, self.headers)
        assert not flag, f"Group with name {name} still exists after deletion which is not expected for test case: {role}"


    @pytest.mark.positive
    @pytest.mark.delete
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_delete_group_user_by_name(self, role, auth, request):
        grp, grp_id = request.getfixturevalue("temp_group")()
        auth = getattr(self, auth)
        name = grp["name"]
        flag = group_exists_by_name(name, self.ranger_admin_config, self.base_url, self.headers)
        assert flag, f"Group with name {name} does not exist before deletion which is expected for test case: {role}"
        assign_groups_to_user(self.user["name"], [name], self.ranger_admin_config, self.base_url, self.headers)
        response = requests.delete(
            f"{self.base_url}/xusers/group/{name}/user/{self.user['name']}",
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"},
        )
        
        assert_response(response, 204, f"Group deletion failed and got {response.status_code} instead of 204 for role {role}")
        data = fetch_groups_for_user_id(self.user["id"], self.ranger_admin_config, self.base_url, self.headers)
        assert not check_group_in_user_groups(grp_id, data), f"Group with id {grp_id} is assigned to user with id {self.user['id']} which is not expected for test case: {role}"


    # NEGATIVE TEST CASES

    @pytest.mark.negative
    @pytest.mark.get
    @pytest.mark.parametrize(
        "test_case",[("invalid access by user with out group membership")])
    def test_get_group_by_id_negative(self, test_case, request):
        temp_user, temp_user_id = request.getfixturevalue("temp_secure_user")()  
        auth = (temp_user["name"], "Test@123")
        data = fetch_groups_for_user_id(temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        assert not check_group_in_user_groups(self.group_id, data), f"Group with id {self.group_id} is assigned to user with id {temp_user_id} which is not expected for test case: {test_case}"
        if test_case == "invalid access by user with out group membership":
            response = requests.get(
                f"{self.base_url}/xusers/groups/{self.group_id}",
                auth = auth,
                headers=self.headers,
            )
        assert_response(response, 403, f"Get group by id should have failed with 403 but got {response.status_code} instead for test case: {test_case}")


    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize(
        "test_case, auth",
        [("invalid auth by keyadmin", "ranger_key_admin_config"), ("invalid auth by auditor", "ranger_auditor_config"), ("invalid auth by user", "ranger_user_config"),
         ("missing mandatory field name", "ranger_admin_config")])
    def test_create_group_by_name_negative(self, test_case, auth):
        auth = getattr(self, auth)
        group_name = "test_group_" + str(uuid.uuid4())[:8]
        payload = {
            "name": group_name
        }
        response_code = 404 # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.

        if test_case == "missing mandatory field name":
            del payload["name"]
            response_code = 404

        response = requests.post(
            f"{self.base_url}/xusers/groups",
            auth=auth,
            json=payload,
            headers=self.headers
        )
        assert_response(response, response_code, f"Group creation should have failed with {response_code} but got {response.status_code} instead for test case: {test_case}")
    
    
    @pytest.mark.negative
    @pytest.mark.put
    @pytest.mark.parametrize(
        "test_case, auth", 
        [("invalid auth by keyadmin", "ranger_key_admin_config"), ("invalid auth by auditor", "ranger_auditor_config"), ("invalid auth by user", "ranger_user_config"), 
         ("missing mandatory field name", "ranger_admin_config"), ("missing mandatory field id", "ranger_admin_config"),("updating immutable field username", "ranger_admin_config")])
    def test_update_group_negative(self, test_case, auth):
        auth = getattr(self, auth)
        payload ={
            "name": self.group["name"],
            "id": self.group_id,
            "groupSource": 0
            }
        response_code = 403
        mandatory_fields = ["id","name"]

        if test_case == "missing mandatory field name":
            del payload["name"]
            response_code = 404

        elif test_case == "missing mandatory field id":
            del payload["id"]
            response_code = 400

        elif test_case == "updating immutable field username":
            payload["name"] = "new_username"
            assert payload["name"] != self.group["name"], f"Name field in payload is same as existing group name for update group which is not expected for test case: {test_case}"
            response_code = 400

        response = requests.put(
            f"{self.base_url}/xusers/groups",
            auth = auth,
            json=payload,
            headers=self.headers,
        )
        assert_response(response, response_code, f"Group update should have failed with {response_code} but got {response.status_code} instead for test case: {test_case}")


    @pytest.mark.negative
    @pytest.mark.delete
    @pytest.mark.parametrize(
        "test_case, auth", [("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config"),
        ("non existent id", "ranger_admin_config"), ("group - assigned to role", "ranger_admin_config")])
    def test_delete_group_negative(self, test_case, auth, request):
        auth = getattr(self, auth)
        if test_case == "non existent id":
            rand_id = random.randint(999999, 9999999)
            flg, data = group_exists(rand_id, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group with id {rand_id} exists which is not expected for test case: {test_case}"
            temp_group_id = rand_id
            expected_response_code = 404
        elif test_case == "group - assigned to role":
            temp_group, temp_group_id = request.getfixturevalue("temp_group")()
            temp_role, temp_role_id = request.getfixturevalue("temp_role")()
            assign_role_to_group(temp_group["name"], temp_role, self.ranger_admin_config, self.base_url, self.headers)
            expected_response_code = 400
            print(" it is executed for test case: ", test_case)
        else:
            temp_group_id = self.group_id
            expected_response_code = 404 # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.
        
        response = requests.delete(
            f"{self.base_url}/xusers/groups/{temp_group_id}",
            auth = auth,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        assert_response(response, expected_response_code, f"Group deletion failed and got {response.status_code} instead of {expected_response_code} for test case {test_case}")

    
    @pytest.mark.negative
    @pytest.mark.delete
    @pytest.mark.parametrize(
        "test_case, auth", [("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config"), 
                            ("non existent name", "ranger_admin_config"), ("group - assigned to role", "ranger_admin_config")])
    def test_delete_group_by_groupName_negative(self, test_case, auth, request):
        auth = getattr(self, auth)
        if test_case == "non existent name":
            name = "non_existent_group_name_" + str(uuid.uuid4())[:8]
            flg = group_exists_by_name(name, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group with name {name} exists which is not expected for test case: {test_case}"
            expected_response_code = 400
        elif test_case == "group - assigned to role":
            temp_group, temp_group_id = request.getfixturevalue("temp_group")()
            temp_role, temp_role_id = request.getfixturevalue("temp_role")()
            assign_role_to_group(temp_group["name"], temp_role, self.ranger_admin_config, self.base_url, self.headers)
            name = temp_group["name"]
            expected_response_code = 400
            print(" it is executed for test case: ", test_case)
        else:
            temp_group = self.group
            name = temp_group["name"]
            expected_response_code = 404 # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.
        
        response = requests.delete(
            f"{self.base_url}/xusers/groups/groupName/{name}",
            auth = auth,
            headers={**self.headers, "X-Requested-By": "ranger"}
        )
        assert_response(response, expected_response_code, f"Group deletion failed and got {response.status_code} instead of {expected_response_code} for test case {test_case}")

        
    @pytest.mark.negative
    @pytest.mark.delete
    @pytest.mark.parametrize(
        "test_case, auth", [
            ("invalid_auth by user", "ranger_user_config"), ("invalid auth by keyadmin", "ranger_key_admin_config"), ("invalid auth by auditor", "ranger_auditor_config"),
            ("non existent user name", "ranger_admin_config"), ("non existent group name", "ranger_admin_config")])
    def test_delete_group_user_by_name_negative(self, test_case, auth, request):
        auth = getattr(self, auth)
        if test_case == "non existent user name":
            user_name = "non_existent_user_name_" + str(uuid.uuid4())[:8]
            flg = user_exists_by_name(user_name, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"User with name {user_name} exists which is not expected for test case: {test_case}"
            expected_response_code = 400
            group_name = self.group["name"]
        elif test_case == "non existent group name":
            group_name = "non_existent_group_name_" + str(uuid.uuid4())[:8]
            flg = group_exists_by_name(group_name, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group with name {group_name} exists which is not expected for test case: {test_case}"
            expected_response_code = 400
            user_name = self.user["name"]
        else:
            user_name = self.user["name"]
            group_name = self.group["name"]
            expected_response_code = 404 # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.

        response = requests.delete(
            f"{self.base_url}/xusers/group/{group_name}/user/{user_name}",
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"},
        )
        
        assert_response(response, expected_response_code, f"Group deletion failed and got {response.status_code} instead of {expected_response_code} for test case {test_case}")





@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestGroupUsers:
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
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config"), ("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")])
    def test_get_groupusers_by_id(self, role, auth):
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        temp_user_id = self.ranger_user_id1

        grp_user = create_groupuser(temp_group["name"], temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        grp_user_id = grp_user["id"]

        assert groupuser_exists(grp_user_id, self.ranger_admin_config, self.base_url, self.headers), f"Group-User association with id {grp_user_id} does not exist which is required for test case: {role}"
        response = requests.get(
            f"{self.base_url}/xusers/groupusers/{grp_user_id}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, 200, f"Get group-user association by id failed and got {response.status_code} instead of 200 for role {role}")
        response_data = response.json()
        assert response_data["parentGroupId"] == temp_group_id
        assert response_data["userId"] == temp_user_id
        delete_groupuser(grp_user_id, self.ranger_admin_config, self.base_url, self.headers)

    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize("role, auth", 
        [("admin", "ranger_admin_config"), ("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")])
    def test_get_groupusers_count(self, role, auth):
        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/xusers/groupusers/count",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 200, f"Failed to get group-user association count and got {response.status_code} instead of 200 for role {role}")
        data = response.json()
        assert "value" in data and isinstance(data["value"], int), f"Invalid response format for group-user association count for role {role}"

    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config"), ("key admin", "ranger_key_admin_config"), ("auditor", "ranger_auditor_config"), ("user", "ranger_user_config")])
    def test_get_groupusers_list(self, role, auth):
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        temp_user_id = self.ranger_user_id1

        grp_user = create_groupuser(temp_group["name"], temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        grp_user_id = grp_user["id"]

        assert groupuser_exists(grp_user_id, self.ranger_admin_config, self.base_url, self.headers), f"Group-User association with id {grp_user_id} does not exist which is required for test case: {role}"
        response = requests.get(
            f"{self.base_url}/xusers/groupusers",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, 200, f"Get group-user association list failed and got {response.status_code} instead of 200 for role {role}")
        response_data = response.json()
        assert "vXGroupUsers" in response_data and isinstance(response_data["vXGroupUsers"], list), f"Invalid response format for group-user association list for role {role}"
        data = response_data["vXGroupUsers"]
        if data != []:
            assert isinstance(data[0]['id'], int), f"Invalid id format in group-user association list for role {role}"    
        delete_groupuser(grp_user_id, self.ranger_admin_config, self.base_url, self.headers)


    @pytest.mark.positive
    @pytest.mark.get
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_get_groupusers_by_groupName(self, role, auth):
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        temp_user_id = self.ranger_user_id1

        grp_user = create_groupuser(temp_group["name"], temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        grp_user_id = grp_user["id"]

        assert groupuser_exists(grp_user_id, self.ranger_admin_config, self.base_url, self.headers), f"Group-User association with id {grp_user_id} does not exist which is required for test case: {role}"
        
        groupname = temp_group["name"]

        response = requests.get(
            f"{self.base_url}/xusers/groupusers/groupName/{groupname}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, 200, f"Get group-user association by group name failed and got {response.status_code} instead of 200 for role {role}")
        response_data = response.json()
        assert "xgroupInfo" in response_data 
        data = response_data["xgroupInfo"]
        validate_xgroup_schema(data)
        assert "xuserInfo" in response_data
        delete_groupuser(grp_user_id, self.ranger_admin_config, self.base_url, self.headers)

    @pytest.mark.positive
    @pytest.mark.post
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_create_groupuser(self, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to create group-user association")
        
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        temp_user_id = self.ranger_user_id1

        payload = {
            "name": temp_group["name"],
            "parentGroupId": temp_group_id,
            "userId": temp_user_id
        }


        assert payload != {}, f"Payload is empty for test case: {role}"
        assert payload["name"] != "", f"Name field in payload is empty for test case: {role}"
        assert payload["userId"] != "", f"UserId field in payload is empty for test case: {role}"
        flg, _ = group_exists(temp_group_id, self.ranger_admin_config, self.base_url, self.headers)
        assert flg, f"Group with id {temp_group_id} does not exist which is required for test case: {role}"
        assert user_exists(temp_user_id, self.ranger_admin_config, self.base_url, self.headers), f"User with id {temp_user_id} does not exist which is required for test case: {role}"

        response = requests.post(
            f"{self.base_url}/xusers/groupusers",
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"},
            json=payload
        )

        returned_data = response.json()
        assert_response(response, 200, f"Group-User association creation failed and got {response.status_code} instead of 200 for role {role}")
        data = fetch_groups_for_user_id(temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        assert check_group_in_user_groups(temp_group_id, data), f"Group with id {temp_group_id} is not assigned to user with id {temp_user_id} which is not expected for test case: {role}"

        print(returned_data)
        delete_groupuser(returned_data["id"], self.ranger_admin_config, self.base_url, self.headers)


    @pytest.mark.positive
    @pytest.mark.put
    @pytest.mark.parametrize(
        "test_case, role, auth", 
            [("Updation of the user deatils","admin", "ranger_admin_config"), ("Updation of the group details","admin", "ranger_admin_config")])
    def test_update_groupuser(self, test_case, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to update group-user association")
        
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        temp_user_id = self.ranger_user_id1

        grp_user = create_groupuser(temp_group["name"], temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        grp_user_id = grp_user["id"]

        assert groupuser_exists(grp_user_id, self.ranger_admin_config, self.base_url, self.headers), f"Group-User association with id {grp_user_id} does not exist which is required for test case: {role}"
        
        validation_grp_name = temp_group["name"]
        validation_user_id = temp_user_id

        if test_case == "Updation of the group details":
            payload = {
                "id": grp_user_id,
                "name": self.group1['name'], 
                "userId": temp_user_id
                }
            validation_grp_name = self.group1['name']
        elif test_case == "Updation of the user deatils":
            payload = {
                "id": grp_user_id,
                "name": temp_group["name"], 
                "userId": self.ranger_user_id1
                }
            validation_user_id = self.ranger_user_id1


        response = requests.put(
            f"{self.base_url}/xusers/groupusers",
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"},
            json=payload
        )

        assert_response(response, 200, f"Group-User association update failed and got {response.status_code} instead of 200 for role {role}")
        returned_data = response.json()
        assert returned_data["id"] == grp_user_id
        assert returned_data["name"] == validation_grp_name
        assert returned_data["userId"] == validation_user_id

        delete_groupuser(grp_user_id, self.ranger_admin_config, self.base_url, self.headers)

    @pytest.mark.positive
    @pytest.mark.delete
    @pytest.mark.parametrize("role, auth", [("admin", "ranger_admin_config")])
    def test_delete_groupuser(self, role, auth):
        if role != "admin":
            pytest.fail(f"Role {role} is not authorized to delete group-user association")
        
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        temp_user_id = self.ranger_user_id1

        grp_user = create_groupuser(temp_group["name"], temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        grp_user_id = grp_user["id"]

        assert groupuser_exists(grp_user_id, self.ranger_admin_config, self.base_url, self.headers), f"Group-User association with id {grp_user_id} does not exist which is required for test case: {role}"
        
        response = requests.delete(
            f"{self.base_url}/xusers/groupusers/{grp_user_id}",
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"},
        )

        assert_response(response, 204, f"Group-User association deletion failed and got {response.status_code} instead of 204 for role {role}")
        assert not groupuser_exists(grp_user_id, self.ranger_admin_config, self.base_url, self.headers), f"Group-User association with id {grp_user_id} still exists after deletion which is not expected for test case: {role}"

    

    # NEGATIVE TEST CASES

    @pytest.mark.negative
    @pytest.mark.get
    @pytest.mark.parametrize(
        "test_case",[("invalid access by non existing id")])
    def test_get_groupusers_by_id_negative(self, test_case):
        auth = self.ranger_admin_config
        rand_id = random.randint(999999, 9999999)
        flg = groupuser_exists(rand_id, self.ranger_admin_config, self.base_url, self.headers)
        assert not flg, f"Group-User association with id {rand_id} exists which is not expected for test case: {test_case}"
        response = requests.get(
            f"{self.base_url}/xusers/groupusers/{rand_id}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, 400, f"Get group-user association by id should have failed with 400 but got {response.status_code} instead for test case: {test_case}")

    @pytest.mark.negative
    @pytest.mark.get
    @pytest.mark.parametrize(
        "test_case, auth",
        [("invalid access by non existing group name", "ranger_admin_config"), ("invalid auth by user", "ranger_user_config"), ("invalid auth by keyadmin", "ranger_key_admin_config"), ("invalid auth by auditor", "ranger_auditor_config")])
    def test_get_groupusers_by_groupName_negative(self, test_case, auth):
        auth = getattr(self, auth)
        if test_case == "invalid access by non existing group name":
            groupname = "non_existent_group_name_" + str(uuid.uuid4())[:8]
            flg = group_exists_by_name(groupname, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group with name {groupname} exists which is not expected for test case: {test_case}"
            expected_response_code = 200
        else:
            groupname = self.group["name"]
            expected_response_code = 403
        response = requests.get(
            f"{self.base_url}/xusers/groupusers/groupName/{groupname}",
            auth = auth,
            headers=self.headers,
        )
        assert_response(response, expected_response_code, f"Get group-user association by group name should have failed with {expected_response_code} but got {response.status_code} instead for test case: {test_case}")
        if test_case == "invalid access by non existing group name":
            assert response.json() == {}, f"Response body is not empty for non existing group name which is not expected for test case: {test_case}"

    @pytest.mark.negative
    @pytest.mark.post
    @pytest.mark.parametrize(
        "test_case, auth",
        [
            ("invalid auth by keyadmin", "ranger_key_admin_config"),
            ("invalid auth by auditor", "ranger_auditor_config"),
            ("invalid auth by user", "ranger_user_config"),
            ("missing mandatory field name", "ranger_admin_config"),
            ("missing mandatory field userId", "ranger_admin_config"),
            ("invalid payload - non existent user id", "ranger_admin_config"),
        ])
    def test_create_groupuser_negative(self, test_case, auth):
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        temp_user_id = self.ranger_user_id1

        payload = {
            "name": temp_group["name"],
            "parentGroupId": temp_group_id,
            "userId": temp_user_id
        }
        response_code = 404 # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.

        if test_case == "missing mandatory field name":
            del payload["name"]
            response_code = 400

        elif test_case == "missing mandatory field userId":
            del payload["userId"]
            response_code = 400

        elif test_case == "invalid payload - non existent user id":
            payload["userId"] = -999999
            response_code = 404

        assert user_exists(temp_user_id, self.ranger_admin_config, self.base_url, self.headers) or test_case == "invalid payload - non existent user id", \
            f"User with id {temp_user_id} does not exist which is required for test case: {test_case}"

        response = requests.post(
            f"{self.base_url}/xusers/groupusers",
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"},
            json=payload
        )

        assert_response(response, response_code, f"Group-User association creation should have failed with {response_code} but got {response.status_code} instead for test case: {test_case}")


    @pytest.mark.negative
    @pytest.mark.put
    @pytest.mark.parametrize(
        "test_case, auth",[
            ("invalid auth by keyadmin", "ranger_key_admin_config"),
            ("invalid auth by auditor", "ranger_auditor_config"),
            ("invalid auth by user", "ranger_user_config"),
            ("missing mandatory field", "ranger_admin_config"),
            ("invalid payload - non existent user id", "ranger_admin_config"),
            ("invalid payload - non existent group name", "ranger_admin_config"),
            ("invalid payload - non existent id", "ranger_admin_config") ])
    def test_update_groupuser_negative(self, test_case, auth):
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        temp_user_id = self.ranger_user_id1
        grp_user = create_groupuser(temp_group["name"], temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        grp_user_id = grp_user["id"]    
        assert groupuser_exists(grp_user_id, self.ranger_admin_config, self.base_url, self.headers), f"Group-User association with id {grp_user_id} does not exist which is required for test case: {test_case}"
        if test_case == "missing mandatory field":
            payload = {
                "id": grp_user_id,
                "name": temp_group["name"], 
                "userId": temp_user_id
            }
            del payload["name"]
            response_code = 400
        elif test_case == "invalid payload - non existent user id":
            payload = {
                "id": grp_user_id,
                "name": temp_group["name"], 
                "userId": -9999
            }
            response_code = 404
        elif test_case == "invalid payload - non existent group name":
            payload = {
                "id": grp_user_id,
                "name": "non_existent_group_name_ugn_" + str(uuid.uuid4())[:8], 
                "userId": temp_user_id
            }
            response_code = 200
        elif test_case == "invalid payload - non existent id":
            payload = {
                "id": -999999,
                "name": temp_group["name"], 
                "userId": temp_user_id
            }
            response_code = 400 
        elif test_case.startswith("invalid auth"):
            payload = {
                "id": grp_user_id,
                "name": temp_group["name"], 
                "userId": temp_user_id
            }
            response_code = 403
        response = requests.put(
            f"{self.base_url}/xusers/groupusers",
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"},
            json=payload
        )   
        assert_response(response, response_code, f"Group-User association update should have failed with {response_code} but got {response.status_code} instead for test case: {test_case}")
        delete_groupuser(grp_user_id, self.ranger_admin_config, self.base_url, self.headers)


    @pytest.mark.negative
    @pytest.mark.delete
    @pytest.mark.parametrize(
        "test_case, auth",[
            ("invalid auth by keyadmin", "ranger_key_admin_config"), 
            ("invalid auth by auditor", "ranger_auditor_config"), 
            ("invalid auth by user", "ranger_user_config"),
            ("non existent id", "ranger_admin_config")])
    def test_delete_groupuser_negative(self, test_case, auth):
        auth = getattr(self, auth)
        temp_group, temp_group_id = self.group, self.group_id
        temp_user_id = self.ranger_user_id1
        grp_user = create_groupuser(temp_group["name"], temp_user_id, self.ranger_admin_config, self.base_url, self.headers)
        grp_user_id = grp_user["id"]    
        assert groupuser_exists(grp_user_id, self.ranger_admin_config, self.base_url, self.headers), f"Group-User association with id {grp_user_id} does not exist which is required for test case: {test_case}"
        if test_case == "non existent id":
            rand_id = random.randint(999999, 9999999)
            flg = groupuser_exists(rand_id, self.ranger_admin_config, self.base_url, self.headers)
            assert not flg, f"Group-User association with id {rand_id} exists which is not expected for test case: {test_case}"
            target_id = rand_id
            expected_response_code = 400
        else:
            target_id = grp_user_id
            expected_response_code = 404 # Set default response to 404 for auth failures to prevent API endpoint discovery due to spring slient failure.

        response = requests.delete(
            f"{self.base_url}/xusers/groupusers/{target_id}",
            auth=auth,
            headers={**self.headers, "X-Requested-By": "ranger"},
        )

        assert_response(response, expected_response_code, f"Group-User association deletion should have failed with {expected_response_code} but got {response.status_code} instead for test case: {test_case}")
        delete_groupuser(grp_user_id, self.ranger_admin_config, self.base_url, self.headers)