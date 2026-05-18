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
from xuserrest.utility.utils import *
import uuid
import pytest
import requests
from datetime import datetime
import random

@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestLookup:

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
        [
            ("admin", "ranger_admin_config"),
            ("key_admin", "ranger_key_admin_config"),
            ("auditor", "ranger_auditor_config"),
            ("user", "ranger_user_config"),
        ]
    )
    def test_get_lookup_users(self, role, auth):
        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/xusers/lookup/users",
            auth=auth,
            headers=self.headers
        )
        assert response.status_code == 200

        data = response.json()
        
        if data != []:
            assert "vXStrings" in data and isinstance(data["vXStrings"], list)
            assert isinstance(data["vXStrings"][0]['value'], str)

    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("admin", "ranger_admin_config"),
            ("key_admin", "ranger_key_admin_config"),
            ("auditor", "ranger_auditor_config"),
            ("user", "ranger_user_config"),
        ]
    )
   
    def test_get_lookup_groups(self, role, auth):
        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/xusers/lookup/groups",
            auth=auth,
            headers=self.headers
        )
        assert response.status_code == 200

        data = response.json()
        
        if data != []:
            assert "vXStrings" in data and isinstance(data["vXStrings"], list)
            assert isinstance(data["vXStrings"][0]['value'], str)

    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("admin", "ranger_admin_config"),
            ("key_admin", "ranger_key_admin_config"),
            ("auditor", "ranger_auditor_config"),
            ("user", "ranger_user_config"),
        ]
    )
    def test_get_lookup_principals(self, role, auth):
        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/xusers/lookup/principals",
            auth=auth,
            headers=self.headers
        )
        assert response.status_code == 200

        data = response.json()
        
        if data != []:
            assert "vXStrings" in data and isinstance(data["vXStrings"], list)
            assert isinstance(data["vXStrings"][0]['value'], str)

@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestAuthSession:

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
        [
            ("admin", "ranger_admin_config"),
            ("key_admin", "ranger_key_admin_config"),
            ("auditor", "ranger_auditor_config"),
        ]
    )
    @pytest.mark.parametrize(
        "params, expected_status, test_case",
        [
            ({}, 200, "default_request"),
            ({"startIndex": -5}, 200, "negative_start_index"),
            ({
                "startIndex": 10, 
                "pageSize": 50, 
                "getCount": "false", 
                "ownerId": 123, 
                "getChildren": "true", 
                "sortBy": "id",
                "sortType": "asc"
            }, 200, "valid_sort_all_params"),

        ],
        ids=[
            "default",
            "negative-start-index",
            "valid-sort-all-params",
        ],
    )
    def test_get_auth_sessions(self, role, auth, params, expected_status, test_case):
        if role == "user":
            pytest.fail("Users should not have access to auth sessions endpoint, but this test case is marked positive which is incorrect. Please fix the test case or its marker.")
        auth = getattr(self, auth)
        response = requests.get(
            f"{self.base_url}/xusers/authSessions",
            params=params,
            auth=auth,
            headers=self.headers
        )

        assert response.status_code == expected_status, f"Expected {expected_status} but got {response.status_code} for test_case: {test_case}"
        

        if expected_status == 200:
            json_resp = response.json()
            assert "vXAuthSessions" in json_resp or "totalCount" in json_resp


    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("admin", "ranger_admin_config"),
            ("key_admin", "ranger_key_admin_config"),
            ("auditor", "ranger_auditor_config"),
        ]
    )
    def test_get_auth_session_info_success(self, role, auth):
        if role == "user":
            pytest.fail("Users should not have access to auth sessions endpoint, but this test case is marked positive which is incorrect. Please fix the test case or its marker.")
        auth = getattr(self, auth)
        auth_response = requests.get(
            f"{self.base_url}/xusers/authSessions",
            auth=auth,
            headers=self.headers
        )
        
        assert auth_response.status_code == 200
        
        # Capture the session ID from the cookie jar
        session_id = auth_response.cookies.get("RANGERADMINSESSIONID")
        assert session_id is not None, "RANGERADMINSESSIONID cookie not found in response"

        # Pass the captured session ID to the /info endpoint
        params = {"extSessionId": session_id}
        info_response = requests.get(
            f"{self.base_url}/xusers/authSessions/info",
            params=params,
            auth=self.ranger_admin_config,
            headers=self.headers
        )

        assert info_response.status_code == 200
        
        data = info_response.json()
        expected_user = auth[0] 
        assert data.get("loginId") == expected_user, f"Expected loginId to be {expected_user} but got {data.get('loginId')}"
        assert data.get("id") is not None

    
    # NEGATIVE TEST CASES

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, auth",
        [
            ("invalid-sort-field", "ranger_admin_config"),
            ("invalid-input-data", "ranger_admin_config"),
            ("unauthorized-access-user", "ranger_user_config"),
        ]
    )
    def test_get_auth_sessions_negative(self, test_case, auth):
        auth = getattr(self, auth)
        params = {}
        if test_case == "invalid-sort-field":
            params = {"sortBy": "unsupportedFieldXYZ", "sortType": "desc"}  # silently ignore sortType when sortBy is invalid to check if it validates sortBy first
        elif test_case == "invalid-input-data":
            params = {"startIndex": "not_a_number"}

        response = requests.get(
            f"{self.base_url}/xusers/authSessions",
            params=params,
            auth=auth,
            headers=self.headers
        )
        if test_case.startswith("unauthorized-access"):
            assert response.status_code == 403, f"Expected 403 Forbidden for {test_case}, but got {response.status_code}"
        elif test_case == "invalid-sort-field":
            assert response.status_code == 200, f"Expected 200 silent failure for {test_case}, but got {response.status_code}"
        elif test_case == "invalid-input-data":
            assert response.status_code == 400, f"Expected 400 Bad Request for {test_case}, but got {response.status_code}"


    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, auth",
        [
            ("missing-session-id", "ranger_admin_config"),
            ("invalid-session-id", "ranger_admin_config"),
            ("unauthorized-access-user", "ranger_user_config"),
        ]
    )   
    def test_get_auth_session_info_negative(self, test_case, auth):
        auth = getattr(self, auth)
        params = {}
        if test_case == "invalid-session-id":
            params = {"extSessionId": "invalidSessionIdXYZ"}
        elif test_case == "missing-session-id":
            params = {}

        response = requests.get(
            f"{self.base_url}/xusers/authSessions/info",
            params=params,
            auth=auth,
            headers=self.headers
        )

        if test_case.startswith("unauthorized-access"):
            assert response.status_code == 403, f"Expected 403 Forbidden for {test_case}, but got {response.status_code}"
        elif test_case in ["missing-session-id", "invalid-session-id"]:
            assert response.status_code == 400, f"Expected 400 Bad Request for {test_case}, but got {response.status_code}"


@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestDownloadServiceName:

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

        cls.headers = default_headers
        cls.base_url = ranger_config["base_url"]

        init_configs(
            cls.ranger_admin_config,
            cls.ranger_key_admin_config,
            cls.ranger_auditor_config,
            cls.ranger_user_config,
        )

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
    #             f"{self.base_url}/xusers/download/{service_name}",
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
    #         f"{self.base_url}/xusers/download/{service_name}",
    #         params=params,
    #         auth=auth,
    #         headers=self.headers
    #     )
    #     assert_response(response, expected_status, test_case)
    #     if expected_status == 200:
    #         data = response.json()
    #         assert "userStoreVersion" in data, f"Response missing 'userStoreVersion' for {test_case}"
    #         assert "userGroupMapping" in data or "users" in data, f"Response missing user/group data for {test_case}"
        

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
    def test_get_secure_download_by_serviceName(self, test_case, auth, service_name, last_known_version, expected_status, role):

        auth = getattr(self, auth)

        base_params = {
            "lastActivationTime": 0,
            "clusterName": "mycluster",
            "pluginId": "hdfs@host1"
        }

        if test_case == "valid-service-no-change":
            seed_response = requests.get(
                f"{self.base_url}/xusers/secure/download/{service_name}",
                params={
                    **base_params,
                    "lastKnownUserStoreVersion": -1
                },
                auth=auth,
                headers=self.headers
            )

            assert seed_response.status_code == 200, \
                f"Seed request failed: {seed_response.status_code}"

            last_known_version = seed_response.json()["userStoreVersion"]

        response = requests.get(
            f"{self.base_url}/xusers/secure/download/{service_name}",
            params={
                **base_params,
                "lastKnownUserStoreVersion": last_known_version
            },
            auth=auth,
            headers=self.headers
        )

        assert_response(response, expected_status, test_case)

        if expected_status == 200:
            data = response.json()

            assert "userStoreVersion" in data
            assert "userGroupMapping" in data or "users" in data


    # NEGATIVE TEST CASES

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
    def test_get_secure_download_by_serviceName_negative(self, test_case, auth, service_name, expected_status):

        auth_credentials = getattr(self, auth)

        params = {
            "lastKnownUserStoreVersion": -1,
            "lastActivationTime": 0,
            "clusterName": "mycluster",
        }

        if test_case != "missing-plugin-id":
            params["pluginId"] = "hdfs@host1"

        response = requests.get(
            f"{self.base_url}/xusers/secure/download/{service_name}",
            params=params,
            auth=auth_credentials,
            headers=self.headers
        )

        assert_response(response, expected_status, test_case)

    
@pytest.mark.usefixtures("ranger_config", "ranger_key_admin_config")
@pytest.mark.xuserrest
class TestMiscellaneous:

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

        # ---------- group details ----------
        cls.group, cls.group_id = temp_group()
        cls.group1, cls.group_id1 = temp_group()


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
        "role, auth, test_case",
        [
            ("admin", "ranger_admin_config", "admin-access"),
            ("key_admin", "ranger_key_admin_config", "key_admin-access"),
            ("auditor", "ranger_auditor_config", "auditor-access"),
            ("user", "ranger_user_config", "user-access"),
            ("admin", "ranger_admin_config", "admin-without-groups-"),
        ])
    def test_get_groups_by_userid(self, role, auth, test_case, request):
        auth = getattr(self, auth)
        assign_groups_to_user(self.user['name'], [self.group['name'], self.group1['name']], self.ranger_admin_config, self.base_url, self.headers)
        
        u_id = self.ranger_user_id
        if test_case == "admin-without-groups-access":
            # create a new user without any groups
            new_user, new_user_id = request.getfixturevalue("temp_secure_user")(["user"])
            u_id = new_user_id
        response = requests.get(
            f"{self.base_url}/xusers/{u_id}/groups",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, test_case)
        data = response.json()
        assert "vXGroups" in data and isinstance(data["vXGroups"], list)
        lis = [self.group_id, self.group_id1]

        if test_case == "admin-without-groups-access":
            assert data["vXGroups"] == [], f"Expected no groups for the user in {test_case}, but found some groups in response"

        for i in data["vXGroups"]:
            assert i["id"] in lis, f"Unexpected group id {i['id']} found in response for {test_case}"
            lis.remove(i["id"])


    @pytest.mark.get
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth, test_case",
        [
            ("admin", "ranger_admin_config", "admin-access"),
            ("key_admin", "ranger_key_admin_config", "key_admin-access"),
            ("auditor", "ranger_auditor_config", "auditor-access"),
            ("admin", "ranger_admin_config", "admin-without-users"),
        ])
    def test_get_users_by_groupid(self, role, auth, test_case, request):

        auth = getattr(self, auth)
        temp_group, temp_group_id = request.getfixturevalue("temp_group")()
        if test_case != "admin-without-users":
            assign_groups_to_user(self.user['name'], [temp_group['name']], self.ranger_admin_config, self.base_url, self.headers)
            assign_groups_to_user(self.audit['name'], [temp_group['name']], self.ranger_admin_config, self.base_url, self.headers)
        response = requests.get(
            f"{self.base_url}/xusers/{temp_group_id}/users",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 200, test_case)
        data = response.json()
        assert "vXUsers" in data and isinstance(data["vXUsers"], list)
        if test_case == "admin-without-users":
            assert data["vXUsers"] == [], f"Expected no users for the group in {test_case}, but found some users in response"
            return
        user_ids = [self.ranger_user_id, self.ranger_auditor_id]
        for i in data["vXUsers"]:
            assert i["id"] in user_ids, f"Unexpected user id {i['id']} found in response for {test_case}"
            user_ids.remove(i["id"])



    @pytest.mark.delete
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("admin", "ranger_admin_config"),
        ])
    @pytest.mark.parametrize(
        "test_case",
        ["single external user", "multiple external users"]
    )
    def test_forcedelete_external_user(self, role, auth, test_case, request):
        if role != "admin":
            pytest.fail("Only admin should have access to force delete users, but this test case is marked positive for other roles which is incorrect. Please fix the test case or its marker.")
        auth = getattr(self, auth)
        ext_user,ext_userid = request.getfixturevalue("temp_secure_user")(["user"])
        
        user_list = [ext_user]

        if test_case == "multiple external users":
            ext_user1,ext_userid1 = request.getfixturevalue("temp_secure_user")(["user"])
            user_list.append(ext_user1)
        
        for i in user_list:
            assert i["userSource"] == 1, f"User {i['name']} is not an external user as expected for this test case"
            assert i["syncSource"] == "EXTERNAL_PYTEST", f"User {i['name']} does not have the expected syncSource for this test case"
            assert i["isVisible"] == 1, f"User {i['name']} is not visible as expected for this test case"


        query_string = "syncSource=EXTERNAL_PYTEST&isVisible=1"

        response = requests.delete(
            f"{self.base_url}/xusers/delete/external/users?{query_string}",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, test_case)

        for i in user_list:
            # Verify the user is deleted
            get_response = requests.get(
                f"{self.base_url}/xusers/{i['id']}",
                auth=auth,
                headers=self.headers
            )
            assert get_response.status_code == 404, f"Expected user {i['name']} to be deleted, but it still exists"        

    @pytest.mark.delete
    @pytest.mark.positive
    @pytest.mark.parametrize(
        "role, auth",
        [
            ("admin", "ranger_admin_config"),
        ])
    @pytest.mark.parametrize(
        "test_case",
        ["single external group", "multiple external group"]
    )
    def test_forcedelete_external_group(self, role, auth, test_case, request):
        if role != "admin":
            pytest.fail("Only admin should have access to force delete groups, but this test case is marked positive for other roles which is incorrect. Please fix the test case or its marker.")
        auth = getattr(self, auth)
        ext_grp,ext_grpid = request.getfixturevalue("temp_group")()
        
        group_list = [ext_grp]

        if test_case == "multiple external groups":
            ext_grp1,ext_grpid1 = request.getfixturevalue("temp_group")()
            group_list.append(ext_grp1)
        
        for i in group_list:
            assert i["groupSource"] == 1, f"Group {i['name']} is not an external group as expected for this test case"
            assert i["syncSource"] == "EXTERNAL_PYTEST", f"Group {i['name']} does not have the expected syncSource for this test case"
            assert i["isVisible"] == 1, f"Group {i['name']} is not visible as expected for this test case"


        query_string = "syncSource=EXTERNAL_PYTEST&isVisible=1"

        response = requests.delete(
            f"{self.base_url}/xusers/delete/external/groups?{query_string}",
            auth=auth,
            headers=self.headers
        )

        assert_response(response, 200, test_case)

        for i in group_list:
            # Verify the group is deleted
            get_response = requests.get(
                f"{self.base_url}/xusers/groups/{i['id']}",
                auth=auth,
                headers=self.headers
            )
            assert get_response.status_code == 400, f"Expected group {i['name']} to be deleted, but it still exists"  

    
    # Negative Tests

    @pytest.mark.get
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, auth",
        [
            ("unauthorized-access-user", "ranger_user_config"),
        ]
    )
    def test_get_users_by_groupid_negative(self, auth, test_case, request):

        auth = getattr(self, auth)
        temp_group, temp_group_id = request.getfixturevalue("temp_group")()
        response = requests.get(
            f"{self.base_url}/xusers/{temp_group_id}/users",
            auth=auth,
            headers=self.headers
        )
        assert_response(response, 403, f'Expected 403 Forbidden for {test_case}, but got {response.status_code}')

    @pytest.mark.delete
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, auth",
        [
            ("unauthorized-access-keyadmin", "ranger_key_admin_config"),
            ("unauthorized-access-auditor", "ranger_auditor_config"),
            ("unauthorized-access-user", "ranger_user_config"),
            ("no_users_matching_criteria", "ranger_admin_config"),
        ])
    def test_forcedelete_external_user_negative(self, test_case, auth):
        auth_credentials = getattr(self, auth)

        if test_case.startswith("unauthorized-access"):
            expected_status = 404
            
            query_string = "syncSource=EXTERNAL_PYTEST&isVisible=1"
        elif test_case == "no_users_matching_criteria":
            expected_status = 200
            query_string = "syncSource=KKjkbjhvytgc_234"

        response = requests.delete(
            f"{self.base_url}/xusers/delete/external/users?{query_string}",
            auth=auth_credentials,
            headers=self.headers
        )

        assert_response(response, expected_status, test_case)

        if test_case == "no_users_matching_criteria":
           assert "No users were deleted!" in response.text

    @pytest.mark.delete
    @pytest.mark.negative
    @pytest.mark.parametrize(
        "test_case, auth",
        [
            ("unauthorized-access-keyadmin", "ranger_key_admin_config"),
            ("unauthorized-access-auditor", "ranger_auditor_config"),
            ("unauthorized-access-user", "ranger_user_config"),
            ("no_users_matching_criteria", "ranger_admin_config"),
        ])
    def test_forcedelete_external_group_negative(self, test_case, auth):
        auth_credentials = getattr(self, auth)

        if test_case.startswith("unauthorized-access"):
            expected_status = 404
            
            query_string = "syncSource=EXTERNAL_PYTEST&isVisible=1"
        elif test_case == "no_users_matching_criteria":
            expected_status = 200
            query_string = "syncSource=KKjkbjhvytgc_234"

        response = requests.delete(
            f"{self.base_url}/xusers/delete/external/groups?{query_string}",
            auth=auth_credentials,
            headers=self.headers
        )

        assert_response(response, expected_status, test_case)

        if test_case == "no_users_matching_criteria":
           assert "No groups were deleted!" in response.text
