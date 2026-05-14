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
class AuthSession:

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

            # ({"sortBy": "unsupportedFieldXYZ", "sortType": "desc"}, 200, "invalid_sort_field"),
            # ({"startIndex": "not_a_number"}, 400, "invalid_input_data"),

            # "invalid-sort-field",
            # "invalid-input-data",