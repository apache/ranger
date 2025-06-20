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


import requests
import pytest
from utils import fetch_logs

BASE_URL = "http://localhost:9292/kms/v1"
PARAMS = {"user.name": "keyadmin"}

class TestKeyDetails:

    @pytest.fixture(autouse=True)
    def setup_class(self, create_test_key):
        self.test_key = create_test_key
    
    # ***********************************************************************************
    #  Get key names 
    # ***********************************************************************************
    def test_get_key_names(self):
        response = requests.get(f"{BASE_URL}/keys/names",params=PARAMS)
        
        if response.status_code!=200:                        #log check
            logs=fetch_logs()
            pytest.fail(f"Get key operation failed. API Response: {response.text}\nLogs:\n{logs}")

        print(response.json())
        assert self.test_key["name"] in response.json()


    # ***********************************************************************************
    #  Parametrized Get key metadata check for existent and non existent key
    #  Note: key1 is coming from create_test_key fixture in conftest.py
    # ***********************************************************************************

    @pytest.mark.parametrize("key_name, expected_status, expected_response", [
    ("key1", 200, "valid"),                   # Key exists, should return valid metadata
    ("non-existent-key", 200, "invalid"),     # Key does not exist but returns 200 with [] should give 404
    ])
    def test_get_key_metadata(self, headers, key_name, expected_status, expected_response):
    
        response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", headers=headers, params=PARAMS)
        
        logs=fetch_logs()                #log check
        assert response.status_code==expected_status,f"Get key metadata operation failed. API Response: {response.text}\nLogs:\n{logs}"           

        if expected_response == "invalid":
         assert response.text.strip() in ["", "[ ]", "{ }"], f"Expected blank response for non-existent key, got: {response.text}"
        



    # ***********************************************************************************
    #  Parametrized Get Key version for existent and non existent key
    #  Note: key1 is coming from create_test_key fixture in conftest.py
    # ***********************************************************************************

    @pytest.mark.parametrize("key_name, expected_status, expected_response", [
        ("key1", 200, "valid"),               # Key exists
        ("non-existent-key", 200,"invalid"),  # Key does not exist but returns 200 with [] should give 404
    ])
    def test_get_key_versions(self, headers, key_name, expected_status,expected_response):

        response = requests.get(f"{BASE_URL}/key/{key_name}/_versions", headers=headers, params=PARAMS)
        
        logs=fetch_logs()    #log check
        assert response.status_code == expected_status,f"Get key version operation failed. API Response: {response.text}\nLogs:\n{logs}"        
          
        if expected_response == "invalid":
         assert response.text.strip() in ["", "[ ]", "{ }"], f"Expected blank response for non-existent key, got: {response.text}"


    # ***********************************************************************************
    #  Get Key metadata for multiple keys at once
    #  Note: key1 is coming from create_test_key fixture in conftest.py
    # ***********************************************************************************

    def test_get_keys_metadata(self, headers):
       
       #Create second key (key2)
        key_name="key2"
        data = {
            "name":key_name
        }
        create_response = requests.post(f"{BASE_URL}/keys", headers=headers, json=data, params=PARAMS)
        assert create_response.status_code == 201, f"Key2 creation failed: {create_response.text}"

        try:
           # Check metadata for existing keys (key1 and key2)
            existing_keys = ["key1", "key2"]
            params = [("key", k) for k in existing_keys]
            params.append(("user.name", "keyadmin"))

            response = requests.get(f"{BASE_URL}/keys/metadata", headers=headers, params=params)
            assert response.status_code == 200, f"Metadata fetch failed: {response.status_code}"

            metadata = response.json()
            returned_keys = [entry["name"] for entry in metadata]
            for key in existing_keys:
                assert key in returned_keys, f"Expected key '{key}' not found in metadata response"

            # Check metadata for non-existent keys
            fake_keys = ["nonExistent_key_1", "nonExistent_key_2"]
            params = [("key", k) for k in fake_keys]
            params.append(("user.name", "keyadmin"))

            response = requests.get(f"{BASE_URL}/keys/metadata", headers=headers, params=params)
            assert response.status_code == 200, f"Metadata fetch failed for non-existent keys: {response.status_code}"

            assert response.json() == [{}, {}], f"Expected blank response for non-existent key, got: {response.text}"

        finally:
            # Cleanup key2
            requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)




    # ***********************************************************************************
    #  Test for endpoint 'get key version' 
    #  Note: key1 is coming from create_test_key fixture in conftest.py
    # ***********************************************************************************
    
    @pytest.mark.parametrize("version_name, expected_status, expected_valid", [
    ("key1@0", 200, True),                      # Valid version
    ("non-existent-key@0", 200, False),         # Key does not exist but returns 200 with [] should give 404
    ])

    def test_get_key_version(self, headers, version_name, expected_status, expected_valid):
        response = requests.get(f"{BASE_URL}/keyversion/{version_name}", headers=headers, params=PARAMS)
        
        logs = fetch_logs()
        assert response.status_code == expected_status,f"Get key version failed. Response: {response.text}\nLogs:\n{logs}"
        
        if expected_valid:
            try:
                version_data = response.json()
                assert "versionName" in version_data, "versionName missing in response"
                assert version_data["versionName"] == version_name, f"Mismatch in version name: expected {version_name}"
            except ValueError:
                pytest.fail(f"Expected valid JSON response, got: {response.text}")
        else:
            assert response.text.strip() in [" ", "{ }", "[ ]"], f"Expected empty for invalid version, got: {response.text}"
               


