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
import json
import pytest
from  Utility.main import get_request_data ,base_url,get_updated_request_data ,get_variable ,compare_response_data,return_random_str ,admin_auth ,headers,keyadmin_auth,str_variable_dict,variable_dict
from requests.auth import HTTPBasicAuth
import time
import os
import copy



BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # Gets Tests_Ranger root
test_data_path = os.path.join(BASE_DIR,"Utility", "test_jsons")
data_folder_path = os.path.join(BASE_DIR, "Utility", "variable_jsons")
variables_data_path = data_folder_path



def test_get_plugins_info_by_different_roles():
    """
    Test retrieves plugin information by auditor user.
    Auditor should have read access to plugin information for auditing purposes.
    """
    request_url = base_url + '/plugins/plugins/info'
    # admin  should be able to access this api

    resp = requests.get(request_url, verify=False, auth=admin_auth,
                        headers=headers)
    assert resp.status_code == 200, "Admin should be able to retrieve plugin information"
    # auditor should be able to access  this api
    resp = requests.get(request_url, verify=False,
                        auth=HTTPBasicAuth(str_variable_dict['auditor_user'], 'Test@12345'),
                        headers=headers)
    assert resp.status_code == 200, "Auditor should be able to retrieve plugin information"
    # Normal user should not be able to access this api
    resp = requests.get(request_url, verify=False,
                        auth=HTTPBasicAuth(str_variable_dict['user2'], 'Test@12345'),
                        headers=headers)
    assert resp.status_code == 403, "User role  should not  be able to retrieve plugin information"



def test_get_plugins_info_by_keyadmin():
    """
    Test retrieves plugin information by keyadmin user.
    """
    request_url = base_url + '/plugins/plugins/info'

    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code ==200 , "Response depends on keyadmin permissions for plugin info"




@pytest.mark.skip
def test_get_plugins_info_with_pagination():
    """
    Test retrieves plugin information with pagination parameters.
    Validates that startIndex and pageSize parameters work correctly.
    """
    # Get first page
    request_url = base_url + '/plugins/plugins/info?startIndex=0&pageSize=5'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code 200"

    page1_data = resp.json()
    assert page1_data.get('pageSize') == 5, "Page size should be 5"
    assert page1_data.get('startIndex') == 0, "Start index should be 0"

    # Get second page if there are more results
    if page1_data.get('totalCount', 0) > 5:
        request_url = base_url + '/plugins/info?startIndex=5&pageSize=5'
        resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
        assert resp.status_code == 200, "Expected status code 200"

        page2_data = resp.json()
        assert page2_data.get('startIndex') == 5, "Start index should be 5"