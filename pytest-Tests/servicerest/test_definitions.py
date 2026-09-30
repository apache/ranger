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



def test_get_definition_using_id_by_admin():

    request_url = base_url + '/plugins/definitions/{plugin_definition_1_id}'
    request_url = request_url.format(**str_variable_dict)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"

serviceList=[]
# serviceList is being used to store the list of service names which will be used in next test case to get the definition of each service using its name.

def test_get_definitions_by_admin():
    request_url = base_url + '/plugins/definitions'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"
    resp_data=resp.json()

    serviceDef_list=resp_data['serviceDefs']
    for dictionaries in serviceDef_list:
        serviceList.append(dictionaries['name'])
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    resp=resp.json()
    assert len(resp_data['serviceDefs'])>=len(resp['serviceDefs']), "Expected service definition list not returned for keyadmin user, admin and keyadmin should have different views "

def test_get_definitions_by_different_users():
    request_url = base_url + '/plugins/definitions'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned , key admin is not able access the get definitions api "
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user2'], 'Test@12345'), headers=headers)
    assert resp.status_code == 200, "Expected status code not returned , user with role user not able  access the get definitions api "
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user4'], 'Test@12345'), headers=headers)
    assert resp.status_code == 200, "Expected status code not returned , user with role admin auditor not able  access the get definitions api "
    # resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user5'], 'Test@12345'), headers=headers)
    # assert resp.status_code == 200, "Expected status code not returned , user with role key admin auditor  not able to  access the get definitions api "api


def test_get_service_defs_pagination():
    """
    Test pagination parameters (startIndex, pageSize) work correctly.
    """
    request_url = base_url + '/plugins/definitions?startIndex=0&pageSize=5'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)

    assert resp.status_code == 200, "Expected status code 200"
    resp_data = resp.json()
    assert resp_data.get('pageSize') == 5, "Page size should match requested value"
    assert resp_data.get('startIndex') == 0, "Start index should match requested value"
    assert len(resp_data.get('serviceDefs', [])) <= 5, "Number of results should not exceed page size"

@pytest.mark.skip
def test_get_service_defs_sorting():
    """
    Test sorting parameters (sortBy, sortType) work correctly.
    """
    # Test ascending sort
    request_url_asc = base_url + '/plugins/definitions?sortBy=name&sortType=asc'
    resp_asc = requests.get(request_url_asc, verify=False, auth=admin_auth, headers=headers)

    # Test descending sort
    request_url_desc = base_url + '/plugins/definitions?sortBy=name&sortType=desc'
    resp_desc = requests.get(request_url_desc, verify=False, auth=admin_auth, headers=headers)

    assert resp_asc.status_code == 200 and resp_desc.status_code == 200, "Expected status code 200"

    data_asc = resp_asc.json()
    data_desc = resp_desc.json()

    if data_asc.get('serviceDefs') and data_desc.get('serviceDefs'):
        names_asc = [sd.get('name', '') for sd in data_asc['serviceDefs']]
        names_desc = [sd.get('name', '') for sd in data_desc['serviceDefs']]
        assert names_asc == sorted(names_asc), "Ascending sort should return items in ascending order"
        assert names_desc == sorted(names_desc, reverse=True), "Descending sort should return items in descending order"
        assert list(reversed(names_asc)) == names_desc, "Descending should be reverse of ascending"


def test_get_definitions_name_by_admin():
    for service_names  in serviceList:
        request_url = base_url + '/plugins/definitions/name/{service_names}'
        request_url = request_url.format(service_names=service_names)
        resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
        assert resp.status_code == 200, "Expected status code not returned"


def test_get_definition_by_name_nonexistent_service():
    """
    Test that requesting a non-existent service definition returns 404.
    """
    request_url = base_url + '/plugins/definitions/name/nonexistent_service_xyziuvjbk43213w'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 404, "Should return 404 for non-existent service definition"


def test_get_definition_by_name_response_structure():
    """
    Test that response contains all expected fields for service definition.
    """
    request_url = base_url + '/plugins/definitions/name/hdfs'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    if resp.status_code == 200:
        resp_data = resp.json()
        # Check for expected fields in service definition
        expected_fields = ['id', 'name', 'displayName', 'implClass', 'resources', 'accessTypes']
        for field in expected_fields:
            assert field in resp_data, f"Service definition should contain {field}"


def test_get_definition_by_name_with_whitespace():
    """
    Test service definition name with leading/trailing whitespace.
    """
    request_url = base_url + '/plugins/definitions/name/ hdfs '
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # Should either trim whitespace or return 404
    assert resp.status_code ==404 , "Should handle whitespace in service name"


def test_get_definition_by_name_performance():
    """
    Test that API responds within acceptable time limits.
    """
    request_url = base_url + '/plugins/definitions/name/hdfs'
    start_time = time.time()
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    end_time = time.time()
    response_time = end_time - start_time
    assert resp.status_code == 200, "Expected status code 200"
    assert response_time < 5.0, f"Response time {response_time}s should be under 5 seconds"




def test_post_create_defintion_by_admin(log):
    request_url = base_url + '/plugins/definitions'
    request_data = get_request_data('test_create_defintion.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 200, "Expected status code not returned"
    # cleaning the created definition
    definition_id=resp.json().get('id')
    if(resp.status_code == 200):
        log.info(f"Definition created with ID: {definition_id}, proceeding to delete it for cleanup.")
        delete_url = base_url + f'/plugins/definitions/{definition_id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        if(delete_resp.status_code in (200,204)):
            log.info(f"Successfully deleted definition with ID: {definition_id} during cleanup.")
        else:
            log.error(f"Failed to delete definition with ID: {definition_id} during cleanup. Status code: {delete_resp.status_code}, Response: {delete_resp.text}")



def test_post_create_definition_by_auditor_and_keyadmin():
    request_url = base_url + '/plugins/definitions'
    request_data = get_request_data('test_create_defintion.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=HTTPBasicAuth('keyadmin','rangerR0cks!'), headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 400, "Expected status code not returned , key admin should not be able to create definition , they are permitted to create only key admin definitions "
    resp = requests.post(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['auditor_user'], 'Test@12345'), headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 400, "Expected status code not returned , user with role admin auditor should not be able to create definition "

def test_post_create_definition_by_user():
    request_url = base_url + '/plugins/definitions'
    request_data = get_request_data('test_create_defintion.json', str_variable_dict, test_data_path)

    resp = requests.post(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user2'], 'Test@12345'), headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 400, "Expected status code not returned ,users should not be able to create definition"

def test_post_create_kms_definition_by_key_admin(log):
    request_url = base_url + '/plugins/definitions'
    request_data = get_request_data('test_create_kms_definition.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=HTTPBasicAuth('keyadmin', 'rangerR0cks!'), headers=headers,
                         data=json.dumps(request_data))
    definition_id = resp.json().get('id')
    assert resp.status_code == 200, "Expected status code not returned , key admin should be able to create kms definition  "
    resp2 = requests.post(request_url, verify=False, auth=HTTPBasicAuth('keyadmin', 'rangerR0cks!'), headers=headers,
                         data=json.dumps(request_data))
    assert resp2.status_code == 400, "Expected status code not returned , duplicate kms definition should not be allowed "
    if resp.status_code == 200:
        log.info(f"KMS Definition created with ID: {definition_id}, proceeding to delete it for cleanup.")
        delete_url = base_url + f'/plugins/definitions/{definition_id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=HTTPBasicAuth('keyadmin','rangerR0cks!'), headers=headers)
        if delete_resp.status_code in [200,204]:
            log.info(f"Successfully deleted KMS definition with ID: {definition_id} during cleanup.")
        else:
            log.error(
                f"Failed to delete KMS definition with ID: {definition_id} during cleanup. Status code: {delete_resp.status_code}, Response: {delete_resp.text}")




def test_put_edit_definition_using_id_by_admin():
    request_url = base_url + '/plugins/definitions/{plugin_definition_1_id}'
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["plugin_definition_1"]
    fields_to_update = {"description": "Modified description"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)

    # Update the definition
    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 200, "Expected status code not returned"

    # Verify the update was reflected
    get_resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert get_resp.status_code == 200, "Failed to retrieve updated definition"

    updated_data = get_resp.json()
    assert updated_data.get('description') == "Modified description", "Description was not updated correctly"
    assert updated_data.get('id') == request_data.get('id'), "Definition ID should remain unchanged"

@pytest.mark.skip
def test_put_different_id_passed_from_url_and_body_in_put_definitions(log):
    # First get the definition data from ID 1
    get_url = base_url + '/plugins/definitions/1'
    get_resp = requests.get(get_url, verify=False, auth=admin_auth, headers=headers)
    if get_resp.status_code == 200 :
        log.info("Successfully retrieved definition with ID 1 for testing mismatched ID scenario.")
    else:
        log.error(f"Failed to retrieve definition with ID 1. Status code: {get_resp.status_code}, Response: {get_resp.text}")
    request_data = get_resp.json()

    # Now use a different ID in the URL (123456789) but keep the original data from ID 1
    put_url = base_url + '/plugins/definitions/123456789'

    # Attempt to update with mismatched ID (URL has 123456789 but body has ID 1)
    resp = requests.put(put_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 400, "Expected status code 400 for mismatched ID in URL and body"

def test_put_auditor_and_user_cannot_update_definition(log):
    request_url = base_url + '/plugins/definitions/{plugin_definition_1_id}'
    request_url = request_url.format(**str_variable_dict)

    # Get the definition data - extract JSON from response
    get_resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    if get_resp.status_code == 200 :
        log.info("Successfully retrieved definition for auditor and user update test.")
    else:
        log.error(f"Failed to retrieve definition for auditor and user update test. Status code: {get_resp.status_code}, Response: {get_resp.text}")
    request_data = get_resp.json()

    # Attempt update with auditor credentials
    auditor_resp = requests.put(request_url, verify=False,
                                auth=HTTPBasicAuth(str_variable_dict['auditor_user'], 'Test@12345'),
                                headers=headers,
                                data=json.dumps(request_data))
    assert auditor_resp.status_code == 400, "Expected status code 400 for auditor attempting to update definition"

    # Attempt update with regular user credentials
    user_resp = requests.put(request_url, verify=False,
                             auth=HTTPBasicAuth(str_variable_dict['user2'], 'Test@12345'),
                             headers=headers,
                             data=json.dumps(request_data))
    assert user_resp.status_code == 400, "Expected status code 400 for regular user attempting to update definition"


def test_create_definition_with_null_id_in_payload_uses_url_id(log):
    """
    Test that when id in payload is null, the id from URL parameter is used.
    """
    request_url = base_url + '/plugins/definitions/{plugin_definition_1_id}'
    request_url = request_url.format(**str_variable_dict)

    request_data = copy.deepcopy(variable_dict["plugin_definition_1"])
    # Set id to null in payload
    request_data["id"] = None

    # Send PUT request
    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 200, "Expected status code 200 when id is null in payload"

    # Verify the response contains the URL's id
    resp_data = resp.json()
    expected_id = int(str_variable_dict['plugin_definition_1_id'] )
    assert resp_data.get('id') == expected_id, f"Response should use URL id {expected_id} when payload id is null"

    # Verify with GET request
    get_resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    if get_resp.status_code == 200 :
        log.info("Successfully retrieved definition after update with null id in payload.")
    else:
        log.error(f"Failed to retrieve definition after update with null id in payload. Status code: {get_resp.status_code}, Response: {get_resp.text}")

    get_data = get_resp.json()
    assert get_data.get('id') == expected_id, f"Retrieved definition should have id {expected_id}"

def test_put_update_definition_with_blank_display_name_retains_previous(log):
    """
    Test that when displayName is blank (empty string or null), the previous displayName is retained.
    """
    request_url = base_url + '/plugins/definitions/{plugin_definition_1_id}'
    request_url = request_url.format(**str_variable_dict)

    # First, get the current definition to store original displayName
    get_resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    if get_resp.status_code == 200 :
        log.info(f"Successfully retrieved definition {request_url} for {str_variable_dict['plugin_definition_1_id']} .")
    else:
        log.error(f"Failed to retrieve definition {request_url} for {str_variable_dict['plugin_definition_1_id']} . Status code: {get_resp.status_code}, Response: {get_resp.text}")
    original_data = get_resp.json()
    original_display_name = original_data.get('displayName')

    # Update with blank displayName (empty string)
    request_data = copy.deepcopy(variable_dict["plugin_definition_1"])
    request_data["displayName"] = ""

    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 200, "Expected status code 200"

    # Verify displayName was retained from previous value
    updated_data = resp.json()
    assert updated_data.get(
        'displayName') == original_display_name, "DisplayName should be retained when blank string is provided"







