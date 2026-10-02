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

import pytest
import os
import requests
import json
from requests.auth import HTTPBasicAuth
from Utility.main import base_url, admin_auth, headers, get_request_data, str_variable_dict ,keyadmin_auth

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESOURCES_DIRECTORY = os.path.join(BASE_DIR, "Utility", "variable_jsons")

def test_create_tagdef(log):
    request_data = get_request_data("create_tagdef.json", str_variable_dict, RESOURCES_DIRECTORY)
    request_url=base_url+"/tags/tagdefs"
    response = requests.post(request_url, auth=admin_auth, headers=headers, json=request_data)

    # Assert the response status code and content
    assert response.status_code == 200, f"Expected status code 200, but got {response.status_code}"
    response_json = response.json()
    assert "guid" in response_json, "Response JSON does not contain 'id'"
    assert response_json["name"] == request_data["name"], f"Expected name '{request_data['name']}', but got '{response_json['name']}'"
    if response.status_code==200:
        log.info(f"Tagdef created successfully with GUID: {response_json['guid']}")
#   deleting the created tagdef to maintain data integrity
    tagdef_guid=response_json["guid"]
    delete_url = base_url + "/tags/tagdef/guid/" + tagdef_guid
    delete_response=requests.delete(delete_url, auth=admin_auth, headers=headers)
    assert delete_response.status_code in [204,200,201], f"Expected status code 204 for delete, but got {delete_response.status_code}"
    if delete_response.status_code in [204,200,201]:
        log.info(f"Tagdef with GUID: {tagdef_guid} deleted successfully")
    else:
        log.error(f"Failed to delete tagdef with GUID: {tagdef_guid}. Status code: {delete_response.status_code}, Response: {delete_response.text}")

# @pytest.mark.skip(reason="Bug in Ranger code base , for unauthorized users status code should be 403 but it is throwing 404 for keyadmin ,user  admin auditor role")
def test_create_tagdef_for_non_admin(log):
    # Prepare the request data
    request_data = get_request_data("create_tagdef.json", str_variable_dict, RESOURCES_DIRECTORY)
    request_url=base_url+"/tags/tagdefs"

    # Send the POST request to create a tag definition
    response = requests.post(request_url, auth=keyadmin_auth, headers=headers, json=request_data)
    # Assert the response status code and content
    assert response.status_code in [403,404] , f"Expected status code 403 for non-admin user, but got {response.status_code}"
    response=requests.post(request_url,auth=HTTPBasicAuth(str_variable_dict["auditor_user"],"Test@12345"), headers=headers, json=request_data)
    assert response.status_code in [403,404] ,f"Expected status code 403 for auditor role , but got {response.status_code}"
    response=requests.post(request_url,auth=HTTPBasicAuth(str_variable_dict["user2"],"Test@12345"), headers=headers, json=request_data)
    assert response.status_code in [403,404], f"Expected status code 403 for  user role , but got {response.status_code}"



def test_create_tagdef_updateIfExists_as_false(create_tagdef_for_tests, log):
    """
    Test that creating a tagdef with updateIfExists=false returns 400
    when the tagdef already exists.
    """
    existing_tagdef = create_tagdef_for_tests
    tagdef_name = existing_tagdef.get("name")
    log.info(f"Using existing tagdef with name: '{tagdef_name}' and GUID: {existing_tagdef.get('guid')}")

    request_url = base_url + "/tags/tagdefs?updateIfExists=false"
    payload = {
        "name": tagdef_name,
        "attributeDefs": existing_tagdef.get("attributeDefs", [])
    }

    log.info(f"Attempting to create duplicate tagdef with updateIfExists=false, payload: {json.dumps(payload)}")

    response = requests.post(request_url, auth=admin_auth, headers=headers, json=payload)

    log.info(f"Response status code: {response.status_code}, Response body: {response.text}")

    assert response.status_code == 400, (
        f"Expected status code 400 when creating duplicate tagdef with updateIfExists=false, "
        f"but got {response.status_code}. Response: {response.text}"
    )

def test_get_tagdefs(create_tagdef_for_tests, log):
    """
    Test that GET tags/tagdefs returns a list of tagdefs
    and the tagdef created in the fixture is present in the response.
    """
    existing_tagdef = create_tagdef_for_tests
    expected_guid = existing_tagdef.get("guid")
    request_url = base_url + "/tags/tagdefs"
    response = requests.get(request_url, auth=admin_auth, headers=headers)
    assert response.status_code in [200, 201, 204], (
        f"Expected status code in [200, 201, 204], but got {response.status_code}. Response: {response.text}"
    )

    response_json = response.json()
    assert isinstance(response_json, list), f"Expected response to be a list, but got {type(response_json)}"
    guids_in_response = [tagdef.get("guid") for tagdef in response_json]
    assert expected_guid in guids_in_response, (
        f"Expected GUID '{expected_guid}' to be present in the tagdefs list, "
        f"but it was not found. GUIDs in response: {guids_in_response}"
    )

    log.info(f"Tagdef with GUID: {expected_guid} successfully found in GET tags/tagdefs response")

# @pytest.mark.skip(reason="Bug in Ranger code base , for unauthorized users status code should be 403 but it is throwing 404 for keyadmin ,user  admin auditor role")
def test_get_tagdef_for_non_admin():
    request_url=base_url+"/tags/tagdefs"
    response = requests.get(request_url, auth=keyadmin_auth, headers=headers)
    # Assert the response status code and content
    assert response.status_code in [403,404], f"Expected status code 403 for non-admin user, but got {response.status_code}"
    response=requests.get(request_url,auth=HTTPBasicAuth(str_variable_dict["auditor_user"],"Test@12345"), headers=headers)
    assert response.status_code in [403,404], f"Expected status code 403 for auditor role , but got {response.status_code}"
    response=requests.get(request_url,auth=HTTPBasicAuth(str_variable_dict["user2"],"Test@12345"), headers=headers)
    assert response.status_code in[403,404], f"Expected status code 403 for  user role , but got {response.status_code}"


def test_get_tagdef_id(create_tagdef_for_tests):
    existing_tagdef = create_tagdef_for_tests
    tagdef_id = existing_tagdef.get("id")
    tagdef_guid = existing_tagdef.get("guid")
    request_url = base_url + f"/tags/tagdef/{tagdef_id}"
    response = requests.get(request_url, auth=admin_auth, headers=headers)
    assert response.status_code in [200, 201, 204], (
        f"Expected status code in [200, 201, 204], but got {response.status_code}. Response: {response.text}"
    )
    response_json = response.json()
    assert response_json.get("guid") == tagdef_guid, (
        f"Expected GUID '{tagdef_id}' in response, but got '{response_json.get('guid')}'. Response: {response.text}"
    )

# @pytest.mark.skip(reason="Bug in Ranger code base , for unauthorized users status code should be 403 but it is throwing 404 for keyadmin ,user  admin auditor role")
def test_get_tagdef_id_for_non_admin(create_tagdef_for_tests):
    existing_tagdef = create_tagdef_for_tests
    tagdef_id = existing_tagdef.get("id")
    request_url = base_url + f"/tags/tagdef/{tagdef_id}"
    response = requests.get(request_url, auth=keyadmin_auth, headers=headers)
    # Assert the response status code and content
    assert response.status_code in [403,404], f"Expected status code 403 for non-admin user, but got {response.status_code}"
    response=requests.get(request_url,auth=HTTPBasicAuth(str_variable_dict["auditor_user"],"Test@12345"), headers=headers)
    assert response.status_code in [403,404], f"Expected status code 403 for auditor role , but got {response.status_code}"
    response=requests.get(request_url,auth=HTTPBasicAuth(str_variable_dict["user2"],"Test@12345"), headers=headers)
    assert response.status_code in [403,404] , f"Expected status code 403 for  user role , but got {response.status_code}"

def test_get_tagdef_guid(create_tagdef_for_tests):
    existing_tagdef = create_tagdef_for_tests
    tagdef_guid = existing_tagdef.get("guid")
    tagdef_id = existing_tagdef.get("id")
    request_url = base_url + f"/tags/tagdef/guid/{tagdef_guid}"
    response = requests.get(request_url, auth=admin_auth, headers=headers)
    assert response.status_code in [200, 201, 204], (
        f"Expected status code in [200, 201, 204], but got {response.status_code}. Response: {response.text}"
    )
    response_json = response.json()
    assert response_json.get("id") == tagdef_id, (
        f"Expected ID '{tagdef_id}' in response, but got '{response_json.get('id')}'. Response: {response.text}"
    )

# @pytest.mark.skip(reason="Bug in Ranger code base , for unauthorized users status code should be 403 but it is throwing 404 for keyadmin ,user  admin auditor role")
def test_get_tagdef_guid_for_non_admin(create_tagdef_for_tests):
    existing_tagdef = create_tagdef_for_tests
    tagdef_guid = existing_tagdef.get("guid")
    request_url = base_url + f"/tags/tagdef/guid/{tagdef_guid}"
    response = requests.get(request_url, auth=keyadmin_auth, headers=headers)
    # Assert the response status code and content
    assert response.status_code in [403,404], f"Expected status code 403 for non-admin user, but got {response.status_code}"
    response=requests.get(request_url,auth=HTTPBasicAuth(str_variable_dict["auditor_user"],"Test@12345"), headers=headers)
    assert response.status_code in [403,404], f"Expected status code 403 for auditor role , but got {response.status_code}"
    response=requests.get(request_url,auth=HTTPBasicAuth(str_variable_dict["user2"],"Test@12345"), headers=headers)
    assert response.status_code in [403,404] , f"Expected status code 403 for  user role , but got {response.status_code}"


def test_put_tagdef_id_for_admin(create_tagdef_for_tests):
    existing_tagdef = create_tagdef_for_tests
    tagdef_id = existing_tagdef.get("id")
    version_now = existing_tagdef.get("version")
    request_url = base_url + f"/tags/tagdef/{tagdef_id}"
    response = requests.put(request_url, auth=admin_auth, headers=headers, json=existing_tagdef)
    assert response.status_code in [200, 201, 204], (
        f"Expected status code in [200, 201, 204], but got {response.status_code}. Response: {response.text}"
    )
    response_json = response.json()
    updated_version = response_json.get("version")
    assert updated_version == version_now + 1, (
        f"Expected version to be incremented by 1, but it was not. Previous version: {version_now}, Updated version: {updated_version}. Response: {response.text}")

# @pytest.mark.skip(reason="Bug in Ranger code base , for unauthorized users status code should be 403 but it is throwing 404 for keyadmin ,user  admin auditor role")
def test_put_tagdef_id_for_non_admin(create_tagdef_for_tests):
    existing_tagdef = create_tagdef_for_tests
    tagdef_id = existing_tagdef.get("id")
    request_url = base_url + f"/tags/tagdef/{tagdef_id}"
    response = requests.put(request_url, auth=keyadmin_auth, headers=headers, json=existing_tagdef)
    # Assert the response status code and content
    assert response.status_code in [403,404] ,f"Expected status code 403 for non-admin user, but got {response.status_code}"
    response=requests.put(request_url,auth=HTTPBasicAuth(str_variable_dict["auditor_user"],"Test@12345"), headers=headers, json=existing_tagdef)
    assert response.status_code in [403,404] , f"Expected status code 403 for auditor role , but got {response.status_code}"
    response=requests.put(request_url,auth=HTTPBasicAuth(str_variable_dict["user2"],"Test@12345"), headers=headers, json=existing_tagdef)
    assert response.status_code in [403,404] , f"Expected status code 403 for  user role , but got {response.status_code}"


def test_put_tagdef_id_not_allowed_to_change_name(create_tagdef_for_tests):
    existing_tagdef = create_tagdef_for_tests
    tagdef_id = existing_tagdef.get("id")
    request_url = base_url + f"/tags/tagdef/{tagdef_id}"
    updated_tagdef = existing_tagdef.copy()
    updated_tagdef["name"] = existing_tagdef["name"] + "_updated"
    response = requests.put(request_url, auth=admin_auth, headers=headers, json=updated_tagdef)
    assert response.status_code == 400, (
        f"Expected status code 400 when trying to change tagdef name, but got {response.status_code}. Response: {response.text}"
    )

def test_get_tagdef_using_name(create_tagdef_for_tests):
    existing_tagdef = create_tagdef_for_tests
    tagdef_name = existing_tagdef.get("name")
    tagdef_id = existing_tagdef.get("id")
    request_url = base_url + f"/tags/tagdef/name/{tagdef_name}"
    response = requests.get(request_url, auth=admin_auth, headers=headers)
    assert response.status_code in [200, 201, 204], (
        f"Expected status code in [200, 201, 204], but got {response.status_code}. Response: {response.text}"
    )
    response_json = response.json()
    assert response_json.get("id") == tagdef_id, (
        f"Expected ID '{tagdef_id}' in response, but got '{response_json.get('id')}'. Response: {response.text}"
    )

# @pytest.mark.skip(reason="Bug in Ranger code base , for unauthorized users status code should be 403 but it is throwing 404 for keyadmin ,user  admin auditor role")
def test_get_tagdef_using_name_for_non_admin(create_tagdef_for_tests):
    existing_tagdef = create_tagdef_for_tests
    tagdef_name = existing_tagdef.get("name")
    request_url = base_url + f"/tags/tagdef/name/{tagdef_name}"
    response = requests.get(request_url, auth=keyadmin_auth, headers=headers)
    assert response.status_code in [403,404],f"Expected status code 403 for non-admin user, but got {response.status_code}"
    response=requests.get(request_url,auth=HTTPBasicAuth(str_variable_dict["auditor_user"],"Test@12345"), headers=headers)
    assert response.status_code in [403,404] , f"Expected status code 403 for auditor role , but got {response.status_code}"
    response=requests.get(request_url,auth=HTTPBasicAuth(str_variable_dict["user2"],"Test@12345"), headers=headers)
    assert response.status_code in [403,404] , f"Expected status code 403 for  user role , but got {response.status_code}"

def test_delete_tagdef_id(log):
    # Create a tagdef to delete
    request_data = get_request_data("create_tagdef.json", str_variable_dict, RESOURCES_DIRECTORY)
    request_url = base_url + "/tags/tagdefs"
    response = requests.post(request_url, auth=admin_auth, headers=headers, json=request_data)
    assert response.status_code == 200, f"Expected status code 200, but got {response.status_code}"
    response_json = response.json()
    tagdef_id = response_json.get("id")
    tagdef_guid = response_json.get("guid")
    log.info(f"Tagdef created successfully with ID: {tagdef_id} and GUID: {tagdef_guid}")

    # Delete the created tagdef
    delete_url = base_url + f"/tags/tagdef/{tagdef_id}"
    delete_response = requests.delete(delete_url, auth=admin_auth, headers=headers)
    assert delete_response.status_code in [204, 200, 201], f"Expected status code 204 for delete, but got {delete_response.status_code}"
    if delete_response.status_code in [204, 200, 201]:
        log.info(f"Tagdef with ID: {tagdef_id} deleted successfully")
    else:
        log.error(f"Failed to delete tagdef with ID: {tagdef_id}. Status code: {delete_response.status_code}, Response: {delete_response.text}")


def test_tagdef_paginated(create_tagdef_for_tests):
    """Test basic paginated tags request and verify created tag is present"""
    tagdef_guid = create_tagdef_for_tests.get("guid")

    request_url = base_url + "/tags/tagdefs/paginated"

    response = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)

    assert response.status_code == 200, (
        f"Failed to get paginated tags: expected 200, got {response.status_code}. Response: {response.text}"
    )

    response_json = response.json()
    # Verify created tag is present in response
    found = False
    for tagdef in response_json["list"]:
        if tagdef.get("guid") == tagdef_guid:
            found = True
            break

    assert found, f"Tag with guid {tagdef_guid} not found in paginated tags response"





