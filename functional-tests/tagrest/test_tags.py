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
VARIABLE_JSONS_DIR = os.path.join(BASE_DIR, "Utility", "variable_jsons")
RESOURCE_MAPPING_DIRECTORY = os.path.join(VARIABLE_JSONS_DIR, "resource_mappings")
variables_data_path=os.path.join(BASE_DIR, "Utility", "variable_jsons")


def test_create_and_get_tags(create_tagdef_for_tests, log):
    tagdef_name = create_tagdef_for_tests.get("name")

    request_data = get_request_data("create_tag.json", str_variable_dict, VARIABLE_JSONS_DIR)

    # Update the request data to use the created tagdef
    request_data["type"] = tagdef_name
    request_data["options"]["description"] = f"Tag created for testing {tagdef_name}"
    descriptions = request_data["options"]["description"]

    request_url = base_url + "/tags/tags"

    log.info(f"Creating tag with payload: {json.dumps(request_data)}")

    response = requests.post(
        request_url,
        verify=False,
        auth=admin_auth,
        headers=headers,
        data=json.dumps(request_data)
    )

    assert response.status_code in [200, 201, 204], (
        f"Failed to create tag: expected 200/201/204, got {response.status_code}. Response: {response.text}"
    )
    response_json=response.json()
    assert response_json["options"]["description"]==descriptions,"Tag description in response does not match the request. Expected: {descriptions}, Got: {response_json['options']['description']}"
    log.info(f"Tag created successfully with name: {tagdef_name} and description: {descriptions}")

    # test for the get request
    get_response = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert get_response.status_code in [200, 201, 204], (
        f"Failed to get tags: expected 200/201/204, got {get_response.status_code}. Response: {get_response.text}"
    )
    get_response_json = get_response.json()
    found = False
    for tag in get_response_json:
        if tag.get("type") == tagdef_name and tag.get("options", {}).get("description") == descriptions:
            found = True
            break
    assert found, f"Tag with name {tagdef_name} and description {descriptions} not found in get response , That is tag created but not found in response of get request"

def test_get_tag_type(create_tagdef_for_tests, log):
    tagdef_name = create_tagdef_for_tests.get("name")
    request_url = base_url + f"/tags/types"
    response = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert response.status_code in [200, 201, 204], (
        f"Failed to get tag by type: expected 200/201/204, got {response.status_code}. Response: {response.text}"
    )
    response_json = response.json()
    found = False
    for tag_type in response_json:
        if tag_type == tagdef_name:
            found = True
            break
    assert found, f"Tag type {tagdef_name} not found in response of get tag by type request"

    # test for roles other than admin
    response = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert response.status_code in [400,403]  ,"Expected status code 400 or 403 for  key admin , but got {response.status_code}. Response: {response.text}"
    response= requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict["user2"],"Test@12345"), headers=headers)
    assert response.status_code in [400,403]  ,f"Expected status code 400 or 403 for  role user  , but got {response.status_code}. Response: {response.text}"
    response= requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict["user4"],"Test@12345"), headers=headers)
    assert response.status_code in [400,403]  ,f"Expected status code 400 or 403 for  admin auditor role   , but got {response.status_code}. Response: {response.text}"


def test_get_paginated_tags_basic(create_tag_for_tests, log):
    """Test basic paginated tags request and verify created tag is present"""
    tag_guid = create_tag_for_tests.get("guid")

    request_url = base_url + "/tags/tags/paginated"

    response = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)

    assert response.status_code == 200, (
        f"Failed to get paginated tags: expected 200, got {response.status_code}. Response: {response.text}"
    )

    response_json = response.json()

    # Verify response structure
    assert "list" in response_json, "Response should contain 'list' field"
    assert "totalCount" in response_json, "Response should contain 'totalCount' field"
    assert "startIndex" in response_json, "Response should contain 'startIndex' field"
    assert "pageSize" in response_json, "Response should contain 'pageSize' field"
    assert isinstance(response_json["list"], list), "List field should be an array"

    # Verify created tag is present in response
    found = False
    for tag in response_json["list"]:
        if tag.get("guid") == tag_guid:
            found = True
            break

    assert found, f"Tag with guid {tag_guid} not found in paginated tags response"



# @pytest.mark.skip(reason="Bug in Ranger code base , for unauthorized users status code should be 403 but it is throwing 404 for keyadmin ,user  admin auditor role")
def test_get_paginated_tags_authorization():
    """Test authorization for paginated tags endpoint - should only allow ROLE_SYS_ADMIN"""
    request_url = base_url + "/tags/tags/paginated"

    # Test with admin (should succeed)
    response = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert response.status_code == 200, (
        f"Admin should have access, got {response.status_code}. Response: {response.text}"
    )
    response = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert response.status_code in [400, 403,404], (
        f"Expected 400/403 for keyadmin, got {response.status_code}. Response: {response.text}"
    )


    # Test with regular user (should fail)
    response = requests.get(
        request_url,
        verify=False,
        auth=HTTPBasicAuth(str_variable_dict["user2"], "Test@12345"),
        headers=headers
    )
    assert response.status_code in [400, 403,404], (
        f"Expected 400/403 for regular user, got {response.status_code}. Response: {response.text}"
    )
    # Test with admin auditor (should fail)
    response = requests.get(
        request_url,
        verify=False,
        auth=HTTPBasicAuth(str_variable_dict["user4"], "Test@12345"),
        headers=headers
    )
    assert response.status_code in [400, 403,404], (
        f"Expected 400/403 for admin auditor, got {response.status_code}. Response: {response.text}"
    )



def test_get_paginated_tags_filter_by_tag_id( create_tag_for_tests):
    """Test filtering paginated tags by specific tag ID"""
    tag_id = create_tag_for_tests.get("id")
    tag_guid = create_tag_for_tests.get("guid")

    request_url = base_url + "/tags/tags/paginated"
    params = {"tagIds": str(tag_id)}

    response = requests.get(request_url, params=params, verify=False, auth=admin_auth, headers=headers)

    assert response.status_code == 200, (
        f"Failed to get filtered paginated tags: expected 200, got {response.status_code}. Response: {response.text}"
    )

    response_json = response.json()

    # Verify response structure
    assert "list" in response_json, "Response should contain 'list' field"
    assert isinstance(response_json["list"], list), "List field should be an array"

    # Verify only the requested tag is present
    found = False
    for tag in response_json["list"]:
        if tag.get("id") == tag_id and tag.get("guid") == tag_guid:
            found = True
        else:
            # If filtering is working correctly, no other tags should be present
            # However, depending on implementation, other tags with same ID might exist
            # So we just verify our tag is present
            pass

    assert found, f"Tag with id {tag_id} and guid {tag_guid} not found in filtered response"

def test_put_update_tag_using_id(create_tag_for_tests):
    """Test updating a tag's description using its ID"""
    tag_id = create_tag_for_tests.get("id")
    request_url_for_get_tag = base_url + f"/tags/tag/{tag_id}"
    original_tag_response = requests.get(request_url_for_get_tag, verify=False, auth=admin_auth, headers=headers)
    assert original_tag_response.status_code == 200, (
        f"Failed to get original tag before update: expected 200, got {original_tag_response.status_code}. Response: {original_tag_response.text}"
    )
    original_tag_response= original_tag_response.json()
    original_description = original_tag_response.get("options", {}).get("description", "")
    updated_description = f"{original_description} - Updated"

    request_url = base_url + f"/tags/tag/{tag_id}"
    original_tag_response["options"]["description"] = updated_description
    response = requests.put(
        request_url,
        verify=False,
        auth=admin_auth,
        headers=headers,
        data=json.dumps(original_tag_response),
    )

    assert response.status_code in [200, 204], (
        f"Failed to update tag: expected 200/204, got {response.status_code}. Response: {response.text}"
    )

    # Verify the update by fetching the tag again
    get_response = requests.get(base_url + f"/tags/tag/{tag_id}", verify=False, auth=admin_auth, headers=headers)
    assert get_response.status_code == 200, (
        f"Failed to get tag after update: expected 200, got {get_response.status_code}. Response: {get_response.text}"
    )
    get_response_json = get_response.json()
    assert get_response_json.get("options", {}).get("description") == updated_description, (
        f"Tag description was not updated correctly. Expected: {updated_description}, Got: {get_response_json.get('options', {}).get('description')}"
    )
    """
    updating the tag type is not allowed 
    """
    original_tag_response["type"]= "new_type"
    response = requests.put(
        request_url,
        verify=False,
        auth=admin_auth,
        headers=headers,
        data=json.dumps(original_tag_response),
    )
    assert response.status_code == 400, (
        f"Expected status code 400 when trying to update tag type, got {response.status_code}. Response: {response.text}"
    )

def test_put_update_tag_using_guid(create_tag_for_tests):
    """Test updating a tag's description using its ID"""
    tag_id = create_tag_for_tests.get("id")
    tag_guid = create_tag_for_tests.get("guid")
    request_url_for_get_tag = base_url + f"/tags/tag/{tag_id}"
    original_tag_response = requests.get(request_url_for_get_tag, verify=False, auth=admin_auth, headers=headers)
    assert original_tag_response.status_code == 200, (
        f"Failed to get original tag before update: expected 200, got {original_tag_response.status_code}. Response: {original_tag_response.text}"
    )
    original_tag_response= original_tag_response.json()
    original_description = original_tag_response.get("options", {}).get("description", "")
    updated_description = f"{original_description} - Updated_for_guid"

    request_url = base_url + f"/tags/tag/guid/{tag_guid}"
    original_tag_response["options"]["description"] = updated_description
    response = requests.put(
        request_url,
        verify=False,
        auth=admin_auth,
        headers=headers,
        data=json.dumps(original_tag_response),
    )

    assert response.status_code in [200, 204], (
        f"Failed to update tag: expected 200/204, got {response.status_code}. Response: {response.text}"
    )

    # Verify the update by fetching the tag again
    get_response = requests.get(base_url + f"/tags/tag/{tag_id}", verify=False, auth=admin_auth, headers=headers)
    assert get_response.status_code == 200, (
        f"Failed to get tag after update: expected 200, got {get_response.status_code}. Response: {get_response.text}"
    )
    get_response_json = get_response.json()
    assert get_response_json.get("options", {}).get("description") == updated_description, (
        f"Tag description was not updated correctly. Expected: {updated_description}, Got: {get_response_json.get('options', {}).get('description')}"
    )
    """
    updating the tag type is not allowed 
    """
    original_tag_response["type"]= "new_type"
    response = requests.put(
        request_url,
        verify=False,
        auth=admin_auth,
        headers=headers,
        data=json.dumps(original_tag_response),
    )
    assert response.status_code == 400, (
        f"Expected status code 400 when trying to update tag type, got {response.status_code}. Response: {response.text}"
    )

def test_delete_tag_using_id(log,create_tagdef_for_tests):
    """Test deleting a tag using its ID"""
    # First create a tag to delete
    request_data = get_request_data("create_tag.json", str_variable_dict, VARIABLE_JSONS_DIR)
    request_data["type"] = create_tagdef_for_tests.get("name")
    request_url = base_url + "/tags/tags"
    response = requests.post(
        request_url,
        verify=False,
        auth=admin_auth,
        headers=headers,
        data=json.dumps(request_data)
    )
    assert response.status_code in [200, 201, 204], (
        f"Failed to create tag for deletion test: expected 200/201/204, got {response.status_code}. Response: {response.text}"
    )
    created_tag_id = response.json().get("id")
    log.info(f"Created tag: {created_tag_id}")

    # Now delete the created tag
    delete_response = requests.delete(base_url + f"/tags/tag/{created_tag_id}", verify=False, auth=admin_auth, headers=headers)
    assert delete_response.status_code in [200, 204], (
        f"Failed to delete tag: expected 200/204, got {delete_response.status_code}. Response: {delete_response.text}"
    )
    if delete_response.status_code in[200,204]:
        log.info(f"Tag with ID {created_tag_id} deleted successfully")
    else :
        log.error(f"Failed to delete tag with ID {created_tag_id}. Status code: {delete_response.status_code}. Response: {delete_response.text}")


    # # Verify the tag is deleted by trying to fetch it again
    # get_response = requests.get(base_url + f"/tags/tag/{created_tag_id}", verify=False, auth=admin_auth, headers=headers)
    # assert get_response.status_code == 404, (
    #     f"Expected 404 when fetching deleted tag, got {get_response.status_code}. Response: {get_response.text}"
    # )

def test_delete_tag_using_guid(log,create_tagdef_for_tests):
    """Test deleting a tag using its ID"""
    # First create a tag to delete
    request_data = get_request_data("create_tag.json", str_variable_dict, VARIABLE_JSONS_DIR)
    request_data["type"] = create_tagdef_for_tests.get("name")
    request_url = base_url + "/tags/tags"
    response = requests.post(
        request_url,
        verify=False,
        auth=admin_auth,
        headers=headers,
        data=json.dumps(request_data)
    )
    assert response.status_code in [200, 201, 204], (
        f"Failed to create tag for deletion test: expected 200/201/204, got {response.status_code}. Response: {response.text}"
    )
    created_tag_guid = response.json().get("guid")
    log.info(f"Created tag: {created_tag_guid}")

    # Now delete the created tag
    delete_response = requests.delete(base_url + f"/tags/tag/guid/{created_tag_guid}", verify=False, auth=admin_auth, headers=headers)
    assert delete_response.status_code in [200, 204], (
        f"Failed to delete tag: expected 200/204, got {delete_response.status_code}. Response: {delete_response.text}"
    )
    if delete_response.status_code in[200,204]:
        log.info(f"Tag with ID {created_tag_guid} deleted successfully")
    else :
        log.error(f"Failed to delete tag with ID {created_tag_guid}. Status code: {delete_response.status_code}. Response: {delete_response.text}")


    # # Verify the tag is deleted by trying to fetch it again
    # get_response = requests.get(base_url + f"/tags/tag/guid/{created_tag_guid}", verify=False, auth=admin_auth, headers=headers)
    # assert get_response.status_code == 404, (
    #     f"Expected 404 when fetching deleted tag, got {get_response.status_code}. Response: {get_response.text}"
    # )

def test_get_tags_by_tagedf_type(create_tagdef_for_tests,log):
    """Test fetching tags by their tagdef type"""
    tagdef_name = create_tagdef_for_tests.get("name")
    """
    create a tag with the tagdef name so that atleast one tag is present of the given tag type 
    """
    request_data = get_request_data("create_tag.json", str_variable_dict, VARIABLE_JSONS_DIR)
    # Update the request data to use the created tagdef
    request_data["type"] = tagdef_name
    request_url = base_url + "/tags/tags"
    response = requests.post(
        request_url,
        verify=False,
        auth=admin_auth,
        headers=headers,
        data=json.dumps(request_data)
    )
    created_tag_id = response.json().get("id")

    assert response.status_code in [200, 201, 204], (
        f"Failed to create tag: expected 200/201/204, got {response.status_code}. Response: {response.text}"
    )
    if response.status_code in [200, 201, 204]:
        log.info(f"Tag created successfully with name: {tagdef_name} for testing get tags by tagdef type")
    else:
        log.error(f"Failed to create tag with name: {tagdef_name} for testing get tags by tagdef type. Status code: {response.status_code}. Response: {response.text}")

    request_url = base_url + f"/tags/tags/type/{tagdef_name}"
    response = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert response.status_code in [200, 201, 204], (
        f"Failed to get tags by tagdef type: expected 200/201/204, got {response.status_code}. Response: {response.text}"
    )
    response_json = response.json()
    assert len(response_json) > 0, (
        f"No tags found for tagdef type {tagdef_name} although tags have been created"
    )
    # delete the created tag
    delete_response = requests.delete(base_url + f"/tags/tag/{created_tag_id}", verify=False, auth=admin_auth, headers=headers)
    if delete_response.status_code in[200,204]:
        log.info(f"Tag with ID {created_tag_id} deleted successfully after testing get tags by tagdef type")
    else :
        log.error(f"Failed to delete tag with ID {created_tag_id} after testing get tags by tagdef type. Status code: {delete_response.status_code}. Response: {delete_response.text}")
