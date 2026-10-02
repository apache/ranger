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

def test_create_and_get_tag_resource_map(create_tag_for_tests, create_service_resource_ids, log):
    # Create a tag resource map
    tag_guid = create_tag_for_tests.get("guid")
    tag_id = create_tag_for_tests.get("id")
    resource_json = create_service_resource_ids["hbase_mapping"]
    resource_guid = resource_json.get("guid")
    resource_id = resource_json.get("id")
    request_url = base_url + f"/tags/tagresourcemaps?tag-guid={tag_guid}&resource-guid={resource_guid}&lenient=true"

    response = requests.post(request_url,  auth=admin_auth, headers=headers)
    assert response.status_code in [200, 201, 204], f"Expected status code (200,201,204), but got {response.status_code}"
    log.info(f"Tag resource map created successfully with tag ID: {tag_id} and resource IDs: {resource_id}")
    tag_resource_map = response.json()
    tag_resource_map_id = tag_resource_map.get("id")
    tag_resource_map_guid= tag_resource_map.get("guid")

#     test the same tag resource with lenient as true and false

    response= requests.post(request_url, auth=admin_auth, headers=headers)
    assert response.status_code in [200,201,204], f"Expected status code (200,201,204), but got {response.status_code} since lenient is false and same tag resource map already exists"

    request_url = base_url + f"/tags/tagresourcemaps?tag-guid={tag_guid}&resource-guid={resource_guid}&lenient=false"
    response= requests.post(request_url,  auth=admin_auth, headers=headers)
    assert response.status_code ==400 , f"Expected status code not returned as 400 since lenient is false and same tag resource map already exists, but got {response.status_code}"


    # Get the created tag resource map
    response = requests.get(f"{base_url}/tags/tagresourcemaps", auth=admin_auth, headers=headers)
    assert response.status_code in [200, 201, 204], f"Expected status code 200, got {response.status_code}"
    data = response.json()

    # Verify that the created tag resource map exists in the response
    found = False
    for item in data:
        if item.get("tagId") == tag_id and item.get("resourceId") == resource_id:
            found = True
            break

    assert found, f"Tag resource map with tagId={tag_id} and resourceId={resource_id} not found in response"
    log.info(f"Verified tag resource map exists with tagId={tag_id} and resourceId={resource_id}")

# get the tagresource map using the tagresourcemap  id
    get_url = base_url + f"/tags/tagresourcemap/{tag_resource_map_id}"
    get_response = requests.get(get_url, auth=admin_auth, headers=headers)
    assert get_response.status_code in [200, 201, 204], f"Expected status code 200, got {get_response.status_code}"
    get_data = get_response.json()
    assert get_data.get("id") == tag_resource_map_id, f"Expected tag resource map ID {tag_resource_map_id}, but got {get_data.get('id')}"

# get the tagresource map using the tagresourcemap guid
    get_url = base_url + f"/tags/tagresourcemap/guid/{tag_resource_map_guid}"
    get_response = requests.get(get_url, auth=admin_auth, headers=headers)
    assert get_response.status_code in [200, 201, 204], f"Expected status code 200, got {get_response.status_code}"
    get_data = get_response.json()
    assert get_data.get("guid") == tag_resource_map_guid, f"Expected tag resource map ID {tag_resource_map_id}, but got {get_data.get('id')}"

# get the  tagresource map using separate tag guid and resource guid
    get_url = base_url + f"/tags/tagresourcemap/tag-resource-guid?tagGuid={tag_guid}&resourceGuid={resource_guid}"
    get_response = requests.get(get_url, auth=admin_auth, headers=headers)
    assert get_response.status_code in [200, 201, 204], f"Expected status code 200, got {get_response.status_code}"
    get_data = get_response.json()
    assert get_data.get("id") == tag_resource_map_id, f"Expected tag resource map ID {tag_resource_map_id}, but got {get_data.get('id')}"

def test_create_resource_map_with_invalid_tag_guid(create_service_resource_ids, log):
    resource_json = create_service_resource_ids["hbase_mapping"]
    resource_guid = resource_json.get("guid")
    request_url = base_url + f"/tags/tagresourcemaps?tag-guid=invalid-tag-guid&resource-guid={resource_guid}&lenient=true"
    response = requests.post(request_url , auth=admin_auth, headers=headers)
    assert response.status_code == 400, f"Expected status code 400 for invalid tag guid, but got {response.status_code}"

def test_create_resource_map_with_invalid_resource_guid(create_tag_for_tests, log):
    tag_guid = create_tag_for_tests.get("guid")
    request_url = base_url + f"/tags/tagresourcemaps?tag-guid={tag_guid}&resource-guid=invalid-resource-guid&lenient=true"
    response = requests.post(request_url,  auth=admin_auth, headers=headers)
    assert response.status_code == 200, f"Expected status code 400 for invalid resource guid, but got {response.status_code}"







