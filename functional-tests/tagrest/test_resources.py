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

# Use absolute path relative to this file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESOURCE_MAPPING_DIRECTORY = os.path.join(BASE_DIR, "Utility", "variable_jsons", "resource_mappings")


def test_create_service_resource(log):
    """Test to create and delete service resource mappings"""
    log.info("Starting test: Creating service and resource IDs")

    # Creating resource ids for different resources in different types of services
    service_resource_responses = {}

    request_url = base_url + "/tags/resources"
    mapping_array = ["hbase_mapping", "hdfs_mapping", "hive_mapping", "kafka_mapping"]

    # Create resources
    for mapping in mapping_array:
        # Verify file exists before making request
        file_path = os.path.join(RESOURCE_MAPPING_DIRECTORY, f"{mapping}.json")
        if not os.path.exists(file_path):
            log.error(f"File not found: {file_path}")
            pytest.fail(f"Mapping file not found: {file_path}")

        request_data = get_request_data(f"{mapping}.json", str_variable_dict, RESOURCE_MAPPING_DIRECTORY)

        # First request - create resource
        response1 = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                                  data=json.dumps(request_data))
        assert response1.status_code == 200, f"Failed to create resource {mapping}: {response1.text}"

        response1_data = response1.json()
        service_resource_responses[mapping] = response1_data
        log.info(
            f"Successfully created resource mapping for {mapping} with version {response1_data.get('version')}")

        # Second request - update should increment version
        response2 = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                                  data=json.dumps(request_data))
        assert response2.status_code == 200, f"Failed to update resource {mapping}: {response2.text}"

        response2_data = response2.json()
        assert response2_data["version"] > response1_data["version"], \
            f"Version should increment: {response2_data['version']} should be > {response1_data['version']}"

        log.info(f"Version incremented from {response1_data['version']} to {response2_data['version']}")
        service_resource_responses[mapping] = response2_data

        # Third request - with updateIfExists=false should fail
        response3 = requests.post(request_url + "?updateIfExists=false", verify=False, auth=admin_auth,
                                  headers=headers, data=json.dumps(request_data))
        assert response3.status_code == 400, \
            f"Resource {mapping} should not be created when updateIfExists is false: {response3.text}"
        log.info(f"Correctly rejected duplicate creation for {mapping} with updateIfExists=false")

    # Verify all resources were created
    assert len(service_resource_responses) == len(mapping_array), "Not all resources were created successfully"

    # Cleanup: Delete all created resources
    log.info("Starting cleanup: Deleting created resources")
    for mapping, response_data in service_resource_responses.items():
        resource_id = response_data.get("id")
        if resource_id:
            delete_url = f"{base_url}/tags/resource/{resource_id}"
            delete_response = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
            if delete_response.status_code in [200, 204, 201]:
                log.info(f"Successfully deleted resource {mapping} (ID: {resource_id})")
            else:
                log.error(f"Failed to delete resource {mapping} (ID: {resource_id}): {delete_response.text}")


def test_get_service_resource_using_id(log, create_service_resource_ids):
    """Test to get service resource mapping using ID"""
    for mapping, response_data in create_service_resource_ids.items():
        resource_id = response_data.get("id")
        if not resource_id:
            log.error(f"No resource ID found for {mapping}")
            pytest.fail(f"No resource ID found for {mapping}")

        get_url = f"{base_url}/tags/resource/{resource_id}"
        get_response = requests.get(get_url, verify=False, auth=admin_auth, headers=headers)
        assert get_response.status_code == 200, f"Failed to get resource {mapping} by ID: {get_response.text}"

        get_response_data = get_response.json()
        assert get_response_data["id"] == resource_id, f"Resource ID mismatch for {mapping}"
        log.info(f"Successfully retrieved resource {mapping} by ID with version {get_response_data.get('version')}")

# @pytest.mark.skip(reason="Bug in Ranger code base , for unauthorized users status code should be 403 but it is throwing 404 for keyadmin ,user  admin auditor role")
def test_get_service_resource_using_id_by_roles_other_than_admin(log, create_service_resource_ids):
    """Test to verify unauthorized users cannot get service resource mapping using ID"""
    for mapping, response_data in create_service_resource_ids.items():
        resource_id = response_data.get("id")
        if not resource_id:
            log.error(f"No resource ID found for {mapping}")
            pytest.fail(f"No resource ID found for {mapping}")

        get_url = f"{base_url}/tags/resource/{resource_id}"

        # Test with keyadmin
        get_response = requests.get(get_url, verify=False, auth=keyadmin_auth, headers=headers)
        assert get_response.status_code in [403, 400,404], \
            f"Unauthorized keyadmin allowed to get resource {mapping} by ID. Status: {get_response.status_code}, Response: {get_response.text}"
        log.info(f"Keyadmin correctly denied access to {mapping} with status {get_response.status_code}")

        # Test with user role
        get_response = requests.get(get_url, verify=False, auth=HTTPBasicAuth(str_variable_dict["user2"], "Test@12345"), headers=headers)
        assert get_response.status_code in [403, 400,404], \
            f"Unauthorized user role allowed to get resource {mapping} by ID. Status: {get_response.status_code}, Response: {get_response.text}"
        log.info(f"User role correctly denied access to {mapping} with status {get_response.status_code}")

        # Test with admin auditor role
        get_response = requests.get(get_url, verify=False, auth=HTTPBasicAuth(str_variable_dict["user4"], "Test@12345"), headers=headers)
        assert get_response.status_code in [403, 400,404], \
            f"Unauthorized admin auditor role allowed to get resource {mapping} by ID. Status: {get_response.status_code}, Response: {get_response.text}"
        log.info(f"Admin auditor correctly denied access to {mapping} with status {get_response.status_code}")

#
# @pytest.mark.skip(reason="Bug in Ranger code base , for unauthorized users status code should be 403 but it is throwing 404 for keyadmin ,user  admin auditor role")
def test_get_service_resource_using_guid_by_roles_other_than_admin(log, create_service_resource_ids):
    """Test to verify unauthorized users cannot get service resource mapping using GUID"""
    for mapping, response_data in create_service_resource_ids.items():
        resource_guid = response_data.get("guid")
        if not resource_guid:
            log.error(f"No resource GUID found for {mapping}")
            pytest.fail(f"No resource GUID found for {mapping}")

        get_url = f"{base_url}/tags/resource/guid/{resource_guid}"

        # Test with keyadmin
        get_response = requests.get(get_url, verify=False, auth=keyadmin_auth, headers=headers)
        assert get_response.status_code in [403, 400,404], \
            f"Unauthorized keyadmin allowed to get resource {mapping} by GUID. Status: {get_response.status_code}, Response: {get_response.text}"
        log.info(f"Keyadmin correctly denied access to {mapping} with status {get_response.status_code}")

        # Test with user role
        get_response = requests.get(get_url, verify=False, auth=HTTPBasicAuth(str_variable_dict["user2"], "Test@12345"), headers=headers)
        assert get_response.status_code in [403, 400,404], \
            f"Unauthorized user role allowed to get resource {mapping} by GUID. Status: {get_response.status_code}, Response: {get_response.text}"
        log.info(f"User role correctly denied access to {mapping} with status {get_response.status_code}")

        # Test with admin auditor role
        get_response = requests.get(get_url, verify=False, auth=HTTPBasicAuth(str_variable_dict["user4"], "Test@12345"), headers=headers)
        assert get_response.status_code in [403, 400,404], \
            f"Unauthorized admin auditor role allowed to get resource {mapping} by GUID. Status: {get_response.status_code}, Response: {get_response.text}"
        log.info(f"Admin auditor correctly denied access to {mapping} with status {get_response.status_code}")

def test_get_service_resource_using_guid(log, create_service_resource_ids):
    """Test to get service resource mapping using GUID"""
    for mapping, response_data in create_service_resource_ids.items():
        resource_guid = response_data.get("guid")
        if not resource_guid:
            log.error(f"No resource GUID found for {mapping}")
            pytest.fail(f"No resource GUID found for {mapping}")

        get_url = f"{base_url}/tags/resource/guid/{resource_guid}"
        get_response = requests.get(get_url, verify=False, auth=admin_auth, headers=headers)
        assert get_response.status_code in [200,201,204], f"Failed to get resource {mapping} by GUID: {get_response.text}"

        get_response_data = get_response.json()
        assert get_response_data["guid"] == resource_guid, f"Resource GUID mismatch for {mapping}"
        log.info(f"Successfully retrieved resource {mapping} by GUID with version {get_response_data.get('version')}")


def test_get_service_resource_by_resource_params(log, create_service_resource_ids):
    """Test to get service resource using resource query parameters"""
    for mapping, response_data in create_service_resource_ids.items():
        service_name = response_data.get("serviceName")
        resource_elements = response_data.get("resourceElements", {})
        actual_guid = response_data.get("guid")

        if not service_name or not resource_elements:
            log.error(f"Missing service name or resource elements for {mapping}")
            pytest.fail(f"Incomplete resource data for {mapping}")

        # Build query parameters from resource elements
        get_url = f"{base_url}/tags/resource/service/{service_name}/resource"
        params = {}

        for resource_key, resource_value in resource_elements.items():
            values = resource_value.get("values", [])
            if values:
                params[resource_key] = values[0]  # Use first value

            # Add boolean properties if present
            if resource_value.get("isExcludes") is not None:
                params[f"{resource_key}.isExcludes"] = str(resource_value.get("isExcludes")).lower()
            if resource_value.get("isRecursive") is not None:
                params[f"{resource_key}.isRecursive"] = str(resource_value.get("isRecursive")).lower()

        get_response = requests.get(get_url, params=params, verify=False, auth=admin_auth, headers=headers)

        log.info(f"Request URL: {get_response.url}")
        log.info(f"Response Status: {get_response.status_code}")
        log.info(f"Response Body: {get_response.text}")

        assert get_response.status_code in [200,204] , f"Failed to get resource {mapping} by params. Expected 200, got {get_response.status_code}: {get_response.text}"


def test_put_resource_id(log):
    request_url_post = base_url + "/tags/resources"
    request_url_put= base_url + "/tags/resource"
    mapping_array = ["hbase_mapping", "hdfs_mapping", "hive_mapping", "kafka_mapping"]
    for mapping in mapping_array:
        # Verify file exists before making request
        file_path = os.path.join(RESOURCE_MAPPING_DIRECTORY, f"{mapping}.json")
        if not os.path.exists(file_path):
            log.error(f"File not found: {file_path}")
            pytest.fail(f"Mapping file not found: {file_path}")

        request_data1 = get_request_data(f"{mapping}.json", str_variable_dict, RESOURCE_MAPPING_DIRECTORY)

        # First request - create resource
        response1 = requests.post(request_url_post, verify=False, auth=admin_auth, headers=headers,
                                  data=json.dumps(request_data1))
        assert response1.status_code == 200, f"Failed to create resource {mapping}: {response1.text}"
        if response1.status_code in [200,204]:
            log.info(f"Successfully created resource mapping for {mapping} with version {response1.json().get('version')}")
        else:
            log.error(f"Failed to create resource mapping for {mapping}: {response1.text}")
        request_data2= get_request_data(f"{mapping}.json", str_variable_dict, RESOURCE_MAPPING_DIRECTORY)
        resource_id=response1.json().get("id")
        final_request_url_put=request_url_put+"/"+str(resource_id)
        response2 = requests.put(final_request_url_put, verify=False, auth=admin_auth, headers=headers,
                                  data=json.dumps(request_data2))
        assert response2.status_code == 200, f"Failed to update resource {mapping}: {response2.text}"
        assert response2.json().get("version") > response1.json().get("version"), \
            f"Version should increment: {response2.json().get('version')} should be > {response1.json().get('version')}"

        # Compare only the resource values, not the boolean flags
        for key in request_data2.get("resourceElements", {}).keys():
            expected_values = request_data2["resourceElements"][key].get("values")
            actual_values = response2.json()["resourceElements"][key].get("values")
            assert actual_values == expected_values, \
                f"Resource values mismatch for {key}: {actual_values} != {expected_values}"

        log.info(f"Successfully verified resource elements for {mapping}")

        assert response1.json().get("guid")==response2.json().get("guid"), f"GUID should not change on update  {mapping}"
        assert response1.json().get("resourceSignature")!=response2.json().get("resourceSignature"), f"resource signature should have changed on update "

        # Deleting the created resource
        delete_url = f"{base_url}/tags/resource/{resource_id}"
        delete_response = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        if delete_response.status_code in [200, 204, 201]:
            log.info(f"Successfully deleted resource {mapping} (ID: {resource_id})")
        else:
            log.error(f"Failed to delete resource {mapping} (ID: {resource_id}): {delete_response.text}")

def test_put_resource_id_guid(log):
    request_url_post = base_url + "/tags/resources"
    request_url_put= base_url + "/tags/resource/guid"
    mapping_array = ["hbase_mapping", "hdfs_mapping", "hive_mapping", "kafka_mapping"]
    for mapping in mapping_array:
        # Verify file exists before making request
        file_path = os.path.join(RESOURCE_MAPPING_DIRECTORY, f"{mapping}.json")
        if not os.path.exists(file_path):
            log.error(f"File not found: {file_path}")
            pytest.fail(f"Mapping file not found: {file_path}")

        request_data1 = get_request_data(f"{mapping}.json", str_variable_dict, RESOURCE_MAPPING_DIRECTORY)

        # First request - create resource
        response1 = requests.post(request_url_post, verify=False, auth=admin_auth, headers=headers,
                                  data=json.dumps(request_data1))
        assert response1.status_code == 200, f"Failed to create resource {mapping}: {response1.text}"
        if response1.status_code in [200,204]:
            log.info(f"Successfully created resource mapping for {mapping} with version {response1.json().get('version')}")
        else:
            log.error(f"Failed to create resource mapping for {mapping}: {response1.text}")
        request_data2= get_request_data(f"{mapping}.json", str_variable_dict, RESOURCE_MAPPING_DIRECTORY)
        resource_guid=response1.json().get("guid")
        final_request_url_put=request_url_put+"/"+str(resource_guid)
        response2 = requests.put(final_request_url_put, verify=False, auth=admin_auth, headers=headers,
                                  data=json.dumps(request_data2))
        assert response2.status_code == 200, f"Failed to update resource {mapping}: {response2.text}"
        assert response2.json().get("version") > response1.json().get("version"), \
            f"Version should increment: {response2.json().get('version')} should be > {response1.json().get('version')}"
        # Compare only the resource values, not the boolean flags
        for key in request_data2.get("resourceElements", {}).keys():
            expected_values = request_data2["resourceElements"][key].get("values")
            actual_values = response2.json()["resourceElements"][key].get("values")
            assert actual_values == expected_values, \
                f"Resource values mismatch for {key}: {actual_values} != {expected_values}"

        log.info(f"Successfully verified resource elements for {mapping}")

        assert response1.json().get("guid")==response2.json().get("guid"), f"GUID should not change on update  {mapping}"
        assert response1.json().get("resourceSignature")!=response2.json().get("resourceSignature"), f"resource signature should have changed on update "

        # Deleting the created resource
        delete_url = f"{base_url}/tags/resource/guid/{resource_guid}"
        delete_response = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        if delete_response.status_code in [200, 204, 201]:
            log.info(f"Successfully deleted resource {mapping} (ID: {resource_guid})")
        else:
            log.error(f"Failed to delete resource {mapping} (ID: {resource_guid}): {delete_response.text}")

def test_get_service_resource_using_resource_signature(log, create_service_resource_ids):
    """Test to get service resource mapping using ID"""
    for mapping, response_data in create_service_resource_ids.items():
        resource_signature = response_data.get("resourceSignature")
        resource_id = response_data.get("id")
        service_name = response_data.get("serviceName")

        if not resource_signature:
            log.error(f"No resource ID found for {mapping}")
            pytest.fail(f"No resource ID found for {mapping}")

        get_url = f"{base_url}/tags/resource/service/{service_name}/signature/{resource_signature}"
        get_response = requests.get(get_url, verify=False, auth=admin_auth, headers=headers)
        assert get_response.status_code in [200,201,204], f"Failed to get resource {mapping} by ID: {get_response.text}"
        get_response_data = get_response.json()
        assert get_response_data["resourceSignature"] == resource_signature, f"Resource ID mismatch for {mapping}"
        assert get_response_data["id"] == resource_id, f"Resource ID mismatch for {mapping}"


