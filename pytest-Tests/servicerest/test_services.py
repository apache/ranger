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
from Utility.main import grant_db_name ,grant_table_name ,grant_policy_name ,  grant_policy_name2,grant_table_name2,grant_db_name2,grant_db_name3,grant_table_name3,grant_policy_name3,grant_db_name4,grant_table_name4
from requests.auth import HTTPBasicAuth
import logging
logger = logging.getLogger(__name__)
import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # Gets Tests_Ranger root
test_data_path = os.path.join(BASE_DIR,"Utility", "test_jsons")
data_folder_path = os.path.join(BASE_DIR, "Utility", "variable_jsons")
variables_data_path = data_folder_path




def test_check_sso_status_by_admin():
    """Verify that the API returns SSO status when requested by  admin"""
    request_url = base_url + '/plugins/checksso'
    text_headers = {"Content-Type": "application/json", "Accept": "*/*"}
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=text_headers)
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    resp_text = resp.text.strip().strip('"')
    assert resp_text in ['true', 'false'], f"Expected 'true' or 'false', but got {resp_text}"


def test_get_csrf_conf_by_admin():
    request_url = base_url + '/plugins/csrfconf'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"


def test_validate_config_by_admin():
    request_url = base_url + '/plugins/services/validateConfig'
    request_data = get_request_data('test_validate_config.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 200, "Expected status code not returned"

@pytest.mark.skip(reason="This test is dependent on the environment setup and might fail if the service servers are not properly configured or file paths are incorrect. Please ensure the environment is correctly set up before running this test.")
def test_service_connection_validation() :
    request_url = base_url + '/plugins/services/validateConfig'
    request_data = get_request_data('test_validate_config.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    response_data = resp.json()
    assert response_data.get("statusCode") == 0,"Service connection validation failed , check file paths and the service servers"


def test_get_services_by_admin():
    request_url = base_url + '/plugins/services'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"


def test_get_service_using_id_by_admin():
    request_url = base_url + '/plugins/services/{service_1_id}'
    request_url = request_url.format(**str_variable_dict)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"


def test_create_service_by_admin(log):
    request_url = base_url + '/plugins/services'
    request_data = get_request_data('test_create_service.json', str_variable_dict, test_data_path)
    # ===== ENHANCED CODE COVERAGE VALIDATIONS =====

    # Store original request data for validation
    original_name = request_data.get('name')
    original_type = request_data.get('type')

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 200, "Expected status code not returned"
    resp_status_for_service_creation = resp.status_code
    if resp.status_code in [200,201,204]:
        log.info("Service created successfully", extra={"response": resp.json()})
    else :
        log.error("Service creation failed ", extra={"response": resp.json()})
    created_service = resp.json()
    assert created_service.get('name') == original_name, "Service name should match request"
    assert created_service.get('type') == original_type, "Service type should match request"
    assert created_service.get('id') is not None, "Service should have an assigned ID"
    assert created_service.get('configs') is not None, "Service configs should not be None"
    # Deleting the created service for cleanup
    if resp_status_for_service_creation in [200,201,204]:
        service__id = created_service.get('id')
        delete_url = base_url + f'/plugins/services/{service__id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        assert delete_resp.status_code in [200, 204], "Failed to delete the created service during cleanup"
        if delete_resp.status_code in [200, 204]:
            log.info("Service deleted successfully during cleanup", extra={"service_id": service__id})
        else:
            log.error("Service deletion failed ", extra={"service_id": service__id})


# @pytest.mark.skip this test has issues investigate and resolve
def test_resource_lookup_by_admin():
    request_url = base_url + '/plugins/services/lookupResource/dev_hdfs'
    request_data = get_request_data('test_resource_lookup.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    expected_status_code = 400
    # it should have been 200
    assert resp.status_code == expected_status_code, "Expected status code not returned"

# Global variable to store response for reuse
resp_for_repeated_use = None

# @pytest.mark.skip
def test_initialize_resource_lookup():
    """Initialize the resource lookup response for repeated use in subsequent tests."""
    global resp_for_repeated_use
    request_url = base_url + '/plugins/services/lookupResource/dev_hdfs'
    request_data = get_request_data('test_resource_lookup.json', str_variable_dict, test_data_path)
    resp_for_repeated_use = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp_for_repeated_use.status_code in [200,204,400], "Failed to initialize resource lookup response"
    # resolve the issue 400 should not be there

@pytest.mark.skip
def test_resource_lookup_by_admin_dev_hdfs():
    request_url = base_url + '/plugins/services/lookupResource/dev_hdfs'
    path_values = resp_for_repeated_use.json()['resources']['path']['values']
    lookup_body = {
        "resourceName": "path",
        "resources": {
            "path": path_values
        },
        "userInput": "",

    }
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(lookup_body))
    assert resp.status_code ==200, "Expected status code not returned"


def test_count_services_by_admin():
    request_url = base_url + '/plugins/services/count'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"

def test_grant_access_create_new_policy_by_admin(log, user2=None):
    """Verify that a user with proper permissions can successfully grant access by creating a new policy."""
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/grant/{service_name}'
    request_data = get_request_data('test_grant_revoke_base2.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    # Search for the created policy to get its ID
    search_url = base_url + f'/plugins/policies/service/name/{service_name}'
    resp = requests.get(search_url, verify=False, auth=admin_auth, headers=headers)
    policies = resp.json().get('policies', [])
    created_policy_id = None
    for policy in policies:
        resources = policy.get('resources', {})
        if (resources.get('database', {}).get('values', []) == [grant_db_name2]
                and resources.get('table', {}).get('values', []) == [grant_table_name2]
                and str_variable_dict['user2'] in str(policy.get('policyItems', []))):
            created_policy_id = policy.get('id')
            break
    #
    assert created_policy_id is not None, f"Failed to find created policy for database \
        '{grant_db_name2}' and table '{grant_table_name2}' with user '{user2}'"


    # new policy has been created we have to delete it after assertions for cleaning up
    if created_policy_id is not None:
        log.info(f"Successfully created policy for grant access test with ID: {created_policy_id}", extra={"policy_id": created_policy_id})
        delete_url =base_url + f'/plugins/policies/{created_policy_id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        assert delete_resp.status_code in [200, 204], f"Failed to delete the created policy during cleanup, status code: {delete_resp.status_code}"
        if delete_resp.status_code in [200, 204]:
            log.info(f"Successfully deleted policy with ID: {created_policy_id} during cleanup", extra={"policy_id": created_policy_id})
        else:
            log.error(f"Failed to delete policy with ID: {created_policy_id} during cleanup", extra={"policy_id": created_policy_id, "status_code": delete_resp.status_code})





def test_secure_grant_access_with_multiple_columns_by_admin(log):
    """
    Verify that the secure grant access API correctly processes grant requests with multiple columns.
    """
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/secure/services/grant/{service_name}'

    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    fields_to_update = {
        "accessTypes": ["select", "update"],
        "grantor": str_variable_dict['user1'],
        "resource": {
            "database": grant_db_name,
            "column": "id,id1,id2",
            "table": grant_table_name,
        },
        "users": [str_variable_dict['user2'], "hive"]
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    # we will have to locate and delete  the policy created for cleanup
    search_url = base_url + f'/plugins/policies/service/name/{service_name}'
    resp = requests.get(search_url, verify=False, auth=admin_auth, headers=headers)
    policies = resp.json().get('policies', [])
    created_policy_id = None
    for policy in policies:
        resources = policy.get('resources', {})
        columns = resources.get('column', {}).get('values', [])
        if (resources.get('database', {}).get('values', []) == [grant_db_name]
                and resources.get('table', {}).get('values', []) == [grant_table_name]
                and set(columns) == {"id", "id1", "id2"}
                and str_variable_dict['user2'] in str(policy.get('policyItems', []))):
            created_policy_id = policy.get('id')
            break

        # new policy created for cleaning we have to delete it after assertions
    if created_policy_id is not None:
        log.info(f"Successfully created policy for grant access test with ID: {created_policy_id}",
                 extra={"policy_id": created_policy_id})
        delete_url = base_url + f'/plugins/policies/{created_policy_id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        assert delete_resp.status_code in [200,204], f"Failed to delete the created policy during cleanup, status code: {delete_resp.status_code}"
        if delete_resp.status_code in [200, 204]:
            log.info(f"Successfully deleted policy with ID: {created_policy_id} during cleanup",
                     extra={"policy_id": created_policy_id})
        else:
            log.error(f"Failed to delete policy with ID: {created_policy_id} during cleanup",
                      extra={"policy_id": created_policy_id, "status_code": delete_resp.status_code})


# @pytest.mark.skip(reason="There might be a bug related to the test , this test grants access to multiple columns but  it is not reflected in the created policy ")
def test_grant_access_with_multiple_columns_by_admin():
    """
    Verify grant access works correctly with complex resources having multiple columns.
    """
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/grant/{service_name}'

    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    fields_to_update = {
        "accessTypes": ["select"],
        "grantor": "admin",
        "resource": {
            "database": grant_db_name,
            "table": grant_table_name,
            "column": "id,model,year"
        },
        "users": [str_variable_dict['user2'], "hive"]
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    print(resp.json())
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"




def test_grant_access_update_existing_policy_by_admin(log):
    """
    Verify that grant request updates an existing policy.
    Creates a policy, updates it, then cleans up.
    """
    service_name = 'dev_hive'

    # Step 1: Create a new policy first
    create_url = base_url + f'/plugins/services/grant/{service_name}'
    request_data = get_request_data('test_grant_revoke_base3.json', str_variable_dict, test_data_path)

    log.info("Creating initial policy for update test", extra={"service_name": service_name})
    create_resp = requests.post(create_url, verify=False, auth=admin_auth, headers=headers,
                                data=json.dumps(request_data))
    assert create_resp.status_code == 200, f"Failed to create initial policy, status code: {create_resp.status_code}"

    if create_resp.status_code == 200:
        log.info("Initial policy created successfully for update test", extra={"response": create_resp.json()})

    # Find the created policy ID
    search_url = base_url + f'/plugins/policies/service/name/{service_name}'
    search_resp = requests.get(search_url, verify=False, auth=admin_auth, headers=headers)
    policies = search_resp.json().get('policies', [])

    policy_id = None
    for policy in policies:
        resources = policy.get('resources', {})
        if (resources.get('database', {}).get('values', []) == [grant_db_name3]
                and resources.get('table', {}).get('values', []) == [grant_table_name3]
                and str_variable_dict['user2'] in str(policy.get('policyItems', []))):
            policy_id = policy.get('id')
            break

    assert policy_id is not None, "Failed to find created policy for update test"
    log.info(f"Found created policy with ID: {policy_id} | policy_id={policy_id}")

    try:
        # Step 2: Update the existing policy
        update_url = base_url + f'/plugins/services/grant/{service_name}'
        fields_to_update = {"users": ["hive", str_variable_dict['user3']]}
        update_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)

        log.info("Updating policy with new users", extra={"policy_id": policy_id, "users": fields_to_update["users"]})
        update_resp = requests.post(update_url, verify=False, auth=admin_auth, headers=headers,
                                   data=json.dumps(update_data))
        assert update_resp.status_code == 200, f"Expected status code 200, but got {update_resp.status_code}"

        if update_resp.status_code == 200:
            log.info("Policy updated successfully", extra={"policy_id": policy_id})

        # Step 3: Verify the update
        get_policy_url = base_url + f'/plugins/policies/{policy_id}'
        verify_resp = requests.get(get_policy_url, verify=False, auth=admin_auth, headers=headers)

        updated_policy = verify_resp.json()
        policy_items = updated_policy.get('policyItems', [])
        hive_found = False
        user3_found = False

        for item in policy_items:
            if "hive" in item.get('users', []):
                hive_found = True
            if str_variable_dict['user3'] in item.get('users', []):
                user3_found = True

        found = hive_found and user3_found
        assert found, "'hive' and 'user3' should be added to the existing policy"

    finally:
        # Step 4: Cleanup - delete the created policy
        delete_url = base_url + f'/plugins/policies/{policy_id}'
        log.info(f"Deleting policy during cleanup", extra={"policy_id": policy_id})
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)

        if delete_resp.status_code in [200, 204]:
            log.info(f"Successfully deleted policy during cleanup", extra={"policy_id": policy_id})
        else:
            log.error(f"Failed to delete policy during cleanup", extra={"policy_id": policy_id, "status_code": delete_resp.status_code})



def test_grant_access_denied_insufficient_permissions_by_admin():  # pylint: disable=redefined-outer-name,unused-argument
    """
    Verify that users without proper grant permissions are denied access. user 2 do not have admin permission
    """
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/grant/{service_name}'
    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    fields_to_update = {"accessTypes": ["select"], "grantor": str_variable_dict['user3'], "users": [str_variable_dict['user2']]}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 403, f"Expected status code 403, but got {resp.status_code}"

def test_grant_request_with_invalid_access_type():
    """
    Verify proper error handling when policy processing fails due to invalid access type.
    """
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/grant/{service_name}'

    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    fields_to_update = {
        "accessTypes": ["invalid-access-type"],
        "resource": {
            "database": "test_db",
            "table": "test_table",
            "column": "*"
        },
        "users": [str_variable_dict['user3']]
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 400, f"Expected status code 400, but got {resp.status_code}"

def test_get_service_using_name_by_keyadmin():
    request_url = base_url + '/plugins/services/name/{service_1_name}'
    request_url = request_url.format(**str_variable_dict)
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code == 400, "Expected status code not returned"

def test_get_service_using_name_hides_sensitive_info_from_non_admin():
    request_url = base_url + '/plugins/services/name/{service_1_name}'
    request_url = request_url.format(**str_variable_dict)
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user2'],'Test@12345'), headers=headers)
    resp=resp.json()
    resp1=requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    resp1=resp1.json()
    assert len(resp)<len(resp1) ,"Expected non-admin response to have less information than admin response"
    assert 'configs' not in resp, "Expected sensitive 'configs' field to be hidden from non-admin users"



def test_revoke_access_by_admin(log):
    """
    Verify successful revocation of access permissions by updating an existing policy.
    """
    # creating a policy for revoke test to ensure we have a policy to revoke and also to get the policy id for assertions after revoke
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/grant/{service_name}'
    request_data = get_request_data('test_grant_revoke_base4.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    created_policy_id = None
    search_url = base_url + f'/plugins/policies/service/name/{service_name}'
    resp = requests.get(search_url, verify=False, auth=admin_auth, headers=headers)
    policies = resp.json().get('policies', [])
    for policy in policies:
        resources = policy.get('resources', {})
        if (resources.get('database', {}).get('values', []) == [grant_db_name4]
                and resources.get('table', {}).get('values', []) == [grant_table_name4]
                and str_variable_dict['user2'] in str(policy.get('policyItems', []))):
            created_policy_id = policy.get('id')
            break

    assert created_policy_id is not None, "Policy ID not found. Ensure grant test ran first."
    if created_policy_id is not None:
        log.info(f"Policy created for revoke test with ID: {created_policy_id}")

    # Get policy state BEFORE revocation
    get_policy_url = base_url + f'/plugins/policies/{created_policy_id}'
    resp_before = requests.get(get_policy_url, verify=False, auth=admin_auth, headers=headers)
    policy_before = resp_before.json()

    # Verify user2 exists in policy before revocation
    user2_found_before = False
    for item in policy_before.get('policyItems', []):
        if str_variable_dict['user2'] in item.get('users', []):
            user2_found_before = True
            break
    assert user2_found_before==True, f"user2 should exist in policy before revocation"
    request_url = base_url + f'/plugins/services/revoke/{service_name}'


    """
    Verify that admin can successfully revoke access  from a resource
    """

    request_data = get_request_data('test_grant_revoke_base4.json', str_variable_dict, test_data_path)

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"

    # Get policy state AFTER revocation
    resp_after = requests.get(get_policy_url, verify=False, auth=admin_auth, headers=headers)
    policy_after = resp_after.json()

    # Verify user2 is removed from policy after revocation
    user2_found_after = False
    for item in policy_after.get('policyItems', []):
        if str_variable_dict['user2'] in item.get('users', []):
            user2_found_after = True
            break

    assert  user2_found_after==False , f"user2 should be removed from policy after revocation"

    if created_policy_id is not None:
        delete_policy_url = base_url + f'/plugins/policies/{created_policy_id}'
        delete_resp = requests.delete(delete_policy_url, verify=False, auth=admin_auth, headers=headers)
        if delete_resp.status_code in [200, 204]:
            log.info(f"Successfully deleted policy with ID: {created_policy_id} during cleanup")
        else:
            log.error(f"Failed to delete policy with ID: {created_policy_id} during cleanup")




@pytest.mark.skip
def test_revoke_access_with_multiple_columns_by_admin():
    """
    Verify revoke access with multiple columns updates matching policy.
    """
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/revoke/{service_name}'

    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    fields_to_update = {
        "accessTypes": ["select"],
        "grantor": "admin",
        "resource": {
            "database": grant_db_name,
            "table": grant_table_name,
            "column": "id,model"
        },
        "users": ["hive"]
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    policy_id = variable_dict.get('grant_created_policy_id')
    get_policy_url = base_url + f'/plugins/policies/{policy_id}'
    resp_policy = requests.get(get_policy_url, verify=False, auth=admin_auth, headers=headers)
    policy = resp_policy.json()

    has_year_permission = False
    has_id_model_permission = False

    for item in policy.get('policyItems', []):
        if "hive" in item.get('users', []):
            resources = policy.get('resources', {})
            columns = resources.get('column', {}).get('values', [])

            # Check if hive has access to year column
            if 'year' in columns:
                has_year_permission = True

            # Check if hive still has access to id or model columns
            if 'id' in columns or 'model' in columns:
                has_id_model_permission = True

    assert has_year_permission==True, "hive user should still have permission to 'year' column"
    assert  has_id_model_permission==False , "hive user should NOT have permission to 'id' or 'model' columns after revocation"


@pytest.mark.skip
def test_revoke_access_with_roles_other_than_admin_by_admin():
    """
    Verify that users with roles other than admin cannot revoke access.
    """
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/revoke/{service_name}'

    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    fields_to_update = {
        "grantor": str_variable_dict['user3']
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))

    assert resp.status_code == 403, f"Expected status code 403 for non-admin revocation attempt, but got {resp.status_code}"
    fields_to_update = {
        "grantor": str_variable_dict['user4']
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 403, f"Expected status code 403 for admin auditor  revocation attempt, but got {resp.status_code}"
    fields_to_update = {
        "grantor": str_variable_dict['user5']
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 400, f"Expected status code 403 for key-admin auditor  revocation attempt, but got {resp.status_code}"


def test_secure_revoke_with_invalid_access_type():  # pylint: disable=redefined-outer-name,unused-argument
    """
    Verify error handling when policy processing fails with invalid access type.
    """
    service_name = 'cm_hive'
    request_url = base_url + f'/plugins/secure/services/revoke/{service_name}'

    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    fields_to_update = {
        "accessTypes": ["invalid-access-type"],
        "grantor": "admin",
        "resource": {
            "database": "sales",
            "table": "transactions",
            "column": "customer_id,amount"
        },
        "users": [str_variable_dict['user3']]
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))

    assert resp.status_code == 404 , f"Expected status code 404, but got {resp.status_code}"

def test_the_access_of_get_services_id_by_different_roles() :
    """
    Verify that users with different roles can access the get service by id API.
    """
    service_id = str_variable_dict['service_1_id']
    request_url = base_url + f'/plugins/services/{service_id}'

    # Test access for admin user
    resp_admin = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp_admin.status_code == 200, f"Admin should have access to get service by id, but got {resp_admin.status_code}"

    # Test access for user without permissions (keyadmin)
    resp_keyadmin = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp_keyadmin.status_code == 400, f"keyadmin should NOT have access to get service by id, but got {resp_keyadmin.status_code}"

def test_get_services_by_id_with_invalid_id_by_admin():
    """
    Verify proper error handling when requesting service by invalid ID.
    """
    invalid_service_id = 999999999999  # Assuming this ID does not exist in the system service_id_type_should_be_long
    request_url = base_url + f'/plugins/services/{invalid_service_id}'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 400, f"Expected status code 404 for invalid service ID, but got {resp.status_code}"
    malformed_service_id = 12.5  # Invalid type for service ID
    request_url_malformed = base_url + f'/plugins/services/{malformed_service_id}'
    resp_malformed = requests.get(request_url_malformed, verify=False, auth=admin_auth, headers=headers)
    assert resp_malformed.status_code == 404, f"Expected status code 400 for malformed service ID, but got {resp_malformed.status_code}"

def test_get_services_by_id_hides_sensitive_info_from_user_roles() :
    """
    Verify that sensitive information is not exposed in the get service by ID response for non-admin roles.
    """
    service_id = str_variable_dict['service_1_id']
    request_url = base_url + f'/plugins/services/{service_id}'

    # Test response for admin user
    resp_admin = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    admin_response_data = resp_admin.json()
    resp_user = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user2'],'Best@12345'), headers=headers)
    resp_user_data = resp_user.json()
    assert len(resp_user_data)< len(admin_response_data), "Response for  user  role should contain less information than admin response"
    assert 'configs' not in resp_user_data, "Response for user role should not contain config "




def test_delete_service_by_roles_other_than_admin() :
    """
    Verify that users with roles other than admin cannot delete a service.
    """
    service_id = str_variable_dict['service_1_id']
    request_url = base_url + f'/plugins/services/{service_id}'

    # Attempt to delete service as keyadmin (should be forbidden)
    resp_keyadmin = requests.delete(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp_keyadmin.status_code == 400, f"keyadmin should NOT have access to delete service, but got {resp_keyadmin.status_code}"

def test_delete_service_using_id_by_admin(log):
    # Step 1: Create a new service
    create_url = base_url + '/plugins/services'
    request_data = get_request_data('test_create_service.json', str_variable_dict, test_data_path)
    create_resp = requests.post(create_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert create_resp.status_code in [200, 201], f"Failed to create service, status code: {create_resp.status_code}"
    service_id = create_resp.json().get('id')
    assert service_id is not None, "Service ID not found in create response"

    log.info("Service created successfully for delete test", extra={"service_id": service_id})

    # Step 2: Delete the created service
    delete_url = base_url + f'/plugins/services/{service_id}'
    delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
    assert delete_resp.status_code == 204, f"Expected status code 204 for delete, but got {delete_resp.status_code}"

    log.info("Service deleted successfully", extra={"service_id": service_id})












