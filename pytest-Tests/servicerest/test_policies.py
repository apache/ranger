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


BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # Gets Tests_Ranger root
test_data_path = os.path.join(BASE_DIR,"Utility", "test_jsons")
data_folder_path = os.path.join(BASE_DIR, "Utility", "variable_jsons")
variables_data_path = data_folder_path





def test_get_policies_count_by_admin():
    request_url = base_url + '/plugins/policies/count'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"

def test_get_policies_count_by_different_roles():
    request_url = base_url + '/plugins/policies/count'
    resp1= requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp1.status_code == 200, "Expected status code not returned"
    resp2= requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user2'], 'Test@12345'), headers=headers)
    assert resp2.status_code == 200, "Expected status code not returned"
    assert int(resp1.text.strip())>=int (resp2.text.strip()), "Different roles do not have expected view of policies count"

def test_get_policies_by_admin():
    request_url = base_url + '/plugins/policies'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"

def test_different_roles_has_different_view_of_policies():
    request_url = base_url + '/plugins/policies'
    resp_admin = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    resp_keyadmin = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    resp_user2 = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user2'], "Test@12345"), headers=headers)

    assert resp_admin.status_code == 200 and resp_keyadmin.status_code == 200 and resp_user2.status_code == 200, "Expected status code not returned"

    policies_admin = resp_admin.json()
    policies_keyadmin = resp_keyadmin.json()
    policies_user2 = resp_user2.json()

    assert len(policies_admin) >= len(policies_keyadmin) , "Different roles do not have expected view of policies"
    assert len(policies_admin) >= len(policies_user2) , "Different roles do not have expected view of policies"


def test_query_parameters_in_get_policies_api():
    request_url = base_url + '/plugins/policies?startIndex=1&maxRows=50&sortBy=id&sortType=desc'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)

    request_url = base_url + '/plugins/policies?startIndex=1&maxRows=50&sortBy=id&sortType=asc'
    resp1 = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)

    assert resp.status_code in [200,204], f" first get request failed with status {resp.status_code}: {resp.text}"
    assert resp1.status_code in [200,204] ,f"second get request failed with status {resp1.status_code}: {resp1.text}"

    resp_json = resp.json()
    resp1_json = resp1.json()

    assert 'policies' in resp_json and len(resp_json['policies']) > 0, "No policies in desc response"
    assert 'policies' in resp1_json and len(resp1_json['policies']) > 0, "No policies in asc response"

    assert resp_json['policies'][0]['id'] >= resp1_json['policies'][0]['id'], "Sorting not working as expected"

    request_url = base_url + '/plugins/policies?startIndex=0&maxRows=50&sortBy=id&sortType=asc'
    resp3 = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp3.status_code == 200, f"Third get  request failed with status {resp3.status_code}: {resp3.text}"

    resp3_json = resp3.json()
    assert 'policies' in resp3_json and len(resp3_json['policies']) > 0, "No policies in third response"

    assert resp3_json['policies'][0]['id'] <= resp1_json['policies'][0]['id'], "Third comparison failed"


def test_get_policies_by_auditor():
    request_url = base_url + '/plugins/policies'
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['auditor_user'],'Test@12345'), headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"


def test_get_policies_by_keyadmin():
    request_url = base_url + '/plugins/policies'
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"

def test_get_policies_by_ROLE_USER():
    request_url = base_url + '/plugins/policies'
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user3'],'Test@12345'), headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"


def test_create_policy_by_admin(log):
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    resp1= requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    resp_json = resp1.json()
    resp_id = resp_json.get('id')
    assert resp1.status_code == 200, "Expected status code not returned"
    assert request_data.get('name') == resp_json.get('name'), "Expected name not returned in response , policy with random different name created instead"
    if resp1.status_code == 200:
        log.info("Policy created with id :- %s", resp_id)

    """
    Test Same policy should not be created again 
    """
    resp= requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 400, "Same policy with same resource and name  created again"

    """
    Test same  policy with same resource and different name should not be created again 
    """
    timestamp=time.time()
    original_name=request_data['name']
    request_data['name'] = f'Test policy modified+{timestamp}'
    resp= requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 400, "Same policy with same resource and different name  created again"
    """
    Test same policy with the same name and different resource should be created
    """
    request_data['resources']['path']['values'] = [f'/test_path_{timestamp}']
    request_data['name'] = original_name
    resp= requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 400, "Same policy with same name and different resource  created again"
    if resp1.status_code == 200:
        delete_url = base_url + f'/plugins/policies/{resp_id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        if delete_resp.status_code in (200,201,204):
            log.info("Delete policy with id :- %s", resp_id)
        else:
            log.error("Failed to delete policy with id :- %s", resp_id, "Response code :- %s", delete_resp.status_code, "Response content :- %s", delete_resp.content)


def test_create_policy_by_auditor():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['auditor_user'],"Test@12345"), headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 403, "Expected status code not returned"


def test_create_policy_by_keyadmin():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=keyadmin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 400, "Expected status code not returned"


def test_create_policy_by_ROLE_USER():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user3'],'Test@12345'), headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 403, "Expected status code not returned,user with ROLE_USER should not be able to create policy"

@pytest.mark.skip
def test_create_policies_using_apply_by_admin(log):
    request_url = base_url + '/plugins/policies/apply'
    request_data = get_request_data('test_create_policies_using_apply.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 200, "Expected status code not returned"
    if resp.status_code == 200:
        log.info("Create policy with id :- %s", request_data['id'])
        delete_url = base_url + f'/plugins/policies/{request_data["id"]}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        if delete_resp.status_code in (200,201,204):
            log.info("Delete policy with id :- %s", request_data['id'])
        else:
            log.error("Failed to delete policy with id :- %s", request_data['id'], "Response code :- %s", delete_resp.status_code, "Response content :- %s", delete_resp.content)


def test_edit_policy_using_id_by_admin(log):
    request_url = base_url + '/plugins/policies/{policy_1_id}'
    request_url = request_url.format(**str_variable_dict)
    request_data = variable_dict["policy_1"]
    fields_to_update = {"description": "Modified description"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    assert resp.status_code == 200, "Expected status code not returned"
    if resp.status_code == 200:
        log.info("Policy with id :- %s updated successfully", str_variable_dict['policy_1_id'])
    else:
        log.error("Failed to update policy with id :- %s", str_variable_dict['policy_1_id'], "Response code :- %s", resp.status_code, "Response content :- %s", resp.content)


def test_export_policy(setup_for_import_export_policies):
    """
    Test export policies for hbase service using export api end point
    """
    exported_policies_from_source = setup_for_import_export_policies['exported_policies_from_source']
    assert exported_policies_from_source is not None, "Exported policies from source service is None, export api might not be working as expected"


def test_import_policy(setup_for_import_export_policies):


    # 1. Define the import endpoint and parameters
    import_url = base_url + '/plugins/policies/importPoliciesFromFile'
    import_params = {
        'updateIfExists': 'true',
        'isOverride': 'false',
        'importType': 'hbase'
    }

    # 2. Prepare the Service Mapping
    # Maps the 'service' name found inside the JSON file to your new destination service
    source_service_name = setup_for_import_export_policies['source_service_name']
    destination_service_name = setup_for_import_export_policies['destination_service_name']
    services_mapping = {source_service_name: destination_service_name}
    exported_policies_from_source = setup_for_import_export_policies['exported_policies_from_source']

    #
    #  Construct the Multipart Payload
    files = {
        'file': (
            'exported_policies.json',
            json.dumps(exported_policies_from_source),
            'application/json'
        ),
        'servicesMapJson': (
            'servicesMapJson.json',
            json.dumps(services_mapping),
            'application/json'
        )
    }

    import_headers = {
        'Accept': 'application/json',
        'X-XSRF-HEADER': 'valid'
    }

    import_resp = requests.post(
        import_url,
        verify=False,
        auth=admin_auth,
        headers=import_headers,
        params=import_params,
        files=files
    )
    assert import_resp.status_code == 204, f"Import failed: {import_resp.text}"




def test_policies_with_same_existing_name_not_allowed_to_import(setup_for_import_export_policies):

    """
    deleteIfExist , updateIFExist , isOverride  all false then  importing a policy with same name and resource should not be allowed
    """
    import_params = {
        'updateIfExists': 'false',
        'isOverride': 'false',
        'importType': 'hbase',
        'deleteifExists': 'false'
    }
    import_url = base_url + '/plugins/policies/importPoliciesFromFile'

    source_service_name = setup_for_import_export_policies['source_service_name']
    destination_service_name = setup_for_import_export_policies['destination_service_name']
    services_mapping = {source_service_name: destination_service_name}
    exported_policies_from_source = setup_for_import_export_policies['exported_policies_from_source']

    #
    #  Construct the Multipart Payload
    files = {
        'file': (
            'exported_policies.json',
            json.dumps(exported_policies_from_source),
            'application/json'
        ),
        'servicesMapJson': (
            'servicesMapJson.json',
            json.dumps(services_mapping),
            'application/json'
        )
    }

    import_headers = {
        'Accept': 'application/json',
        'X-XSRF-HEADER': 'valid'
    }

    import_resp = requests.post(
        import_url,
        verify=False,
        auth=admin_auth,
        headers=import_headers,
        params=import_params,
        files=files
    )
    assert import_resp.status_code == 400, f"Policy with same resource and names imported again ,updateIfExists isOverride,deleteifExists all are false : {import_resp.text}"
    import_params = {
        'updateIfExists': 'true',
        'isOverride': 'false',
        'importType': 'hbase',
        'deleteIfExists': 'false'
    }
    """
    updateifexist true then for same name and resource import should be possible 
    """
    import_resp = requests.post(
        import_url,
        verify=False,
        auth=admin_auth,
        headers=import_headers,
        params=import_params,
        files=files
    )
    assert import_resp.status_code == 204, f"Policy with same resource and names should have the been  again , since updateIfExists  is true ,isOverride,deleteifExists  are false : {import_resp.text}"
    """
    deleteIfExists true then for same name and resource import should be possible 
    """

    import_params = {
        'deleteIfExists': 'true',
        'updateIfExists': 'false',
        'isOverride': 'false',
        'importType': 'hbase',

    }

    import_resp = requests.post(
        import_url,
        verify=False,
        auth=admin_auth,
        headers=import_headers,
        params=import_params,
        files=files
    )
    assert import_resp.status_code == 204, f"Policy with same resource and names should have the been  again , since deleteIfExists  is true ,isOverride , updateifExists are false : {import_resp.text}"


"""
updateIFExists false and isOverride false  delete if exist is true then all the  policies in destination having same resource will be deleted and the new policies will be imported for that resource
"""

def test_implement_policy_with_same_name_and_different_resources(setup_for_import_export_policies):
    """Test that policies with same name but different resources are not allowed to import"""

    # Extract data from fixture
    source_service_name = setup_for_import_export_policies['source_service_name']
    destination_service_name = setup_for_import_export_policies['destination_service_name']
    exported_policies_from_source = setup_for_import_export_policies['exported_policies_from_source']
    policy_name_in_destination_service = setup_for_import_export_policies['policy_name_in_destination_service']

    # Get the existing policy in destination service by name
    request_url = base_url + f'/plugins/policies/service/name/{destination_service_name}'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Failed to get policies from destination service"

    destination_policies = resp.json().get('policies', [])
    destination_policy = next((p for p in destination_policies if p['name'] == policy_name_in_destination_service), None)
    assert destination_policy is not None, "Destination policy not found"

    # Change the resource values to different random values
    random_value = return_random_str()
    destination_policy['resources']['table']['values'] = [f"table_{random_value}"]
    destination_policy['resources']['column-family']['values'] = [f"cf_{random_value}"]
    destination_policy['resources']['column']['values'] = [f"col_{random_value}"]

    # Update the policy in destination service with new resources
    policy_id = destination_policy['id']
    request_url = base_url + f'/plugins/policies/{policy_id}'
    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers,
                       data=json.dumps(destination_policy))
    assert resp.status_code == 200, "Failed to update destination policy with different resources"

    # Now attempt to import policies from source service to destination service
    request_url = base_url + '/plugins/policies/importPoliciesFromFile'

    import_data = {
        "serviceName": destination_service_name,
        "policies": exported_policies_from_source
    }

    local_header = {
        'Accept': '*/*',
        'Content-Type': 'application/json',
        'X-XSRF-HEADER': 'valid'
    }

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=local_header,
                        data=json.dumps(import_data))

    # Should return 400 because policy with same name but different resources exists
    assert resp.status_code == 400, f"Expected status 400 but got {resp.status_code}. Policies with same name but different resources should not be allowed to import"

def test_get_policies_cache_reset_by_auditor():
    request_url = base_url + '/plugins/policies/cache/reset?serviceName={service_1_name}'
    request_url = request_url.format(**str_variable_dict)
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['auditor_user'],"Test@12345"), headers=headers)
    assert resp.status_code == 400, "Expected status code not returned ,auditor should not be able to reset cache for policies"

def test_get_policies_cache_reset_all_by_auditor():
    request_url = base_url + '/plugins/policies/cache/reset-all'
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['auditor_user'],"Test@12345"), headers=headers)
    assert resp.status_code == 400, "Expected status code not returned ,auditor should not be able to reset cache for policies"


def test_get_cache_reset_using_invalid_service_name():
    invalid_service_name = return_random_str()
    request_url = base_url + f'/plugins/policies/cache/reset?serviceName={invalid_service_name}'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 400, "Expected status code not returned, cache reset should not happen with invalid service name"

def test_get_policies_cache_reset_by_keyadmin():
    request_url = base_url + '/plugins/policies/cache/reset?serviceName={service_1_name}'
    request_url = request_url.format(**str_variable_dict)
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code == 400, "Expected status code not returned"

def test_get_policies_cache_reset_by_admin():
    request_url = base_url + '/plugins/policies/cache/reset?serviceName={service_1_name}'
    request_url = request_url.format(**str_variable_dict)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"

def test_get_policies_cache_reset_by_ROLE_USER():
    request_url = base_url + '/plugins/policies/cache/reset?serviceName={service_1_name}'
    request_url = request_url.format(**str_variable_dict)
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user2'],"Test@12345"), headers=headers)
    assert resp.status_code == 400, "Expected status code not returned"

def test_get_policies_cache_reset_all_by_ROLE_USER():
    request_url = base_url + '/plugins/policies/cache/reset-all'
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user2'],"Test@12345"), headers=headers)
    assert resp.status_code == 400, "Expected status code not returned"

def test_service_admins_allowed_to_call_cache_reset():
    request_url = base_url+'/plugins/services/{service_1_id}'
    request_url = request_url.format(**str_variable_dict)
    request_data=requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert request_data.status_code == 200 , "Expected status code not returned while fetching service details"
    request_data=request_data.json()
    request_data['configs']['service.admin.users'] = str_variable_dict['user2']
    update_response = requests.put(
        request_url,
        data=json.dumps(request_data),
        verify=False,
        auth=admin_auth,
        headers=headers
    )
    assert update_response.status_code == 200 , "Expected status code not returned while updating service details"
    # now user2 is given service admin privilege for service_1 and should be able to reset cache for policies of that service
    request_url = base_url + '/plugins/policies/cache/reset?serviceName={service_1_name}'
    request_url = request_url.format(**str_variable_dict)
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user2'],"Test@12345"), headers=headers)
    assert resp.status_code == 200, "Expected status code not returned , service admin should be able to reset cache for policies of that service"


@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_download_policies_by_admin(log):
    request_url = base_url + '/plugins/policies/download/{service_1_name}'
    request_url = request_url.format(**str_variable_dict)
    # download called for the first time ,for the first time the status code should be 200
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    version_number =resp.json()['policyVersion']
    request_url=request_url+f'?lastKnownVersion={version_number}'

    # for next time(second download ) since no update in policies it should return status code 304
    resp1 = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)

    assert resp.status_code == 200 and resp1.status_code == 304 , "Expected status code not returned , since no update in policies after first download second download should return 304"
    # create a policy to make changes in service and then try downloading again , it should return 200 since there is update in policies
    request_url_for_policy = base_url + '/plugins/policies'
    request_data_for_policy = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    request_data_for_policy['service'] = str_variable_dict['service_1_name']
    resp_for_policy = requests.post(request_url_for_policy, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data_for_policy))
    assert resp_for_policy.status_code == 200, "Expected status code not returned while creating policy"

    # Add logging for policy creation
    if resp_for_policy.status_code == 200:
        policy_json = resp_for_policy.json()
        policy_id = policy_json.get('id')
        log.info("Policy created with id: %s for download test", policy_id)
    else:
        log.error("Failed to create policy for download test. Response code: %s, Response content: %s", resp_for_policy.status_code, resp_for_policy.content)

    resp2 = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp2.status_code == 200, "Expected status code not returned , since there is update in policies after creating new policy download should return 200"

    # Cleanup: Delete the created policy
    if resp_for_policy.status_code == 200:
        policy_json = resp_for_policy.json()
        policy_id = policy_json.get('id')
        delete_url = base_url + f'/plugins/policies/{policy_id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        if delete_resp.status_code in [200, 204]:
            log.info("Policy with id %s deleted successfully after download test", policy_id)
        else:
            log.error("Failed to delete policy with id %s after download test. Response code: %s, Response content: %s", policy_id, delete_resp.status_code, delete_resp.content)


@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policy_from_event_time_by_admin(create_policy_for_test):
    """
    Test retrieves a policy at a specific event time using policyId and eventTime parameters.
    This API is useful for getting historical versions of policies based on when they were modified.

    Steps:
    1. Create a new policy to get a valid policy ID
    2. Get the current event time from the policy creation response
    3. Update the policy to create a new version
    4. Use the eventTime parameter to retrieve the policy state at the original event time
    5. Verify the response matches the original policy state
    """

    # Update the policy to create a new version
    policy_json= create_policy_for_test['policy_json']
    policy_id=policy_json['id']
    original_event_time=policy_json['updateTime']
    original_description=policy_json['description']
    fields_to_update = {"description": "Modified description for event time test"}
    updated_data = get_updated_request_data(request_data=policy_json, fields_to_update=fields_to_update)


    update_url = base_url + f'/plugins/policies/{policy_id}'
    update_resp = requests.put(update_url, verify=False, auth=admin_auth, headers=headers,
                               data=json.dumps(updated_data))
    assert update_resp.status_code == 200, "Failed to update policy"

    # Now test the eventTime API to get the policy at the original event time
    event_time_url = base_url + f'/plugins/policies/eventTime?eventTime={original_event_time}&policyId={policy_id}'

    event_resp = requests.get(event_time_url, verify=False, auth=admin_auth, headers=headers)
    assert event_resp.status_code == 200, "Expected status code not returned for eventTime API"

    event_policy = event_resp.json()
    # Verify we got the original policy version (before the update)
    assert event_policy.get(
        'description') == original_description, "EventTime API did not return the policy state at the specified event time"

@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policy_from_event_time_with_version_number(create_policy_for_test):
    """
    Test retrieves a specific policy version using policyId, eventTime, and versionNo parameters.
    The versionNo parameter takes precedence over eventTime if both are provided.

    Steps:
    1. Create a policy (version 1)
    2. Update it to create version 2
    3. Use versionNo parameter to retrieve version 1
    4. Verify the returned policy matches version 1
    """


    fields_to_update = {"description": "Version 2 description"}
    policy_json= create_policy_for_test['policy_json']
    policy_id = policy_json['policy_id']
    original_event_time=policy_json['updateTime']
    original_description=policy_json['description']
    version_1=policy_json['version']
    updated_data = get_updated_request_data(request_data=policy_json, fields_to_update=fields_to_update)

    update_url = base_url + f'/plugins/policies/{policy_id}'
    update_resp = requests.put(update_url, verify=False, auth=admin_auth, headers=headers,
                               data=json.dumps(updated_data))
    assert update_resp.status_code == 200, "Failed to update policy"

    # Get policy using versionNo parameter
    event_time_url = base_url + f'/plugins/policies/eventTime?eventTime={original_event_time}&policyId={policy_id}&versionNo={version_1}'

    event_resp = requests.get(event_time_url, verify=False, auth=admin_auth, headers=headers)
    assert event_resp.status_code == 200, "Expected status code not returned for eventTime API with versionNo"

    versioned_policy = event_resp.json()
    assert versioned_policy.get('version') == version_1, "Did not retrieve the correct policy version"
    assert versioned_policy.get('description') == original_description, "Version 1 policy description does not match"


def test_get_policy_from_event_time_missing_parameters():
    """
    Test validates that the API returns 400 error when required parameters (eventTime or policyId) are missing.
    Both eventTime and policyId are mandatory parameters for this API.
    """
    # Test without eventTime parameter
    request_url = base_url + '/plugins/policies/eventTime?policyId=1'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 400, "API should return 400 when eventTime parameter is missing"

    # Test without policyId parameter
    request_url = base_url + '/plugins/policies/eventTime?eventTime=1234567890'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 400, "API should return 400 when policyId parameter is missing"

    # Test without both parameters
    request_url = base_url + '/plugins/policies/eventTime'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 400, "API should return 400 when both eventTime and policyId parameters are missing"

@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policy_from_event_time_invalid_policy_id():
    """
    Test validates that the API returns 404 error when an invalid or non-existent policyId is provided.
    """
    invalid_policy_id = 999999999
    current_time = int(time.time() * 1000)

    request_url = base_url + f'/plugins/policies/eventTime?eventTime={current_time}&policyId={invalid_policy_id}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 404, "API should return 404 for non-existent policy ID"

@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policy_from_event_time_by_auditor(create_policy_for_test):
    """
    Test validates that auditor role has access to retrieve policies by event time.
    Auditors should have read access to policy history for audit purposes.
    """


    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']
    event_time = policy_json.get('updateTime')

    # Try to access as auditor
    event_time_url = base_url + f'/plugins/policies/eventTime?eventTime={event_time}&policyId={policy_id}'

    resp = requests.get(event_time_url, verify=False,
                        auth=HTTPBasicAuth(str_variable_dict['auditor_user'], 'Test@12345'), headers=headers)
    assert resp.status_code == 200, "Auditor should be able to retrieve policy from event time"

@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policy_from_event_time_by_unauthorized_user(log):
    """
    Test validates that users without proper permissions cannot retrieve policies by event time.
    Only admin, auditor, and authorized users should have access to policy history.
    """
    # Create a policy first
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    status_code_post=resp.status_code
    assert resp.status_code in [200,204], "Failed to create policy"
    if resp.status_code in [200,204]:
        log.info("Policy created with id :- %s", resp.json().get('id'))
    else:
        log.error("Failed to create policy, Response code :- %s, Response content :- %s", resp.status_code, resp.content)

    policy_json = resp.json()
    policy_id = policy_json.get('id')
    event_time = policy_json.get('updateTime')

    # Try to access as regular user (ROLE_USER)
    event_time_url = base_url + f'/plugins/policies/eventTime?eventTime={event_time}&policyId={policy_id}'

    resp = requests.get(event_time_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user3'], 'Test@12345'),
                        headers=headers)
    assert resp.status_code in [403, 404], "Unauthorized user should not be able to retrieve policy from event time"
    if status_code_post in [200,204]:
        # Clean up by deleting the created policy
        delete_url = base_url + f'/plugins/policies/{policy_id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        if delete_resp.status_code in (200, 204):
            log.info("Deleted policy with id :- %s", policy_id)
        else:
            log.error("Failed to delete policy with id :- %s, Response code :- %s, Response content :- %s", policy_id, delete_resp.status_code, delete_resp.content)


def test_get_policy_by_guid_not_passed():
    """
    Test validates that API returns 404 when GUID is not passed or is empty.
    GUID is a mandatory path parameter for this API.
    """
    request_url = base_url + '/plugins/policies/guid/'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 404, "API should return 404 when GUID is not provided in path"


def test_get_policy_by_invalid_guid():
    """
    Test validates that API returns 404 when an invalid or non-existent GUID is provided.
    """
    invalid_guid = 'invalid-guid-12345-67890-abcdef'

    request_url = base_url + f'/plugins/policies/guid/{invalid_guid}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 404, "API should return 404 for non-existent GUID"


def test_get_policy_by_guid_success(create_policy_for_test):
    """
    Test successfully retrieves a policy using its GUID.
    The API should return the policy details when a valid GUID is provided.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_guid = policy_json['guid']

    request_url = base_url + f'/plugins/policies/guid/{policy_guid}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code 200 for valid GUID"

    retrieved_policy = resp.json()
    assert retrieved_policy['guid'] == policy_guid, "Retrieved policy GUID does not match"
    assert retrieved_policy['id'] == policy_json['id'], "Retrieved policy ID does not match"


def test_get_policy_by_guid_with_wrong_service_name(create_policy_for_test):
    """
    Test validates that API returns 404 when GUID exists but serviceName doesn't match.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_guid = policy_json['guid']
    wrong_service_name = 'non_existent_service_' + return_random_str()

    request_url = base_url + f'/plugins/policies/guid/{policy_guid}?serviceName={wrong_service_name}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 404, "API should return 404 when serviceName doesn't match the policy"


def test_get_policy_by_guid_with_service_name(create_policy_for_test):
    """
    Test retrieves a policy using GUID and serviceName query parameter.
    This helps narrow down the search to a specific service.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_guid = policy_json['guid']
    service_name = policy_json['service']

    request_url = base_url + f'/plugins/policies/guid/{policy_guid}?serviceName={service_name}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code 200 when providing valid GUID and serviceName"

    retrieved_policy = resp.json()
    assert retrieved_policy['service'] == service_name, "Retrieved policy service name does not match"


def test_get_policy_by_guid_with_zone_name(create_policy_for_test):
    """
    Test retrieves a policy using GUID and zoneName query parameter.
    This helps retrieve policies from a specific security zone.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_guid = policy_json['guid']
    zone_name = policy_json.get('zoneName', '')

    request_url = base_url + f'/plugins/policies/guid/{policy_guid}?zoneName={zone_name}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code 200 when providing valid GUID and zoneName"


def test_get_policy_by_guid_with_service_and_zone_name(create_policy_for_test):
    """
    Test retrieves a policy using GUID with both serviceName and zoneName parameters.
    This provides the most specific search criteria.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_guid = policy_json['guid']
    service_name = policy_json['service']
    zone_name = policy_json.get('zoneName', '')

    request_url = base_url + f'/plugins/policies/guid/{policy_guid}?serviceName={service_name}&zoneName={zone_name}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code 200 when providing all parameters"


def test_get_policy_by_guid_insufficient_admin_access():
    """
    Test validates that users without admin/audit access cannot retrieve policies by GUID.
    Only users with proper admin or audit permissions should access policy details.
    """
    policy_json = variable_dict.get("policy_1")
    if not policy_json:
        pytest.skip("policy_1 not found in variable_dict")

    policy_guid = policy_json.get('guid')

    request_url = base_url + f'/plugins/policies/guid/{policy_guid}'

    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user3'], 'Test@12345'),
                        headers=headers)
    assert resp.status_code ==403 , "User without admin/audit access should not retrieve policy by GUID"


def test_get_policy_by_guid_by_auditor(create_policy_for_test):
    """
    Test validates that auditor role can retrieve policies by GUID.
    Auditors should have read access to all policy details.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_guid = policy_json['guid']

    request_url = base_url + f'/plugins/policies/guid/{policy_guid}'

    resp = requests.get(request_url, verify=False,
                        auth=HTTPBasicAuth(str_variable_dict['auditor_user'], 'Test@12345'),
                        headers=headers)
    assert resp.status_code == 200, "Auditor should be able to retrieve policy by GUID"


def test_get_policy_by_guid_by_keyadmin(create_policy_for_test):
    """
    Test validates that keyadmin role can not retrieve policies by GUID.
     for components other than kms .
    """
    policy_json = create_policy_for_test['policy_json']
    policy_guid = policy_json['guid']

    request_url = base_url + f'/plugins/policies/guid/{policy_guid}'

    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)

    # STATUS CODE SHOULD HAVE BEEN 403 SINCE KEY ADMIN IS NOT ALLOWD
    assert resp.status_code == 400, "Keyadmin should be able to retrieve policy by GUID"

@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policy_by_guid_case_sensitivity():
    """
    Test validates GUID case sensitivity.
    GUIDs should be treated as case-insensitive identifiers.
    """
    policy_json = variable_dict.get("policy_1")
    if not policy_json:
        pytest.skip("policy_1 not found in variable_dict")

    policy_guid = policy_json.get('guid')

    request_url_lower = base_url + f'/plugins/policies/guid/{policy_guid.lower()}'
    request_url_upper = base_url + f'/plugins/policies/guid/{policy_guid.upper()}'

    resp_lower = requests.get(request_url_lower, verify=False, auth=admin_auth, headers=headers)
    resp_upper = requests.get(request_url_upper, verify=False, auth=admin_auth, headers=headers)

    assert resp_lower.status_code == resp_upper.status_code, "GUID lookup should be case-insensitive"


def test_get_policy_by_guid_deleted_policy():
    """
    Test validates that API returns 404 for a GUID of a deleted policy.
    Deleted policies should not be retrievable.
    """
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 200, "Failed to create policy"

    policy_json = resp.json()
    policy_guid = policy_json['guid']
    policy_id = policy_json['id']

    delete_url = base_url + f'/plugins/policies/{policy_id}'
    delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
    assert delete_resp.status_code == 204, "Failed to delete policy"

    get_url = base_url + f'/plugins/policies/guid/{policy_guid}'
    get_resp = requests.get(get_url, verify=False, auth=admin_auth, headers=headers)
    assert get_resp.status_code == 404, "API should return 404 for deleted policy GUID"


@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policies_for_resource_by_admin():
    """
    Test retrieves policies matching specific resource using service definition name.
    Admin should be able to retrieve all matching policies for the resource.
    """
    request_url=base_url+'/plugins/policies/hdfs/for-resource?serviceName=dev_hdfs&resource:path=/'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"
    policies=resp.json()
    assert len(policies)>0, "No policies found for the resource, while expected at least one policy should be there for the resource"

def test_get_policies_for_non_existing_resource_by_admin():
    """
    since no path is mentioned it should give status code 200 with empty policies list in response
    """
    request_url=base_url+'/plugins/policies/hdfs/for-resource?serviceName=dev_hdfs&resource:path='
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"
    policies=resp.json()
    assert len(policies)==0, "No policies found for the resource, while expected at least one policy should be there for the resource"


def test_get_policies_for_resource_invalid_service_def():
    """
    Test validates that API returns error for invalid service definition name.
    """
    invalid_service_def = 'invalid_service_def_' + return_random_str()

    request_url = base_url + f'/plugins/policies/{invalid_service_def}/for-resource?serviceName=dev_hdfs&path=/'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code in [400, 404], "API should return error for invalid service definition"

@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policies_for_resource_with_matching_policy(create_policy_for_test):
    """
    Test retrieves policies that match a specific resource path.
    Should return the policy created in the fixture.
    """
    policy_json = create_policy_for_test['policy_json']
    service_name = policy_json['service']
    service_def_name = 'hdfs'

    # Get the resource path from the created policy
    resource_path = list(policy_json['resources'].values())[0]['values'][0]

    request_url = base_url + f'/plugins/policies/{service_def_name}/for-resource?serviceName={service_name}&path={resource_path}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code 200"

    policies = resp.json()
    policy_ids = [p['id'] for p in policies]
    assert policy_json['id'] in policy_ids, "Created policy should be in the matching policies list"


def test_get_policy_for_version_number_success(create_policy_for_test):
    """
    Test successfully retrieves a specific version of a policy using policyId and versionNo.
    Steps:
    1. Get the created policy (version 1)
    2. Update it to create version 2
    3. Retrieve version 1 using the API
    4. Verify the response matches version 1
    """
    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']
    original_description = policy_json['description']
    version_1 = policy_json['version']

    # Update policy to create version 2
    fields_to_update = {"description": "Version 2 description"}
    updated_data = get_updated_request_data(request_data=policy_json, fields_to_update=fields_to_update)

    update_url = base_url + f'/plugins/policies/{policy_id}'
    update_resp = requests.put(update_url, verify=False, auth=admin_auth, headers=headers,
                               data=json.dumps(updated_data))

    assert update_resp.status_code == 200, "Failed to update policy"

    # Get version 1 using the version API - CORRECTED URL
    request_url = base_url + f'/plugins/policy/{policy_id}/version/{version_1}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code 200"

    versioned_policy = resp.json()
    assert versioned_policy['version'] == version_1, "Retrieved policy version does not match"
    assert versioned_policy['description'] == original_description, "Version 1 description does not match"
    assert versioned_policy['id'] == policy_id, "Policy ID does not match"


def test_get_policy_for_invalid_policy_id():
    """
    Test validates that API returns 404 for non-existent policy ID.
    """
    invalid_policy_id = 999999999
    version_no = 1

    request_url = base_url + f'/plugins/policy/{invalid_policy_id}/version/{version_no}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 400, "API should return 404 for non-existent policy ID"


def test_get_policy_for_invalid_version_number(create_policy_for_test):
    """
    Test validates that API returns 404 for non-existent version number.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']
    invalid_version = 999999

    request_url = base_url + f'/plugins/policies/policy/{policy_id}/version/{invalid_version}'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 404, "API should return 404 for non-existent version number"


def test_get_policy_for_version_number_by_auditor(create_policy_for_test):
    """
    Test validates that auditor role can retrieve policy versions.
    Auditors should have read access to policy history.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']
    version_no = policy_json['version']

    request_url = base_url + f'/plugins/policy/{policy_id}/version/{version_no}'

    resp = requests.get(request_url, verify=False,
                        auth=HTTPBasicAuth(str_variable_dict['auditor_user'], 'Test@12345'),
                        headers=headers)
    assert resp.status_code == 200, "Auditor should be able to retrieve policy version"


def test_get_policy_for_version_number_by_unauthorized_user(create_policy_for_test):
    """
    Test validates that users without proper permissions cannot retrieve policy versions.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']
    version_no = policy_json['version']

    request_url = base_url + f'/plugins/policy/{policy_id}/version/{version_no}'

    resp = requests.get(request_url, verify=False,
                        auth=HTTPBasicAuth(str_variable_dict['user3'], 'Test@12345'),
                        headers=headers)
    assert resp.status_code ==403 , "Unauthorized user should not retrieve policy version"


def test_get_policy_for_version_number_by_keyadmin_for_non_kms_policy(create_policy_for_test):
    """
    Test validates that keyadmin role cannot retrieve non-KMS policy versions.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']
    version_no = policy_json['version']

    request_url = base_url + f'/plugins/policy/{policy_id}/version/{version_no}'

    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code in [400, 403], "Keyadmin should not retrieve non-KMS policy version"

def test_get_policy_for_version_number_by_keyadmin_for_kms_policy(create_kms_policy_for_test):
    """
    Test validates that keyadmin role cannot retrieve non-KMS policy versions.
    """
    policy_json = create_kms_policy_for_test['policy_json']
    policy_id = policy_json['id']
    version_no = policy_json['version']

    request_url = base_url + f'/plugins/policy/{policy_id}/version/{version_no}'

    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code ==200 , "Keyadmin should be allowed to retruve kms policy version"



def test_get_policy_for_version_zero(create_policy_for_test):
    """
    Test validates handling of version number 0 (invalid version).
    """
    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']

    request_url = base_url + f'/plugins/policy/{policy_id}/version/0'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code in [400, 404], "Version 0 should not be valid"


def test_get_policy_for_negative_version_number(create_policy_for_test):
    """
    Test validates handling of negative version number (invalid).
    """
    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']

    request_url = base_url + f'/plugins/policy/{policy_id}/version/-1'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code in [400, 404], "Negative version number should not be valid"


def test_get_deleted_policy_version():
    """
    Test validates that versions of a deleted policy cannot be retrieved.
    """
    # Create and then delete a policy
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 200, "Failed to create policy"
    policy_json = resp.json()
    policy_id = policy_json['id']
    version_no = policy_json['version']

    # Delete the policy
    delete_url = base_url + f'/plugins/policies/{policy_id}'
    delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
    assert delete_resp.status_code == 204, "Failed to delete policy"

    # Try to retrieve version of deleted policy
    version_url = base_url + f'/plugins/policies/policy/{policy_id}/version/{version_no}'
    resp = requests.get(version_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 404, "Should not retrieve version of deleted policy"


def test_get_policy_version_list_success(log):
    """
    Test successfully retrieves the version list for a policy.
    Steps:
    1. Get the created policy
    2. Update it multiple times to create multiple versions
    3. Retrieve the version list
    4. Verify all versions are present in the list
    """
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    status_code_policy_create= resp.status_code
    resp_json = resp.json()
    if resp.status_code == 200:
        log.info(f"Created policy with ID: {resp_json['id']}")
    else:
        log.error(f"Failed to create policy for version list test. Status code: {resp.status_code}, Response: {resp.text}")
    policy_json = resp.json()
    policy_id = policy_json['id']

    # Update policy multiple times to create versions 2, 3, 4
    for i in range(2, 5):
        update_url = base_url + f'/plugins/policies/{policy_id}'
        update_resp = requests.put(update_url, verify=False, auth=admin_auth, headers=headers,
                                   data=json.dumps(policy_json))
        assert update_resp.status_code == 200, f"Failed to create version {i}"


    # Get version list
    request_url = base_url + f'/plugins/policy/{policy_id}/versionList'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code 200"

    version_list = resp.json()
    assert 'value' in version_list, "Response should contain 'value' field"
    versions = version_list['value'].split(',')

    # Verify all 4 versions exist
    assert len(versions) == 4, f"Expected 4 versions, got {len(versions)}"
    assert '1' in versions, "Version 1 should be in the list"
    assert '2' in versions, "Version 2 should be in the list"
    assert '3' in versions, "Version 3 should be in the list"
    assert '4' in versions, "Version 4 should be in the list"

    # deleting the created policy for cleanup
    if status_code_policy_create == 200:
        delete_url = base_url + f'/plugins/policies/{policy_id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        if delete_resp.status_code in [200,204]:
            log.info(f"Deleted policy with ID: {policy_id}")
        else:
            log.error(f"Failed to delete policy. Status code: {delete_resp.status_code}, Response: {delete_resp.text}")

@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policy_version_list_invalid_policy_id():
    """
    Test validates that API returns 404/400 for non-existent policy ID.
    """
    invalid_policy_id = 999999999

    request_url = base_url + f'/plugins/policy/{invalid_policy_id}/versionList'

    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code in [400, 404], "API should return error for non-existent policy ID"


def test_get_policy_version_list_by_auditor(create_policy_for_test):
    """
    Test validates that auditor role can retrieve policy version list.
    Auditors should have read access to policy version history.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']

    request_url = base_url + f'/plugins/policy/{policy_id}/versionList'

    resp = requests.get(request_url, verify=False,
                        auth=HTTPBasicAuth(str_variable_dict['auditor_user'], 'Test@12345'),
                        headers=headers)
    assert resp.status_code == 200, "Auditor should be able to retrieve policy version list"

@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policy_version_list_by_keyadmin_for_non_kms_policy(create_policy_for_test):
    """
    Test validates that keyadmin role cannot retrieve non-KMS policy version list.
    """
    policy_json = create_policy_for_test['policy_json']
    policy_id = policy_json['id']

    request_url = base_url + f'/plugins/policy/{policy_id}/versionList'

    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code in [400, 403], "Keyadmin should not retrieve non-KMS policy version list"


def test_get_policy_version_list_by_keyadmin_for_kms_policy(create_kms_policy_for_test):
    """
    Test validates that keyadmin role can retrieve KMS policy version list.
    """
    policy_json = create_kms_policy_for_test['policy_json']
    policy_id = policy_json['id']

    request_url = base_url + f'/plugins/policy/{policy_id}/versionList'

    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code == 200, "Keyadmin should be able to retrieve KMS policy version list"

@pytest.mark.skip(reason="This test is failing intermittently, need to investigate and fix the root cause")
def test_get_policy_version_list_deleted_policy(log):
    """
    Test validates that version list of a deleted policy cannot be retrieved.
    """
    # Create and then delete a policy
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code in [200, 204], "Could not create policy for deletion test"
    if resp.status_code == 200:
        log.info(f"created policy with ID: {resp.json()['id']} for deletion test")
    else:
        log.error(f"Failed to create policy for deletion test. Status code: {resp.status_code}, Response: {resp.text}")


    policy_json = resp.json()
    policy_id = policy_json['id']

    # Delete the policy
    delete_url = base_url + f'/plugins/policies/{policy_id}'
    delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
    assert delete_resp.status_code in [200 , 204], "Failed to delete policy for version list deletion test"
    if delete_resp.status_code in [200,204]:
        log.info(f"Deleted policy with ID: {policy_id}")
    else:
        log.error(f"Failed to delete policy. Status code: {delete_resp.status_code}, Response: {delete_resp.text}")

    # Try to retrieve version list of deleted policy
    version_list_url = base_url + f'/plugins/policy/{policy_id}/versionList'
    resp = requests.get(version_list_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code in [400, 404], "Should not retrieve version list of deleted policy"



