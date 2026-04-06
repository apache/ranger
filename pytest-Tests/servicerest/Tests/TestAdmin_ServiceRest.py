import requests
import json
import pytest
from  Utility.main import get_request_data ,base_url,get_updated_request_data ,get_variable ,compare_response_data,return_random_str ,admin_auth ,headers,keyadmin_auth,str_variable_dict,variable_dict
from requests.auth import HTTPBasicAuth
import os
from Tests.conftest import user1, user2,user3,user4,user5
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Gets Tests_Ranger root
test_data_path = os.path.join(BASE_DIR,"Utility", "test_jsons")
data_folder_path = os.path.join(BASE_DIR, "Utility", "variable_jsons")
variables_data_path = data_folder_path
grant_db_name = f"vehicle_{return_random_str(7)}"
grant_table_name = f"cars_{return_random_str(7)}"
grant_policy_name = f"grant_policy_{return_random_str(7)}"
# def setup_module():
#     global resp_for_repeated_use
#     variable_specifier_list = [
#         ('plugin_definition_1_id', 'POST', '/plugins/definitions', 'plugin_definition_1_id.json', 'id'),
#         ('plugin_definition_1', 'GET', '/plugins/definitions/{plugin_definition_1_id}', None, 'same'),
#         ('plugin_definition_1_name', 'GET', '/plugins/definitions/{plugin_definition_1_id}', None, 'name'),
#         ('policy_1_id', 'POST', '/plugins/policies', 'policy_1_id.json', 'id'),
#         ('policy_1', 'GET', '/plugins/policies/{policy_1_id}', None, 'same'),
#         ('policy_1_guid', 'GET', '/plugins/policies/{policy_1_id}', None, 'guid'),
#         ('policy_1_resource', 'GET', '/plugins/policies/{policy_1_id}', None, 'resources,path,values,0'),
#         ('service_1_id', 'POST', '/plugins/services', 'service_1_id.json', 'id'),
#         ('service_1', 'GET', '/plugins/services/{service_1_id}', None, 'same'),
#         ('service_1_name', 'GET', '/plugins/services/{service_1_id}', None, 'name'),
#         ('policy_2_id', 'POST', '/plugins/policies', 'policy_2_id.json', 'id'),
#         ('policy_3_id', 'POST', '/plugins/policies', 'policy_3_id.json', 'id')]
#
#     for variable_specification in variable_specifier_list:
#         variable_name = variable_specification[0]
#         variable_dict[variable_name] = get_variable(variable_specification, str_variable_dict, variables_data_path)
#         str_variable_dict[variable_name] = str(variable_dict[variable_name])
#     request_url_for_repeated_use = base_url + '/plugins/policies/{policy_1_id}'
#     request_url_for_repeated_use = request_url_for_repeated_use.format(**str_variable_dict)
#     resp_for_repeated_use = requests.get(request_url_for_repeated_use, verify=False, auth=admin_auth, headers=headers)
#
#




# str_variable_dict['user20'] = user20
str_variable_dict['grant_db_name'] = grant_db_name
str_variable_dict['grant_table_name'] = grant_table_name
str_variable_dict['grant_policy_name'] = grant_policy_name
# str_variable_dict['user1'] = user1.get('name')
# str_variable_dict['user2'] = user2.get('name')
# str_variable_dict['user3'] = user3.get('name')

# UPGARDE THIS TEST IN MAIN REPO WITHOUT SERVICE IT WILL NOT WORK

# @TaskReporter.report_test()
# @pytest.mark.skipif(not is_version_7_3_2_0, reason="test supported from CDH 7.3.2.0 onwards")
def test_check_sso_status_by_admin():
    """Verify that the API returns SSO status when requested by key admin"""
    request_url = base_url + '/plugins/checksso'
    # logger.info("The request url is :- %s", request_url)
    text_headers = {"Content-Type": "application/json", "Accept": "*/*"}
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=text_headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)

    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    resp_text = resp.text.strip().strip('"')
    # logger.info("SSO status: %s", resp_text)
    assert resp_text in ['true', 'false'], f"Expected 'true' or 'false', but got {resp_text}"


def test_get_policies_count_by_admin():
    request_url = base_url + '/plugins/policies/count'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_get_csrf_conf_by_admin():
    request_url = base_url + '/plugins/csrfconf'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


# POST TEST CASES

# Use the full absolute path


def test_create_policy_by_admin():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"
#     # IN OPENSOURCE mapred IS NOT PRESENT






# @pytest.mark.L1
# @TaskReporter.report_test()
def test_create_policies_using_apply_by_admin():
    request_url = base_url + '/plugins/policies/apply'
    request_data = get_request_data('test_create_policies_using_apply.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


# @pytest.mark.L1
# @TaskReporter.report_test()
def test_validate_config_by_admin():
    request_url = base_url + '/plugins/services/validateConfig'
    request_data = get_request_data('test_validate_config.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

def test_service_connection_validation() :
    request_url = base_url + '/plugins/services/validateConfig'
    request_data = get_request_data('test_validate_config.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    response_data = resp.json()
    assert response_data.get("statusCode") == 0,"Service connection validation failed , check file paths and the service servers"


# @pytest.mark.L1
# @TaskReporter.report_test()
def test_get_services_by_admin():
    request_url = base_url + '/plugins/services'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

# @pytest.mark.L1
# @TaskReporter.report_test()
def test_get_service_using_id_by_admin():

    request_url = base_url + '/plugins/services/{service_1_id}'
    request_url = request_url.format(**str_variable_dict)

    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_create_service_by_admin():
    request_url = base_url + '/plugins/services'
    request_data = get_request_data('test_create_service.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)

    # ===== ENHANCED CODE COVERAGE VALIDATIONS =====

    # Store original request data for validation
    original_name = request_data.get('name')
    original_type = request_data.get('type')

    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

    # Test mapViewToEntityBean() method - verify request data was properly mapped
    created_service = resp.json()

    # Verify mapViewToEntityBean() worked correctly
    assert created_service.get('name') == original_name, "Service name should match request"
    assert created_service.get('type') == original_type, "Service type should match request"
    assert created_service.get('id') is not None, "Service should have an assigned ID"
    assert created_service.get('configs') is not None, "Service configs should not be None"






# # @pytest.mark.L1
# # @TaskReporter.report_test()
def test_edit_policy_using_id_by_admin():
    request_url = base_url + '/plugins/policies/{policy_1_id}'
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["policy_1"]
    fields_to_update = {"description": "Modified description"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


# @pytest.mark.L1
# @TaskReporter.report_test()
def test_get_policies_by_admin():
    request_url = base_url + '/plugins/policies'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


# @pytest.mark.L1
# @TaskReporter.report_test()
def test_get_policy_labels_by_admin():
    request_url = base_url + '/plugins/policyLabels'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"





# @TaskReporter.report_test()
# @pytest.mark.L1
# @TaskReporter.report_test()
def test_create_defintion_by_admin():
    request_url = base_url + '/plugins/definitions'
    request_data = get_request_data('test_create_defintion.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"








# @pytest.mark.L1
# @TaskReporter.report_test()
def test_get_definition_using_id_by_admin():

    request_url = base_url + '/plugins/definitions/{plugin_definition_1_id}'
    request_url = request_url.format(**str_variable_dict)

    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


# @pytest.mark.L1
# @TaskReporter.report_test()
def test_edit_definition_using_id_by_admin():
    request_url = base_url + '/plugins/definitions/{plugin_definition_1_id}'
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["plugin_definition_1"]
    fields_to_update = {"description": "Modified description"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"



serviceList=[]
# @pytest.mark.L1
# @TaskReporter.report_test()
def test_get_definitions_by_admin():
    request_url = base_url + '/plugins/definitions'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"
    resp_data=resp.json()
    serviceDef_list=resp_data['serviceDefs']
    for dictionaries in serviceDef_list:
        serviceList.append(dictionaries['name'])


def test_definitions_name_by_admin():
    for service_names  in serviceList:
        request_url = base_url + '/plugins/definitions/name/{service_names}'
        request_url = request_url.format(service_names=service_names)
        # logger.info("The request url is :- %s", request_url)
        resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
        # logger.info("The resp status code is :- %s", resp.status_code)
        # logger.info("The resp content is :- %s", resp.content)
        assert resp.status_code == 200, "Expected status code not returned"

# @TaskReporter.report_test()
def test_resource_lookup_by_admin():
    request_url = base_url + '/plugins/services/lookupResource/dev_hdfs'
    request_data = get_request_data('test_resource_lookup.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)

    expected_status_code = 400

    assert resp.status_code == expected_status_code, "Expected status code not returned"

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

    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(lookup_body))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)


    assert resp.status_code ==200, "Expected status code not returned"


def test_count_services_by_admin():
    request_url = base_url + '/plugins/services/count'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

def test_grant_access_create_new_policy_by_admin():
    """Verify that a user with proper permissions can successfully grant access by creating a new policy."""
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/grant/{service_name}'
    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The grant request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    # Search for the created policy to get its ID
    search_url = base_url + f'/plugins/policies/service/name/{service_name}'
    resp = requests.get(search_url, verify=False, auth=admin_auth, headers=headers)
    #
    policies = resp.json().get('policies', [])
    created_policy_id = None
    for policy in policies:
        resources = policy.get('resources', {})
        if (resources.get('database', {}).get('values', []) == [grant_db_name]
                and resources.get('table', {}).get('values', []) == [grant_table_name]
                and str_variable_dict['user2'] in str(policy.get('policyItems', []))):
            created_policy_id = policy.get('id')
            # logger.info("Found created policy with ID: %s", created_policy_id)
            break
    #
    assert created_policy_id is not None, f"Failed to find created policy for database \
        '{grant_db_name}' and table '{grant_table_name}' with user '{user2}'"
    variable_dict['grant_created_policy_id'] = created_policy_id
    # logger.info("Grant access created new policy successfully with ID: %s", created_policy_id)


def test_secure_grant_access_with_multiple_columns_by_admin():  # pylint: disable=redefined-outer-name,unused-argument
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

    # logger.info("The request url is :- %s", request_url)
    # logger.info("The secure grant request data with multiple columns is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    # logger.info("Secure grant access with multiple columns completed successfully")

def test_grant_access_with_multiple_columns_by_admin():  # pylint: disable=redefined-outer-name,unused-argument
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

    # logger.info("The request url is :- %s", request_url)
    # logger.info("The grant request data with multiple columns is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"




def test_grant_access_update_existing_policy_by_admin():  # pylint: disable=redefined-outer-name,unused-argument
    """
    Verify that grant request updates an existing policy created in test_grant_access_create_new_policy_by_admin.
    """
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/grant/{service_name}'
    # Get policy ID from previous test
    policy_id = variable_dict.get('grant_created_policy_id')
    # logger.info("Updating existing policy (ID: %s) by adding hive user", policy_id)

    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    fields_to_update = {"users": ["hive",str_variable_dict['user3']]}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)

    # logger.info("The request url is :- %s", request_url)
    # logger.info("The grant request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    # Verify hive user is added to the existing policy
    get_policy_url = base_url + f'/plugins/policies/{policy_id}'
    resp = requests.get(get_policy_url, verify=False, auth=admin_auth, headers=headers)

    updated_policy = resp.json()
    policy_items = updated_policy.get('policyItems', [])
    hive_found = False
    user3_found = False
    for item in policy_items:
        if "hive" in item.get('users', []):
            hive_found = True
            # logger.info("'hive' user found in policy items with accesses: %s", item.get('accesses'))
        if str_variable_dict['user3'] in item.get('users', []):
            # logger.info("user3 found in policy items with accesses: %s", item.get('accesses'))
            user3_found = True
            pass
    found=False
    if(hive_found and user3_found):
        found=True

    assert found , "'hive and  user3 should be added to the existing policy"



def test_grant_access_denied_insufficient_permissions_by_admin():  # pylint: disable=redefined-outer-name,unused-argument
    """
    Verify that users without proper grant permissions are denied access. user 2 do not have admin permission
    """
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/grant/{service_name}'
    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    fields_to_update = {"accessTypes": ["select"], "grantor": str_variable_dict['user3'], "users": [str_variable_dict['user2']]}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)

    # logger.info("The request url is :- %s", request_url)
    # logger.info("Attempting grant as user2 (without delegate admin permissions)")
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    # logger.info("The resp content is :- %s", resp.content)
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

    # logger.info("The request url is :- %s", request_url)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 403, f"Expected status code 403, but got {resp.status_code}"

def test_get_service_using_name_by_keyadmin():
    request_url = base_url + '/plugins/services/name/{service_1_name}'
    request_url = request_url.format(**str_variable_dict)

    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, "Expected status code not returned"

def test_get_service_using_name_hides_sensitive_info_from_non_admin():
    request_url = base_url + '/plugins/services/name/{service_1_name}'
    request_url = request_url.format(**str_variable_dict)

    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict[user2],'Test@12345'), headers=headers)
    resp=resp.json()
    resp1=requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    resp1=resp1.json()
    assert len(resp)<len(resp1) ,"Expected non-admin response to have less information than admin response"
    assert 'configs' not in resp, "Expected sensitive 'configs' field to be hidden from non-admin users"
    


def test_revoke_access_by_admin():
    """
    Verify successful revocation of access permissions by updating an existing policy.
    """
    service_name = 'dev_hive'

    # Get the policy ID that was created in test_grant_access_create_new_policy_by_admin
    policy_id = variable_dict.get('grant_created_policy_id')
    assert policy_id is not None, "Policy ID not found. Ensure grant test ran first."

    # Get policy state BEFORE revocation
    get_policy_url = base_url + f'/plugins/policies/{policy_id}'
    resp_before = requests.get(get_policy_url, verify=False, auth=admin_auth, headers=headers)
    policy_before = resp_before.json()

    # Verify user2 exists in policy before revocation
    user2_found_before = False
    for item in policy_before.get('policyItems', []):
        if str_variable_dict['user2'] in item.get('users', []):
            user2_found_before = True
            break
    assert user2_found_before, f"user2 should exist in policy before revocation"
    request_url = base_url + f'/plugins/services/revoke/{service_name}'


    """
    Verify that admin can successfully revoke access  from a resource 
    """

    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)

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

    assert not user2_found_after, f"user2 should be removed from policy after revocation"






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

    # logger.info("The request url is :- %s", request_url)
    # logger.info("The revoke request data with multiple columns is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"
    # logger.info("Revoke access with multiple columns completed successfully")

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

    assert has_year_permission, "hive user should still have permission to 'year' column"
    assert not has_id_model_permission, "hive user should NOT have permission to 'id' or 'model' columns after revocation"

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

    # logger.info("The request url is :- %s", request_url)
    # logger.info("Attempting revoke as user3 (without admin permissions)")
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    # logger.info("The resp content is :- %s", resp.content)
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
    assert resp.status_code == 403, f"Expected status code 403 for key-admin auditor  revocation attempt, but got {resp.status_code}"


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
        "users": ["analyst1"]
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    # logger.info("The request url is :- %s", request_url)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    assert resp.status_code == 403, f"Expected status code 403, but got {resp.status_code}"

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
    assert resp_keyadmin.status_code == 403, f"keyadmin should NOT have access to get service by id, but got {resp_keyadmin.status_code}"

def test_get_services_by_id_with_invalid_id_by_admin():
    """
    Verify proper error handling when requesting service by invalid ID.
    """
    invalid_service_id = 999999999999  # Assuming this ID does not exist in the system service_id_type_should_be_long
    request_url = base_url + f'/plugins/services/{invalid_service_id}'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp.status_code == 404, f"Expected status code 404 for invalid service ID, but got {resp.status_code}"
    malformed_service_id = 12.5  # Invalid type for service ID
    request_url_malformed = base_url + f'/plugins/services/{malformed_service_id}'
    resp_malformed = requests.get(request_url_malformed, verify=False, auth=admin_auth, headers=headers)
    assert resp_malformed.status_code == 400, f"Expected status code 400 for malformed service ID, but got {resp_malformed.status_code}"

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
    assert resp_keyadmin.status_code == 403, f"keyadmin should NOT have access to delete service, but got {resp_keyadmin.status_code}"

def test_delete_service_using_id_by_admin():
    request_url = base_url + '/plugins/services/{service_1_id}'
    request_url = request_url.format(**str_variable_dict)

    # logger.info("The request url is :- %s", request_url)
    resp = requests.delete(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 204, "Expected status code not returned"









