from contextlib import nullcontext

import pytest
import os
import requests
import json

from  Utility.main import get_request_data ,base_url,get_updated_request_data ,get_variable ,compare_response_data,return_random_str,global_dict,admin_auth,headers,str_variable_dict,variable_dict


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
variables_data_path=os.path.join(BASE_DIR, "Utility", "variable_jsons")
# setting test user before running the tests and unsetting it after the tests are done

def create_test_user(roles=None):
    """Helper function to create a test user with specified roles"""
    if roles is None:
        roles = ["ROLE_SYS_ADMIN"]

    request_data = get_request_data('create_user_for_test.json', global_dict, variables_data_path)

    # Update with roles
    fields_to_update = {
        "userRoleList": roles
    }

    updated_data = get_updated_request_data(request_data, fields_to_update)

    request_url = base_url + "/xusers/secure/users"
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(updated_data))
    return resp.json()


# Create user objects with different roles
user1 = create_test_user(["ROLE_SYS_ADMIN"])
user2 = create_test_user(["ROLE_USER"])
user3 = create_test_user(["ROLE_USER"])
user4 = create_test_user(["ROLE_ADMIN_AUDITOR"])
user5 = create_test_user(["ROLE_KEY_ADMIN_AUDITOR"])


@pytest.fixture(scope="session", autouse=True)
def setup_module():
    global resp_for_repeated_use
    variable_specifier_list = [
        ('plugin_definition_1_id', 'POST', '/plugins/definitions', 'plugin_definition_1_id.json', 'id'),
        ('plugin_definition_1', 'GET', '/plugins/definitions/{plugin_definition_1_id}', None, 'same'),
        ('plugin_definition_1_name', 'GET', '/plugins/definitions/{plugin_definition_1_id}', None, 'name'),
        ('policy_1_id', 'POST', '/plugins/policies', 'policy_1_id.json', 'id'),
        ('policy_1', 'GET', '/plugins/policies/{policy_1_id}', None, 'same'),
        ('policy_1_guid', 'GET', '/plugins/policies/{policy_1_id}', None, 'guid'),
        ('policy_1_resource', 'GET', '/plugins/policies/{policy_1_id}', None, 'resources,path,values,0'),
        ('service_1_id', 'POST', '/plugins/services', 'service_1_id.json', 'id'),
        ('service_1', 'GET', '/plugins/services/{service_1_id}', None, 'same'),
        ('service_1_name', 'GET', '/plugins/services/{service_1_id}', None, 'name'),
        ('policy_2_id', 'POST', '/plugins/policies', 'policy_2_id.json', 'id'),
        ('policy_3_id', 'POST', '/plugins/policies', 'policy_3_id.json', 'id')]

    for variable_specification in variable_specifier_list:
        variable_name = variable_specification[0]
        variable_dict[variable_name] = get_variable(variable_specification, str_variable_dict, variables_data_path)
        str_variable_dict[variable_name] = str(variable_dict[variable_name])
    request_url_for_repeated_use = base_url + '/plugins/policies/{policy_1_id}'
    request_url_for_repeated_use = request_url_for_repeated_use.format(**str_variable_dict)
    resp_for_repeated_use = requests.get(request_url_for_repeated_use, verify=False, auth=admin_auth, headers=headers)
    yield

# str_variable_dict['user20'] = user20
str_variable_dict['user1'] = user1.get('name')
str_variable_dict['user2'] = user2.get('name')
str_variable_dict['user3'] = user3.get('name')




