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
#
#
#
#
# # str_variable_dict['user20'] = user20
# str_variable_dict['grant_db_name'] = grant_db_name
# str_variable_dict['grant_table_name'] = grant_table_name
# str_variable_dict['grant_policy_name'] = grant_policy_name
# str_variable_dict['user1'] = user1.get('name')
# str_variable_dict['user2'] = user2.get('name')
# str_variable_dict['user3'] = user3.get('name')



def test_get_policies_count_by_admin():
    request_url = base_url + '/plugins/policies/count'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

def test_get_policies_count_by_different_roles():
    request_url = base_url + '/plugins/policies/count'
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"
    resp= requests.get(request_url, verify=False, auth=HTTPBasicAuth(user2.get('name'), user2.get('password')), headers=headers)
    assert resp.status_code == 200, "Expected status code not returned"

def test_different_roles_has_different_view_of_policies():
    request_url = base_url + '/plugins/policies'
    resp_admin = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    resp_keyadmin = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    resp_user2 = requests.get(request_url, verify=False, auth=HTTPBasicAuth(user2.get('name'), user2.get('password')), headers=headers)

    assert resp_admin.status_code == 200 and resp_keyadmin.status_code == 200 and resp_user2.status_code == 200, "Expected status code not returned"

    policies_admin = resp_admin.json()
    policies_keyadmin = resp_keyadmin.json()
    policies_user2 = resp_user2.json()

    assert len(policies_admin) >= len(policies_keyadmin) , "Different roles do not have expected view of policies"
    assert len(policies_admin) >= len(policies_user2) , "Different roles do not have expected view of policies"

def test_query_parameters_in_get_policies_api():
    request_url=base_url+'plugins/policies?startIndex=1&maxRows=50&sortBy=id&sortType=desc'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    resp=resp.json()
    request_url=base_url+'plugins/policies?startIndex=1&maxRows=50&sortBy=id&sortType=asc'
    resp1 = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    resp1=resp1.json()
    assert resp.get('policies',[])[0].get('id')>=resp1.get('policies',[])[0].get('id') , "Sorting not working as expected"
    request_url = base_url + 'plugins/policies?startIndex=0&maxRows=50&sortBy=id&sortType=asc'
    resp3= requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    assert resp3.get('policies',[])[0].get('id')<=resp1.get('policies',[])[0].get('id')


def test_create_policy_by_admin():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

def test_create_policy_using_invalid_payload():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy_using_invalid_payload.json', str_variable_dict,
                                    test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

# @TaskReporter.report_test()
def test_get_policies_by_auditor():
    request_url = base_url + '/plugins/policies'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=auditor_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

def test_create_policy_by_auditor():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=auditor_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 403, "Expected status code not returned"

# @TaskReporter.report_test()
def test_get_policies_by_keyadmin():
    request_url = base_url + '/plugins/policies'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

def test_create_policy_by_keyadmin():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=keyadmin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, "Expected status code not returned"

def test_get_policies_by_ROLE_USER():
    request_url = base_url + '/plugins/policies'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=HTTPBasicAuth(str_variable_dict['user3'],'Test@12345'), headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

# @TaskReporter.report_test()
def test_create_policy_by_ROLE_USER():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=user_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 403, "Expected status code not returned"

# @TaskReporter.report_test()
def test_create_policy_using_invalid_payload():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy_using_invalid_payload.json', str_variable_dict,
                                    test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

# @TaskReporter.report_test()
def test_import_individual_policy():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_import_policy.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

    policy_id = resp.json().get('id')
    # created_policy_ids.append(policy_id)


# @TaskReporter.report_test()
def test_import_individual_policy_with_same_data():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('policy_1_id.json', str_variable_dict, variables_data_path)
    # logger.info("The request url is :- %s", request_url)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    resp_json = resp.json()
    message = resp_json.get("msgDesc")
    # logger.info("The response message is: %s", message)
    assert resp.status_code == 400, "Expected status code not returned"
    assert "Another policy already exists for matching resource" in message, \
        "Expected error message not found in response"


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
