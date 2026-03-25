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
from  Utility.main import get_request_data ,base_url,get_updated_request_data ,get_variable ,compare_response_data
from requests.auth import HTTPBasicAuth
import os
admin_user = 'admin'
admin_password = 'rangerR0cks!'



BASE_DIR = "/Users/mkrishna/cloudera_code/PyTest-Ranger/services/servicerest"  # Gets Tests_Ranger root
test_data_path = os.path.join(BASE_DIR, "Utility", "test_jsons")
data_folder_path = os.path.join(BASE_DIR, "Utility", "variable_jsons")
variables_data_path = data_folder_path
admin_auth = HTTPBasicAuth(admin_user, admin_password)
headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'X-XSRF-HEADER': 'valid'
    }
str_variable_dict = {}
variable_dict = {}
def setup_module():
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




# str_variable_dict['user1'] = user1
# str_variable_dict['user2'] = user2
# str_variable_dict['user20'] = user20
# str_variable_dict['grant_db_name'] = grant_db_name
# str_variable_dict['grant_table_name'] = grant_table_name
# str_variable_dict['grant_policy_name'] = grant_policy_name
# UPGARDE THIS TEST IN MAIN REPO WITHOUT SERVICE IT WILL NOT WORK

# @TaskReporter.report_test()
# @pytest.mark.skipif(not is_version_7_3_2_0, reason="test supported from CDH 7.3.2.0 onwards")
@pytest.mark.post
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

@pytest.mark.post
def test_get_policies_count_by_admin():
    request_url = base_url + '/plugins/policies/count'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.post
def test_get_csrf_conf_by_admin():
    request_url = base_url + '/plugins/csrfconf'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


# POST TEST CASES

# Use the full absolute path

@pytest.mark.post
def test_create_policy_by_admin():
    request_url = base_url + '/plugins/policies'
    request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"
    # IN OPENSOURCE mapred IS NOT PRESENT





@pytest.mark.post
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
@pytest.mark.post
def test_validate_config_by_admin():
    request_url = base_url + '/plugins/services/validateConfig'
    request_data = get_request_data('test_validate_config.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"
@pytest.mark.post
def test_service_connection_validation() :
    request_url = base_url + '/plugins/services/validateConfig'
    request_data = get_request_data('test_validate_config.json', str_variable_dict, test_data_path)
    # logger.info("The request url is :- %s", request_url)
    # logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    response_data = resp.json()
    assert response_data.get("statusCode") == 1,"Service connection validation failed , check file paths and the service servers"




# # @pytest.mark.L1
# # @TaskReporter.report_test()
@pytest.mark.post
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
@pytest.mark.post
def test_get_policies_by_admin():
    request_url = base_url + '/plugins/policies'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


# @pytest.mark.L1
# @TaskReporter.report_test()
@pytest.mark.post
def test_get_policy_labels_by_admin():
    request_url = base_url + '/plugins/policyLabels'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

# @pytest.mark.L1
# @TaskReporter.report_test()
@pytest.mark.post
def test_get_services_by_admin():
    request_url = base_url + '/plugins/services'
    # logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    # logger.info("The resp status code is :- %s", resp.status_code)
    # logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"






# # @TaskReporter.report_test()
# # @pytest.mark.L1
# # @TaskReporter.report_test()
# def test_create_defintion_by_admin():
#     request_url = base_url + '/plugins/definitions'
#     request_data = get_request_data('test_create_defintion.json', str_variable_dict, test_data_path)
#     # logger.info("The request url is :- %s", request_url)
#     # logger.info("The request data is :- %s", request_data)
#     resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
#     # logger.info("The resp status code is :- %s", resp.status_code)
#     # logger.info("The resp content is :- %s", resp.content)
#     assert resp.status_code == 200, "Expected status code not returned"








# @pytest.mark.L1
# @TaskReporter.report_test()
@pytest.mark.post
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
@pytest.mark.post
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
@pytest.mark.post
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

@pytest.mark.post
def test_definitions_name_by_admin():
    for service_names  in serviceList:
        request_url = base_url + '/plugins/definitions/name/{service_names}'
        request_url = request_url.format(service_names=service_names)
        # logger.info("The request url is :- %s", request_url)
        resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
        # logger.info("The resp status code is :- %s", resp.status_code)
        # logger.info("The resp content is :- %s", resp.content)
        assert resp.status_code == 200, "Expected status code not returned"







