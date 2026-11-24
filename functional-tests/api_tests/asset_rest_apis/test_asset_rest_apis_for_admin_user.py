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

import json
import os

import pytest
import requests

from ranger_test_utils.api_test_utils import (
    admin_auth,
    auditor_auth,
    base_url,
    compare_response_data,
    get_expected_response_data,
    get_request_data,
    get_updated_request_data,
    get_variable,
    hdfs_repo_name,
    headers,
    keyadmin_auth,
    logger,
    user_auth,
)


from ranger_test_utils.api_data import api_data_paths

variable_dict = {}
str_variable_dict = {}
_data_paths = api_data_paths(__file__, "asset_rest_apis", layout="ranger_qe")
data_folder_path = _data_paths["data_folder_path"]
variables_data_path = _data_paths["variables_data_path"]
test_data_path = _data_paths["test_data_path"]


def setup_module():
    repoName = hdfs_repo_name()
    request_url = base_url + '/plugins/services/name/' + repoName
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    get_id = resp.json()

    # Update resource_id.json with actual assetId
    resource_id = variables_data_path + os.sep + 'resource_id.json'
    with open(resource_id, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)
    data["assetId"] = get_id['id']
    with open(resource_id, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)

    # Update search_resource_id.json with actual assetId
    search_resource_id = variables_data_path + os.sep + 'search_resource_id.json'
    with open(search_resource_id, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)
    data["assetId"] = get_id['id']
    with open(search_resource_id, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)

    variable_specifier_list = [('asset_id', 'POST', '/assets/assets', 'asset_id.json', 'id'),
                               ('resource_id', 'POST', '/assets/resources', 'resource_id.json', 'id'),
                               ('search_resource_id', 'POST', '/assets/resources', 'search_resource_id.json', 'id'),
                               ('asset_data', 'GET', '/assets/assets/{asset_id}', None, 'same'),
                               ('asset_count', 'GET', '/assets/assets/count', None, 'value'),
                               ('resource_data', 'GET', '/assets/resources/{resource_id}', None, 'same'),
                               ('search_resource_data', 'GET', '/assets/resources/{search_resource_id}', None, 'same'),
                               ('resource_count', 'GET', '/assets/resources/count', None, 'value')]

    for variable_specification in variable_specifier_list:
        variable_name = variable_specification[0]
        variable_dict[variable_name] = get_variable(variable_specification, variable_dict, variables_data_path)
        str_variable_dict[variable_name] = str(variable_dict[variable_name])


def teardown_module():
    delete_resources = [
        ("search_resource_id", "/assets/resources/{}"),
        ("resource_id", "/assets/resources/{}"),
        ("asset_id", "/assets/assets/{}"),
    ]
    for key, endpoint in delete_resources:
        id_value = str_variable_dict[key]
        url = base_url + endpoint.format(id_value)
        resp = requests.delete(url, verify=False, auth=admin_auth, headers=headers)
        logger.info("Deleted %s %s, status: %s", key, id_value, resp.status_code)


@pytest.mark.L1
def test_get_assets_count_by_admin():
    request_url = base_url + '/assets/assets/count'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"
    resp_data = resp.json()

    expected_response_data = '{"value": {asset_count}}'
    expected_response_data = get_expected_response_data(expected_response_data, str_variable_dict)
    expected_response_data = json.loads(expected_response_data)
    compare_response_data(resp_data, expected_response_data, True)


@pytest.mark.L1
def test_get_resources_count_by_admin():
    request_url = base_url + '/assets/resources/count'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"
    resp_data = resp.json()

    expected_response_data = '{"value": {resource_count} }'
    expected_response_data = get_expected_response_data(expected_response_data, str_variable_dict)
    expected_response_data = json.loads(expected_response_data)
    compare_response_data(resp_data, expected_response_data, True)


@pytest.mark.L1
def test_get_access_audits_by_admin():
    request_url = base_url + '/assets/accessAudit'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_get_assets_by_admin():
    request_url = base_url + '/assets/assets'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_get_assets_v2_report_by_admin():
    request_url = base_url + '/assets/v2/report'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_get_transaction_logs_by_transaction_id_by_admin():
    """Verify that API returns transaction logs when given a valid transaction ID"""
    # Step 1: Get list of transaction logs
    request_url = base_url + '/assets/v2/report'
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"
    resp_data = resp.json()
    # Step 2: Extract transactionId from the first transaction log
    transaction_id = resp_data["vXTrxLogs"][0]["transactionId"]
    logger.info("Extracted transaction ID: %s", transaction_id)

    # Step 3: Query by specific transaction ID
    request_url = base_url + '/assets/v2/report/' + str(transaction_id)
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

    # Step 4: Verify response contains matching transaction logs
    resp_data = resp.json()
    trx_logs = resp_data.get("vXTrxLogs", [])
    assert len(trx_logs) > 0, "No transaction logs returned"
    assert trx_logs[0]["transactionId"] == str(transaction_id), "Transaction ID mismatch"


def test_get_transaction_logs_with_invalid_transaction_id_by_admin():
    """Verify that API returns empty list when transaction ID is invalid"""
    invalid_transaction_id = 'INVALID-ID-999'
    request_url = base_url + '/assets/v2/report/' + invalid_transaction_id
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

    resp_data = resp.json()
    assert resp_data.get("vXTrxLogs", []) == [], "Expected empty transaction logs list"
    assert resp_data.get("totalCount", -1) == 0, "Expected totalCount to be 0"


def test_create_asset_by_admin():
    request_url = base_url + '/assets/assets'
    request_data = get_request_data('test_create_asset.json', str_variable_dict, test_data_path)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_export_audit_by_admin():
    request_url = base_url + '/assets/exportAudit'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_get_reports_by_admin():
    request_url = base_url + '/assets/report'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_get_resources_by_admin():
    request_url = base_url + '/assets/resources'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_search_resources_by_exact_policy_name_by_admin():
    """Verify search by exact policy name using policyName parameter"""
    policy_name = variable_dict["search_resource_data"]["policyName"]
    request_url = base_url + '/assets/resources?policyName=' + policy_name
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

    resp_data = resp.json()
    first_resource = resp_data.get("vXResources", [])[0]
    assert first_resource.get("policyName") == policy_name, f"Expected policy name '{policy_name}', \
    got '{first_resource.get('policyName')}'"


def test_search_resources_by_repository_type_by_admin():
    """Verify search by repository type"""
    request_url = base_url + '/assets/resources?repositoryType=hdfs'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

    resp_data = resp.json()
    resources = resp_data.get("vXResources", [])
    first_resource = resources[0]
    asset_name = first_resource.get("assetName", "")
    assert asset_name.startswith("dev_hdfs"), (f"Expected asset name to start with 'dev_hdfs', got '{asset_name}'")


def test_search_resources_by_resource_name_by_admin():
    """Verify search by resource name using resourceName parameter"""
    resource_name = variable_dict["search_resource_data"]["name"]
    request_url = base_url + '/assets/resources?resourceName=' + resource_name
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

    resp_data = resp.json()
    first_resource = resp_data.get("vXResources", [])[0]
    assert first_resource.get(
        "name") == resource_name, f"Expected resource name '{resource_name}', got '{first_resource.get('name')}'"


def test_search_resources_by_asset_id_by_admin():
    """Verify search by asset ID using assetId parameter """
    asset_id = variable_dict["resource_data"]["assetId"]
    request_url = base_url + '/assets/resources?assetId=' + str(asset_id)
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

    resp_data = resp.json()
    first_resource = resp_data.get("vXResources", [])[0]
    assert first_resource.get(
        "assetId") == asset_id, f"Expected assetId '{asset_id}', got '{first_resource.get('assetId')}'"


def test_search_resources_by_repository_name_by_admin():
    """Verify search by exact repository name (service name) using repositoryName parameter"""
    repository_name = variable_dict["resource_data"]["assetName"]
    request_url = base_url + '/assets/resources?repositoryName=' + repository_name
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"

    resp_data = resp.json()
    first_resource = resp_data.get("vXResources", [])[0]
    assert first_resource.get(
        "assetName") == repository_name, f"Expected assetName '{repository_name}', \
        got '{first_resource.get('assetName')}'"


@pytest.mark.L1
def test_create_resource_by_admin():
    request_url = base_url + '/assets/resources'
    request_data = get_request_data('test_create_resource.json', str_variable_dict, test_data_path)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_get_ugsync_audits_by_admin():
    request_url = base_url + '/assets/ugsyncAudits'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_test_service_config_by_admin():
    request_url = base_url + '/assets/assets/testConfig'
    request_data = variable_dict["asset_data"]
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_get_asset_using_id_by_admin():
    request_url = base_url + '/assets/assets/{asset_id}'
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"
    resp_data = resp.json()

    expected_response_data = variable_dict["asset_data"]
    compare_response_data(resp_data, expected_response_data, True)


@pytest.mark.L1
def test_edit_asset_using_id_by_admin():
    request_url = base_url + '/assets/assets/{asset_id}'
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["asset_data"]
    fields_to_update = {"name": "dev_hdfs_2_{random_str}"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_get_report_using_id_by_admin():
    request_url = base_url + '/assets/report/1'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_get_resource_using_id_by_admin():
    request_url = base_url + '/assets/resources/{resource_id}'
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"
    resp_data = resp.json()

    expected_response_data = variable_dict["resource_data"]
    compare_response_data(resp_data, expected_response_data)


@pytest.mark.L1
def test_get_asset_access_audits_filtered_with_valid_timezone_by_admin():
    request_url = base_url + '/assets/accessAudit?timeZone=UTC'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_edit_resource_using_id_by_admin():
    request_url = base_url + '/assets/resources/{resource_id}'
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["resource_data"]
    fields_to_update = {"name": "{random_str}"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_update_resource_using_null_id_by_admin():
    request_url = base_url + '/assets/resources/{resource_id}'
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["resource_data"]
    fields_to_update = {"id": None}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.put(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_get_ugsync_audits_by_source_by_admin():
    request_url = base_url + '/assets/ugsyncAudits/Unix'
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


@pytest.mark.L1
def test_delete_resource_using_id_by_admin():
    request_url = base_url + '/assets/resources/{resource_id}'
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.delete(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 204, "Expected status code not returned"


@pytest.mark.L1
def test_delete_asset_using_id_by_admin():
    request_url = base_url + '/assets/assets/{asset_id}'
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.delete(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 204, "Expected status code not returned"
