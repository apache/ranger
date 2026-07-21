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


from contextlib import nullcontext

import pytest
import os
import requests
import json
import logging
from datetime import datetime
import inspect
from requests.auth import HTTPBasicAuth

from  Utility.main import get_request_data ,base_url,get_updated_request_data ,get_variable ,compare_response_data,return_random_str,global_dict,admin_auth,headers,str_variable_dict,variable_dict
from Utility.main import grant_db_name,grant_policy_name2,grant_table_name

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
test_data_path = os.path.join(BASE_DIR,"Utility", "test_jsons")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
variables_data_path=os.path.join(BASE_DIR, "Utility", "variable_jsons")
LOG_FILE_PATH = os.path.join(BASE_DIR, "automation.log")

# --- 1. The Logger Configuration ---


def custlogger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Only add handler if one doesn't already exist
    if not logger.handlers:
        fh = logging.FileHandler(LOG_FILE_PATH, mode='a')
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s : %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p'
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


# --- 2. Clear log file at the start of the suite ---
@pytest.fixture(scope="session", autouse=True)
def clear_log():
    with open(LOG_FILE_PATH, 'w'):
        pass


# --- 3. The Fixture you use in tests ---
@pytest.fixture(scope="function")
def log(request):
    # Captures: test_file.py::test_function_name
    file_path, _, test_name = request.node.location
    file_name = os.path.basename(file_path)
    full_name = f"{file_name}::{test_name}"

    return custlogger(full_name)

@pytest.fixture(scope="session")
def session_log():
    """Session-level logger """
    return custlogger("GlobalSessionSetup")



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
@pytest.fixture(scope="session", autouse=True)
def setup_test_users(session_log):
    """Create test users with different roles and cleanup after tests"""
    created_user_ids = []

    try:
        session_log.info("Creating test users with different roles...")

        # Create users
        global user1, user2, user3, user4, user5, auditor_user

        user1 = create_test_user(["ROLE_SYS_ADMIN"])
        created_user_ids.append(user1.get('id'))
        session_log.info(f"Created user1 (ROLE_SYS_ADMIN) with ID: {user1.get('id')}, name: {user1.get('name')}")

        user2 = create_test_user(["ROLE_USER"])
        created_user_ids.append(user2.get('id'))
        session_log.info(f"Created user2 (ROLE_USER) with ID: {user2.get('id')}, name: {user2.get('name')}")

        user3 = create_test_user(["ROLE_USER"])
        created_user_ids.append(user3.get('id'))
        session_log.info(f"Created user3 (ROLE_USER) with ID: {user3.get('id')}, name: {user3.get('name')}")

        user4 = create_test_user(["ROLE_ADMIN_AUDITOR"])
        created_user_ids.append(user4.get('id'))
        session_log.info(f"Created user4 (ROLE_ADMIN_AUDITOR) with ID: {user4.get('id')}, name: {user4.get('name')}")

        # user5 = create_test_user(["ROLE_KEY_ADMIN_AUDITOR"])
        # created_user_ids.append(user5.get('id'))
        # session_log.info(
        #     f"Created user5 (ROLE_KEY_ADMIN_AUDITOR) with ID: {user5.get('id')}, name: {user5.get('name')}")

        auditor_user = create_test_user(["ROLE_ADMIN_AUDITOR"])
        created_user_ids.append(auditor_user.get('id'))
        session_log.info(
            f"Created auditor_user (ROLE_ADMIN_AUDITOR) with ID: {auditor_user.get('id')}, name: {auditor_user.get('name')}")

        # Add to string variable dictionary
        str_variable_dict['user1'] = user1.get('name')
        print(str_variable_dict['user1'])
        str_variable_dict['user2'] = user2.get('name')
        str_variable_dict['user3'] = user3.get('name')
        str_variable_dict['user4'] = user4.get('name')
        # str_variable_dict['user5'] = user5.get('name')
        str_variable_dict['auditor_user'] = auditor_user.get('name')

        session_log.info("Test users created successfully and added to str_variable_dict")

        yield

    except Exception as e:
        session_log.error(f"Failed to create test users: {str(e)}")
        raise

    finally:
        session_log.info("Starting cleanup for test users...")

        for user_id in created_user_ids:
            try:
                delete_url = base_url + f'/xusers/users/{user_id}?forceDelete=true'
                resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)

                if resp.status_code in [200, 204]:
                    session_log.info(f"Successfully deleted user with ID: {user_id}")
                else:
                    session_log.error(f"Failed to delete user with ID: {user_id}",
                                      extra={"status_code": resp.status_code, "response_text": resp.text})
            except Exception as e:
                session_log.error(f"Exception while deleting user {user_id}: {str(e)}")

        session_log.info("Cleanup for test users completed")


@pytest.fixture(scope="session", autouse=True)
def setup_module(session_log):
    global resp_for_repeated_use
    session_log.info("Setting up test environment , setup_module started")

    # Track created resource IDs for cleanup
    created_resources = {
        'policy_ids': [],
        'service_ids': [],
        'plugin_definition_ids': []
    }

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
        ('policy_3_id', 'POST', '/plugins/policies', 'policy_3_id.json', 'id')
    ]

    try:
        for variable_specification in variable_specifier_list:
            variable_name = variable_specification[0]
            variable_dict[variable_name] = get_variable(variable_specification, str_variable_dict, variables_data_path)
            str_variable_dict[variable_name] = str(variable_dict[variable_name])

            # Track created resources
            if variable_specification[1] == 'POST':
                resource_id = variable_dict[variable_name]
                if '/policies' in variable_specification[2]:
                    created_resources['policy_ids'].append(resource_id)
                    session_log.info(f"Created policy with ID : {resource_id} in setup_module")
                elif '/services' in variable_specification[2]:
                    created_resources['service_ids'].append(resource_id)
                    session_log.info(f"Created service with ID : {resource_id} in setup_module")
                elif '/definitions' in variable_specification[2]:
                    created_resources['plugin_definition_ids'].append(resource_id)
                    session_log.info(f"Created plugin definition with ID: {resource_id} in setup_module")

        request_url_for_repeated_use = base_url + '/plugins/policies/{policy_1_id}'
        request_url_for_repeated_use = request_url_for_repeated_use.format(**str_variable_dict)
        resp_for_repeated_use = requests.get(request_url_for_repeated_use, verify=False, auth=admin_auth,
                                             headers=headers)

        session_log.info("Setup module completed successfully in setup_module")
        yield

    except Exception as e:
        session_log.error(f"setup_module failed  in : {str(e)}")
        raise
    finally:
        session_log.info("Starting cleanup for setup_module...")

        # Delete policies first
        for policy_id in created_resources['policy_ids']:
            try:
                delete_url = base_url + f'/plugins/policies/{policy_id}'
                resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
                if resp.status_code in [200, 204]:
                    session_log.info(f"Successfully deleted policy with ID: {policy_id} in setup_module cleanup")
                else:
                    session_log.error(f"Failed to delete policy with ID: {policy_id}",
                                      extra={"status_code": resp.status_code, "response_text": resp.text})
            except Exception as e:
                session_log.error(f"Exception while deleting policy {policy_id}: {str(e)}")

        # Delete services second
        for service_id in created_resources['service_ids']:
            try:
                delete_url = base_url + f'/plugins/services/{service_id}'
                resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
                if resp.status_code in [200, 204]:
                    session_log.info(f"Successfully deleted service with ID: {service_id} in setup_module cleanup")
                else:
                    session_log.error(f"Failed to delete service with ID: {service_id}" ,
                                      extra={"status_code": resp.status_code, "response_text": resp.text})
            except Exception as e:
                session_log.error(f"Exception while deleting service {service_id}: {str(e)}")

        # Delete plugin definitions last
        for plugin_def_id in created_resources['plugin_definition_ids']:
            try:
                delete_url = base_url + f'/plugins/definitions/{plugin_def_id}'
                resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
                if resp.status_code in [200, 204]:
                    session_log.info(f"Successfully deleted plugin definition with ID: {plugin_def_id}")
                else:
                    session_log.error(f"Failed to delete plugin definition with ID: {plugin_def_id}",
                                      extra={"status_code": resp.status_code, "response_text": resp.text})
            except Exception as e:
                session_log.error(f"Exception while deleting plugin definition {plugin_def_id}: {str(e)}")

        session_log.info("Cleanup for setup_module completed")




@pytest.fixture(scope="session")
def setup_for_import_export_policies(session_log):
    source_service_id = None
    destination_service_id = None
    source_user_name = None
    destination_user_name = None

    try:
        # create source hbase service
        session_log.info("Creating source hbase service...")
        request_url = base_url + '/plugins/services'
        request_data = get_request_data('test_create_hbase_service.json', str_variable_dict, test_data_path)
        source_service_name = request_data['name']
        source_user_name = request_data['configs']['username']

        resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
        assert resp.status_code == 200, f"Failed to create source service '{source_service_name}': {resp.status_code}"
        source_service_id = resp.json().get('id')

        # create destination hbase service
        session_log.info("Creating destination hbase service...")
        request_data = get_request_data('test_create_hbase_service.json', str_variable_dict, test_data_path)
        destination_service_name = request_data['name']
        destination_user_name = request_data['configs']['username']

        resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
        assert resp.status_code == 200, f"Failed to create destination service '{destination_service_name}': {resp.status_code}"
        destination_service_id = resp.json().get('id')

        # create policies
        session_log.info("Creating policies...")
        request_url = base_url + '/plugins/policies'
        request_data = get_request_data('test_create_hbase_policy.json', str_variable_dict, test_data_path)
        request_data['service'] = source_service_name
        policy_name_in_source_service = request_data['name']

        resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
        assert resp.status_code == 200, f"Failed to create source policy: {resp.status_code}"

        request_data['service'] = destination_service_name
        policy_name_in_destination_service = request_data['name']
        resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
        assert resp.status_code == 200, f"Failed to create destination policy: {resp.status_code}"

        # export policies
        session_log.info(f"Exporting policies from service: {source_service_name}")
        export_url = base_url + f'/plugins/policies/exportJson?serviceName={source_service_name}&checkPoliciesExists=true'
        local_header = {
            'Accept': '*/*',
            'Content-Type': 'application/json',
            'X-XSRF-HEADER': 'valid'
        }

        exported_policies_from_source = requests.get(export_url, verify=False, auth=admin_auth, headers=local_header)
        assert exported_policies_from_source.status_code == 200, f"Export failed: {exported_policies_from_source.status_code}"

        session_log.info("Setup completed successfully")

        yield {
            "source_service_name": source_service_name,
            "destination_service_name": destination_service_name,
            "exported_policies_from_source": exported_policies_from_source.json(),
            "policy_name_in_source_service": policy_name_in_source_service,
            "policy_name_in_destination_service": policy_name_in_destination_service,
        }

    except Exception as e:
        session_log.error(f"Setup failed for setup_for_import_export_policies: {str(e)}")
        raise

    finally:
        session_log.info("Starting cleanup for import export policies ...")
        if source_service_id:
            resp1=requests.delete(base_url + f'/plugins/services/{source_service_id}', verify=False, auth=admin_auth, headers=headers)
            if resp1.status_code in  [200,204]:
                session_log.info(f"Deleted source service ID: {source_service_id}")
            else:
                session_log.error(f"Failed to delete source service ID: {source_service_id}", extra={"response_status": resp1.status_code, "response_text": resp1.text})

        if destination_service_id:
            resp2=requests.delete(base_url + f'/plugins/services/{destination_service_id}', verify=False, auth=admin_auth, headers=headers)
            if resp2.status_code in [200,204]:
                session_log.info(f"Deleted destination service ID: {destination_service_id}")
            else:
                session_log.error(f"Failed to delete destination service ID: {destination_service_id}", extra={"response_status": resp2.status_code, "response_text": resp2.text})

        for user_name in [source_user_name, destination_user_name]:
            if user_name:
                # 1. Get the User ID
                lookup_url = base_url + f'/xusers/users?name={user_name}'
                lookup_resp = requests.get(lookup_url, verify=False, auth=admin_auth, headers=headers)

                if lookup_resp.status_code == 200:
                    users_list = lookup_resp.json().get('vXUsers', [])
                    if users_list:
                        user_id = users_list[0].get('id')
                        # 2. Delete using the verified ID endpoint
                        delete_url = base_url + f'/xusers/secure/users/id/{user_id}?forceDelete=true'
                        resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)

                        if resp.status_code in [200, 204]:
                            session_log.info(f"Deleted hbase service user: {user_name} (ID: {user_id})")
                        else:
                            session_log.error(f"Failed to delete hbase service user: {user_name} (Status: {resp.status_code})")
                    else:
                        session_log.info(f"User {user_name} not found for deletion.")
                else:
                    session_log.error(f"Failed to lookup user {user_name} for deletion.")


@pytest.fixture(scope="session")
def create_policy_for_test(session_log):
    policy_id = None

    try:
        session_log.info("Creating policy for test...")
        request_url = base_url + '/plugins/policies'
        request_data = get_request_data('test_create_policy.json', str_variable_dict, test_data_path)

        resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
        assert resp.status_code == 200, "Failed to create policy"

        policy_json = resp.json()
        policy_id = policy_json.get('id')
        session_log.info(f"Created policy with ID: {policy_id}")

        yield {
            "policy_id": policy_id,
            "policy_json": policy_json
        }

    except Exception as e:
        session_log.error(f"Setup failed for create_policy_for_test: {str(e)}")
        raise
    finally:
        session_log.info("Starting cleanup for policy created through create_policy_for_test...")
        if policy_id:
            resp = requests.delete(base_url + f'/plugins/policies/{policy_id}', verify=False, auth=admin_auth,
                                   headers=headers)
            if resp.status_code in [200, 204]:
                session_log.info(f"Deleted policy ID: {policy_id}")
            else:
                session_log.error(f"Failed to delete policy ID: {policy_id}",
                          extra={"response_status": resp.status_code, "response_text": resp.text})



@pytest.fixture(scope="session")
def create_kms_policy_for_test(session_log):
    policy_id = None

    try:
        session_log.info("Creating KMS policy for test...")
        request_url = base_url + '/plugins/policies'
        request_data = get_request_data('test_create_kms_policy.json', str_variable_dict, test_data_path)

        resp = requests.post(request_url, verify=False, auth=HTTPBasicAuth('keyadmin','rangerR0cks!'), headers=headers, data=json.dumps(request_data))
        assert resp.status_code == 200, "Failed to create KMS policy"

        policy_json = resp.json()
        policy_id = policy_json.get('id')
        session_log.info(f"Created KMS policy with ID: {policy_id}")

        yield {
            "policy_id": policy_id,
            "policy_json": policy_json
        }

    except Exception as e:
        session_log.error(f"Setup failed for create_kms_policy_for_test: {str(e)} ")
        raise

    finally:
        session_log.info("Starting cleanup for KMS policy through create_kms_policy_for_test...")
        if policy_id:
            resp = requests.delete(base_url + f'/plugins/policies/{policy_id}', verify=False, auth=HTTPBasicAuth('keyadmin','rangerR0cks!'), headers=headers)

            if resp.status_code in [200, 204]:
                session_log.info(f"Deleted KMS policy ID: {policy_id}")
            else:
                session_log.error(f"Failed to delete KMS policy ID: {policy_id} ",
                          extra={"response_status": resp.status_code, "response_text": resp.text})


@pytest.fixture(scope="session")
def setup_for_grant_and_revoke_tests(session_log):
    service_name = 'dev_hive'
    request_url = base_url + f'/plugins/services/grant/{service_name}'
    request_data = get_request_data('test_grant_revoke_base.json', str_variable_dict, test_data_path)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers,
                         data=json.dumps(request_data))
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code}"


    # Search for the created policy to get its ID

    if resp.status_code == 200:
        session_log.info(f"Successfully granted access for service '{service_name}' to user '{str_variable_dict['user2']}'")
    else:
        session_log.error(f"Failed to grant access for service '{service_name}' to user '{str_variable_dict['user2']}'",
                          extra={"response_status": resp.status_code, "response_text": resp.text})

    search_url = base_url + f'/plugins/policies/service/name/{service_name}'
    resp = requests.get(search_url, verify=False, auth=admin_auth, headers=headers)
    policies = resp.json().get('policies', [])
    created_policy_id = None
    for policy in policies:
        resources = policy.get('resources', {})
        if (resources.get('database', {}).get('values', []) == [grant_db_name]
                and resources.get('table', {}).get('values', []) == [grant_table_name]
                and str_variable_dict['user2'] in str(policy.get('policyItems', []))):
            created_policy_id = policy.get('id')
            break
    #
    assert created_policy_id is not None, f"Failed to find created policy for database \
            '{grant_db_name}' and table '{grant_table_name}' with user '{user2}'"

    if created_policy_id is not None:
        session_log.info(f"Found created policy with ID: {created_policy_id} for database '{grant_db_name}' and table '{grant_table_name}' with user '{str_variable_dict['user2']}'")
    else:
        session_log.error(f"Failed to find created policy for database '{grant_db_name}' and table '{grant_table_name}' with user '{str_variable_dict['user2']}'")

    variable_dict['grant_created_policy_id'] = created_policy_id

    yield

    # new policy created for cleaning we have to delete it after assertions
    if created_policy_id is not None:
        session_log.info(f"Successfully created policy for grant access test with ID: {created_policy_id}",
                 extra={"policy_id": created_policy_id})
        delete_url = base_url + f'/plugins/policies/{created_policy_id}'
        delete_resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        assert delete_resp.status_code in [200,204], f"Failed to delete the created policy during cleanup, status code: {delete_resp.status_code}"
        if delete_resp.status_code in [200, 204]:
            session_log.info(f"Successfully deleted policy with ID: {created_policy_id} during cleanup",
                     extra={"policy_id": created_policy_id})
        else:
            session_log.error(f"Failed to delete policy with ID: {created_policy_id} during cleanup",
                      extra={"policy_id": created_policy_id, "status_code": delete_resp.status_code})


