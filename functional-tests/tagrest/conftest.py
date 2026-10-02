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
import logging
from datetime import datetime
import inspect
from requests.auth import HTTPBasicAuth
from Utility.main import base_url ,admin_auth ,headers ,get_request_data ,str_variable_dict ,global_dict ,get_updated_request_data

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE_PATH = os.path.join(BASE_DIR, "automation.log")
VARIABLE_JSONS_DIR = os.path.join(BASE_DIR, "Utility", "variable_jsons")
RESOURCE_MAPPING_DIRECTORY = os.path.join(VARIABLE_JSONS_DIR, "resource_mappings")
variables_data_path=os.path.join(BASE_DIR, "Utility", "variable_jsons")

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



@pytest.fixture(scope="session")
def create_service_resource_ids(session_log):
    """Session-level fixture to create service and resource IDs"""
    session_log.info("Starting session setup: Creating service and resource IDs")
    # creating resource ids for different resources in different types of services
    service_resource_responses = {}
    try:
        request_url = base_url + "/tags/resources"
        mapping_arry = ["hbase_mapping", "hdfs_mapping", "hive_mapping", "kafka_mapping"]
        for mapping in mapping_arry:
            request_data = get_request_data(f"{mapping}.json", str_variable_dict,RESOURCE_MAPPING_DIRECTORY)
            response = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data))
            assert response.status_code == 200, f"Failed to create resource {mapping}: {response.text}"
            if response.status_code in [200, 201, 204]:
                service_resource_responses[mapping] = response.json()
                session_log.info(f"Successfully created resource mapping for {mapping}")
            else:
                session_log.error(f"Failed to create resource mapping for {mapping}: {response.text}")
    except Exception as e:
        session_log.error(f"Exception during resource creation: {str(e)}")
        raise

    yield service_resource_responses

    # Cleanup: Delete all created resources after tests complete
    session_log.info("Starting session teardown: Deleting created resources")
    for mapping, response_data in service_resource_responses.items():
        try:
            resource_id = response_data.get("id")
            if resource_id:
                delete_url = f"{base_url}/tags/resource/{resource_id}"
                delete_response = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
                if delete_response.status_code in [200, 204,201]:
                    session_log.info(f"Successfully deleted resource {mapping} (ID: {resource_id})")
                else:
                    session_log.error(f"Failed to delete resource {mapping} (ID: {resource_id}): {delete_response.text}")
        except Exception as e:
            session_log.error(f"Exception while deleting resource {mapping}: {str(e)}")

@pytest.fixture(scope="session")
def create_tagdef_for_tests(session_log):
    """
    Session level fixture to create tagdef and use across multiple test cases.
    Yields the created tagdef response JSON for use in tests, then deletes it on teardown.
    """
    response_json = None
    tagdef_guid = None

    # --- SETUP ---
    try:
        request_data = get_request_data("create_tagdef.json", str_variable_dict, VARIABLE_JSONS_DIR)
        request_url = base_url + "/tags/tagdefs"

        session_log.info(f"Creating tagdef with payload: {json.dumps(request_data)}")

        response = requests.post(
            request_url,
            verify=False,
            auth=admin_auth,
            headers=headers,
            data=json.dumps(request_data)
        )

        assert response.status_code in [200, 201], (
            f"Failed to create tagdef: expected 200/201, got {response.status_code}. Response: {response.text}"
        )

        response_json = response.json()
        assert "guid" in response_json, "Response JSON does not contain 'guid'"

        tagdef_guid = response_json["guid"]
        session_log.info(f"Tagdef created successfully with GUID: {tagdef_guid}")

    except Exception as e:
        session_log.error(f"Exception while creating tagdef: {str(e)}")
        raise

    yield response_json

    # --- TEARDOWN ---
    try:
        if tagdef_guid:
            delete_url = f"{base_url}/tags/tagdef/guid/{tagdef_guid}"
            session_log.info(f"Deleting tagdef with GUID: {tagdef_guid}")

            delete_response = requests.delete(
                delete_url,
                verify=False,
                auth=admin_auth,
                headers=headers
            )

            assert delete_response.status_code in [200, 201, 204], (
                f"Expected 200/201/204 for delete, got {delete_response.status_code}. Response: {delete_response.text}"
            )

            session_log.info(f"Tagdef with GUID: {tagdef_guid} deleted successfully")

    except Exception as e:
        session_log.error(
            f"Exception while deleting tagdef with GUID: {tagdef_guid}. Error: {str(e)}"
        )


@pytest.fixture(scope="session")
def create_tag_for_tests(session_log, create_tagdef_for_tests):
    """
    Session level fixture to create a tag based on the created tagdef.
    Depends on create_tagdef_for_tests to ensure tagdef exists first.
    """
    response_json = None
    tag_guid = None

    # --- SETUP ---
    try:
        # Get the tagdef name to use in tag creation
        tagdef_name = create_tagdef_for_tests.get("name")

        request_data = get_request_data("create_tag.json", str_variable_dict, VARIABLE_JSONS_DIR)

        # Update the request data to use the created tagdef
        request_data["type"] = tagdef_name

        request_url = base_url + "/tags/tags"

        session_log.info(f"Creating tag with payload: {json.dumps(request_data)}")

        response = requests.post(
            request_url,
            verify=False,
            auth=admin_auth,
            headers=headers,
            data=json.dumps(request_data)
        )

        assert response.status_code in [200, 201,204], (
            f"Failed to create tag: expected 200/201, got {response.status_code}. Response: {response.text}"
        )

        response_json = response.json()
        assert "guid" in response_json, "Response JSON does not contain 'guid'"

        tag_guid = response_json["guid"]
        session_log.info(f"Tag created successfully with GUID: {tag_guid}")

    except Exception as e:
        session_log.error(f"Exception while creating tag: {str(e)}")
        raise

    yield response_json

    # --- TEARDOWN ---
    try:
        if tag_guid:
            delete_url = f"{base_url}/tags/guid/{tag_guid}"
            session_log.info(f"Deleting tag with GUID: {tag_guid}")

            delete_response = requests.delete(
                delete_url,
                verify=False,
                auth=admin_auth,
                headers=headers
            )

            assert delete_response.status_code in [200, 201, 204], (
                f"Expected 200/201/204 for delete, got {delete_response.status_code}. Response: {delete_response.text}"
            )

            session_log.info(f"Tag with GUID: {tag_guid} deleted successfully")

    except Exception as e:
        session_log.error(
            f"Exception while deleting tag with GUID: {tag_guid}. Error: {str(e)}"
        )
