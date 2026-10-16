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

"""
Ranger Admin REST helpers for migrating ranger-qe ``api_tests`` suites.

This module mirrors ``ranger-qe/ranger_test_utils/api_test_utils.py`` without
Beaver / XA dependencies. Migrated tests should import from here::

    from ranger_test_utils.api_test_utils import (
        base_url, admin_auth, headers, get_variable, get_request_data,
        compare_response_data, create_required_test_users, logger,
    )
    from ranger_test_utils.api_data import api_data_paths

Variable setup uses the same tuple spec as ranger-qe::

    ('asset_id', 'POST', '/assets/assets', 'asset_id.json', 'id')

Apache Ranger Docker functional-tests do not use CDH parcel version gates; port
tests as-is and drop ``Version`` / ``skipif`` from ranger-qe when migrating.

See ``api_tests/`` for a reference port of ``asset_rest_apis``.
"""

import copy
import json
import os
import random
import string

import requests
from requests.auth import HTTPBasicAuth

from ranger_test_utils.test_env import get_env, hdfs_repo_name, ranger_service_base_url
from ranger_test_utils.utils import get_test_logger
base_url = ranger_service_base_url()

admin_user = get_env("XA_ADMIN_USER", "admin")
keyadmin_user = get_env("XA_KEYADMIN_USER", "keyadmin")
admin_password = get_env("XA_ADMIN_PASSWORD", "rangerR0cks!")
keyadmin_password = get_env("XA_KEYADMIN_PASSWORD", "rangerR0cks!")

user_role_user = "user_role_test_user"
auditor_role_test_user = "auditor_role_test_user"
kmsauditor_role_test_user = "kmsauditor_role_test_user"

test_user_password = "Admin123"

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-XSRF-HEADER": "valid",
}

admin_auth = HTTPBasicAuth(admin_user, admin_password)
keyadmin_auth = HTTPBasicAuth(keyadmin_user, keyadmin_password)
user_auth = HTTPBasicAuth(user_role_user, test_user_password)
auditor_auth = HTTPBasicAuth(auditor_role_test_user, test_user_password)
kmsauditor_auth = HTTPBasicAuth(kmsauditor_role_test_user, test_user_password)

logger = get_test_logger(__name__)

def return_random_str(length=10):
    letters = string.ascii_lowercase + string.digits
    return "".join(random.choice(letters) for _ in range(length))


def compare_object(a, b):
    if type(a) != type(b):  # pylint: disable=unidiomatic-typecheck
        return False
    if isinstance(a, dict):
        return compare_dict(a, b)
    if isinstance(a, list):
        return compare_list(a, b)
    return a == b


def compare_dict(a, b):
    if len(a) != len(b):
        return False
    for k, v in a.items():
        if k not in b:
            return False
        if not compare_object(v, b[k]):
            return False
    return True


def compare_list(a, b):
    if len(a) != len(b):
        return False
    for i, _ in enumerate(a):
        if not compare_object(a[i], b[i]):
            return False
    return True


def get_ignore_fields_for_comparison():
    """Fields ignored when ``compare_response_data(..., complete_comparison=True)``."""
    return [
        "id",
        "guid",
        "createdBy",
        "updatedBy",
        "createTime",
        "updateTime",
        "version",
        "resourceSignature",
        "lastLoginTime",
    ]


def compare_response_data(response_data, expected_response_data, complete_comparison=False):
    logger.info("The response data is :- %s", response_data)
    logger.info("The expected response data is :- %s", expected_response_data)
    logger.info("Complete comparison = %s", complete_comparison)

    if complete_comparison:
        json_obj_1 = copy.deepcopy(response_data)
        json_obj_2 = copy.deepcopy(expected_response_data)

        for dict_key in json_obj_1:
            if dict_key in get_ignore_fields_for_comparison():
                json_obj_1[dict_key] = None
                json_obj_2[dict_key] = None

        for dict_key in json_obj_2:
            if dict_key in get_ignore_fields_for_comparison():
                json_obj_1[dict_key] = None
                json_obj_2[dict_key] = None

        assert compare_dict(json_obj_1, json_obj_2), (
            "Obtained response not matching the expected response"
        )
    else:
        for key in expected_response_data:
            logger.info("The key being compared is :- %s", key)
            assert response_data[key] == expected_response_data[key], (
                "Obtained response not matching expected response"
            )


def compare_response_data_without_assert(
    response_data, expected_response_data, complete_comparison=False
):
    logger.info("The response data is :- %s", response_data)
    logger.info("The expected response data is :- %s", expected_response_data)
    logger.info("Complete comparison = %s", complete_comparison)

    if complete_comparison:
        json_obj_1 = copy.deepcopy(response_data)
        json_obj_2 = copy.deepcopy(expected_response_data)

        for dict_key in json_obj_1:
            if dict_key in get_ignore_fields_for_comparison():
                json_obj_1[dict_key] = None
                json_obj_2[dict_key] = None

        for dict_key in json_obj_2:
            if dict_key in get_ignore_fields_for_comparison():
                json_obj_1[dict_key] = None
                json_obj_2[dict_key] = None

        return compare_dict(json_obj_1, json_obj_2)
    for key in expected_response_data:
        logger.info("The key being compared is :- %s", key)
        if response_data.get(key) != expected_response_data[key]:
            return False
    return True


def check_if_response_list_contains_expected_obj(response_list, expected_obj):
    is_expected_obj_found = False
    for json_obj in response_list:
        if compare_response_data_without_assert(json_obj, expected_obj, True):
            is_expected_obj_found = True
            break
    assert is_expected_obj_found, "Expected response obj not found"


def _delete_user_if_exists(username):
    request_url = base_url + "/xusers/users/userName/{}".format(username)
    resp = requests.get(request_url, auth=admin_auth, verify=False, headers=headers)
    if resp.status_code == 200:
        delete_url = request_url + "?forceDelete=true"
        resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        assert resp.status_code == 204, "Existing user deletion failed"


def create_required_test_users():
    """Create ``user_role_test_user`` and ``auditor_role_test_user`` for role API tests."""
    for username, role in (
        (user_role_user, "ROLE_USER"),
        (auditor_role_test_user, "ROLE_ADMIN_AUDITOR"),
    ):
        _delete_user_if_exists(username)

        request_payload = {
            "name": username,
            "firstName": username,
            "password": test_user_password,
            "description": username,
            "groupIdList": [],
            "groupNameList": [],
            "status": 1,
            "isVisible": 1,
            "userSource": 0,
            "userRoleList": [role],
        }

        request_url = base_url + "/xusers/secure/users"
        resp = requests.post(
            request_url,
            verify=False,
            auth=admin_auth,
            data=json.dumps(request_payload),
            headers=headers,
        )
        assert resp.status_code == 200, "Test user creation failed"


def create_kmsauditor_test_user():
    """Create ``kmsauditor_role_test_user`` with ``ROLE_KEY_ADMIN_AUDITOR`` (keyadmin promote)."""
    username = kmsauditor_role_test_user
    request_url = base_url + "/xusers/users/userName/{}".format(username)

    resp = requests.get(request_url, auth=admin_auth, verify=False, headers=headers)
    if resp.status_code == 200:
        delete_url = request_url + "?forceDelete=true"
        resp = requests.delete(delete_url, verify=False, auth=admin_auth, headers=headers)
        assert resp.status_code == 204, "Existing kmsauditor user deletion failed"

    request_payload = {
        "name": username,
        "firstName": username,
        "password": test_user_password,
        "description": username,
        "groupIdList": [],
        "groupNameList": [],
        "status": 1,
        "isVisible": 1,
        "userSource": 0,
        "userRoleList": ["ROLE_USER"],
    }

    request_url = base_url + "/xusers/secure/users"
    resp = requests.post(
        request_url,
        verify=False,
        auth=admin_auth,
        data=json.dumps(request_payload),
        headers=headers,
    )
    if resp.status_code != 200:
        request_url = base_url + "/xusers/users"
        resp = requests.post(
            request_url,
            verify=False,
            auth=admin_auth,
            data=json.dumps(request_payload),
            headers=headers,
        )

    assert resp.status_code == 200, (
        "KMS auditor user creation failed. Status: {}, Response: {}".format(
            resp.status_code, resp.text
        )
    )

    user_data = resp.json()
    user_id = user_data["id"]

    update_payload = {
        "id": user_id,
        "name": username,
        "firstName": username,
        "password": test_user_password,
        "description": username,
        "groupIdList": [],
        "groupNameList": [],
        "status": 1,
        "isVisible": 1,
        "userSource": 0,
        "userRoleList": ["ROLE_KEY_ADMIN_AUDITOR"],
    }

    update_url = base_url + "/xusers/secure/users/{}".format(user_id)
    resp = requests.put(
        update_url,
        verify=False,
        auth=keyadmin_auth,
        data=json.dumps(update_payload),
        headers=headers,
    )
    if resp.status_code != 200:
        update_url = base_url + "/xusers/users/{}".format(user_id)
        resp = requests.put(
            update_url,
            verify=False,
            auth=keyadmin_auth,
            data=json.dumps(update_payload),
            headers=headers,
        )

    assert resp.status_code == 200, (
        "KMS auditor user role update failed. Status: {}, Response: {}".format(
            resp.status_code, resp.text
        )
    )


def get_variable(
    variable_specification, variable_dict, data_folder_path, is_keyadmin=False
):
    user = keyadmin_user if is_keyadmin else admin_user
    password = keyadmin_password if is_keyadmin else admin_password
    auth = HTTPBasicAuth(user, password)

    variable_name = variable_specification[0]
    request_method = variable_specification[1]

    request_url = base_url + variable_specification[2]
    request_url = request_url.format(**variable_dict)

    response_to_be_saved = variable_specification[4]
    request_payload = None

    if variable_specification[3] is not None:
        request_data_file_path = os.path.join(data_folder_path, variable_specification[3])
        logger.info("The file path is :- %s", request_data_file_path)

        with open(request_data_file_path, "r", encoding="utf-8") as fp:
            request_payload = fp.read()
            request_payload = request_payload.replace("{random_str}", return_random_str(7))

            for key in variable_dict:
                regex = "{" + str(key) + "}"
                if regex in request_payload:
                    request_payload = request_payload.replace(regex, str(variable_dict[key]))

    if request_method == "POST":
        resp = requests.post(
            request_url, data=request_payload, verify=False, auth=auth, headers=headers
        )
    elif request_method == "GET":
        resp = requests.get(request_url, verify=False, auth=auth, headers=headers)
    else:
        raise ValueError("Unsupported HTTP method: {}".format(request_method))

    logger.info("response status: %s, response body: %s", resp.status_code, resp.content)

    assert resp.status_code in (200, 201), (
        "get_variable failed for {}: {} {}".format(
            variable_name, resp.status_code, resp.text
        )
    )

    if response_to_be_saved == "same":
        variable = resp.json()
    else:
        dict_path = response_to_be_saved.split(",")
        variable = resp.json()
        for key in dict_path:
            if isinstance(variable, list):
                key = int(key)
            variable = variable[key]

    logger.info("The variable %s is %s", variable_name, variable)
    return variable


def get_request_data(file_name, variable_dict, test_data_path):
    file_path = os.path.join(test_data_path, file_name)

    with open(file_path, "r", encoding="utf-8") as fp:
        request_payload = fp.read()
        request_payload = request_payload.replace("{random_str}", return_random_str(7))

        for key in variable_dict:
            regex = "{" + str(key) + "}"
            if regex in request_payload:
                request_payload = request_payload.replace(regex, variable_dict[key])

    return json.loads(request_payload)


def get_updated_request_data(request_data, fields_to_update=None, field_to_del=None):
    request_payload = copy.deepcopy(request_data)
    fields_to_update = json.dumps(fields_to_update)
    fields_to_update = fields_to_update.replace("{random_str}", return_random_str(7))
    fields_to_update = json.loads(fields_to_update)

    if fields_to_update:
        logger.info("The fields to update are :- %s", fields_to_update)
        for key in fields_to_update:
            request_payload[key] = fields_to_update[key]

    if field_to_del:
        logger.info("The field to del is :- %s", field_to_del)
        del request_payload[field_to_del]

    return request_payload


def get_expected_response_data(expected_response_data_str, variable_dict):
    for key in variable_dict:
        regex = "{" + str(key) + "}"
        if regex in expected_response_data_str:
            expected_response_data_str = expected_response_data_str.replace(
                regex, variable_dict[key]
            )
    return expected_response_data_str


def create_custom_user(
    user_name,
    first_name=None,
    last_name=None,
    email=None,
    password=None,
    user_roles=None,
):
    if first_name is None:
        first_name = user_name
    if last_name is None:
        last_name = "{}_lstname".format(user_name)
    if email is None:
        email = "{}@test.com".format(user_name)
    if password is None:
        password = test_user_password
    if user_roles is None:
        user_roles = ["ROLE_USER"]

    user_data = {
        "name": user_name,
        "firstName": first_name,
        "lastName": last_name,
        "emailAddress": email,
        "password": password,
        "userRoleList": user_roles,
        "groupIdList": [],
        "groupNameList": [],
        "status": 1,
        "isVisible": 1,
        "userSource": 0,
    }

    response = requests.post(
        "{}/xusers/secure/users".format(base_url),
        json=user_data,
        auth=admin_auth,
        headers=headers,
        verify=False,
    )

    if response.status_code in (200, 201):
        logger.info("Successfully created user: %s", user_name)
        return response.json()
    logger.error(
        "Failed to create user %s: %s - %s",
        user_name,
        response.status_code,
        response.text,
    )
    return None


def create_custom_group(group_name, description=None, group_type=1):
    if description is None:
        description = "Test group {} for testing".format(group_name)

    group_data = {
        "name": group_name,
        "description": description,
        "groupType": group_type,
    }

    response = requests.post(
        "{}/xusers/secure/groups".format(base_url),
        json=group_data,
        auth=admin_auth,
        headers=headers,
        verify=False,
    )

    if response.status_code in (200, 201):
        logger.info("Successfully created group: %s", group_name)
        return response.json()
    logger.error(
        "Failed to create group %s: %s - %s",
        group_name,
        response.status_code,
        response.text,
    )
    return None


def create_custom_role(role_name, description=None, users=None, groups=None, roles=None):
    if description is None:
        description = "Test role {} for testing".format(role_name)
    if users is None:
        users = []
    if groups is None:
        groups = []
    if roles is None:
        roles = []

    role_data = {
        "name": role_name,
        "description": description,
        "users": users,
        "groups": groups,
        "roles": roles,
    }

    response = requests.post(
        "{}/roles/roles".format(base_url),
        json=role_data,
        auth=admin_auth,
        headers=headers,
        verify=False,
    )

    if response.status_code in (200, 201):
        logger.info("Successfully created role: %s", role_name)
        return response.json()
    logger.error(
        "Failed to create role %s: %s - %s",
        role_name,
        response.status_code,
        response.text,
    )
    return None


# Re-export for setup_module blocks that used getEnv / repo name helpers.
getEnv = get_env  # pylint: disable=invalid-name

__all__ = [
    "admin_auth",
    "auditor_auth",
    "base_url",
    "check_if_response_list_contains_expected_obj",
    "compare_response_data",
    "compare_response_data_without_assert",
    "create_custom_group",
    "create_custom_role",
    "create_custom_user",
    "create_kmsauditor_test_user",
    "create_required_test_users",
    "getEnv",
    "get_expected_response_data",
    "get_request_data",
    "get_updated_request_data",
    "get_variable",
    "headers",
    "hdfs_repo_name",
    "keyadmin_auth",
    "kmsauditor_auth",
    "logger",
    "return_random_str",
    "test_user_password",
    "user_auth",
    "user_role_user",
    "auditor_role_test_user",
    "kmsauditor_role_test_user",
]
