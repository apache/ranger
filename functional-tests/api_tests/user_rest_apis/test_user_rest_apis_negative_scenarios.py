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
import random

import requests

from ranger_test_utils.api_data import api_data_paths
from ranger_test_utils.api_test_utils import (
    admin_auth,
    base_url,
    get_request_data,
    get_updated_request_data,
    get_variable,
    headers,
    logger,
    return_random_str,
)

variable_dict = {}
str_variable_dict = {}
variable_dict["random_num"] = random.randint(100000, 1000000)
variable_dict["random_str"] = return_random_str(7)
str_variable_dict["random_num"] = str(variable_dict["random_num"])
str_variable_dict["random_str"] = str(variable_dict["random_str"])
_data_paths = api_data_paths(__file__, "user_rest_apis", layout="ranger_qe")
variables_data_path = _data_paths["variables_data_path"]
test_data_path = _data_paths["test_data_path"]


def setup_module():
    variable_specifier_list = [
        ("user_1_id", "POST", "/users", "user_1_id.json", "id"),
        ("user_1", "GET", "/users/{user_1_id}", None, "same"),
    ]

    for variable_specification in variable_specifier_list:
        variable_name = variable_specification[0]
        variable_dict[variable_name] = get_variable(
            variable_specification, str_variable_dict, variables_data_path
        )
        str_variable_dict[variable_name] = str(variable_dict[variable_name])


def test_get_user_by_invalid_id():
    request_url = base_url + "/users/{random_num}"
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 204, "Expected status code not returned"


def test_create_user_with_invalid_payload():
    request_url = base_url + "/users"
    request_data = get_request_data(
        "test_create_user_with_invalid_payload.json", str_variable_dict, test_data_path
    )
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, "Expected status code not returned"


def test_create_default_user_with_invalid_payload():
    request_url = base_url + "/users/default"
    request_data = get_request_data(
        "test_create_default_user_with_invalid_payload.json", str_variable_dict, test_data_path
    )
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 204, "Expected status code not returned"


def test_edit_user_with_invalid_payload():
    request_url = base_url + "/users"
    request_data = variable_dict["user_1"]
    fields_to_update = {"firstName": "{random_str}"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    field_to_del = "loginId"
    request_data = get_updated_request_data(request_data=request_data, field_to_del=field_to_del)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.put(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, "Expected status code not returned"


def test_edit_user_role_with_invalid_id():
    request_url = base_url + "/users/{user_1_id}/roles"
    request_url = request_url.format(**str_variable_dict)

    request_data = get_request_data(
        "test_edit_user_role_with_invalid_id.json", str_variable_dict, test_data_path
    )
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.put(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 403, "Expected status code not returned"


def test_emailchange_with_invalid_payload():
    request_url = base_url + "/users/{user_1_id}/emailchange"
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["user_1"]
    fields_to_update = {"oldPassword": "{random_str}"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, "Expected status code not returned"


def test_change_email_with_null_body():
    """Verify that null request body throws an error when attempting email change."""
    request_url = base_url + "/users/{user_1_id}/emailchange"
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data="{}")
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, f"Expected 400 Bad Request, but got {resp.status_code}"


def test_change_email_with_empty_loginId():
    """Verify that empty loginId is rejected when attempting email change."""
    request_url = base_url + "/users/{user_1_id}/emailchange"
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["user_1"]
    fields_to_update = {"loginId": "", "emailAddress": "newemail_{random_str}@example.com"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, f"Expected 400 Bad Request, but got {resp.status_code}"


def test_change_email_with_mismatched_userId():
    """Verify mismatched userId between path parameter and request body is rejected."""
    request_url = base_url + "/users/{user_1_id}/emailchange"
    request_url = request_url.format(**str_variable_dict)
    mismatched_id = 999999

    request_data = variable_dict["user_1"]
    fields_to_update = {"id": mismatched_id, "emailAddress": "newemail_{random_str}@example.com"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data with mismatched ID is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, f"Expected 400 Bad Request for ID mismatch, but got {resp.status_code}"


def test_change_email_with_nonexistent_loginId():
    """Verify that non-existent loginId is rejected when attempting email change."""
    request_url = base_url + "/users/{user_1_id}/emailchange"
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["user_1"]
    fields_to_update = {
        "loginId": "nonexistent_user_{random_str}",
        "emailAddress": "newemail_{random_str}@example.com",
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data with non-existent loginId is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert (
        resp.status_code == 400
    ), f"Expected 400 Bad Request for non-existent loginId, but got {resp.status_code}"


def test_password_change_with_invalid_payload():
    request_url = base_url + "/users/{user_1_id}/passwordchange"
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["user_1"]
    fields_to_update = {"oldPassword": "Password@123"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, "Expected status code not returned"


def test_deactivate_user_with_invalid_id():
    request_url = base_url + "/users/{random_num}/deactivate"
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, "Expected status code not returned"


def test_update_user_with_non_existent_id():
    """Verify PUT to update a user profile with a userId that does not exist."""
    request_url = base_url + "/users"

    request_data = {
        "id": 999999,
        "loginId": "admin",
        "status": 1,
        "firstName": "Admin",
        "lastName": "",
        "publicScreenName": "Admin",
        "userSource": 0,
        "userRoleList": ["ROLE_SYS_ADMIN"],
        "userPermList": [],
        "groupPermissions": [],
    }

    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.put(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 403, "Expected status code 403 Forbidden not returned"


def test_get_user_profile_without_authentication():
    """Verify getUserProfile() without authentication credentials."""
    request_url = base_url + "/users/profile"
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 401, f"Expected 401 Unauthorized, but got {resp.status_code}"


def test_change_password_with_null_body():
    """Verify that null request body throws an error when attempting password change."""
    request_url = base_url + "/users/{user_1_id}/passwordchange"
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.post(request_url, verify=False, auth=admin_auth, headers=headers, data="{}")
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, f"Expected 400 Bad Request, but got {resp.status_code}"


def test_change_password_with_empty_loginId():
    """Verify that empty loginId is rejected when attempting password change."""
    request_url = base_url + "/users/{user_1_id}/passwordchange"
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["user_1"]
    fields_to_update = {
        "loginId": "",
        "oldPassword": "Password@123456789#!",
        "updPassword": "Password@123456789#!56",
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, f"Expected 400 Bad Request, but got {resp.status_code}"


def test_change_password_with_mismatched_userId():
    """Verify mismatched userId between path parameter and request body is rejected."""
    request_url = base_url + "/users/{user_1_id}/passwordchange"
    request_url = request_url.format(**str_variable_dict)
    mismatched_id = 999999

    request_data = variable_dict["user_1"]
    fields_to_update = {
        "id": mismatched_id,
        "oldPassword": "Password@123456789#!",
        "updPassword": "Password@123456789#!56",
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data with mismatched ID is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 400, f"Expected 400 Bad Request for ID mismatch, but got {resp.status_code}"


def test_change_password_with_nonexistent_loginId():
    """Verify that non-existent loginId is rejected when attempting password change."""
    request_url = base_url + "/users/{user_1_id}/passwordchange"
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["user_1"]
    fields_to_update = {
        "loginId": "nonexistent_user_{random_str}",
        "oldPassword": "Password@123456789#!",
        "updPassword": "Password@123456789#!56",
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data with non-existent loginId is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=admin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert (
        resp.status_code == 400
    ), f"Expected 400 Bad Request for non-existent loginId, but got {resp.status_code}"
