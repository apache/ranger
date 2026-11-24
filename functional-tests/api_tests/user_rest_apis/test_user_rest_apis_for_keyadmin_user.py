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

import requests

from ranger_test_utils.api_data import api_data_paths
from ranger_test_utils.api_test_utils import (
    base_url,
    get_request_data,
    get_updated_request_data,
    get_variable,
    headers,
    keyadmin_auth,
    logger,
)

variable_dict = {}
str_variable_dict = {}
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


def test_get_users_by_keyadmin():
    request_url = base_url + "/users"
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_get_user_profile_by_keyadmin():
    request_url = base_url + "/users/profile"
    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_get_user_using_id_by_keyadmin():
    request_url = base_url + "/users/{user_1_id}"
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.get(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_create_user_by_keyadmin():
    request_url = base_url + "/users"
    request_data = get_request_data("test_create_user.json", str_variable_dict, test_data_path)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=keyadmin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 403, "Expected status code not returned"


def test_create_default_user_by_keyadmin():
    request_url = base_url + "/users/default"
    request_data = get_request_data(
        "test_create_default_user.json", str_variable_dict, test_data_path
    )
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=keyadmin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 403, "Expected status code not returned"


def test_edit_user_by_keyadmin():
    request_url = base_url + "/users"
    request_data = variable_dict["user_1"]
    fields_to_update = {"firstName": "{random_str}"}
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.put(
        request_url, verify=False, auth=keyadmin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_edit_user_using_id_by_keyadmin():
    request_url = base_url + "/users/{user_1_id}/roles"
    request_url = request_url.format(**str_variable_dict)

    request_data = get_request_data("test_edit_user_using_id.json", str_variable_dict, test_data_path)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.put(
        request_url, verify=False, auth=keyadmin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_change_email_by_keyadmin():
    request_url = base_url + "/users/{user_1_id}/emailchange"
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["user_1"]
    fields_to_update = {
        "oldPassword": "Password@123456789#!",
        "emailAddress": "test_user_{random_str}_2@abc.com",
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=keyadmin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_change_password_by_keyadmin():
    request_url = base_url + "/users/{user_1_id}/passwordchange"
    request_url = request_url.format(**str_variable_dict)

    request_data = variable_dict["user_1"]
    fields_to_update = {
        "oldPassword": "Password@123456789#!",
        "updPassword": "Password@123456789#!56",
    }
    request_data = get_updated_request_data(request_data=request_data, fields_to_update=fields_to_update)
    logger.info("The request url is :- %s", request_url)
    logger.info("The request data is :- %s", request_data)
    resp = requests.post(
        request_url, verify=False, auth=keyadmin_auth, headers=headers, data=json.dumps(request_data)
    )
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 200, "Expected status code not returned"


def test_deactivate_user_by_keyadmin():
    request_url = base_url + "/users/{user_1_id}/deactivate"
    request_url = request_url.format(**str_variable_dict)

    logger.info("The request url is :- %s", request_url)
    resp = requests.post(request_url, verify=False, auth=keyadmin_auth, headers=headers)
    logger.info("The resp status code is :- %s", resp.status_code)
    logger.info("The resp content is :- %s", resp.content)
    assert resp.status_code == 403, "Expected status code not returned"
