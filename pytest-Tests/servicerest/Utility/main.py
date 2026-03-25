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


import copy
import os
base_url="http://localhost:6080/service"
import json
import random
import string
import requests
from requests.auth import HTTPBasicAuth
from Utility.Helper_Directory.Helping_Functions import getEnv , Version
admin_user = getEnv('XA_ADMIN_USER', 'admin')
keyadmin_user = getEnv('XA_KEYADMIN_USER', 'keyadmin')
keyadmin_password = getEnv('XA_KEYADMIN_PASSWORD', 'rangerR0cks!')
admin_password = getEnv('XA_ADMIN_PASSWORD', 'rangerR0cks!')

is_version_7_3_2 = Version.current_cdh_parcel_version() >= Version.of("7.3.2.0")

headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'X-XSRF-HEADER': 'valid'
    }


def return_random_str(length=7):
    """Generates a random string of fixed length """
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(length))

def get_request_data(file_name, variable_dict, test_data_path):
    file_path = os.path.join(test_data_path, file_name)

    with open(file_path, 'r', encoding='utf-8') as fp:
        request_payload = fp.read()
        request_payload = request_payload.replace('{random_str}', return_random_str(7))

        for key in variable_dict:
            regex = '{' + str(key) + '}'
            if regex in request_payload:
                request_payload = request_payload.replace(regex, variable_dict[key])

    return json.loads(request_payload)
def get_updated_request_data(request_data, fields_to_update=None, field_to_del=None):
    request_payload = copy.deepcopy(request_data)
    fields_to_update = json.dumps(fields_to_update)
    fields_to_update = fields_to_update.replace('{random_str}', return_random_str(7))
    fields_to_update = json.loads(fields_to_update)

    if fields_to_update:
        # logger.info('The fields to update are :- %s', fields_to_update)

        for key in fields_to_update:
            request_payload[key] = fields_to_update[key]

    if field_to_del:
        # logger.info("The field to del is :- %s", field_to_del)

        del request_payload[field_to_del]

    return request_payload


def get_variable(variable_specification, variable_dict, data_folder_path, is_keyadmin=False):
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
        # logger.info("The file path is :- %s", request_data_file_path)

        with open(request_data_file_path, 'r', encoding='utf-8') as fp:
            request_payload = fp.read()
            request_payload = request_payload.replace('{random_str}', return_random_str(7))

            for key in variable_dict:
                regex = '{' + str(key) + '}'
                if regex in request_payload:
                    request_payload = request_payload.replace(regex, variable_dict[key])

    if request_method == "POST":
        resp = requests.post(request_url, data=request_payload, verify=False, auth=auth, headers=headers)
    elif request_method == "GET":
        resp = requests.get(request_url, verify=False, auth=auth, headers=headers)

    # XALogger.logInfo(f"response status: {resp.status_code}, response body: {str(resp.content)}")

    if response_to_be_saved == "same":
        variable = resp.json()
    else:
        dict_path = response_to_be_saved.split(",")
        variable = resp.json()

        for key in dict_path:
            if isinstance(variable, type([])):
                key = int(key)
            variable = variable[key]

    # logger.info("The variable %s is %s :- ", variable_name, variable")

    return variable


def compare_list(a, b):
    if len(a) != len(b):
        return False
    else:
        for i, _ in enumerate(a):
            if not compare_object(a[i], b[i]):
                return False
    return True

def compare_object(a, b):
    if type(a) != type(b):  # pylint: disable=unidiomatic-typecheck
        return False
    elif isinstance(a, dict):
        return compare_dict(a, b)
    elif isinstance(a, list):
        return compare_list(a, b)
    else:
        return a == b

def compare_dict(a, b):
    if len(a) != len(b):
        return False
    else:
        for k, v in a.items():
            if k not in b:
                return False
            else:
                if not compare_object(v, b[k]):
                    return False
    return True

def get_ignore_fields_for_comparison():
    if is_version_7_3_2:
        base_fields = ["id", "guid", "createdBy", "updatedBy", "createTime",
                            "updateTime", "version", "resourceSignature","lastLoginTime"]
    else:
        base_fields = ["id", "guid", "createdBy", "updatedBy", "createTime",
                            "updateTime", "version", "resourceSignature"]
    return base_fields

def compare_response_data(response_data, expected_response_data, complete_comparison=False):
    # logger.info("The response data is :- %s", response_data)
    # logger.info("The expected response data is :- %s", expected_response_data)
    # logger.info("Complete comparison = %s", complete_comparison)

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

        assert compare_dict(json_obj_1, json_obj_2), "Obtained response not matching the expected response"
    else:
        for key in expected_response_data:
            # logger.info("The key being compared is :- %s", key)
            assert response_data[key] == expected_response_data[key], \
                "Obtained response not matching expected response"

