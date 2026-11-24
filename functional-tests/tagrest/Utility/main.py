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


import copy
import os
import json
import random
import string
from requests.auth import HTTPBasicAuth

base_url="http://localhost:6080/service"
str_variable_dict = {}
admin_auth = HTTPBasicAuth('admin','rangerR0cks!')
keyadmin_auth = HTTPBasicAuth('keyadmin','rangerR0cks!')
global_dict={}
headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'X-XSRF-HEADER': 'valid'
    }


def return_random_str(length=10):
    """Generates a random string of fixed length """
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(length))


def get_request_data(file_name, variable_dictionary, test_data_path):
    file_path = os.path.join(test_data_path, file_name)

    with open(file_path, 'r', encoding='utf-8') as fp:
        request_payload = fp.read()
        request_payload = request_payload.replace('{random_str}', return_random_str(7))

        for key in variable_dictionary:
            regex = '{' + str(key) + '}'
            if regex in request_payload:
                request_payload = request_payload.replace(regex, variable_dictionary[key])

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

