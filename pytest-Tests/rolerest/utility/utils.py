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


import json
import uuid
import pytest
import requests
from datetime import datetime
import inspect
import subprocess
import xuserrest.utility.utils as xutils


BASE_URL = "http://localhost:6080/service"
AUTH = ("admin", "rangerR0cks!")
HEADERS = {"Content-Type": "application/json"}

RANGER_CONTAINER_NAME = "ranger"
RANGER_LOG_FILE = "/var/log/ranger/ranger-admin-ranger.rangernw-.log"

BIGINT_MIN = -9223372036854775808
BIGINT_MAX = 9223372036854775807

SERVICE_NAME = "admin"

RANGER_CONFIG = None
RANGER_KEY_ADMIN_CONFIG = None
RANGER_AUDITOR_CONFIG = None
RANGER_USER_CONFIG = None


def init_configs(ranger_config, ranger_key_admin_config, ranger_auditor_config, ranger_user_config):
    global RANGER_CONFIG, RANGER_KEY_ADMIN_CONFIG, RANGER_AUDITOR_CONFIG, RANGER_USER_CONFIG

    RANGER_CONFIG = ranger_config
    RANGER_KEY_ADMIN_CONFIG = ranger_key_admin_config
    RANGER_AUDITOR_CONFIG = ranger_auditor_config
    RANGER_USER_CONFIG = ranger_user_config




def create_service():
    
    unique_name = f"test_service_{uuid.uuid4().hex[:8]}"
    # Create service
    service_payload = {
        "name": unique_name,
        "displayName": unique_name,
        "type": "hdfs",
        "isEnabled": True,
        "configs": {
            "username": "hdfs",
            "password": "hdfs",
            "fs.default.name": "hdfs://localhost:9000",
            "hadoop.security.authentication": "simple",
            "hadoop.security.authorization": "true",
            "policy.download.auth.users": "hdfs",
            "tag.download.auth.users": "hdfs",
            "userstore.download.auth.users": "hdfs"
        }
    }
    svc_resp = requests.post(
        f"{BASE_URL}/plugins/services",
        auth=AUTH,
        headers=HEADERS,
        data=json.dumps(service_payload)
    )
    svc_resp.raise_for_status()
    service = svc_resp.json()
    service_id = service["id"]
    print(f"[+] Service created: name={service['name']}, id={service_id}")


    return service, service_id


def assign_service_admin(service_id, service, username):
    configs = service.get("configs", {})
    
    # Ranger stores service admins as a comma-separated string in configs
    existing_admins = configs.get("service.admin.users", "")
    admin_set = set(filter(None, existing_admins.split(",")))
    admin_set.add(username)
    
    configs["service.admin.users"] = ",".join(admin_set)

    update_payload = {
        "id": service_id,
        "name": service["name"],
        "displayName": service.get("displayName", service["name"]),
        "type": service["type"],
        "isEnabled": service.get("isEnabled", True),
        "configs": configs
    }
    
    resp = requests.put(
        f"{BASE_URL}/plugins/services/{service_id}",
        auth=AUTH,
        headers=HEADERS,
        json=update_payload 
    )
    resp.raise_for_status()
    print(f"[+] User '{username}' added as service admin for service id={service_id}")


def delete_service(service_id):
    resp = requests.delete(
        f"{BASE_URL}/plugins/services/{service_id}",
        auth=AUTH,
        headers=HEADERS
    )
    resp.raise_for_status()
    print(f"[+] Service deleted: id={service_id}")



def assert_response(response, expected_status, text = None, service_name=SERVICE_NAME):
    xutils.assert_response(response, expected_status, text, service_name)


# Remove Content-Type for DELETE requests
def delete_role(role_id):
    resp = requests.delete(
        f"{BASE_URL}/roles/roles/{role_id}",
        auth=AUTH,
        headers={"Accept": "application/json"}  # Changed from string to dictionary
    )
    #assert_response(resp, [200, 204], f"Failed to delete role id={role_id}: {resp.text}")
    assert resp.status_code in [200, 204], f"Failed to delete role id={role_id}: {resp.text}"
    print(f" /n /n [+] Role deleted successfully with id={role_id}. Response status: {resp.status_code}. Response text: {resp.text} /n /n")
