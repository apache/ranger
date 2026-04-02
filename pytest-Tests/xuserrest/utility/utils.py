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
import pytest
import requests
from datetime import datetime
import inspect
import subprocess


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


def assert_response(response, expected_status, text = None, service_name=SERVICE_NAME):

    actual_status = response.status_code

    if isinstance(expected_status, int):
        valid = actual_status == expected_status

    elif isinstance(expected_status, (list, tuple, set)):
        valid = actual_status in expected_status

    else:
        raise TypeError(
            f"expected_status must be int or list/tuple/set, "
            f"got {type(expected_status)}"
        )

    if not valid:
        logs = fetch_ranger_logs(actual_status)

        pytest.fail(
                
            f"\nExpected: {expected_status}"
            f"\nActual: {actual_status}"
            + (f"\n{text}" if text is not None else "")
            + f"\nLogs:\n{logs}"
        )


def validate_user_schema(data):

    assert "id" in data
    assert isinstance(data["id"], int)
    assert BIGINT_MIN <= data["id"] <= BIGINT_MAX

    assert isinstance(data.get("name"), str)
    assert 1 <= len(data["name"]) <= 767

    if data.get("firstName"):
        assert isinstance(data.get("firstName"), str)
        assert len(data["firstName"]) <= 256

    if data.get("emailAddress"):
        assert isinstance(data["emailAddress"], str)
        assert len(data["emailAddress"]) <= 512

    if "status" in data:
        assert data.get("status") in [0, 1]
    
    if "isVisible" in data:
        assert data.get("isVisible") in [0, 1]

    if "password" in data:
        assert data.get("password") == "*****"

    for field in ["createDate", "updateDate"]:
        if field in data:
            datetime.fromisoformat(
                data[field].replace("Z", "+00:00")
            )
    
    roles = data.get("userRoleList")
    assert isinstance(roles, list)
    assert len(roles) > 0

    for role in roles:
        assert isinstance(role, str)
        assert len(role) <= 255

    group_names = data.get("groupNameList", [])
    group_ids = data.get("groupIdList", [])

    for name in group_names:
        assert isinstance(name, str)
        assert len(name) <= 767

    for gid in group_ids:
        assert isinstance(gid, int)
        assert BIGINT_MIN <= gid <= BIGINT_MAX

    assert len(group_names) == len(group_ids), \
        "Mismatch between groupNameList and groupIdList"

    sync_source = data.get("syncSource")
    if sync_source:
        assert isinstance(sync_source, str)

    other_attributes = data.get("otherAttributes")
    if other_attributes:
        parsed = json.loads(other_attributes)
        if "sync_source" in parsed and sync_source:
            assert parsed["sync_source"] == sync_source

    for field in ["owner", "updatedBy"]:
        if data.get(field):
            assert isinstance(data[field], str)
            assert len(data[field]) <= 256



def validate_secure_user_schema(data):

    assert "id" in data
    assert isinstance(data["id"], int)
    assert BIGINT_MIN <= data["id"] <= BIGINT_MAX

    assert isinstance(data.get("name"), str)
    assert 1 <= len(data["name"]) <= 767

    assert isinstance(data.get("firstName"), str)
    assert len(data["firstName"]) <= 256

    if data.get("emailAddress"):
        assert isinstance(data["emailAddress"], str)
        assert len(data["emailAddress"]) <= 512

    assert data.get("status") in [0, 1]
    assert data.get("isVisible") in [0, 1]

    assert data.get("password") == "*****"

    for field in ["createDate", "updateDate"]:
        if field in data:
            datetime.fromisoformat(
                data[field].replace("Z", "+00:00")
            )

    roles = data.get("userRoleList")
    assert isinstance(roles, list)
    assert len(roles) > 0

    for role in roles:
        assert isinstance(role, str)
        assert len(role) <= 255

    group_names = data.get("groupNameList", [])
    group_ids = data.get("groupIdList", [])

    for name in group_names:
        assert isinstance(name, str)
        assert len(name) <= 767

    for gid in group_ids:
        assert isinstance(gid, int)
        assert BIGINT_MIN <= gid <= BIGINT_MAX

    assert len(group_names) == len(group_ids), \
        "Mismatch between groupNameList and groupIdList"

    sync_source = data.get("syncSource")
    if sync_source:
        assert isinstance(sync_source, str)

    other_attributes = data.get("otherAttributes")
    if other_attributes:
        parsed = json.loads(other_attributes)
        if "sync_source" in parsed and sync_source:
            assert parsed["sync_source"] == sync_source

    for field in ["owner", "updatedBy"]:
        if data.get(field):
            assert isinstance(data[field], str)
            assert len(data[field]) <= 256



def user_exists(user_id, ranger_admin_config, base_url, headers, auth=None):
    RANGER_CONFIG = {"base_url": base_url, "auth": ranger_admin_config, "headers": headers}
    if RANGER_CONFIG is None:
        raise RuntimeError("RANGER_CONFIG not initialized")

    if auth is None:
        auth = RANGER_CONFIG["auth"]

    response = requests.get(
        f"{RANGER_CONFIG['base_url']}/xusers/secure/users/{user_id}",
        auth=auth,
        headers=RANGER_CONFIG["headers"]
    )

    return response.status_code == 200



def delete_user(user_id,  ranger_admin_config, base_url, headers, force=False):

    RANGER_CONFIG = {"base_url": base_url, "auth": ranger_admin_config, "headers": headers}
    if RANGER_CONFIG is None:
        raise RuntimeError("RANGER_CONFIG not initialized")

    if force:
        url = f"{RANGER_CONFIG['base_url']}/xusers/secure/users/id/{user_id}"
        params = {"forceDelete": "true"}
    else:
        url = f"{RANGER_CONFIG['base_url']}/xusers/secure/users/{user_id}"
        params = None

    response = requests.delete(
        url,
        auth=RANGER_CONFIG["auth"],
        headers={
            **RANGER_CONFIG["headers"],
            "X-Requested-By": "ranger"
        },
        params=params
    )

    print("DELETE:", response.status_code, response.text)

    return response.status_code in (200, 204)


def validate_external_user_schema(data):

    assert "startIndex" in data and isinstance(data["startIndex"], int)
    assert "pageSize" in data and isinstance(data["pageSize"], int)
    assert "totalCount" in data and isinstance(data["totalCount"], int)
    assert "resultSize" in data and isinstance(data["resultSize"], int)

    assert "vXStrings" in data
    assert isinstance(data["vXStrings"], list)
    assert len(data["vXStrings"]) > 0

    assert "value" in data["vXStrings"][0]
    assert isinstance(data["vXStrings"][0]["value"], str)
    assert len(data["vXStrings"][0]["value"]) <= 255

    assert "queryTimeMS" in data and isinstance(data["queryTimeMS"], int)

def validate_xgroup_schema(data):

    assert "id" in data
    assert isinstance(data["id"], int)
    assert BIGINT_MIN <= data["id"] <= BIGINT_MAX

    assert isinstance(data.get("name"), str)
    assert 1 <= len(data["name"]) <= 767

    if data.get("description"):
        assert isinstance(data["description"], str)
        assert len(data["description"]) <= 1024

    if "groupType" in data:
        assert data.get("groupType") in [0, 1]

    if "isVisible" in data:
        assert data.get("isVisible") in [0, 1]
    
    if "groupSource" in data:
        assert data.get("groupSource") in [0, 1]
        
    for field in ["createDate", "updateDate"]:
        if field in data:
            datetime.fromisoformat(
                data[field].replace("Z", "+00:00")
            )


def assign_groups_to_user(user_name, group_names, ranger_admin_config, base_url, headers):
    RANGER_CONFIG = {"base_url": base_url, "auth": ranger_admin_config, "headers": headers}
    if RANGER_CONFIG is None:
        raise RuntimeError("RANGER_CONFIG not initialized")

    payload = {
            "xuserInfo": {
                    "name": user_name
                },
            "xgroupInfo": [
                {"name": group} for group in group_names
                ]
        }
    response = requests.post(
            f"{RANGER_CONFIG['base_url']}/xusers/users/userinfo",
            json=payload,
            auth=RANGER_CONFIG["auth"],
            headers=RANGER_CONFIG["headers"]
        )
    print("Assign Groups Response:", response.status_code)
    assert response.status_code == 200, f"Failed to assign groups to user: {response.text}"
    

    

def fetch_ranger_logs(resp_code=None, lines: int = 80) -> str:
    """
    Fetch useful Ranger debug logs.

    Sections:
      1. Recent Activity
      2. Errors / Warnings (system health)
      3. Recent REST API calls
      4. Logs related to HTTP response meaning
    """

    keyword_map = {
    # SUCCESS
    200: (r"createUser|updateUser|deleteUser|added|updated|removed|success|create|update|delete|assigned|completed"),
    201: (r"created|create|success"),
    204: (r"delete|removed|no content|success"),

    # FOR CLIENT ERRORS, focus on common patterns and real log snippets
    400: (r"invalid|validation|bad request|missing"),

    # AUTHENTICATION

    401: (r"authentication|login failed|Bad credentials|Invalid username|unauthorized"),
    # AUTHORIZATION (REAL RANGER STRINGS)
    403: (r"User is not allowed to access the API|Access denied|not authorized|RESTErrorUtil|Request failed |denied|forbidden|permission|not allowed"),

    # NOT FOUND
    404: (r"not found|No record"),

    # METHOD
    405: (r"Request method.*not supported|method not allowed|unsupported method"),

    # CONFLICT
    409: (r"already exists|duplicate|conflict"),

    # SERVER
    500: (r"ERROR|Exception|failed|NullPointerException"),

    502: (r"gateway|proxy|upstream"),
    503: (r"unavailable|timeout|overloaded")
    }

    resp_filter = r"ERROR|WARN|Exception|denied|failed"

    if isinstance(resp_code, int):
        resp_filter = keyword_map.get(resp_code, resp_filter)

    elif isinstance(resp_code, (list, tuple, set)):
        filters = [
            keyword_map.get(code, resp_filter)
            for code in resp_code
        ]
        resp_filter = "|".join(filters)

    # Docker command
    cmd = [
        "docker", "exec",
        RANGER_CONTAINER_NAME,
        "bash", "-c",
        f"""
        LOG="{RANGER_LOG_FILE}"

        echo "========== RECENT ACTIVITY =========="
        tail -n 12 $LOG 2>/dev/null

        echo
        echo "========== RECENT ERRORS / WARNS =========="
        grep -i -E "ERROR|WARN|Exception|FATAL" $LOG 2>/dev/null | tail -n 25

        echo
        echo "========== RECENT API CALLS =========="
        grep -i -E "REST|XUserREST|ServiceREST|PolicyREST" $LOG 2>/dev/null | tail -n 20
        echo
        echo "========== RELATED TO HTTP {resp_code} =========="
        tail -n $(( {lines} * 5 )) $LOG 2>/dev/null | \
        grep -i -E "{resp_filter}" -A3 -B3 | tail -n 30
        """
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        output = result.stdout.strip()

        return output if output else "No relevant Ranger logs found."

    except Exception as e:
        return f"Failed to fetch Ranger logs: {e}"
    
def permission_module_exists(module_name= None, ranger_admin_config = None, base_url=None, headers=None):
    RANGER_CONFIG = {"base_url": base_url, "auth": ranger_admin_config, "headers": headers}
    if RANGER_CONFIG is None:
        raise RuntimeError("RANGER_CONFIG not initialized")
    if module_name is None:
        raise ValueError("module_name must be provided")
    resp = requests.get(
        f"{base_url}/xusers/permission",
        params={"module": module_name},
        auth=ranger_admin_config,
        headers=headers
    )
    if resp.status_code != 200:
        return False

    data = resp.json()
    print("Permission module response data:", data)
    module_list = data.get("vXModuleDef", [])

    return len(module_list) > 0

def build_user_permission(user_id, module_id, is_allowed):
    return {
        "userId": user_id,
        "moduleId": module_id,
        "isAllowed": is_allowed
    }


def build_group_permission(group_id, module_id, is_allowed):
    return {
        "groupId": group_id,
        "moduleId": module_id,
        "isAllowed": is_allowed
    }


def build_permission_payload(module_id, module_name, user_perm_list=None, group_perm_list=None):
    return {
        "id": module_id,
        "module": module_name,
        "userPermList": user_perm_list or [],
        "groupPermList": group_perm_list or []
    }


def group_permission_schema(data):
    assert "startIndex" in data and isinstance(data["startIndex"], int)
    assert "pageSize" in data and isinstance(data["pageSize"], int)
    assert "totalCount" in data and isinstance(data["totalCount"], int)
    assert "resultSize" in data and isinstance(data["resultSize"], int)
    assert "queryTimeMS" in data and isinstance(data["queryTimeMS"], int)
    assert "vXGroupPermission" in data
    print("Group Permission Response Data:", data)
    if len(data["vXGroupPermission"]) > 0:
        perm = data["vXGroupPermission"][0]
        assert "id" in perm and isinstance(perm["id"], int)
        assert "groupId" in perm and isinstance(perm["groupId"], int)
        assert "isAllowed" in perm and isinstance(perm["isAllowed"], int) and perm["isAllowed"] in [0, 1]
        assert "groupName" in perm and isinstance(perm["groupName"], str) and len(perm["groupName"]) <= 767

def user_permission_exists(user_id, module_id, auth, base_url, headers, user_perm_id = None):
    page_size = 200
    start_index = 0

    while True:
        response = requests.get(
            f"{base_url}/xusers/permission/user",
            params={"startIndex": start_index, "pageSize": page_size},
            auth=auth,
            headers=headers
        )

        if response.status_code != 200:
            return False

        data = response.json()
        permissions = data.get("vXUserPermission", [])

        for p in permissions:
            if p.get("userId") == user_id and p.get("moduleId") == module_id:
                if user_perm_id is not None and p.get("id") != user_perm_id:
                    continue
                return True

        total = int(data.get("totalCount", 0))
        start_index += page_size

        if start_index >= total:
            break

    return False