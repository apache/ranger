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
#from common.utils import fetch_logs
import inspect
import subprocess

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
    print("Assign Groups Response:", response.status_code, response.text)
    assert response.status_code == 200, f"Failed to assign groups to user: {response.text}"
    


import subprocess

RANGER_CONTAINER_NAME = "ranger"
RANGER_LOG_FILE = "/var/log/ranger/ranger-admin-ranger.rangernw-.log"


def fetch_ranger_logs(resp_code=None, lines: int = 80) -> str:
    """
    Generic Ranger debugging logs.

    Sections:
      1. Recent Activity (always)
      2. Recent Errors/Warns (system health)
      3. Response-code related events
    """

    # ---- HTTP response meaning map ----
    keyword_map = {
        # SUCCESS
        200: "success|create|update|delete|assigned|completed",
        201: "created|create|success",
        204: "delete|removed|no content|success",

        # CLIENT ERRORS
        400: "invalid|validation|bad request|missing",
        401: "authentication|unauthorized|login failed",
        403: "denied|forbidden|permission|not allowed",
        404: "not found",
        405: "method not allowed|unsupported method",
        409: "conflict|already exists|duplicate",

        # SERVER ERRORS
        500: "error|exception|failed",
        502: "gateway|proxy|upstream",
        503: "unavailable|timeout|overloaded"
    }
    if isinstance(resp_code, int):

        resp_filter = keyword_map.get(
        resp_code,
        "error|warn|exception|failed|denied"
        )
    elif isinstance(resp_code, (list, tuple, set)):
        filters = [keyword_map.get(code, "error|warn|exception|failed|denied") for code in resp_code]
        resp_filter = "|".join(filters)

    try:
        cmd = [
            "docker", "exec",
            RANGER_CONTAINER_NAME,
            "bash", "-c",
            f"""
            LOG="{RANGER_LOG_FILE}"

            echo "========== RECENT ACTIVITY =========="
            tail -n {lines} $LOG 2>/dev/null | tail -n 12

            echo
            echo "========== RECENT ERRORS / WARNS =========="
            tail -n {lines} $LOG 2>/dev/null | \
            grep -i -E "error|warn|exception|fatal" -A2 -B2 | tail -n 25

            echo
            echo "========== RELATED TO HTTP {resp_code} =========="
            tail -n {lines} $LOG 2>/dev/null | \
            grep -i -E "{resp_filter}" -A2 -B2 | tail -n 25
            """
        ]

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