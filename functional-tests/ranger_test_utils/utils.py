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
Shared helpers for Apache Ranger functional-tests (Docker pytest).

"""

import logging
import uuid
from datetime import datetime, timezone

import requests

# --- Ranger Admin defaults (local Docker) ---

RANGER_ADMIN_BASE_URL = "http://localhost:6080/service"

PUBLIC_V2_BASE = RANGER_ADMIN_BASE_URL + "/public/v2/api"
SERVICE_URL = PUBLIC_V2_BASE + "/service/"
POLICY_URL = PUBLIC_V2_BASE + "/policy/"
XUSERS_BASE = RANGER_ADMIN_BASE_URL + "/xusers"

DEFAULT_ADMIN_AUTH = ("admin", "rangerR0cks!")

DEFAULT_JSON_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Users/groups used by migrated Argus public-API policy tests (created via xusers if missing).
ARGUS_POLICY_USERS = ["hrt_21", "hrt_22"]
ARGUS_POLICY_GROUPS = ["finance", "audit"]

HIVE_JDBC_URL = "jdbc:hive2://127.0.0.1:10000/default"
HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver"

# --- Logging ---


def configure_test_logging(level=logging.INFO, module_name=None):
    """
    Call once from setup_module.

    """
    logging.getLogger().setLevel(level)
    if module_name:
        logging.getLogger(module_name).setLevel(level)


def get_test_logger(module_name):
    """
    Module logger for tests.
    """
    log = logging.getLogger(module_name)
    log.setLevel(logging.INFO)
    return log


def log_http_response(logger, step, response, max_body_length=400):
    """Log HTTP status and a short response body for each REST call."""
    logger.info("%s: HTTP %s", step, response.status_code)
    if not response.text:
        return
    body = response.text
    if len(body) > max_body_length:
        body = body[:max_body_length] + "..."
    logger.info("%s response body: %s", step, body)


# --- HTTP assertions ---


def assert_http_ok(response, ok_codes, step, url=""):
    """
    Check response.status_code. On failure, raise with step, URL, and body text.
    ok_codes can be 200 or (200, 201).
    """
    if isinstance(ok_codes, int):
        expected = (ok_codes,)
    else:
        expected = ok_codes
    if response.status_code in expected:
        return
    message = step + " failed: HTTP " + str(response.status_code)
    message += " (expected " + str(expected) + ")"
    if url:
        message += "\nURL: " + url
    message += "\nResponse body:\n" + response.text
    raise AssertionError(message)


def create_ranger_admin_session(auth=None, headers=None):
    """
    Build a requests.Session for Ranger Admin REST calls (Docker defaults).

    Sets HTTP basic auth and JSON Accept/Content-Type headers on the session.
    """
    if auth is None:
        auth = DEFAULT_ADMIN_AUTH
    if headers is None:
        headers = DEFAULT_JSON_HEADERS
    session = requests.Session()
    session.auth = auth
    session.headers.update(headers)
    return session


# --- Test run IDs ---


def unique_suffix():
    """Unique string for service/policy names so parallel runs do not clash."""
    time_part = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    random_part = uuid.uuid4().hex[:6]
    return time_part + random_part


# --- xusers: ensure principals exist ---


def ensure_user_exists(session, username):
    """Create a Ranger user via xusers REST if it is not already present."""
    lookup_url = XUSERS_BASE + "/users/userName/" + username
    lookup = session.get(lookup_url)
    if lookup.status_code == 200:
        return
    payload = {
        "name": username,
        "firstName": username,
        "lastName": "api_test",
        "emailAddress": username + "@example.com",
        "password": "Test@123",
        "status": 1,
        "isVisible": 1,
        "userSource": 1,
        "userRoleList": ["ROLE_USER"],
        "groupIdList": [],
        "groupNameList": [],
    }
    create_url = XUSERS_BASE + "/secure/users"
    create = session.post(create_url, json=payload)
    assert_http_ok(create, 200, "Create Ranger user " + username, create_url)


def ensure_group_exists(session, group_name):
    """Create a Ranger group via xusers REST if it is not already present."""
    lookup_url = XUSERS_BASE + "/groups/groupName/" + group_name
    lookup = session.get(lookup_url)
    if lookup.status_code == 200:
        return
    payload = {"name": group_name, "groupSource": 0}
    create_url = XUSERS_BASE + "/secure/groups"
    create = session.post(create_url, json=payload)
    assert_http_ok(create, 200, "Create Ranger group " + group_name, create_url)


def ensure_argus_policy_principals(session):
    """Create users and groups referenced by migrated Argus policy tests."""
    for username in ARGUS_POLICY_USERS:
        ensure_user_exists(session, username)
    for group_name in ARGUS_POLICY_GROUPS:
        ensure_group_exists(session, group_name)


# --- Hive public API v2 JSON builders ---


def split_csv(value):
    """Turn 'a, b ,c' into ['a', 'b', 'c']."""
    return [part.strip() for part in value.split(",") if part.strip()]


def build_hive_service_payload(
    name,
    description,
    username="policymgr",
    password="policymgr",
    jdbc_url=HIVE_JDBC_URL,
    is_enabled=True,
):
    """JSON body for POST/PUT on /public/v2/api/service/ (Hive type)."""
    return {
        "name": name,
        "type": "hive",
        "description": description,
        "isEnabled": is_enabled,
        "configs": {
            "username": username,
            "password": password,
            "jdbc.driverClassName": HIVE_DRIVER,
            "jdbc.url": jdbc_url,
            "commonNameForCertificate": "",
        },
    }


def resource_block(values):
    """Standard Ranger policy resource block (database/table/column)."""
    return {
        "values": values,
        "isExcludes": False,
        "isRecursive": False,
    }


def perm_map_list_to_policy_items(perm_map_list):
    """
    Convert legacy test shape to Public API v2 policyItems.

    Each entry: {"userList": [...], "groupList": [...], "permList": ["select", ...]}.
    """
    items = []
    for entry in perm_map_list:
        accesses = [
            {"type": perm, "isAllowed": True}
            for perm in entry.get("permList", [])
        ]
        items.append(
            {
                "users": entry.get("userList", []),
                "groups": entry.get("groupList", []),
                "accesses": accesses,
                "delegateAdmin": False,
            }
        )
    return items


def build_hive_policy_payload(
    service_name,
    policy_name,
    database_list,
    table_list,
    column_list,
    perm_map_list,
    description="",
    is_enabled=True,
    is_audit_enabled=True,
    policy_id=None,
):
    """JSON body for POST/PUT on /public/v2/api/policy/."""
    payload = {
        "service": service_name,
        "name": policy_name,
        "description": description,
        "isEnabled": is_enabled,
        "isAuditEnabled": is_audit_enabled,
        "resources": {
            "database": resource_block(split_csv(database_list)),
            "table": resource_block(split_csv(table_list)),
            "column": resource_block(split_csv(column_list)),
        },
        "policyItems": perm_map_list_to_policy_items(perm_map_list),
    }
    if policy_id is not None:
        payload["id"] = policy_id
    return payload


# --- Compare expected vs actual service/policy JSON ---


def service_matches(expected, actual, attributes=None):
    """True if selected top-level service fields match."""
    if attributes is None:
        attributes = ["name", "description", "type"]
    return all(actual.get(key) == expected.get(key) for key in attributes)


def get_allowed_access_types(policy_item):
    """Sorted list of permission types marked isAllowed on a policy item."""
    types = [
        access.get("type")
        for access in policy_item.get("accesses", [])
        if access.get("isAllowed", True)
    ]
    return sorted(types)


def single_policy_item_matches(expected_item, actual_item):
    """True if users, groups, and allowed access types match one policy item."""
    if sorted(expected_item.get("users", [])) != sorted(actual_item.get("users", [])):
        return False
    if sorted(expected_item.get("groups", [])) != sorted(
        actual_item.get("groups", [])
    ):
        return False
    return get_allowed_access_types(expected_item) == get_allowed_access_types(
        actual_item
    )


def policy_items_match(expected_items, actual_items):
    """True if every expected policy item has a matching actual item (order ignored)."""
    if len(expected_items) != len(actual_items):
        return False
    for expected_item in expected_items:
        if not any(
            single_policy_item_matches(expected_item, actual_item)
            for actual_item in actual_items
        ):
            return False
    return True


def resource_values_match(expected_resources, actual_resources, resource_name):
    """True if database/table/column value lists match (order ignored)."""
    exp_vals = sorted(
        expected_resources.get(resource_name, {}).get("values", [])
    )
    act_vals = sorted(actual_resources.get(resource_name, {}).get("values", []))
    return exp_vals == act_vals


def policy_matches(expected, actual):
    """True if name, service, hive resources, and policy items match."""
    if expected.get("name") != actual.get("name"):
        return False
    if expected.get("service") != actual.get("service"):
        return False
    exp_res = expected.get("resources", {})
    act_res = actual.get("resources", {})
    for resource_name in ("database", "table", "column"):
        if not resource_values_match(exp_res, act_res, resource_name):
            return False
    return policy_items_match(
        expected.get("policyItems", []),
        actual.get("policyItems", []),
    )


def find_service_by_id(services, service_id):
    """Return the service dict with the given id, or None."""
    for service in services:
        if service.get("id") == service_id:
            return service
    return None


def find_policy_by_id(policies, policy_id):
    """Return the policy dict with the given id, or None."""
    for policy in policies:
        if policy.get("id") == policy_id:
            return policy
    return None
