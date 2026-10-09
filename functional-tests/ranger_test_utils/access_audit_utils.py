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
Ranger Admin access-audit query helpers (Docker functional-tests).

Hive plugin tests do not call these yet — Hive access audits are not reliably
indexed in the default Docker stack. Wire into hive tests once audit pipeline works.
"""

import logging
import time

from ranger_test_utils.utils import RANGER_ADMIN_BASE_URL

logger = logging.getLogger(__name__)

ACCESS_AUDIT_PATH = RANGER_ADMIN_BASE_URL + "/assets/accessAudit"


def fetch_last_access_audit_event_time(session):
    """
    Return the newest access audit eventTime, or None if none / request failed.
    """
    params = {
        "pageSize": 25,
        "startIndex": 0,
        "sortBy": "eventTime",
        "sortType": "desc",
    }
    response = session.get(ACCESS_AUDIT_PATH, params=params)
    if response.status_code != 200:
        return None
    audits = response.json().get("vXAccessAudits") or []
    if not audits:
        return None
    return audits[0].get("eventTime")


def wait_for_access_audits(
    session,
    *,
    after_event_time=None,
    request_user=None,
    repo_name=None,
    access_type=None,
    resource_path=None,
    access_result=None,
    acl_enforcer=None,
    resource_type=None,
    security_zone=None,
    max_iterations=12,
    sleep_time=5,
):
    """
    Poll Ranger Admin access audits matching query filters.

    When after_event_time is set, only audits with eventTime greater than that
    value are returned.
    """
    query_params = {
        "pageSize": 25,
        "startIndex": 0,
        "sortBy": "eventTime",
        "sortType": "desc",
    }
    if request_user is not None:
        query_params["requestUser"] = request_user
    if repo_name is not None:
        query_params["repoName"] = repo_name
    if resource_type is not None:
        query_params["resourceType"] = resource_type
    if access_type is not None:
        query_params["accessType"] = access_type
    if resource_path is not None:
        query_params["resourcePath"] = resource_path
    if access_result is not None:
        query_params["accessResult"] = access_result
    if security_zone is not None:
        query_params["zoneName"] = security_zone
    if acl_enforcer is not None:
        query_params["aclEnforcer"] = acl_enforcer

    for _ in range(max_iterations):
        response = session.get(ACCESS_AUDIT_PATH, params=query_params)
        if response.status_code != 200:
            time.sleep(sleep_time)
            continue

        audits = response.json().get("vXAccessAudits") or []
        if not audits:
            time.sleep(sleep_time)
            continue

        if after_event_time is None:
            return audits

        audits.sort(key=lambda row: row.get("eventTime", 0), reverse=True)
        newer = [
            row
            for row in audits
            if row.get("eventTime", 0) > after_event_time
        ]
        if newer:
            return newer
        time.sleep(sleep_time)

    return None


def assert_access_audits_found(session, **kwargs):
    """Convenience assert for future hive audit tests."""
    audits = wait_for_access_audits(session, **kwargs)
    assert audits, "Expected access audit events matching filters: " + str(kwargs)
