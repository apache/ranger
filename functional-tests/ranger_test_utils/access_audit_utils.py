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

Audits are indexed in Solr or OpenSearch when ``AUDIT_INDEX_STORE`` is not
``none``; tests poll the Admin ``/assets/accessAudit`` REST API.
"""

import os
import time

from ranger_test_utils.utils import RANGER_ADMIN_BASE_URL, get_test_logger

logger = get_test_logger(__name__)

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
    """Assert at least one access audit row matches the given filters."""
    audits = wait_for_access_audits(session, **kwargs)
    assert audits, "Expected access audit events matching filters: " + str(kwargs)


def access_audit_pipeline_enabled():
    """True when Docker was started with Solr/OpenSearch audit indexing."""
    return os.environ.get("AUDIT_INDEX_STORE", "opensearch").lower() != "none"


def audit_filters_to_query_kwargs(audit_filter, username, repo_name):
    """Map plugin test ``audit_filter`` dict keys to ``wait_for_access_audits`` kwargs."""
    if not audit_filter:
        return {}
    return {
        "request_user": audit_filter.get("request_user", username),
        "repo_name": audit_filter.get("repoName", audit_filter.get("repo_name", repo_name)),
        "access_type": audit_filter.get("accessType", audit_filter.get("access_type")),
        "resource_path": audit_filter.get(
            "resourceName", audit_filter.get("resource_path")
        ),
        "access_result": audit_filter.get(
            "accessResult", audit_filter.get("access_result")
        ),
        "acl_enforcer": audit_filter.get(
            "aclEnforcer", audit_filter.get("acl_enforcer")
        ),
        "resource_type": audit_filter.get(
            "resourceType", audit_filter.get("resource_type")
        ),
        "max_iterations": audit_filter.get("max_iterations", 12),
        "sleep_time": audit_filter.get("sleep_time", 5),
    }


def verify_access_audits_after_operation(
    session,
    audit_filter,
    username,
    repo_name,
    *,
    after_event_time,
    expect_audit_event=True,
):
    """
    Poll access audits newer than ``after_event_time``; assert presence/absence.
    """
    query_kwargs = audit_filters_to_query_kwargs(audit_filter, username, repo_name)
    query_kwargs["after_event_time"] = after_event_time
    audits = wait_for_access_audits(session, **query_kwargs)
    if expect_audit_event:
        assert audits, (
            "Expected access audit events matching filters: " + str(query_kwargs)
        )
    else:
        assert not audits, (
            "Unexpected access audit events matching filters: " + str(query_kwargs)
        )
    return audits


class AccessAuditCapture:
    """
    Baseline Solr/OpenSearch-backed access audits before a plugin client action.

    Use from any enforcement suite (Hive, HBase, Kafka, HDFS):

        capture = begin_access_audit_capture(admin_session, repo_name, request_user)
        ... run beeline / hbase shell / kafka producer / hdfs dfs ...
        capture.verify(audit_filter)
    """

    def __init__(self, session, repo_name, request_user):
        self.session = session
        self.repo_name = repo_name
        self.request_user = request_user
        self.enabled = (
            session is not None
            and access_audit_pipeline_enabled()
        )
        self.after_event_time = None
        if self.enabled:
            self.after_event_time = fetch_last_access_audit_event_time(session)
            logger.info(
                "Access audit baseline eventTime=%s repo=%s requestUser=%s",
                self.after_event_time,
                repo_name,
                request_user,
            )
        elif session is not None and not access_audit_pipeline_enabled():
            logger.info(
                "Skipping access audit checks (AUDIT_INDEX_STORE=none); "
                "use opensearch or solr when starting Docker to enable"
            )

    def verify(
        self,
        audit_filter,
        *,
        expect_audit_event=True,
        request_user=None,
        repo_name=None,
    ):
        """Assert access audit rows in Solr/OpenSearch (via Admin REST) after the action."""
        if not audit_filter:
            return None
        if not self.enabled:
            return None
        user = request_user if request_user is not None else self.request_user
        repo = repo_name if repo_name is not None else self.repo_name
        logger.info("Verifying access audits: %s", audit_filter)
        return verify_access_audits_after_operation(
            self.session,
            audit_filter,
            user,
            repo,
            after_event_time=self.after_event_time,
            expect_audit_event=expect_audit_event,
        )


def begin_access_audit_capture(session, repo_name, request_user):
    """
    Record newest access audit timestamp before a plugin enforcement action.

    Returns an :class:`AccessAuditCapture` for ``verify()`` after the client runs.
    """
    return AccessAuditCapture(session, repo_name, request_user)


def verify_plugin_access_audit(
    session,
    audit_filter,
    *,
    request_user,
    repo_name,
    after_event_time,
    expect_audit_event=True,
):
    """
    One-shot access audit verify when the baseline timestamp is already known.

    Prefer :func:`begin_access_audit_capture` + ``capture.verify()`` for new tests.
    """
    return verify_access_audits_after_operation(
        session,
        audit_filter,
        request_user,
        repo_name,
        after_event_time=after_event_time,
        expect_audit_event=expect_audit_event,
    )
