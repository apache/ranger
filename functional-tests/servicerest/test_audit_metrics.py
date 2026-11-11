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

import time
from datetime import datetime, timezone
from urllib.parse import quote
from zoneinfo import ZoneInfo

import pytest

from ranger_test_utils.utils import (
    POLICY_URL,
    RANGER_ADMIN_BASE_URL,
    assert_http_ok,
    build_hive_policy_payload,
    create_ranger_admin_session,
    get_test_logger,
    log_http_response,
    unique_suffix,
)

HIVE_SERVICE_NAME = "dev_hive"
logger = get_test_logger(__name__)


@pytest.fixture(scope="module")
def admin_session():
    session = create_ranger_admin_session()
    yield session
    session.close()


def test_policy_change_metric_uses_requested_timezone(admin_session):
    suffix = unique_suffix()
    timezone_name, expected_date = _timezone_with_different_date()
    count_before = _get_policy_create_count(
        admin_session, timezone_name, expected_date
    )
    logger.info(
        "Policy metric baseline: timezone=%s date=%s count=%s",
        timezone_name,
        expected_date,
        count_before,
    )

    policy = build_hive_policy_payload(
        service_name=HIVE_SERVICE_NAME,
        policy_name="audit_metrics_timezone_" + suffix,
        database_list="default",
        table_list="timezone_test",
        column_list="*",
        perm_map_list=[{"userList": ["admin"], "permList": ["select"]}],
    )
    started_at = datetime.now(timezone.utc)
    response = admin_session.post(POLICY_URL, json=policy)
    finished_at = datetime.now(timezone.utc)
    log_http_response(logger, "Create policy for audit metric", response)
    assert_http_ok(response, (200, 201), "Create policy", POLICY_URL)
    policy_id = response.json()["id"]

    try:
        _assert_operation_stayed_in_expected_date(
            started_at, finished_at, timezone_name, expected_date
        )
        count_after = _get_policy_create_count(
            admin_session, timezone_name, expected_date
        )
        logger.info("Policy metric count after create: %s", count_after)
        assert count_after == count_before + 1
    finally:
        admin_session.delete(POLICY_URL + str(policy_id))


def test_plugin_policy_sync_metric_uses_requested_timezone(admin_session):
    timezone_name, expected_date = _timezone_with_different_date()
    count_before = _get_plugin_sync_count(
        admin_session, timezone_name, expected_date
    )
    logger.info(
        "Plugin-sync metric baseline: timezone=%s date=%s count=%s",
        timezone_name,
        expected_date,
        count_before,
    )
    plugin_id = "audit-metrics-" + unique_suffix()
    url = (
        RANGER_ADMIN_BASE_URL
        + "/plugins/secure/policies/download/"
        + quote(HIVE_SERVICE_NAME, safe="")
    )

    started_at = datetime.now(timezone.utc)
    response = admin_session.get(
        url,
        params={"lastKnownVersion": -1, "pluginId": plugin_id},
    )
    finished_at = datetime.now(timezone.utc)
    log_http_response(logger, "Download policies for audit metric", response)
    assert_http_ok(response, 200, "Download policies", url)
    _assert_operation_stayed_in_expected_date(
        started_at, finished_at, timezone_name, expected_date
    )

    count_after = _wait_for_count(
        lambda: _get_plugin_sync_count(
            admin_session, timezone_name, expected_date
        ),
        count_before + 1,
    )
    logger.info("Plugin-sync metric count after download: %s", count_after)


def _get_policy_create_count(session, timezone_name, expected_date):
    url = RANGER_ADMIN_BASE_URL + "/audit/days-audit-admin-metrics"
    response = session.get(
        url,
        params={
            "objectClassTypes": 1020,
            "actions": "create",
            "olderThanInDays": 7,
            "timezone": timezone_name,
        },
    )
    assert_http_ok(response, 200, "Get policy change metrics", url)
    return _count_for_date(
        response.json().get("RangerPolicyMetricsByDays", []),
        "createCount",
        expected_date,
    )


def _get_plugin_sync_count(session, timezone_name, expected_date):
    url = RANGER_ADMIN_BASE_URL + "/audit/days-plugin-policy-sync-metrics"
    response = session.get(
        url,
        params={"olderThanInDays": 7, "timezone": timezone_name},
    )
    assert_http_ok(response, 200, "Get plugin sync metrics", url)
    return _count_for_date(
        response.json().get("PluginPolicySyncMetricsByDays", []),
        "numberOfPolicySyncCount",
        expected_date,
    )


def _count_for_date(metrics, count_field, expected_date):
    for metric in metrics:
        metric_date = datetime.fromtimestamp(
            int(metric["auditDate"]) / 1000,
            tz=timezone.utc,
        ).date()
        if metric_date == expected_date:
            return int(metric[count_field])
    return 0


def _timezone_with_different_date():
    now = datetime.now(timezone.utc)
    # Select a zone whose calendar date is guaranteed to differ from UTC now.
    timezone_name = "Etc/GMT+12" if now.hour < 10 else "Pacific/Kiritimati"
    expected_date = now.astimezone(ZoneInfo(timezone_name)).date()
    assert expected_date != now.date()
    return timezone_name, expected_date


def _assert_operation_stayed_in_expected_date(
    started_at, finished_at, timezone_name, expected_date
):
    zone = ZoneInfo(timezone_name)
    if started_at.date() != finished_at.date():
        pytest.skip("Operation crossed UTC midnight")
    assert started_at.astimezone(zone).date() == expected_date
    assert finished_at.astimezone(zone).date() == expected_date


def _wait_for_count(fetch_count, expected_count):
    actual_count = None
    for _ in range(15):
        actual_count = fetch_count()
        if actual_count == expected_count:
            return actual_count
        time.sleep(1)
    assert actual_count == expected_count
