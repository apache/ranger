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
Session setup for Docker plugin enforcement suites (Hive, HDFS, HBase, Kafka).

Used by ``functional-tests/conftest.py`` when collected tests carry a registered
plugin marker. Suites can also call ``ensure_plugin_test_users_for_markers`` from
``setup_module`` if they need explicit control.
"""

import sys

import pytest
import requests

from ranger_test_utils.plugin_test_users import (
    PLUGIN_KERBEROS_STACK_HADOOP,
    PLUGIN_KERBEROS_STACK_HBASE,
    PLUGIN_KERBEROS_STACK_HIVE,
    PLUGIN_KERBEROS_STACK_KAFKA,
    ensure_plugin_test_users,
)
from ranger_test_utils.utils import (
    create_ranger_admin_session,
    get_test_logger,
    preflight_ranger_admin_http,
)

logger = get_test_logger(__name__)


def _plugin_setup_progress(message):
    """Visible on console during session setup (log_cli is off until module tests run)."""
    print("[plugin-setup] " + message, file=sys.stderr, flush=True)

# pytest marker name -> Kerberos stacks to provision for that suite
PLUGIN_MARKER_KERBEROS_STACKS = {
    "hive": (PLUGIN_KERBEROS_STACK_HIVE,),
    "hbase": (PLUGIN_KERBEROS_STACK_HBASE,),
    "kafka": (PLUGIN_KERBEROS_STACK_KAFKA,),
    "hdfs_policy": (PLUGIN_KERBEROS_STACK_HADOOP,),
}

PLUGIN_ENFORCEMENT_MARKERS = frozenset(PLUGIN_MARKER_KERBEROS_STACKS.keys())


def _preflight_for_marker(marker_name):
    if marker_name == "hive":
        from ranger_test_utils.hive_utils import preflight_hive_stack

        preflight_hive_stack()
    elif marker_name == "hbase":
        from ranger_test_utils.hbase_utils import preflight_hbase_stack

        preflight_hbase_stack()
    elif marker_name == "kafka":
        from ranger_test_utils.kafka_utils import preflight_kafka_stack

        preflight_kafka_stack()
    elif marker_name == "hdfs_policy":
        from ranger_test_utils.hdfs_utils import preflight_hdfs_plugin_stack

        preflight_hdfs_plugin_stack()


def markers_present_in_session(session):
    """Return plugin enforcement marker names present on collected tests."""
    found = set()
    for item in session.items:
        for marker_name in PLUGIN_ENFORCEMENT_MARKERS:
            if item.get_closest_marker(marker_name) is not None:
                found.add(marker_name)
    return found


def kerberos_stacks_for_markers(marker_names):
    """Merge Kerberos stack tuples for the given marker names (stable order)."""
    stacks = []
    seen = set()
    for marker_name in sorted(marker_names):
        for stack in PLUGIN_MARKER_KERBEROS_STACKS.get(marker_name, ()):
            if stack not in seen:
                seen.add(stack)
                stacks.append(stack)
    return tuple(stacks)


def ensure_plugin_test_users_for_markers(marker_names):
    """
    Run stack preflights, then Ranger xusers + Kerberos for merged stacks.

    Safe to call from pytest session setup or from a suite ``setup_module``.
    """
    marker_names = set(marker_names)
    if not marker_names:
        return

    _plugin_setup_progress(
        "starting for markers: " + ", ".join(sorted(marker_names))
    )

    for marker_name in sorted(marker_names):
        logger.info("Plugin preflight for marker: %s", marker_name)
        _plugin_setup_progress("preflight: " + marker_name)
        _preflight_for_marker(marker_name)

    _plugin_setup_progress("checking Ranger Admin HTTP")
    preflight_ranger_admin_http()

    stacks = kerberos_stacks_for_markers(marker_names)
    _plugin_setup_progress(
        "Ranger xusers + Kerberos for stacks: " + ", ".join(stacks)
    )
    logger.info(
        "Ensuring plugin test users (Ranger + Kerberos) for stacks: %s",
        stacks,
    )
    try:
        session = create_ranger_admin_session()
        ensure_plugin_test_users(session, kerberos_stacks=stacks)
    except (requests.RequestException, RuntimeError) as exc:
        pytest.skip("Plugin user setup failed: " + str(exc))

    _plugin_setup_progress("done")
    logger.info("Plugin test users ready for markers: %s", sorted(marker_names))


def ensure_plugin_test_users_for_session(pytest_session):
    """If any plugin enforcement tests were collected, provision shared users."""
    marker_names = markers_present_in_session(pytest_session)
    if not marker_names:
        return
    ensure_plugin_test_users_for_markers(marker_names)
