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
Scenario (``test_only_database_part_of_zone``)
----------------------------------------------
* security zone ``finance`` owns resource ``database=finance, table=*, column=*``
* policy **inside** the zone grants ``select`` on ``finance.stocks`` to hrt_21
* policy **outside** the zone grants ``select`` on ``finance.stocks`` to hrt_22

Because the resource belongs to a zone, only zone policies are evaluated:

* ``select * from finance.stocks`` as hrt_21 -> allowed
* ``select * from finance.stocks`` as hrt_22 -> denied (non-zone policy ignored)
"""

import pytest

from ranger_test_utils.hive_utils import (
    HIVE_AUDIT_POLICY_ACCESS_TYPES,
    HIVE_AUTHZ_DENIED_SUBSTR,
    HIVE_SERVICE_NAME,
    HivePluginTest,
    HivePluginTestContext,
    build_hive_table_policy_payload,
    build_zone_payload,
    create_policy,
    delete_all_policies_for_service,
    delete_security_zone,
    preflight_hive_stack,
    recreate_security_zone,
    run_beeline,
    wait_for_policy_propagation,
)
from ranger_test_utils.plugin_enforcement_utils import (
    configure_test_logging,
    create_ranger_admin_session,
    logger,
)

pytestmark = pytest.mark.hive

FINANCE_ZONE = "finance"
FINANCE_DB = "finance"
STOCKS_TABLE = "stocks"

TEST_CASE = None
_admin_session = None
_zone_id = None


def _create_test_schema():
    """Create the ``finance`` database and its ``stocks`` table (as hive)."""
    run_beeline(None, "create database if not exists " + FINANCE_DB + ";",
                service_user=True)
    run_beeline(
        None,
        "create table if not exists "
        + FINANCE_DB
        + "."
        + STOCKS_TABLE
        + " (id string, name string, alias string);",
        service_user=True,
    )


def setup_module():
    """
    * verify the Docker Hive stack is up,
    * wipe every ``dev_hive`` policy so the run starts from a known state,
    * grant the ``hive`` service user full access so it can build the schema,
    * create the ``finance`` database + ``stocks`` table,
    * (re)create the ``finance`` security zone over ``database=finance``,
    * add one zone policy (hrt_21) and one non-zone policy (hrt_22).
    """
    global TEST_CASE, _admin_session, _zone_id

    configure_test_logging(module_name=__name__)
    logger.info("=== setup_module: start ===")
    preflight_hive_stack()

    TEST_CASE = HivePluginTestContext()
    _admin_session = create_ranger_admin_session()
    TEST_CASE.admin_session = _admin_session

    delete_all_policies_for_service(_admin_session, HIVE_SERVICE_NAME)

    # the service user performs all DDL, so it needs an explicit grant once the
    # default policies are gone
    create_policy(
        _admin_session,
        build_hive_table_policy_payload(
            HIVE_SERVICE_NAME,
            "all-access-hive-service-user",
            "*",
            ["*"],
            ["hive"],
            accesses=HIVE_AUDIT_POLICY_ACCESS_TYPES,
        ),
    )
    wait_for_policy_propagation()

    _create_test_schema()

    zone = recreate_security_zone(
        _admin_session,
        build_zone_payload(
            FINANCE_ZONE,
            {
                HIVE_SERVICE_NAME: {
                    "resources": [
                        {"database": [FINANCE_DB], "table": ["*"], "column": ["*"]}
                    ]
                }
            },
        ),
    )
    _zone_id = zone.get("id")

    # a fresh zone may inherit policies - start it empty
    delete_all_policies_for_service(_admin_session, HIVE_SERVICE_NAME,
                                    zone_name=FINANCE_ZONE)

    # policy INSIDE the zone -> the only one that counts for finance.*
    create_policy(
        _admin_session,
        build_hive_table_policy_payload(
            HIVE_SERVICE_NAME,
            "zone-policy-finance-stocks",
            FINANCE_DB,
            [STOCKS_TABLE],
            [HivePluginTestContext.USER_USER1],
            zone_name=FINANCE_ZONE,
        ),
    )
    # identical policy OUTSIDE the zone -> must be ignored
    create_policy(
        _admin_session,
        build_hive_table_policy_payload(
            HIVE_SERVICE_NAME,
            "non-zone-policy-finance-stocks",
            FINANCE_DB,
            [STOCKS_TABLE],
            [HivePluginTestContext.USER_USER2],
        ),
    )
    wait_for_policy_propagation()
    logger.info("=== setup_module: done ===")


def teardown_module():
    """Drop the test database, the zone and every policy created here."""
    if _admin_session is None:
        return

    logger.info("=== teardown_module: start ===")

    # The zone must go FIRST. While ``finance`` belongs to the zone, only zone
    # policies are evaluated for it, so the non-zone all-access grant does not
    # apply and the service user cannot DROP the database. Removing the zone
    # puts the database back under the non-zone policy.
    if _zone_id is not None:
        delete_all_policies_for_service(_admin_session, HIVE_SERVICE_NAME,
                                        zone_name=FINANCE_ZONE)
        delete_security_zone(_admin_session, _zone_id)
        wait_for_policy_propagation()

    _, output = run_beeline(
        None, "drop database if exists " + FINANCE_DB + " cascade;",
        service_user=True,
    )
    if HIVE_AUTHZ_DENIED_SUBSTR in (output or ""):
        logger.warning(
            "Could not drop database %s during teardown - it will be reused by "
            "the next run: %s", FINANCE_DB, output,
        )

    delete_all_policies_for_service(_admin_session, HIVE_SERVICE_NAME)
    logger.info("=== teardown_module: done ===")


def test_only_database_part_of_zone():
    """Only the zone policy may grant access to a resource owned by a zone."""
    HivePluginTest(
        "HIVE.ZONE.test_only_database_part_of_zone.allow_zone_user",
        TEST_CASE,
        "select * from " + FINANCE_DB + "." + STOCKS_TABLE + ";",
        HivePluginTestContext.USER_USER1,
        HivePluginTestContext.PASS_USER1,
        verifyCriteria=HivePluginTest.STDERR_NOT_CONTAINS,
        verifyStr=HIVE_AUTHZ_DENIED_SUBSTR,
    ).execute(zone_name=FINANCE_ZONE)

    HivePluginTest(
        "HIVE.ZONE.test_only_database_part_of_zone.deny_non_zone_user",
        TEST_CASE,
        "select * from " + FINANCE_DB + "." + STOCKS_TABLE + ";",
        HivePluginTestContext.USER_USER2,
        HivePluginTestContext.PASS_USER2,
        verifyCriteria=HivePluginTest.STDERR_CONTAINS,
        verifyStr=HIVE_AUTHZ_DENIED_SUBSTR,
    ).execute(zone_name=FINANCE_ZONE)

