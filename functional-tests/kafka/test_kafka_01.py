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
Authorization is asserted via Kafka console producer output only. Access-audit asserts are
deferred until Kafka plugin audits are reliably indexed in Docker — use
ranger_test_utils.access_audit_utils when ready.

AUDIT_FILTERS (future):
  test_01_01 positive: accessType=publish, accessResult=1, aclEnforcer=ranger-acl,
                       resourceName=<topic>, repoName=dev_kafka, requestUser=hrt_21
  test_01_01 negative: accessType=publish, accessResult=0, aclEnforcer=ranger-acl,
                       resourceName=<topic>, repoName=dev_kafka, requestUser=hrt_22
"""

import pytest

from ranger_test_utils.hive_utils import delete_policies_by_id
from ranger_test_utils.kafka_utils import (
    KAFKA_ALLOWED_GROUP,
    KAFKA_ALLOWED_USER,
    KAFKA_AUTHZ_DENIED_SUBSTR,
    KAFKA_DENIED_USER,
    create_topic_allow_policy,
    ensure_kerberos_user,
    ensure_os_group_membership,
    preflight_kafka_stack,
    restore_missing_policies,
    run_kafka_producer,
    snapshot_policies,
    wait_for_kafka_policy_propagation,
)
from ranger_test_utils.utils import (
    configure_test_logging,
    create_ranger_admin_session,
    ensure_group_exists,
    get_test_logger,
    log_testcase_begin_for_pytest,
    unique_suffix,
)

pytestmark = pytest.mark.kafka

logger = get_test_logger(__name__)

MESSAGE = "Hello Ranger Kafka"

_admin_session = None
_policy_id = None
_original_policies = []
_topic = None


def _restore_dev_kafka_policies():
    try:
        if _policy_id is not None:
            delete_policies_by_id(_admin_session, [_policy_id])
    finally:
        restore_missing_policies(_admin_session, _original_policies)


def setup_module():
    configure_test_logging(module_name=__name__)
    logger.info("=== setup_module: start ===")
    preflight_kafka_stack()

    global _admin_session, _policy_id, _original_policies, _topic
    _admin_session = create_ranger_admin_session()
    _topic = "kafkaranger_test" + unique_suffix(6)
    logger.info("Test topic: %s", _topic)

    ensure_group_exists(_admin_session, KAFKA_ALLOWED_GROUP)
    ensure_os_group_membership(KAFKA_ALLOWED_USER, KAFKA_ALLOWED_GROUP)
    ensure_kerberos_user(KAFKA_ALLOWED_USER)
    ensure_kerberos_user(KAFKA_DENIED_USER)

    _original_policies = snapshot_policies(_admin_session)
    try:
        delete_policies_by_id(_admin_session, [policy["id"] for policy in _original_policies])
        _policy_id = create_topic_allow_policy(_admin_session, _topic, KAFKA_ALLOWED_GROUP)
        logger.info("Created topic allow policy id=%s for group %s", _policy_id, KAFKA_ALLOWED_GROUP)
        wait_for_kafka_policy_propagation()
    except BaseException:
        # pytest does not run teardown_module when setup_module raises, so restore here.
        _restore_dev_kafka_policies()
        raise
    logger.info("=== setup_module: done ===")


def teardown_module():
    if _admin_session is None:
        return

    logger.info("=== teardown_module: start ===")
    _restore_dev_kafka_policies()
    logger.info("=== teardown_module: done ===")


def test_01_01_kafka_publish_topic_test_positive(request):
    """hrt_21 is in the finance group, which the topic allow policy lets publish (creating the topic)."""
    log_testcase_begin_for_pytest(logger, request.node.name, suite="KAFKA")
    exit_code, output = run_kafka_producer(KAFKA_ALLOWED_USER, _topic, MESSAGE)
    assert "TOPIC_AUTHORIZATION_FAILED" not in output and KAFKA_AUTHZ_DENIED_SUBSTR not in output, (
        "Unexpected authorization failure publishing to "
        + _topic
        + "; exit_code="
        + str(exit_code)
        + " output="
        + output
    )
    assert exit_code == 0, (
        "Kafka producer failed; exit_code=" + str(exit_code) + " output=" + output
    )


def test_01_01_kafka_publish_topic_test_negative(request):
    """hrt_22 has no policy on the topic, so publishing is denied."""
    log_testcase_begin_for_pytest(logger, request.node.name, suite="KAFKA")
    exit_code, output = run_kafka_producer(KAFKA_DENIED_USER, _topic, MESSAGE)
    assert KAFKA_AUTHZ_DENIED_SUBSTR in output, (
        "Expected "
        + KAFKA_AUTHZ_DENIED_SUBSTR
        + " in Kafka producer output; exit_code="
        + str(exit_code)
        + " output="
        + output
    )
