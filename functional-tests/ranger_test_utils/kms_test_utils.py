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
Shared logging and module setup for KMS REST functional tests.

Test modules under ``kms/`` should import ``logger`` and ``configure_test_logging``
from here (same role as ``plugin_enforcement_utils`` for plugin suites).

Session Kerberos setup (like Hive plugin users) is triggered from
``functional-tests/conftest.py`` via :func:`ensure_kms_kerberos_users_for_session`.
"""

import sys
from pathlib import Path

from ranger_test_utils.kms_utils import (
    BASE_URL,
    KmsRestTestContext,
    PARAMS,
    ensure_keyadmin_keytab,
    ensure_keyadmin_ticket,
    ensure_testuser_keytab,
    krb_requests,
)
from ranger_test_utils.plugin_test_users import ensure_kerberos_principal
from ranger_test_utils.utils import configure_test_logging, get_test_logger

logger = get_test_logger(__name__)

FUNCTIONAL_TESTS_DIR = Path(__file__).resolve().parent.parent
KMS_SUITE_DIR = FUNCTIONAL_TESTS_DIR / "kms"

# Kerberos principals used by KMS REST tests (keyadmin REST user + policy testuser).
KMS_KERBEROS_TEST_USERS = ("keyadmin", "testuser")


def session_collects_kms_tests(pytest_session):
    """True when any collected test module lives under ``functional-tests/kms/``."""
    for item in pytest_session.items:
        try:
            item_path = Path(str(item.fspath)).resolve()
        except OSError:
            continue
        if KMS_SUITE_DIR in item_path.parents:
            return True
    return False


def ensure_kms_kerberos_users_for_session(pytest_session):
    """
    Before KMS tests run, create Kerberos principals and keytabs (Hive session pattern).

    Uses the same KDC helper as plugin enforcement (:func:`ensure_kerberos_principal`),
    then installs keytabs on ``ranger-kms`` and obtains the keyadmin ticket for REST calls.
    """
    if not session_collects_kms_tests(pytest_session):
        return

    print(
        "[kms-setup] Kerberos principals + keytabs for KMS suite",
        file=sys.stderr,
        flush=True,
    )
    for username in KMS_KERBEROS_TEST_USERS:
        print(
            "[kms-setup] Kerberos principal: " + username,
            file=sys.stderr,
            flush=True,
        )
        ensure_kerberos_principal(username)

    print("[kms-setup] keyadmin keytab on ranger-kms", file=sys.stderr, flush=True)
    ensure_keyadmin_keytab()
    print("[kms-setup] testuser keytab on ranger-kms", file=sys.stderr, flush=True)
    ensure_testuser_keytab()
    ensure_keyadmin_ticket()
    logger.info(
        "KMS Kerberos users ready: %s (keyadmin ticket active for REST)",
        KMS_KERBEROS_TEST_USERS,
    )


def init_kms_test_module(module_name):
    """
    Module setup for KMS REST tests: logging and ``TEST_CASE`` context.

    Returns a :class:`KmsRestTestContext` for ``TEST_CASE`` in the test module.
    """
    configure_test_logging(module_name=module_name)
    logger.info("=== setup_module: start ===")
    logger.info(
        "KMS Kerberos users %s ensured by functional-tests/conftest.py session setup",
        ", ".join(KMS_KERBEROS_TEST_USERS),
    )
    logger.info(
        "REST calls use user.name=%s (%s)",
        PARAMS.get("user.name"),
        KmsRestTestContext.USER_KEYADMIN,
    )
    context = KmsRestTestContext()
    logger.info("=== setup_module: done ===")
    return context


def teardown_kms_test_module(cleanup_key_names=None):
    """Best-effort delete of keys left behind if a test failed mid-run."""
    if not cleanup_key_names:
        return
    logger.info("=== teardown_module: start ===")
    for key_name in cleanup_key_names:
        krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    logger.info("=== teardown_module: done ===")
