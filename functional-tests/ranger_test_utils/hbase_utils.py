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

"""HBase Docker helpers for Ranger functional-tests (HBase shell + plugin enforcement)."""

import base64
import socket
import time

import pytest

from ranger_test_utils.utils import (
    POLICY_URL,
    assert_http_ok,
    get_test_logger,
    unique_suffix,
)
from ranger_test_utils.hive_utils import (
    HIVE_TEST_USER,
    KDC_CONTAINER,
    POLICY_PROPAGATION_SLEEP_SEC,
    get_docker_client,
)

logger = get_test_logger(__name__)

HBASE_SHELL_LOG_OUTPUT_MAX_CHARS = 12000

HBASE_SERVICE_NAME = "dev_hbase"
HBASE_CONTAINER = "ranger-hbase"
HADOOP_CONTAINER = "ranger-hadoop"
ADMIN_CONTAINER = "ranger"

KERBEROS_REALM = "EXAMPLE.COM"
HBASE_SERVICE_PRINCIPAL = "hbase/ranger-hbase.rangernw@" + KERBEROS_REALM
HBASE_SERVICE_KEYTAB = "/etc/keytabs/hbase.keytab"

HBASE_TEST_USER = HIVE_TEST_USER

HBASE_AUTHZ_DENIED_SUBSTR = "AccessDeniedException"

HBASE_AUDIT_POLICY_SERVICE_USERS = ["hbase"]
HBASE_AUDIT_POLICY_ACCESS_TYPES = ["create", "read", "write", "admin"]


def _container_running(name):
    client = get_docker_client()
    try:
        container = client.containers.get(name)
    except Exception:
        return False
    container.reload()
    return container.status == "running"


def _master_ui_port_open(host="127.0.0.1", port=16010, timeout=2.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def preflight_hbase_stack():
    """Skip tests when required Docker services or HBase master UI are unavailable."""
    required = [
        ADMIN_CONTAINER,
        KDC_CONTAINER,
        HADOOP_CONTAINER,
        HBASE_CONTAINER,
    ]
    missing = [name for name in required if not _container_running(name)]
    if missing:
        pytest.skip(
            "HBase functional-tests need running containers: "
            + ", ".join(missing)
            + ". Run ./run-tests.sh postgres hbase from functional-tests/."
        )

    if not _master_ui_port_open():
        pytest.skip(
            "HBase master UI is not accepting connections on localhost:16010 "
            "(is ranger-hbase healthy?)"
        )


def _user_principal(username):
    return username + "@" + KERBEROS_REALM


def _user_keytab_in_hbase_container(username):
    return "/etc/keytabs/" + username + ".keytab"


def _log_multiline_block(header, text, max_chars=HBASE_SHELL_LOG_OUTPUT_MAX_CHARS):
    body = text if text else "(empty)"
    if len(body) > max_chars:
        body = (
            body[:max_chars]
            + "\n... [truncated, total "
            + str(len(text))
            + " chars]"
        )
    logger.info("%s", header)
    for line in body.splitlines():
        logger.info("  %s", line)


def ensure_kerberos_user(username):
    """Create a user Kerberos principal and keytab on KDC, visible in ranger-hbase."""
    kdc = get_docker_client().containers.get(KDC_CONTAINER)
    principal = _user_principal(username)
    keytab_in_kdc = "/etc/keytabs/ranger-hbase/" + username + ".keytab"
    keytab_in_hbase = _user_keytab_in_hbase_container(username)

    logger.info(
        "Kerberos setup on %s: addprinc/ktadd for %s -> %s (visible in hbase as %s)",
        KDC_CONTAINER,
        principal,
        keytab_in_kdc,
        keytab_in_hbase,
    )

    add_cmd = 'kadmin.local -q "addprinc -randkey ' + principal + '"'
    exit_add, out_add = kdc.exec_run(add_cmd, user="root", demux=True)
    _log_multiline_block(
        "kadmin addprinc exit_code=%s" % exit_add,
        ((out_add[0] or b"") + (out_add[1] or b"")).decode(),
        max_chars=2000,
    )

    kdc.exec_run("rm -f " + keytab_in_kdc, user="root")
    ktadd_cmd = (
        'kadmin.local -q "ktadd -k '
        + keytab_in_kdc
        + " "
        + principal
        + '"'
    )
    exit_kt, out_kt = kdc.exec_run(ktadd_cmd, user="root", demux=True)
    kt_out = ((out_kt[0] or b"") + (out_kt[1] or b"")).decode()
    _log_multiline_block(
        "kadmin ktadd exit_code=%s" % exit_kt,
        kt_out,
        max_chars=2000,
    )
    if exit_kt != 0:
        raise RuntimeError(
            "ktadd failed for "
            + principal
            + " (keytab "
            + keytab_in_hbase
            + "): "
            + kt_out
        )
    kdc.exec_run("chmod 444 " + keytab_in_kdc, user="root")
    verify_kerberos_keytab_in_hbase(username)


def verify_kerberos_keytab_in_hbase(username):
    """kinit in ranger-hbase using the user's keytab (fail fast if keytab is bad)."""
    hbase = get_docker_client().containers.get(HBASE_CONTAINER)
    principal = _user_principal(username)
    keytab = _user_keytab_in_hbase_container(username)
    shell = (
        "kdestroy -A 2>/dev/null || true; "
        "kinit -kt "
        + keytab
        + " "
        + principal
        + "; klist"
    )
    exit_code, output = hbase.exec_run(["bash", "-c", shell], user="hbase", demux=True)
    combined = ((output[0] or b"") + (output[1] or b"")).decode()
    if exit_code != 0:
        raise RuntimeError(
            "Kerberos kinit failed in "
            + HBASE_CONTAINER
            + " for "
            + principal
            + ": "
            + combined
        )
    logger.info(
        "Verified Kerberos keytab for %s in %s",
        principal,
        HBASE_CONTAINER,
    )


def run_hbase_shell(username, shell_lines, service_user=False, expect_substring=None):
    """
    Run HBase shell commands inside ranger-hbase (stdin).

    Returns (exit_code, combined_output).
    """
    hbase = get_docker_client().containers.get(HBASE_CONTAINER)
    if service_user:
        principal = HBASE_SERVICE_PRINCIPAL
        keytab = HBASE_SERVICE_KEYTAB
        ranger_user_label = "hbase (service)"
    else:
        principal = _user_principal(username)
        keytab = _user_keytab_in_hbase_container(username)
        ranger_user_label = username

    script_body = "\n".join(shell_lines) + "\n"
    logger.info("Going to run HBase shell script:\n%s", script_body)
    logger.info(
        "HBase shell context: container=%s docker_exec_user=hbase ranger_user=%s",
        HBASE_CONTAINER,
        ranger_user_label,
    )

    encoded = base64.b64encode(script_body.encode("utf-8")).decode("ascii")
    hbase_cmd = "echo " + encoded + " | base64 -d | /opt/hbase/bin/hbase shell"
    shell = (
        "kdestroy -A 2>/dev/null || true; "
        "kinit -kt "
        + keytab
        + " "
        + principal
        + "; "
        + "kinit_rc=$?; "
        + 'if [ "$kinit_rc" -ne 0 ]; then exit "$kinit_rc"; fi; '
        + hbase_cmd
    )
    exit_code, output = hbase.exec_run(["bash", "-c", shell], user="hbase", demux=True)
    stdout = (output[0] or b"").decode()
    stderr = (output[1] or b"").decode()
    combined = stdout + stderr

    _log_multiline_block("HBase shell exit_code=%s stdout:" % exit_code, stdout)
    _log_multiline_block("HBase shell stderr:", stderr)

    if expect_substring:
        if expect_substring in combined:
            logger.info(
                'Found expected string "%s" in HBase shell output',
                expect_substring,
            )
        else:
            logger.info(
                'Expected string "%s" NOT found in HBase shell output',
                expect_substring,
            )

    return exit_code, combined


def prepare_hbase_test_table(table_name):
    """Ensure test table is absent — runs as HBase service user."""
    run_hbase_shell(
        None,
        [
            "if exists '" + table_name + "'",
            "  disable '" + table_name + "'",
            "  drop '" + table_name + "'",
            "end",
        ],
        service_user=True,
    )


def cleanup_hbase_test_table(table_name):
    """Drop test table if present — runs as HBase service user."""
    run_hbase_shell(
        None,
        [
            "if exists '" + table_name + "'",
            "  disable '" + table_name + "'",
            "  drop '" + table_name + "'",
            "end",
        ],
        service_user=True,
    )


def build_audit_only_policy_payload(service_name, suffix):
    accesses = [
        {"type": perm, "isAllowed": True}
        for perm in HBASE_AUDIT_POLICY_ACCESS_TYPES
    ]
    return {
        "isEnabled": True,
        "service": service_name,
        "name": "audit policy for hbase - " + suffix,
        "policyType": 0,
        "policyPriority": 0,
        "description": "HBase audit policy for functional-tests",
        "isAuditEnabled": True,
        "resources": {
            "table": {
                "values": ["*"],
                "isExcludes": False,
                "isRecursive": False,
            },
            "column-family": {
                "values": ["*"],
                "isExcludes": False,
                "isRecursive": False,
            },
            "column": {
                "values": ["*"],
                "isExcludes": False,
                "isRecursive": False,
            },
        },
        "policyItems": [
            {
                "accesses": accesses,
                "users": list(HBASE_AUDIT_POLICY_SERVICE_USERS),
                "groups": [],
                "conditions": [],
                "delegateAdmin": False,
            }
        ],
        "denyPolicyItems": [],
        "allowExceptions": [],
        "denyExceptions": [],
        "dataMaskPolicyItems": [],
        "rowFilterPolicyItems": [],
        "serviceType": "hbase",
        "options": {},
        "validitySchedules": [],
        "policyLabels": [""],
        "zoneName": "",
    }


def create_audit_only_policy(session, suffix=None, service_name=HBASE_SERVICE_NAME):
    if suffix is None:
        suffix = unique_suffix()
    payload = build_audit_only_policy_payload(service_name, suffix)
    response = session.post(POLICY_URL, json=payload)
    assert_http_ok(response, (200, 201), "Create audit-only hbase policy", POLICY_URL)
    body = response.json()
    policy_id = body.get("id")
    assert policy_id is not None, "Create policy response missing id: " + response.text
    return policy_id, suffix


def wait_for_hbase_policy_propagation(seconds=POLICY_PROPAGATION_SLEEP_SEC):
    logger.info("Waiting %ss for HBase plugin policy propagation", seconds)
    time.sleep(seconds)
