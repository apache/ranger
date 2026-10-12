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

"""Hive Docker helpers for Ranger functional-tests (HS2 + plugin enforcement)."""

import socket
import time

import pytest

from ranger_test_utils.utils import (
    DOCKER_PLUGIN_TEST_USER_PASSWORD,
    POLICY_URL,
    RANGER_ADMIN_BASE_URL,
    assert_http_ok,
    get_test_logger,
    unique_suffix,
)

logger = get_test_logger(__name__)

BEELINE_LOG_OUTPUT_MAX_CHARS = 12000
BEELINE_COMPLETE_LOG_MAX_CHARS = 100000

HIVE_SERVICE_NAME = "dev_hive"
HIVE_CONTAINER = "ranger-hive"
HADOOP_CONTAINER = "ranger-hadoop"
KDC_CONTAINER = "ranger-kdc"
ADMIN_CONTAINER = "ranger"

KERBEROS_REALM = "EXAMPLE.COM"
HIVE_HS2_PRINCIPAL = "hive/ranger-hive.rangernw@" + KERBEROS_REALM
HIVE_SERVICE_KEYTAB = "/etc/keytabs/hive.keytab"
HIVE_JDBC_BEELINE = (
    "jdbc:hive2://localhost:10000/default;principal=" + HIVE_HS2_PRINCIPAL
)

HIVE_TEST_USER = "hrt_21"
HIVE_TEST_USER_PASSWORD = DOCKER_PLUGIN_TEST_USER_PASSWORD

HIVE_AUTHZ_DENIED_SUBSTR = "Permission denied"

HIVE_AUDIT_POLICY_SERVICE_USERS = ["hive"]
HIVE_AUDIT_POLICY_ACCESS_TYPES = [
    "select",
    "update",
    "create",
    "drop",
    "alter",
    "index",
    "lock",
    "all",
]

POLICY_PROPAGATION_SLEEP_SEC = 15

_docker_client = None


def get_docker_client():
    global _docker_client
    if _docker_client is None:
        import docker

        _docker_client = docker.from_env()
    return _docker_client


def _container_running(name):
    client = get_docker_client()
    try:
        container = client.containers.get(name)
    except Exception:
        return False
    container.reload()
    return container.status == "running"


def _hs2_port_open(host="127.0.0.1", port=10000, timeout=2.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def preflight_hive_stack():
    """Skip tests when required Docker services or HS2 are unavailable."""
    required = [ADMIN_CONTAINER, KDC_CONTAINER, HADOOP_CONTAINER, HIVE_CONTAINER]
    missing = [name for name in required if not _container_running(name)]
    if missing:
        pytest.skip(
            "Hive functional-tests need running containers: "
            + ", ".join(missing)
            + ". Run ./run-tests.sh postgres hive from functional-tests/."
        )

    if not _hs2_port_open():
        pytest.skip(
            "HiveServer2 is not accepting connections on localhost:10000 "
            "(is ranger-hive healthy?)"
        )


def _user_principal(username):
    return username + "@" + KERBEROS_REALM


def _user_keytab_in_hive_container(username):
    return "/etc/keytabs/" + username + ".keytab"


def _log_multiline_block(header, text, max_chars=BEELINE_LOG_OUTPUT_MAX_CHARS):
    """Write multi-line command output to pytest.log (one line per source line)."""
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


def _log_text_block(title, text, max_chars=BEELINE_COMPLETE_LOG_MAX_CHARS):
    """Log a full stdout/stderr block."""
    logger.info("%s", title)
    body = text if text and text.strip() else "(empty)"
    if len(body) > max_chars:
        body = (
            body[:max_chars]
            + "\n... [truncated, total "
            + str(len(text))
            + " chars]"
        )
    for line in body.splitlines():
        logger.info("%s", line)


def _log_beeline_qe_style(exit_code, stdout, stderr, combined, sql, beeline_cmd, expect_substring=None):
    """beeline logging: query, command, exit code, stdout, stderr."""
    logger.info("Exit Code: %s", exit_code)
    _log_text_block("Output of beeline command is:", stdout)
    _log_text_block("Error of beeline command is:", stderr)

    if "Error:" in combined or "FAILED:" in combined:
        sql_result = "FAILED (server/beeline error in output)"
    elif exit_code != 0:
        sql_result = "FAILED (non-zero exit)"
    elif "INFO  : OK" in combined or "No rows affected" in combined:
        sql_result = "SUCCESS"
    else:
        sql_result = "UNKNOWN"

    logger.info(
        "Beeline summary: exit_code=%s sql_result=%s user_query=%s",
        exit_code,
        sql_result,
        sql,
    )
    logger.info("Beeline command: %s", beeline_cmd)

    if expect_substring:
        if expect_substring in combined:
            logger.info(
                'Found expected string "%s" in beeline output',
                expect_substring,
            )
        else:
            logger.info(
                'Expected string "%s" NOT found in beeline output',
                expect_substring,
            )


def ensure_kerberos_user(username):
    """Create a user Kerberos principal and keytab on KDC, visible in ranger-hive."""
    kdc = get_docker_client().containers.get(KDC_CONTAINER)
    principal = _user_principal(username)
    keytab_in_kdc = "/etc/keytabs/ranger-hive/" + username + ".keytab"
    keytab_in_hive = _user_keytab_in_hive_container(username)

    logger.info(
        "Kerberos setup on %s: addprinc/ktadd for %s -> %s (visible in hive as %s)",
        KDC_CONTAINER,
        principal,
        keytab_in_kdc,
        keytab_in_hive,
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
            + keytab_in_hive
            + "): "
            + kt_out
        )
    kdc.exec_run("chmod 444 " + keytab_in_kdc, user="root")
    verify_kerberos_keytab_in_hive(username)


def verify_kerberos_keytab_in_hive(username):
    """kinit in ranger-hive using the user's keytab (fail fast if keytab is bad)."""
    hive = get_docker_client().containers.get(HIVE_CONTAINER)
    principal = _user_principal(username)
    keytab = _user_keytab_in_hive_container(username)
    shell = (
        "kdestroy -A 2>/dev/null || true; "
        "kinit -kt "
        + keytab
        + " "
        + principal
        + "; klist"
    )
    exit_code, output = hive.exec_run(["bash", "-c", shell], user="hive", demux=True)
    combined = ((output[0] or b"") + (output[1] or b"")).decode()
    if exit_code != 0:
        raise RuntimeError(
            "Kerberos kinit failed in "
            + HIVE_CONTAINER
            + " for "
            + principal
            + ": "
            + combined
        )
    logger.info(
        "Verified Kerberos keytab for %s in %s: %s",
        principal,
        HIVE_CONTAINER,
        combined.strip().splitlines()[-1] if combined.strip() else "(ok)",
    )


def prepare_hive_test_namespace(database, table):
    """Create DB and ensure test table absent — runs as hive service user."""
    run_beeline(
        None,
        "create database if not exists " + database + ";",
        service_user=True,
    )
    run_beeline(
        None,
        "drop table if exists " + database + "." + table + ";",
        service_user=True,
    )


def cleanup_hive_test_namespace(database, table):
    """Drop test table and database — runs as hive service user."""
    run_beeline(
        None,
        "drop table if exists " + database + "." + table + ";",
        service_user=True,
    )
    run_beeline(
        None,
        "drop database if exists " + database + " cascade;",
        service_user=True,
    )


def run_beeline(username, sql, service_user=False, expect_substring=None):
    """
    Run a beeline statement inside ranger-hive (-e).

    Returns (exit_code, combined_output).
    """
    hive = get_docker_client().containers.get(HIVE_CONTAINER)
    if service_user:
        principal = HIVE_HS2_PRINCIPAL
        keytab = HIVE_SERVICE_KEYTAB
        ranger_user_label = "hive (service)"
    else:
        principal = _user_principal(username)
        keytab = _user_keytab_in_hive_container(username)
        ranger_user_label = username

    sql_stripped = sql.strip()
    logger.info("Going to run query is: %s", sql_stripped)
    logger.info(
        "Beeline context: container=%s docker_exec_user=hive ranger_user=%s",
        HIVE_CONTAINER,
        ranger_user_label,
    )
    logger.info(
        "Kerberos: kdestroy -A; kinit -kt %s %s",
        keytab,
        principal,
    )

    escaped_sql = sql_stripped.replace("\\", "\\\\").replace('"', '\\"')
    beeline_cmd = (
        '/opt/hive/bin/beeline -u "'
        + HIVE_JDBC_BEELINE
        + '" --outputformat=tsv2 -e "'
        + escaped_sql
        + '"'
    )
    logger.info("RUNNING: %s", beeline_cmd)

    # Clear shared hive-user credential cache so service-user and test-user runs
    # do not reuse each other's tickets (klist -s alone would skip kinit incorrectly).
    shell = (
        "kdestroy -A 2>/dev/null || true; "
        "kinit -kt "
        + keytab
        + " "
        + principal
        + "; "
        + "kinit_rc=$?; "
        + 'if [ "$kinit_rc" -ne 0 ]; then exit "$kinit_rc"; fi; '
        + beeline_cmd
    )
    exit_code, output = hive.exec_run(["bash", "-c", shell], user="hive", demux=True)
    stdout = (output[0] or b"").decode()
    stderr = (output[1] or b"").decode()
    combined = stdout + stderr

    _log_beeline_qe_style(
        exit_code,
        stdout,
        stderr,
        combined,
        sql_stripped,
        beeline_cmd,
        expect_substring=expect_substring,
    )

    return exit_code, combined


def list_policies_for_service(session, service_name=HIVE_SERVICE_NAME):
    url = RANGER_ADMIN_BASE_URL + "/plugins/policies/service/name/" + service_name
    response = session.get(url)
    assert_http_ok(response, 200, "List policies for " + service_name, url)
    return response.json().get("policies") or []


def set_all_policies_enabled(session, enabled, service_name=HIVE_SERVICE_NAME):
    policies = list_policies_for_service(session, service_name)
    for policy in policies:
        policy_id = policy.get("id")
        if policy_id is None:
            continue
        policy["isEnabled"] = enabled
        put_url = RANGER_ADMIN_BASE_URL + "/plugins/policies/" + str(policy_id)
        response = session.put(put_url, json=policy)
        assert_http_ok(
            response,
            200,
            "Set isEnabled=" + str(enabled) + " on policy " + str(policy_id),
            put_url,
        )


def build_audit_only_policy_payload(service_name, suffix):
    accesses = [
        {"type": perm, "isAllowed": True}
        for perm in HIVE_AUDIT_POLICY_ACCESS_TYPES
    ]
    return {
        "isEnabled": True,
        "service": service_name,
        "name": "audit policy for hive - " + suffix,
        "policyType": 0,
        "policyPriority": 0,
        "description": "Hive audit policy for functional-tests",
        "isAuditEnabled": True,
        "resources": {
            "database": {
                "values": ["*"],
                "isExcludes": False,
                "isRecursive": False,
            },
            "table": {
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
                "users": list(HIVE_AUDIT_POLICY_SERVICE_USERS),
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
        "serviceType": "hive",
        "options": {},
        "validitySchedules": [],
        "policyLabels": [""],
        "zoneName": "",
    }


def create_audit_only_policy(session, suffix=None, service_name=HIVE_SERVICE_NAME):
    if suffix is None:
        suffix = unique_suffix()
    payload = build_audit_only_policy_payload(service_name, suffix)
    response = session.post(POLICY_URL, json=payload)
    assert_http_ok(response, (200, 201), "Create audit-only hive policy", POLICY_URL)
    body = response.json()
    policy_id = body.get("id")
    assert policy_id is not None, "Create policy response missing id: " + response.text
    return policy_id, suffix


def delete_policies_by_id(session, policy_ids):
    for policy_id in policy_ids:
        url = POLICY_URL + str(policy_id)
        response = session.delete(url)
        assert_http_ok(response, (200, 204), "Delete policy " + str(policy_id), url)


def wait_for_policy_propagation(seconds=POLICY_PROPAGATION_SLEEP_SEC):
    logger.info("Waiting %ss for Hive plugin policy propagation", seconds)
    time.sleep(seconds)
