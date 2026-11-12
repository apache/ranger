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

"""Hive Docker helpers and declarative plugin scenarios (beeline, policies, ``HivePluginTest``)."""

import socket
import time

import pytest

from ranger_test_utils.plugin_test_users import PLUGIN_TEST_USER
from ranger_test_utils.plugin_enforcement_utils import (
    logger as plugin_enforcement_logger,
    normalize_audit_log_filter,
)
from ranger_test_utils.utils import (
    DOCKER_PLUGIN_TEST_USER_PASSWORD,
    POLICY_URL,
    PUBLIC_V2_BASE,
    RANGER_ADMIN_BASE_URL,
    assert_http_ok,
    get_test_logger,
    log_testcase_begin,
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

HIVE_TEST_USER = PLUGIN_TEST_USER
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

# HiveServer2 binds port 10000 only at the very end of a long startup
# (~60s in the docker stack), so a just-restarted container needs retries.
HS2_READY_TIMEOUT_SEC = 120
HS2_READY_POLL_SEC = 10

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


# Beeline/HS2 transport failures. These are infrastructure problems, not
# authorization decisions: when HS2 is down the output carries no
# "Permission denied", so a STDERR_NOT_CONTAINS assertion would pass falsely.
HIVE_INFRA_ERROR_SUBSTRINGS = (
    "Could not open client transport",
    "Could not open connection to the HS2 server",
    "Connection refused",
    "SocketTimeoutException",
    "GSS initiate failed",
    "Client cannot authenticate via",
)


def find_hive_infra_error(output):
    """Return the first infra-failure marker present in ``output``, else None."""
    text = output or ""
    for marker in HIVE_INFRA_ERROR_SUBSTRINGS:
        if marker in text:
            return marker
    return None


def _hs2_session_usable(timeout_sec=HS2_READY_TIMEOUT_SEC):
    """
    Open a real beeline session and run ``SHOW DATABASES;``.

    An open TCP port is not proof of readiness - HiveServer2 binds 10000 only
    after a long startup, and can also accept the socket while HDFS/Kerberos
    are still broken. Retries until ``timeout_sec`` so a just-restarted
    container is not reported as dead.

    A Ranger *authorization denial* counts as success: it proves the client
    connected, authenticated and reached policy evaluation. The service user
    legitimately has no policy before ``setup_module`` recreates it.
    """
    deadline = time.time() + timeout_sec
    last_error = "no attempt made"
    while True:
        exit_code, output = run_beeline(None, "SHOW DATABASES;", service_user=True)
        infra_error = find_hive_infra_error(output)
        reached_authorizer = HIVE_AUTHZ_DENIED_SUBSTR in (output or "")
        if infra_error is None and (exit_code == 0 or reached_authorizer):
            return True, ""
        last_error = infra_error or ("exit_code=" + str(exit_code))
        if time.time() >= deadline:
            return False, last_error
        logger.info(
            "HiveServer2 not ready yet (%s); retrying for %ss",
            last_error,
            int(max(0, deadline - time.time())),
        )
        time.sleep(HS2_READY_POLL_SEC)


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

    usable, reason = _hs2_session_usable()
    if not usable:
        pytest.skip(
            "HiveServer2 port 10000 is open but no beeline session could be "
            "established (" + reason + "). Check `docker logs ranger-hive` and "
            "that ranger-hadoop is healthy."
        )
    logger.info("HiveServer2 preflight OK: beeline session established")


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


def _log_beeline_run_detail(exit_code, stdout, stderr, combined, sql, beeline_cmd, expect_substring=None):
    """Log beeline query, command, exit code, stdout, and stderr."""
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
    from ranger_test_utils.plugin_test_users import (
        PLUGIN_KERBEROS_STACK_HIVE,
        ensure_kerberos_keytab_for_stack,
    )

    ensure_kerberos_keytab_for_stack(username, PLUGIN_KERBEROS_STACK_HIVE)


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

    _log_beeline_run_detail(
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


# --------------------------------------------------------------------------- #
# Generic policy helpers (used by the security-zone suites)
# --------------------------------------------------------------------------- #

ZONE_URL = PUBLIC_V2_BASE + "/zones"


def build_resource(values, is_excludes=False, is_recursive=False):
    """Build one Ranger policy resource entry."""
    return {
        "values": list(values),
        "isExcludes": is_excludes,
        "isRecursive": is_recursive,
    }


def build_policy_item(users=None, groups=None, accesses=None, delegate_admin=False):
    """Build one Ranger policy item (``accesses`` are access-type names)."""
    return {
        "accesses": [
            {"type": access, "isAllowed": True} for access in (accesses or ["select"])
        ],
        "users": list(users or []),
        "groups": list(groups or []),
        "conditions": [],
        "delegateAdmin": delegate_admin,
    }


def build_hive_policy_payload(
    service_name,
    policy_name,
    resources,
    policy_items,
    zone_name="",
    description=None,
):
    """Build a Ranger Hive access-policy payload (optionally scoped to a zone)."""
    return {
        "isEnabled": True,
        "service": service_name,
        "name": policy_name,
        "policyType": 0,
        "policyPriority": 0,
        "description": description or policy_name,
        "isAuditEnabled": True,
        "resources": resources,
        "policyItems": policy_items,
        "denyPolicyItems": [],
        "allowExceptions": [],
        "denyExceptions": [],
        "dataMaskPolicyItems": [],
        "rowFilterPolicyItems": [],
        "serviceType": "hive",
        "options": {},
        "validitySchedules": [],
        "policyLabels": [""],
        "zoneName": zone_name,
    }


def build_hive_table_policy_payload(
    service_name,
    policy_name,
    database,
    tables,
    users,
    columns=("*",),
    accesses=("select",),
    zone_name="",
    tables_excluded=False,
):
    """Shorthand for a db/table/column access policy granted to ``users``."""
    resources = {
        "database": build_resource([database]),
        "table": build_resource(tables, is_excludes=tables_excluded),
        "column": build_resource(columns),
    }
    return build_hive_policy_payload(
        service_name,
        policy_name,
        resources,
        [build_policy_item(users=users, accesses=list(accesses))],
        zone_name=zone_name,
    )


def create_policy(session, payload):
    """POST a policy payload; returns the created policy id."""
    response = session.post(POLICY_URL, json=payload)
    assert_http_ok(
        response, (200, 201), "Create policy " + str(payload.get("name")), POLICY_URL
    )
    policy_id = response.json().get("id")
    assert policy_id is not None, "Create policy response missing id: " + response.text
    logger.info("Created policy id=%s name=%s zone=%s",
                policy_id, payload.get("name"), payload.get("zoneName") or "-")
    return policy_id


def delete_all_policies_for_service(session, service_name=HIVE_SERVICE_NAME, zone_name=None):
    """Delete every policy of a service, optionally only those inside a zone."""
    url = RANGER_ADMIN_BASE_URL + "/public/v2/api/service/" + service_name + "/policy"
    params = {"zoneName": zone_name} if zone_name else None
    response = session.get(url, params=params)
    assert_http_ok(response, 200, "List policies of " + service_name, url)
    policies = response.json() or []
    policy_ids = [policy["id"] for policy in policies if policy.get("id") is not None]
    logger.info(
        "Deleting %s policies of %s (zone=%s)",
        len(policy_ids),
        service_name,
        zone_name or "-",
    )
    delete_policies_by_id(session, policy_ids)
    return policy_ids


# --------------------------------------------------------------------------- #
# Security zone helpers
# --------------------------------------------------------------------------- #

def build_zone_payload(zone_name, services, admin_users=("admin",), audit_users=("admin",),
                       description=None):
    """
    Build a security-zone payload.

    ``services`` maps a Ranger service name to its zone resources, e.g.::

        {"dev_hive": {"resources": [{"database": ["finance"],
                                     "table": ["*"], "column": ["*"]}]}}
    """
    return {
        "name": zone_name,
        "description": description or zone_name,
        "adminUsers": list(admin_users),
        "adminUserGroups": [],
        "auditUsers": list(audit_users),
        "auditUserGroups": [],
        "services": services,
    }


def get_security_zone_by_name(session, zone_name):
    """Return the zone dict, or None when the zone does not exist."""
    url = ZONE_URL + "/name/" + zone_name
    response = session.get(url)
    if response.status_code in (400, 404):
        return None
    assert_http_ok(response, 200, "Get zone " + zone_name, url)
    return response.json()


def delete_security_zone(session, zone_id):
    url = ZONE_URL + "/" + str(zone_id)
    response = session.delete(url)
    assert_http_ok(response, (200, 204), "Delete zone " + str(zone_id), url)
    logger.info("Deleted security zone id=%s", zone_id)


def create_security_zone(session, payload):
    """POST a zone payload; returns the created zone dict."""
    response = session.post(ZONE_URL, json=payload)
    assert_http_ok(response, (200, 201), "Create zone " + payload["name"], ZONE_URL)
    zone = response.json()
    logger.info("Created security zone id=%s name=%s", zone.get("id"), zone.get("name"))
    return zone


def recreate_security_zone(session, payload):
    """
    Create a zone, dropping a leftover zone of the same name first.

    Ranger rejects duplicates with "Another security zone already exists for
    this name", so a failed previous run would otherwise block the suite.
    """
    existing = get_security_zone_by_name(session, payload["name"])
    if existing and existing.get("id") is not None:
        logger.info("Zone %s already exists (id=%s) - recreating",
                    payload["name"], existing["id"])
        delete_security_zone(session, existing["id"])
    return create_security_zone(session, payload)


def execute_hive_test(
    sql,
    username=HIVE_TEST_USER,
    *,
    admin_session=None,
    verify_output_contains=None,
    verify_output_not_contains=None,
    audit_filter=None,
    expect_audit_event=True,
    repo_name=HIVE_SERVICE_NAME,
):
    """
    Run beeline as ``username``, assert on CLI output, optionally validate access audits.

    Uses :func:`ranger_test_utils.access_audit_utils.begin_access_audit_capture`
    for Solr/OpenSearch-backed audit checks (shared with other plugin suites).
    """
    from ranger_test_utils.access_audit_utils import begin_access_audit_capture

    audit_capture = None
    if audit_filter is not None and admin_session is not None:
        audit_capture = begin_access_audit_capture(
            admin_session,
            repo_name,
            username,
        )

    exit_code, output = run_beeline(
        username,
        sql,
        expect_substring=verify_output_contains,
    )

    # A transport failure produces output with no "Permission denied" in it, so
    # a STDERR_NOT_CONTAINS check would pass without the query ever running.
    # Report it as an infrastructure failure instead of a bogus authz result.
    infra_error = find_hive_infra_error(output)
    assert infra_error is None, (
        "HiveServer2 is unreachable ("
        + infra_error
        + ") - this is an infrastructure failure, not an authorization result. "
        + "sql="
        + str(sql)
        + " user="
        + str(username)
        + " exit_code="
        + str(exit_code)
        + " output="
        + output
    )

    if verify_output_contains is not None:
        assert verify_output_contains in output, (
            "Expected text not found in beeline output; exit_code="
            + str(exit_code)
            + " output="
            + output
        )
    if verify_output_not_contains is not None:
        assert verify_output_not_contains not in output, (
            "Unexpected text in beeline output; exit_code="
            + str(exit_code)
            + " output="
            + output
        )

    if audit_capture is not None:
        audit_capture.verify(
            audit_filter,
            expect_audit_event=expect_audit_event,
        )

    return exit_code, output


class HivePluginTestContext:
    """
    Per-module context (assign ``admin_session`` in ``setup_module``).

    Class attributes mirror common ranger-qe ``XAHiveTestCase`` fields used in ports.
    """

    USER_USER1 = HIVE_TEST_USER
    USER_USER2 = "hrt_22"
    PASS_USER1 = None
    PASS_USER2 = None

    result_denied = "0"
    result_allowed = "1"

    access_type = {
        "CREATE": "CREATE",
        "USE": "USE",
        "DROP": "DROP",
        "SELECT": "SELECT",
        "UPDATE": "UPDATE",
        "ALTER": "ALTER",
        "INSERT": "INSERT",
        "DELETE": "DELETE",
        "GRANT": "GRANT",
        "REVOKE": "REVOKE",
    }

    def __init__(self):
        self.admin_session = None
        self.repo_name = HIVE_SERVICE_NAME


class HivePluginTest:
    """One Hive enforcement scenario: log id, beeline SQL, verify output, optional audit."""

    STDERR_CONTAINS = "stderr_contains"
    STDOUT_CONTAINS = "stdout_contains"
    STDERR_NOT_CONTAINS = "stderr_not_contains"
    STDOUT_NOT_CONTAINS = "stdout_not_contains"

    def __init__(
        self,
        test_id,
        test_case,
        sql,
        req_user,
        req_user_pass=None,
        *,
        verify_criteria=STDERR_CONTAINS,
        verify_str=None,
        new_audit_log_must_exist=True,
        audit_log_filter=None,
        validate_impala=False,
        verifyCriteria=None,
        verifyStr=None,
        newAuditLogMustExist=None,
        auditLogFilter=None,
        validateImpala=None,
        hSql=None,
    ):
        self.test_id = test_id
        self.test_case = test_case
        self.sql = hSql if hSql is not None else sql
        self.req_user = req_user
        self.req_user_pass = req_user_pass

        self.verify_criteria = (
            verifyCriteria if verifyCriteria is not None else verify_criteria
        )
        self.verify_str = (
            verifyStr
            if verifyStr is not None
            else (verify_str if verify_str is not None else HIVE_AUTHZ_DENIED_SUBSTR)
        )
        self.new_audit_log_must_exist = (
            newAuditLogMustExist
            if newAuditLogMustExist is not None
            else new_audit_log_must_exist
        )
        self.audit_log_filter = (
            auditLogFilter if auditLogFilter is not None else audit_log_filter
        )
        self.validate_impala = (
            validateImpala if validateImpala is not None else validate_impala
        )

    def execute(self, zone_name=None):
        """
        Run the scenario.

        ``zone_name`` names the security zone whose policy is expected to
        decide this request; it is logged (and added to the audit filter as
        ``zoneName``) so a zone scenario can be told apart from a non-zone one.
        """
        if self.validate_impala:
            pytest.skip("Impala validation is not enabled in Docker functional-tests")

        log_testcase_begin(plugin_enforcement_logger, self.test_id)
        if zone_name:
            plugin_enforcement_logger.info(
                "Security zone under test: %s", zone_name
            )

        verify_contains = None
        verify_not_contains = None
        if self.verify_criteria in (
            self.STDERR_CONTAINS,
            self.STDOUT_CONTAINS,
        ):
            verify_contains = self.verify_str
        elif self.verify_criteria in (
            self.STDERR_NOT_CONTAINS,
            self.STDOUT_NOT_CONTAINS,
        ):
            verify_not_contains = self.verify_str
        else:
            raise ValueError("Unsupported verify_criteria: " + str(self.verify_criteria))

        admin_session = getattr(self.test_case, "admin_session", None)
        repo_name = getattr(self.test_case, "repo_name", HIVE_SERVICE_NAME)
        audit_filter = normalize_audit_log_filter(self.audit_log_filter)

        execute_hive_test(
            self.sql,
            username=self.req_user,
            admin_session=admin_session,
            verify_output_contains=verify_contains,
            verify_output_not_contains=verify_not_contains,
            audit_filter=audit_filter,
            expect_audit_event=self.new_audit_log_must_exist,
            repo_name=repo_name,
        )
