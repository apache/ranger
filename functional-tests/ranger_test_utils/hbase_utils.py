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

"""HBase Docker helpers and declarative plugin scenarios (shell, policies, ``HBasePluginTest``)."""

import base64
import socket
import time

import pytest

from ranger_test_utils.plugin_enforcement_utils import (
    logger as plugin_enforcement_logger,
    normalize_audit_log_filter,
)
from ranger_test_utils.utils import (
    POLICY_URL,
    assert_http_ok,
    get_test_logger,
    log_testcase_begin,
    unique_suffix,
)
from ranger_test_utils.hive_utils import (
    KDC_CONTAINER,
    POLICY_PROPAGATION_SLEEP_SEC,
    get_docker_client,
)
from ranger_test_utils.plugin_test_users import PLUGIN_TEST_USER

logger = get_test_logger(__name__)

HBASE_SHELL_LOG_OUTPUT_MAX_CHARS = 12000

HBASE_SERVICE_NAME = "dev_hbase"
HBASE_CONTAINER = "ranger-hbase"
HADOOP_CONTAINER = "ranger-hadoop"
ADMIN_CONTAINER = "ranger"

KERBEROS_REALM = "EXAMPLE.COM"
HBASE_SERVICE_PRINCIPAL = "hbase/ranger-hbase.rangernw@" + KERBEROS_REALM
HBASE_SERVICE_KEYTAB = "/etc/keytabs/hbase.keytab"

HBASE_TEST_USER = PLUGIN_TEST_USER

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
    from ranger_test_utils.plugin_test_users import (
        PLUGIN_KERBEROS_STACK_HBASE,
        ensure_kerberos_keytab_for_stack,
    )

    ensure_kerberos_keytab_for_stack(username, PLUGIN_KERBEROS_STACK_HBASE)


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


def execute_hbase_test(
    shell_lines,
    username=HBASE_TEST_USER,
    *,
    admin_session=None,
    verify_output_contains=None,
    verify_output_not_contains=None,
    audit_filter=None,
    expect_audit_event=True,
    repo_name=HBASE_SERVICE_NAME,
    service_user=False,
):
    """
    Run HBase shell as ``username``, assert on output, optionally validate access audits.

    Uses :func:`ranger_test_utils.access_audit_utils.begin_access_audit_capture`
    (Solr/OpenSearch via Admin REST when the audit pipeline is enabled).
    """
    from ranger_test_utils.access_audit_utils import begin_access_audit_capture

    if isinstance(shell_lines, str):
        shell_lines = [shell_lines]

    audit_capture = None
    if (
        audit_filter is not None
        and admin_session is not None
        and not service_user
    ):
        audit_capture = begin_access_audit_capture(
            admin_session,
            repo_name,
            username,
        )

    exit_code, output = run_hbase_shell(
        username,
        shell_lines,
        service_user=service_user,
        expect_substring=verify_output_contains,
    )

    if verify_output_contains is not None:
        assert verify_output_contains in output, (
            "Expected text not found in HBase shell output; exit_code="
            + str(exit_code)
            + " output="
            + output
        )
    if verify_output_not_contains is not None:
        assert verify_output_not_contains not in output, (
            "Unexpected text in HBase shell output; exit_code="
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


class HBasePluginTestContext:
    """
    Per-module context (assign ``admin_session`` in ``setup_module``).

    Class attributes mirror common ranger-qe ``XAHBaseTestCase`` fields used in ports.
    """

    USER_USER1 = HBASE_TEST_USER
    USER_USER2 = "hrt_22"

    result_denied = "0"
    result_allowed = "1"

    access_type = {
        "CREATE_TABLE": "createTable",
        "PUT": "put",
        "SCAN": "scannerOpen",
        "GET": "get",
        "DISABLE_TABLE": "disableTable",
        "DELETE_TABLE": "deleteTable",
        "GRANT": "grant",
        "REVOKE": "revoke",
    }

    def __init__(self):
        self.admin_session = None
        self.repo_name = HBASE_SERVICE_NAME


class HBasePluginTest:
    """One HBase enforcement scenario: log id, shell cmds, verify output, optional audit."""

    STDERR_CONTAINS = "stderr_contains"
    STDOUT_CONTAINS = "stdout_contains"
    STDERR_NOT_CONTAINS = "stderr_not_contains"
    STDOUT_NOT_CONTAINS = "stdout_not_contains"

    def __init__(
        self,
        test_id,
        test_case,
        shell_cmds=None,
        req_user=None,
        *,
        verify_criteria=STDOUT_CONTAINS,
        verify_str=None,
        new_audit_log_must_exist=True,
        audit_log_filter=None,
        logoutput=True,
        verifyCriteria=None,
        verifyStr=None,
        newAuditLogMustExist=None,
        auditLogFilter=None,
        hBaseCmds=None,
        reqUser=None,
        checkAuditLog=None,
    ):
        self.test_id = test_id
        self.test_case = test_case
        if hBaseCmds is not None:
            self.shell_cmds = hBaseCmds
        elif shell_cmds is not None:
            self.shell_cmds = shell_cmds
        else:
            raise ValueError("Provide shell_cmds or hBaseCmds")
        self.req_user = reqUser if reqUser is not None else req_user
        if self.req_user is None:
            raise ValueError("Provide req_user or reqUser")
        self.logoutput = logoutput

        self.verify_criteria = (
            verifyCriteria if verifyCriteria is not None else verify_criteria
        )
        self.verify_str = (
            verifyStr
            if verifyStr is not None
            else (verify_str if verify_str is not None else HBASE_AUTHZ_DENIED_SUBSTR)
        )
        self.new_audit_log_must_exist = (
            newAuditLogMustExist
            if newAuditLogMustExist is not None
            else new_audit_log_must_exist
        )
        self.audit_log_filter = (
            auditLogFilter if auditLogFilter is not None else audit_log_filter
        )
        _ = checkAuditLog

    def execute(self):
        log_testcase_begin(plugin_enforcement_logger, self.test_id)

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
        repo_name = getattr(self.test_case, "repo_name", HBASE_SERVICE_NAME)
        audit_filter = normalize_audit_log_filter(self.audit_log_filter)

        execute_hbase_test(
            self.shell_cmds,
            username=self.req_user,
            admin_session=admin_session,
            verify_output_contains=verify_contains,
            verify_output_not_contains=verify_not_contains,
            audit_filter=audit_filter,
            expect_audit_event=self.new_audit_log_must_exist,
            repo_name=repo_name,
        )
