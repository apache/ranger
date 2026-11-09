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

"""HDFS Docker helpers for Ranger functional-tests (dfs + plugin enforcement)."""

import socket
import subprocess
import tempfile
import time

import pytest

from ranger_test_utils.utils import (
    POLICY_URL,
    assert_http_ok,
    get_test_logger,
    perm_map_list_to_policy_items,
)

logger = get_test_logger(__name__)

HDFS_SERVICE_NAME = "dev_hdfs"
HADOOP_CONTAINER = "ranger-hadoop"
KDC_CONTAINER = "ranger-kdc"
ADMIN_CONTAINER = "ranger"

KERBEROS_REALM = "EXAMPLE.COM"
HDFS_SERVICE_USER = "hdfs"
HDFS_SERVICE_PRINCIPAL = "hdfs/ranger-hadoop.rangernw@" + KERBEROS_REALM
HDFS_SERVICE_KEYTAB = "/etc/keytabs/hdfs.keytab"

USER_FINANCE = "hrt_21"
USER_AUDIT = "hrt_22"
GROUP_FINANCE = "finance"
GROUP_AUDIT = "audit"

HDFS_AUTHZ_DENIED_SUBSTR = "Permission denied"

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


def _namenode_rpc_port_open(host="127.0.0.1", port=9000, timeout=2.0):
    """NameNode RPC — published by docker-compose.ranger-hadoop.yml (9000:9000)."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def preflight_hdfs_plugin_stack():
    """Skip tests when Ranger Admin, KDC, or ranger-hadoop are unavailable."""
    required = [ADMIN_CONTAINER, KDC_CONTAINER, HADOOP_CONTAINER]
    missing = [name for name in required if not _container_running(name)]
    if missing:
        pytest.skip(
            "HDFS plugin functional-tests need running containers: "
            + ", ".join(missing)
            + ". Run ./run-tests.sh postgres hdfs with SELECTED_CONTAINERS=hadoop."
        )

    if not _namenode_rpc_port_open():
        pytest.skip(
            "HDFS NameNode RPC is not accepting connections on localhost:9000 "
            "(is ranger-hadoop up and healthy? export SELECTED_CONTAINERS=hadoop)."
        )


def _user_principal(username):
    return username + "@" + KERBEROS_REALM


def _user_keytab_in_hadoop_container(username):
    return "/etc/keytabs/" + username + ".keytab"


def ensure_unix_user_with_primary_group(container, username, primary_group):
    """Create Linux user/group on ranger-hadoop for UGI group resolution."""
    container.exec_run("groupadd -f " + primary_group, user="root")
    exit_code, _ = container.exec_run("id -u " + username, user="root")
    if exit_code == 0:
        container.exec_run(
            "usermod -g " + primary_group + " " + username,
            user="root",
        )
        container.exec_run("usermod -aG hadoop " + username, user="root")
        return

    container.exec_run(
        "useradd -m -s /bin/bash -g "
        + primary_group
        + " -G hadoop "
        + username,
        user="root",
    )


def ensure_kerberos_user_on_hadoop(username):
    """Create user@REALM principal and install keytab on ranger-hadoop."""
    kdc = get_docker_client().containers.get(KDC_CONTAINER)
    hadoop = get_docker_client().containers.get(HADOOP_CONTAINER)
    principal = _user_principal(username)
    keytab_name = username + ".keytab"
    kdc_tmp = "/tmp/" + keytab_name
    dest_path = _user_keytab_in_hadoop_container(username)

    exit_code, output = kdc.exec_run(
        'kadmin.local -q "getprinc ' + principal + '"',
        user="root",
    )
    combined = (output or b"").decode()
    if exit_code != 0 or "does not exist" in combined:
        ec, out = kdc.exec_run(
            'kadmin.local -q "addprinc -randkey ' + principal + '"',
            user="root",
        )
        if ec != 0:
            raise RuntimeError(
                "addprinc failed for " + principal + ": " + (out or b"").decode()
            )

    kdc.exec_run("rm -f " + kdc_tmp, user="root")
    ec, out = kdc.exec_run(
        'kadmin.local -q "xst -k ' + kdc_tmp + " " + principal + '"',
        user="root",
    )
    if ec != 0:
        raise RuntimeError(
            "keytab export failed for " + principal + ": " + (out or b"").decode()
        )

    local = tempfile.NamedTemporaryFile(delete=False, suffix=".keytab")
    local.close()
    try:
        subprocess.check_call(
            ["docker", "cp", KDC_CONTAINER + ":" + kdc_tmp, local.name]
        )
        subprocess.check_call(
            ["docker", "cp", local.name, HADOOP_CONTAINER + ":" + dest_path]
        )
    finally:
        import os

        os.remove(local.name)

    hadoop.exec_run("chmod 444 " + dest_path, user="root")
    hadoop.exec_run("chown " + username + ":" + username + " " + dest_path, user="root")
    logger.info("Kerberos keytab installed for %s at %s", principal, dest_path)


def _log_hdfs_output(exit_code, stdout, stderr, cmd):
    logger.info("HDFS command exit_code=%s cmd=%s", exit_code, cmd)
    for label, text in (("stdout", stdout), ("stderr", stderr)):
        body = text if text else "(empty)"
        logger.info("HDFS %s:", label)
        for line in body.splitlines():
            logger.info("  %s", line)


def run_hdfs_dfs(
    username,
    dfs_subcommand,
    service_user=False,
    fail_on_error=True,
):
    """
    Run `hdfs dfs <dfs_subcommand>` inside ranger-hadoop.

    Returns (exit_code, combined_stdout_stderr).
    """
    hadoop = get_docker_client().containers.get(HADOOP_CONTAINER)
    if service_user:
        principal = HDFS_SERVICE_PRINCIPAL
        keytab = HDFS_SERVICE_KEYTAB
        exec_user = HDFS_SERVICE_USER
        ranger_user_label = HDFS_SERVICE_USER
    else:
        principal = _user_principal(username)
        keytab = _user_keytab_in_hadoop_container(username)
        exec_user = username
        ranger_user_label = username

    cmd = "hdfs dfs " + dfs_subcommand
    shell = (
        "kdestroy -A 2>/dev/null || true; "
        "kinit -kt "
        + keytab
        + " "
        + principal
        + "; "
        + 'kinit_rc=$?; if [ "$kinit_rc" -ne 0 ]; then exit "$kinit_rc"; fi; '
        + 'HADOOP_OPTS="-Dhadoop.security.authentication=kerberos" '
        + cmd
    )
    logger.info(
        "HDFS context: container=%s exec_user=%s ranger_user=%s",
        HADOOP_CONTAINER,
        exec_user,
        ranger_user_label,
    )
    exit_code, output = hadoop.exec_run(["bash", "-c", shell], user=exec_user, demux=True)
    stdout = (output[0] or b"").decode()
    stderr = (output[1] or b"").decode()
    combined = stdout + stderr
    _log_hdfs_output(exit_code, stdout, stderr, cmd)

    if fail_on_error and exit_code != 0:
        raise AssertionError(
            "HDFS command failed (exit "
            + str(exit_code)
            + "): "
            + cmd
            + "\n"
            + combined
        )
    return exit_code, combined


def cleanup_hdfs_paths(paths):
    """Remove HDFS paths if present (as hdfs service user)."""
    for path in paths:
        run_hdfs_dfs(
            HDFS_SERVICE_USER,
            "-rm -r -f " + path,
            service_user=True,
            fail_on_error=False,
        )


def build_hdfs_policy_payload(
    policy_name,
    path_values,
    perm_map_list,
    description="",
    is_recursive=False,
    is_audit_enabled=True,
    service_name=HDFS_SERVICE_NAME,
):
    """JSON body for POST/PUT on /public/v2/api/policy/ (HDFS path resource)."""
    return {
        "service": service_name,
        "name": policy_name,
        "description": description,
        "isEnabled": True,
        "isAuditEnabled": is_audit_enabled,
        "resources": {
            "path": {
                "values": path_values,
                "isExcludes": False,
                "isRecursive": is_recursive,
            }
        },
        "policyItems": perm_map_list_to_policy_items(perm_map_list),
    }


def create_hdfs_policy(session, payload):
    response = session.post(POLICY_URL, json=payload)
    assert_http_ok(response, (200, 201), "Create HDFS policy", POLICY_URL)
    body = response.json()
    policy_id = body.get("id")
    assert policy_id is not None, "Create policy response missing id: " + response.text
    return policy_id


def wait_for_hdfs_policy_propagation(seconds=POLICY_PROPAGATION_SLEEP_SEC):
    logger.info("Waiting %ss for HDFS plugin policy propagation", seconds)
    time.sleep(seconds)
