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
Shared Ranger xusers and Kerberos principals for Docker plugin enforcement suites.

Call ``ensure_plugin_test_users`` from suite ``conftest.py`` (session scope) or
``setup_module`` before service-specific policy setup. Kerberos uses ``ktadd
-norandkey`` on KDC volume paths so the same principal can receive keytabs on
multiple stacks without invalidating existing keytabs.
"""

import subprocess
import sys
import tempfile

from ranger_test_utils.utils import (
    ARGUS_POLICY_USERS,
    DOCKER_PLUGIN_TEST_USER_PASSWORD,
    ensure_group_exists,
    ensure_user_assigned_to_groups,
    ensure_user_exists,
    get_test_logger,
)

logger = get_test_logger(__name__)

KDC_CONTAINER = "ranger-kdc"
HADOOP_CONTAINER = "ranger-hadoop"
KERBEROS_REALM = "EXAMPLE.COM"

# Primary enforcement user (Hive/HBase tests); second user used by HDFS/Kafka deny cases.
PLUGIN_TEST_USER = ARGUS_POLICY_USERS[0]
PLUGIN_TEST_USER_ALT = ARGUS_POLICY_USERS[1]

# Policy / multi-user Hive-HDFS scenarios in ranger-qe (hrt_1 .. hrt_10).
PLUGIN_HRT_USERS_1_TO_10 = ["hrt_" + str(i) for i in range(1, 11)]

# Ranger xusers provisioned on QE clusters (ansible create-test-users role test_users).
# Source: ranger-qe/.qaas/ansible/roles/create-test-users/vars/main.yml
PLUGIN_RANGER_CLUSTER_TEST_USERS = [
    "hiveuser",
    "hrt_21",
    "hrt_22",
    "hivetest",
    "impala",
    "hbaseuser",
    "hrt_qa",
]


def _unique_usernames_in_order(names):
    seen = set()
    result = []
    for name in names:
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result


# Default Ranger xusers for plugin session setup: hrt_1..10 plus QE cluster test_users.
PLUGIN_RANGER_DEFAULT_TEST_USERS = _unique_usernames_in_order(
    PLUGIN_HRT_USERS_1_TO_10 + PLUGIN_RANGER_CLUSTER_TEST_USERS
)

# Subset that need Kerberos keytabs for current Docker plugin enforcement suites.
PLUGIN_KERBEROS_TEST_USERS = list(ARGUS_POLICY_USERS)

# Ranger group membership aligned with QE ranger_test_users_groups (hrt_21 / hrt_22).
PLUGIN_RANGER_USER_GROUP_MAP = {
    "hrt_21": ["finance"],
    "hrt_22": ["audit"],
}

# Kerberos stacks plugin suites can request (see ``KDC_VOLUME_KEYTAB_DIRS`` / hadoop).
PLUGIN_KERBEROS_STACK_HIVE = "hive"
PLUGIN_KERBEROS_STACK_HBASE = "hbase"
PLUGIN_KERBEROS_STACK_KAFKA = "kafka"
PLUGIN_KERBEROS_STACK_HADOOP = "hadoop"

PLUGIN_KERBEROS_STACKS_ALL = (
    PLUGIN_KERBEROS_STACK_HIVE,
    PLUGIN_KERBEROS_STACK_HBASE,
    PLUGIN_KERBEROS_STACK_KAFKA,
    PLUGIN_KERBEROS_STACK_HADOOP,
)

# KDC paths are volume-mounted into service containers as /etc/keytabs/<user>.keytab
KDC_VOLUME_KEYTAB_DIRS = {
    PLUGIN_KERBEROS_STACK_HIVE: "/etc/keytabs/ranger-hive",
    PLUGIN_KERBEROS_STACK_HBASE: "/etc/keytabs/ranger-hbase",
    PLUGIN_KERBEROS_STACK_KAFKA: "/etc/keytabs/ranger-kafka",
}

_docker_client = None


def get_docker_client():
    global _docker_client
    if _docker_client is None:
        import docker

        _docker_client = docker.from_env(timeout=10)
        try:
            _docker_client.ping()
        except Exception as exc:
            raise RuntimeError(
                "Docker is not available (is the daemon running?): " + str(exc)
            ) from exc
    return _docker_client


def user_kerberos_principal(username):
    return username + "@" + KERBEROS_REALM


def user_keytab_in_service_container(username):
    return "/etc/keytabs/" + username + ".keytab"


def ensure_plugin_ranger_groups(session):
    """Create finance/audit groups and assign hrt_21 / hrt_22 like QE cluster seed."""
    for group_name in ("finance", "audit"):
        ensure_group_exists(session, group_name)
    for username, group_names in PLUGIN_RANGER_USER_GROUP_MAP.items():
        ensure_user_assigned_to_groups(session, username, group_names)


def ensure_plugin_test_users(
    session,
    usernames=None,
    kerberos_usernames=None,
    kerberos_stacks=None,
    ranger_password=DOCKER_PLUGIN_TEST_USER_PASSWORD,
    verify_keytabs=True,
    ensure_groups=True,
):
    """
    Create Ranger xusers and Kerberos keytabs for plugin enforcement tests.

    Parameters
    ----------
    session:
        Ranger Admin ``requests.Session`` from ``create_ranger_admin_session``.
    usernames:
        Ranger user names to ensure (default: ``PLUGIN_RANGER_DEFAULT_TEST_USERS``:
        ``hrt_1``..``hrt_10`` plus ranger-qe ansible ``test_users``).
    kerberos_usernames:
        Users to receive keytabs (default: ``PLUGIN_KERBEROS_TEST_USERS`` / hrt_21, hrt_22).
    kerberos_stacks:
        Subset of ``PLUGIN_KERBEROS_STACKS_ALL``; default is Hive only.
    verify_keytabs:
        When True, kinit in the target container after each stack keytab is installed.
    ensure_groups:
        When True, ensure finance/audit groups and hrt_21/hrt_22 membership.
    """
    if usernames is None:
        usernames = list(PLUGIN_RANGER_DEFAULT_TEST_USERS)
    if kerberos_usernames is None:
        kerberos_usernames = list(PLUGIN_KERBEROS_TEST_USERS)
    if kerberos_stacks is None:
        kerberos_stacks = (PLUGIN_KERBEROS_STACK_HIVE,)

    logger.info(
        "Ensuring plugin test users in Ranger: %s; Kerberos users: %s; stacks: %s",
        usernames,
        kerberos_usernames,
        kerberos_stacks,
    )
    for username in usernames:
        print(
            "[plugin-setup] Ranger user: " + username,
            file=sys.stderr,
            flush=True,
        )
        ensure_user_exists(
            session,
            username,
            password=ranger_password,
            last_name="plugin_test",
        )

    if ensure_groups:
        print("[plugin-setup] Ranger groups: finance, audit", file=sys.stderr, flush=True)
        ensure_plugin_ranger_groups(session)

    for username in kerberos_usernames:
        for stack in kerberos_stacks:
            print(
                "[plugin-setup] Kerberos "
                + username
                + " @ stack "
                + stack,
                file=sys.stderr,
                flush=True,
            )
            ensure_kerberos_keytab_for_stack(
                username,
                stack,
                verify=verify_keytabs,
            )


def ensure_kerberos_keytab_for_stack(username, stack, verify=True):
    """Idempotent principal + keytab for one plugin stack."""
    ensure_kerberos_principal(username)
    if stack in KDC_VOLUME_KEYTAB_DIRS:
        _install_keytab_on_kdc_volume(username, KDC_VOLUME_KEYTAB_DIRS[stack])
    elif stack == PLUGIN_KERBEROS_STACK_HADOOP:
        _install_keytab_on_hadoop_container(username)
    else:
        raise ValueError("Unknown Kerberos stack: " + str(stack))

    if not verify:
        return

    if stack == PLUGIN_KERBEROS_STACK_HIVE:
        from ranger_test_utils.hive_utils import verify_kerberos_keytab_in_hive

        verify_kerberos_keytab_in_hive(username)
    elif stack == PLUGIN_KERBEROS_STACK_HBASE:
        from ranger_test_utils.hbase_utils import verify_kerberos_keytab_in_hbase

        verify_kerberos_keytab_in_hbase(username)
    elif stack == PLUGIN_KERBEROS_STACK_KAFKA:
        _verify_kerberos_keytab_in_kafka(username)
    elif stack == PLUGIN_KERBEROS_STACK_HADOOP:
        _verify_kerberos_keytab_in_hadoop(username)


def ensure_kerberos_principal(username):
    """Create user@REALM in KDC if it does not exist."""
    kdc = get_docker_client().containers.get(KDC_CONTAINER)
    principal = user_kerberos_principal(username)
    exit_code, output = kdc.exec_run(
        'kadmin.local -q "getprinc ' + principal + '"',
        user="root",
    )
    combined = (output or b"").decode()
    if exit_code == 0 and "does not exist" not in combined:
        logger.info("Kerberos principal already exists: %s", principal)
        return

    exit_add, out_add = kdc.exec_run(
        'kadmin.local -q "addprinc -randkey ' + principal + '"',
        user="root",
        demux=True,
    )
    add_out = ((out_add[0] or b"") + (out_add[1] or b"")).decode()
    if exit_add != 0:
        raise RuntimeError(
            "addprinc failed for " + principal + ": " + add_out
        )
    logger.info("Created Kerberos principal: %s", principal)


def _install_keytab_on_kdc_volume(username, kdc_keytab_dir):
    kdc = get_docker_client().containers.get(KDC_CONTAINER)
    principal = user_kerberos_principal(username)
    keytab_path = kdc_keytab_dir + "/" + username + ".keytab"

    logger.info(
        "Kerberos keytab on KDC volume: principal=%s path=%s",
        principal,
        keytab_path,
    )
    kdc.exec_run("rm -f " + keytab_path, user="root")
    exit_code, output = kdc.exec_run(
        'kadmin.local -q "ktadd -norandkey -k '
        + keytab_path
        + " "
        + principal
        + '"',
        user="root",
        demux=True,
    )
    kt_out = ((output[0] or b"") + (output[1] or b"")).decode()
    if exit_code != 0:
        raise RuntimeError(
            "ktadd failed for " + principal + " at " + keytab_path + ": " + kt_out
        )
    kdc.exec_run("chmod 444 " + keytab_path, user="root")


def _install_keytab_on_hadoop_container(username):
    kdc = get_docker_client().containers.get(KDC_CONTAINER)
    hadoop = get_docker_client().containers.get(HADOOP_CONTAINER)
    principal = user_kerberos_principal(username)
    keytab_name = username + ".keytab"
    kdc_tmp = "/tmp/" + keytab_name
    dest_path = user_keytab_in_service_container(username)

    ensure_kerberos_principal(username)

    kdc.exec_run("rm -f " + kdc_tmp, user="root")
    exit_code, output = kdc.exec_run(
        'kadmin.local -q "xst -k ' + kdc_tmp + " " + principal + '"',
        user="root",
    )
    if exit_code != 0:
        raise RuntimeError(
            "keytab export failed for "
            + principal
            + ": "
            + (output or b"").decode()
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
    hadoop.exec_run(
        "chown " + username + ":" + username + " " + dest_path,
        user="root",
    )
    logger.info(
        "Kerberos keytab on %s: principal=%s path=%s",
        HADOOP_CONTAINER,
        principal,
        dest_path,
    )


def _verify_kerberos_keytab_in_kafka(username):
    from ranger_test_utils.kafka_utils import KAFKA_CONTAINER

    container = get_docker_client().containers.get(KAFKA_CONTAINER)
    principal = user_kerberos_principal(username)
    keytab = user_keytab_in_service_container(username)
    shell = (
        "kdestroy -A 2>/dev/null || true; "
        "kinit -kt "
        + keytab
        + " "
        + principal
        + "; klist"
    )
    exit_code, output = container.exec_run(
        ["bash", "-c", shell], user="root", demux=True
    )
    combined = ((output[0] or b"") + (output[1] or b"")).decode()
    if exit_code != 0:
        raise RuntimeError(
            "Kerberos kinit failed in "
            + KAFKA_CONTAINER
            + " for "
            + principal
            + ": "
            + combined
        )


def _verify_kerberos_keytab_in_hadoop(username):
    hadoop = get_docker_client().containers.get(HADOOP_CONTAINER)
    principal = user_kerberos_principal(username)
    keytab = user_keytab_in_service_container(username)
    shell = (
        "kdestroy -A 2>/dev/null || true; "
        "kinit -kt "
        + keytab
        + " "
        + principal
        + "; klist"
    )
    exit_code, output = hadoop.exec_run(
        ["bash", "-c", shell], user=username, demux=True
    )
    combined = ((output[0] or b"") + (output[1] or b"")).decode()
    if exit_code != 0:
        raise RuntimeError(
            "Kerberos kinit failed in "
            + HADOOP_CONTAINER
            + " for "
            + principal
            + ": "
            + combined
        )
