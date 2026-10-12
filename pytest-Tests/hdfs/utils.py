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


# This workflow will build a Java project with Maven, and cache/restore any dependencies to improve the workflow execution time
# For more information see: https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-java-with-maven

# This workflow uses actions that are not certified by GitHub.
# They are provided by a third-party and are governed by
# separate terms of service, privacy policy, and support
# documentation.

import time
import pytest
import docker
import requests
from hdfs.test_config import (KMS_CONTAINER, HADOOP_NAMENODE_LOG_PATH, KMS_LOG_PATH)
from kms.utils import krb_requests, ensure_ticket, BASE_URL, PARAMS
from hdfs.test_config import HDFS_USER
import subprocess
import tempfile
import os

client = docker.from_env()

RANGER_AUTH = ("keyadmin", "rangerR0cks!")
RANGER_ADMIN_AUTH = ("admin", "rangerR0cks!")
RANGER_POLICY_URL = "http://localhost:6080/service/public/v2/api/policy"
RANGER_SERVICE_POLICY_URL = "http://localhost:6080/service/public/v2/api/service/dev_kms/policy"
RANGER_USERS_URL = "http://localhost:6080/service/xusers/secure/users"
RANGER_USER_BY_NAME_URL = "http://localhost:6080/service/xusers/users/userName"
DEFAULT_KMS_POLICY_NAME = "all - keyname"
HDFS_KMS_USERS = ["keyadmin", "nn", "hdfs", "hive", "hbase", "dn"]
HDFS_KMS_ACCESSES = [
    "CREATE", "DELETE", "ROLLOVER", "GET", "GETKEYS", "GETMETADATA",
    "GENERATEEEK", "DECRYPTEEK", "SETKEYMATERIAL",
]
KMS_SITE_PATH = "/opt/ranger/ranger-kms/ews/webapp/WEB-INF/classes/conf/kms-site.xml"


HADOOP_CONTAINER = "ranger-hadoop"
HIVE_PRINCIPAL = "hive/ranger-hadoop.rangernw@EXAMPLE.COM"
HIVE_KEYTAB = "/etc/keytabs/hive.keytab"

KDC_CONTAINER = "ranger-kdc"

HDFS_PRINCIPAL = "hdfs/ranger-hadoop.rangernw@EXAMPLE.COM"
HDFS_KEYTAB = "/etc/keytabs/hdfs.keytab"

def _ensure_kdc_service_principal(username: str, service: str = "ranger-hadoop") -> str:
    """Create service principal in KDC and copy keytab to ranger-hadoop."""
    principal = f"{username}/{service}.rangernw@EXAMPLE.COM"
    keytab_name = f"{username}.keytab"
    dest_path = f"/etc/keytabs/{keytab_name}"
    kdc_tmp = f"/tmp/{keytab_name}"

    kdc = client.containers.get(KDC_CONTAINER)
    hadoop = client.containers.get(HADOOP_CONTAINER)

    exit_code, output = kdc.exec_run(f'kadmin.local -q "getprinc {principal}"', user="root")
    if exit_code != 0 or b"does not exist" in output:
        kdc.exec_run(f'kadmin.local -q "addprinc -randkey {principal}"', user="root")

    exit_code, output = kdc.exec_run(
        f'kadmin.local -q "xst -k {kdc_tmp} {principal}"', user="root"
    )
    if exit_code != 0:
        raise RuntimeError(f"keytab export failed for {principal}: {output.decode()}")

    local = os.path.join(tempfile.gettempdir(), keytab_name)
    subprocess.check_call(f"docker cp {KDC_CONTAINER}:{kdc_tmp} {local}", shell=True)
    subprocess.check_call(f"docker cp {local} {HADOOP_CONTAINER}:{dest_path}", shell=True)
    hadoop.exec_run(f"chmod 444 {dest_path}", user="root")
    hadoop.exec_run(f"chown {username}:{username} {dest_path}", user="root")
    os.remove(local)
    return principal


def ensure_hadoop_user_keytab(username: str) -> None:
    _ensure_kdc_service_principal(username)

def run_kerberos_command(container, cmd, user, principal, keytab, **kwargs):
    shell = (
        f"kinit -kt {keytab} {principal} && "
        f'HADOOP_OPTS="-Dhadoop.security.authentication=kerberos" {cmd}'
    )
    return run_command(container, ["bash", "-c", shell], user, **kwargs)

def _ranger_policies_from_response(data):
    if isinstance(data, list):
        return data
    return data.get("policies", [])


def ensure_ranger_user(username: str) -> None:
    """Create a Ranger xuser if missing (required before adding to policies)."""
    resp = requests.get(
        f"{RANGER_USER_BY_NAME_URL}/{username}",
        auth=RANGER_ADMIN_AUTH,
        timeout=30,
    )
    if resp.status_code == 200:
        return

    payload = {
        "name": username,
        "firstName": username,
        "lastName": "User",
        "password": "Password123!",
        "description": "hdfs encryption test user",
        "status": 1,
        "isVisible": 1,
        "userSource": 0,
        "userRoleList": ["ROLE_USER"],
    }
    resp = requests.post(RANGER_USERS_URL, auth=RANGER_ADMIN_AUTH, json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        print(f"WARN: create Ranger user {username}: {resp.status_code} {resp.text}")


def ensure_kms_kerberos_rules(kms_container) -> bool:
    """Patch KMS kerberos name rules at runtime. Returns True if KMS was restarted."""
    exit_code, _ = kms_container.exec_run(
        f"grep -q 'hive/ranger-hadoop' {KMS_SITE_PATH}", user="root"
    )
    if exit_code == 0:
        return False

    rules = [
        "RULE:[2:$1/$2@$0]([ndj]n/ranger-hadoop\\.rangernw@EXAMPLE\\.COM)s/.*/keyadmin/",
        "RULE:[2:$1/$2@$0](dn/ranger-hadoop\\.rangernw@EXAMPLE\\.COM)s/.*/keyadmin/",
        "RULE:[2:$1/$2@$0](hive/ranger-hadoop\\.rangernw@EXAMPLE\\.COM)s/.*/hive/",
    ]
    for rule in rules:
        kms_container.exec_run(f"sed -i '/DEFAULT/i {rule}' {KMS_SITE_PATH}", user="root")

    kms_container.exec_run(
        "/opt/ranger/ranger-kms/ranger-kms-services.sh restart", user="root"
    )
    print("Patched KMS kerberos rules, restarted ranger-kms.")
    time.sleep(30)
    return True


def ensure_kms_hdfs_policy() -> None:
    """Grant HDFS principals KMS access by updating the default wildcard policy."""

    resp = requests.get(RANGER_SERVICE_POLICY_URL, auth=RANGER_AUTH, timeout=30)

    if resp.status_code != 200:
        print(f"WARN: could not fetch KMS policies: {resp.status_code} {resp.text}")
        return

    policies = _ranger_policies_from_response(resp.json())
    target = None
    for policy in policies:
        name = policy.get("name") or policy.get("policyName", "")
        keyname_values = policy.get("resources", {}).get("keyname", {}).get("values", [])
        if name == DEFAULT_KMS_POLICY_NAME or keyname_values == ["*"]:
            target = policy
            break

    if target is None:
        print("WARN: default KMS wildcard policy not found, skipping update.")
        return

    policy_items = target.get("policyItems") or [{"accesses": [], "users": []}]
    if not policy_items:
        policy_items = [{"accesses": [], "users": []}]

    existing_users = set(policy_items[0].get("users") or [])
    needed_users = set(HDFS_KMS_USERS)
    existing_access = {a["type"] for a in policy_items[0].get("accesses", [])}

    if needed_users.issubset(existing_users) and set(HDFS_KMS_ACCESSES).issubset(existing_access):
        print("KMS policy already grants required HDFS users and accesses.")
        return

    policy_items[0]["users"] = sorted(existing_users | needed_users)
    for access_type in HDFS_KMS_ACCESSES:
        if access_type not in existing_access:
            policy_items[0].setdefault("accesses", []).append(
                {"type": access_type, "isAllowed": True}
            )

    target["policyItems"] = policy_items
    policy_id = target["id"]
    resp = requests.put(
        f"{RANGER_POLICY_URL}/{policy_id}",
        auth=RANGER_AUTH,
        json=target,
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        print(f"WARN: KMS policy update failed: {resp.status_code} {resp.text}")
        return

    print(f"Updated KMS policy {target.get('name', policy_id)}, waiting for sync...")
    time.sleep(30)


def _kms_key_missing(resp) -> bool:
    return "does not exist" in resp.text.lower()


def delete_kms_key(key_name: str) -> None:
    """Delete KMS key if present. Ignores missing-key responses."""
    ensure_ticket()
    resp = krb_requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    if resp.status_code in (200, 404):
        return
    if resp.status_code in (403, 500) and _kms_key_missing(resp):
        return
    if resp.status_code == 403:
        print(f"WARN: delete key {key_name}: {resp.status_code} {resp.text}")
        return


def ensure_kms_key(key_name: str, cipher="AES/CTR/NoPadding", length=128) -> None:
    """Idempotent: remove stale key, then create fresh."""
    delete_kms_key(key_name)
    resp = krb_requests.post(
        f"{BASE_URL}/keys",
        json={"name": key_name, "cipher": cipher, "length": length},
        params=PARAMS,
    )
    assert resp.status_code == 201, f"Key creation failed: {resp.text}"

def delete_hdfs_path(container, path: str) -> None:
    """Remove HDFS path if present. Kinit as hdfs first (kerberos mode)."""
    container.exec_run(
        ["bash", "-c",
         f"kinit -kt {HDFS_KEYTAB} {HDFS_PRINCIPAL} 2>/dev/null; hdfs dfs -rm -R -f {path}"],
        user=HDFS_USER,
    )

def cleanup_encryption_zone(container, ez_path: str) -> None:
    # Remove EZ directory (Must run before deleting bound KMS key)
    delete_hdfs_path(container, ez_path)

def cleanup_test_artifacts(container, keys: list, ez_paths: list) -> None:
    """Full cleanup: EZ paths first, then KMS keys."""
    for path in ez_paths:
        cleanup_encryption_zone(container, path)
    for key in keys:
        delete_kms_key(key)

def create_encryption_zone(container, ez_name: str, key_name: str) -> None:
    """Idempotent EZ setup: clean path, mkdir, createZone."""
    ez_path = f"/{ez_name}"
    cleanup_encryption_zone(container, ez_path)

    run_command(container, f"hdfs dfs -mkdir {ez_path}", HDFS_USER)
    run_command(container, f"hdfs crypto -createZone -keyName {key_name} -path {ez_path}", HDFS_USER)

#to run all HDFS commands
# Map each OS user to their kerberos principal and keytab (pre-existing in container)
_KERBEROS_CREDENTIALS = {
    "hdfs": ("hdfs/ranger-hadoop.rangernw@EXAMPLE.COM", "/etc/keytabs/hdfs.keytab"),
    "hive": ("hive/ranger-hadoop.rangernw@EXAMPLE.COM", "/etc/keytabs/hive.keytab"),
}

def run_command(container, cmd, user, fail_on_error=True, return_exit_code=False):
    # For string commands on kerberos-capable users, wrap with kinit automatically.
    # List commands (e.g. from run_kerberos_command) are passed through unchanged.
    if isinstance(cmd, str) and user in _KERBEROS_CREDENTIALS:
        principal, keytab = _KERBEROS_CREDENTIALS[user]
        actual_cmd = ["bash", "-c",
                      f"kinit -kt {keytab} {principal} 2>/dev/null; {cmd}"]
    else:
        actual_cmd = cmd

    exit_code, output = container.exec_run(actual_cmd, user=user)
    output_response = output.decode()

    if exit_code != 0 and fail_on_error:
        kms_container = client.containers.get(KMS_CONTAINER)
        hadoop_logs, kms_logs = get_error_logs(container, kms_container)
        pytest.fail(f"""
            Command failed: {cmd}
            Exit Code: {exit_code}

            Output:
            {output_response}

            Hadoop Container Logs:
            {hadoop_logs}

            KMS Container Logs:
            {kms_logs}
            """)
    if return_exit_code:
        return output_response, exit_code
    return output_response

#fetch logs from hadoop and KMS file
def get_error_logs(hadoop_container, kms_container):

    # Get Hadoop NameNode logs
    hadoop_log_cmd = f"tail -n 50 {HADOOP_NAMENODE_LOG_PATH}"
    _, hadoop_logs = hadoop_container.exec_run(hadoop_log_cmd, user='hdfs')
    hadoop_logs_decoded = hadoop_logs.decode()
    hadoop_error_lines = [line for line in hadoop_logs_decoded.split("\n") if "ERROR" in line or "Exception" in line or "WARN" in line]
    hadoop_error_text = "\n".join(hadoop_error_lines) if hadoop_error_lines else "No recent errors in Hadoop Namenode logs."

    # Get KMS logs
    kms_log_cmd = f"tail -n 50 {KMS_LOG_PATH}"
    _, kms_logs = kms_container.exec_run(kms_log_cmd, user='root')
    kms_logs_decoded = kms_logs.decode()
    kms_error_lines = [line for line in kms_logs_decoded.split("\n") if "ERROR" in line or "Exception" in line or "WARN" in line]
    kms_error_text = "\n".join(kms_error_lines) if kms_error_lines else "No recent errors in KMS logs."

    return hadoop_error_text, kms_error_text
