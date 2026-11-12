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

"""DECRYPT_EEK KMS policy enforcement on HDFS encryption zones (hdfs denied, hrt21 allowed)."""

import time

import docker
import pytest
import requests

from ranger_test_utils.kms_hdfs_test_config import HADOOP_CONTAINER, HDFS_USER
from hdfs.utils import (
    RANGER_AUTH,
    RANGER_POLICY_URL,
    cleanup_encryption_zone,
    create_encryption_zone,
    delete_kms_key,
    ensure_hadoop_user_keytab,
    ensure_ranger_user,
    run_command,
    run_kerberos_command,
)
from ranger_test_utils.kms_test_utils import init_kms_test_module, logger
from ranger_test_utils.kms_utils import (
    KmsXaTestCase,
    ensure_keyadmin_can_list_kms_keys,
    fetch_kms_key_names,
    teardown_kms_keys_created_since,
)

TEST_CASE = None
list_of_keys_before_test = None
_hadoop_container = None

KMS_SERVICE_NAME = "dev_kms"
POLICY_NAME = "pytest-decrypt-eek-policy"

HRT21_USER = "hrt21"
HRT21_PRINCIPAL = "hrt21/ranger-hadoop.rangernw@EXAMPLE.COM"
HRT21_KEYTAB = "/etc/keytabs/hrt21.keytab"

EZ_KEY = "decrypt-eek-key"
EZ_NAME = "decrypt_eek_zone"
TEST_FILE = "decrypt_eek_testfile.txt"
PLAIN_TEXT = "KMS DECRYPT_EEK policy enforcement test content"

PLAIN_LOCAL_FILE = f"/tmp/{TEST_FILE}"
PLAIN_HDFS_FILE = f"/tmp/{TEST_FILE}"
DENIED_MSG = "not allowed to do 'DECRYPT_EEK'"

KMS_SYNC_TIMEOUT = 120
KMS_SYNC_INTERVAL = 5

KEY_PAYLOAD = {
    "name": EZ_KEY,
    "cipher": "AES/CTR/NoPadding",
    "length": 128,
}


def _ensure_os_user(container, username):
    exit_code, _ = container.exec_run(f"id -u {username}", user="root")
    if exit_code != 0:
        container.exec_run(f"useradd -m -s /bin/bash {username}", user="root")
        container.exec_run(f"usermod -aG hadoop {username}", user="root")


def _delete_test_policy():
    resp = requests.get(
        RANGER_POLICY_URL,
        params={"serviceName": KMS_SERVICE_NAME, "policyName": POLICY_NAME},
        auth=RANGER_AUTH,
        timeout=30,
    )
    if resp.status_code != 200:
        return
    for policy in resp.json():
        requests.delete(
            f"{RANGER_POLICY_URL}/{policy['id']}", auth=RANGER_AUTH, timeout=30
        )


def _create_test_policy():
    """Deny hdfs / allow hrt21 DECRYPT_EEK on EZ_KEY only."""
    _delete_test_policy()
    policy = {
        "service": KMS_SERVICE_NAME,
        "name": POLICY_NAME,
        "isEnabled": True,
        "isAuditEnabled": True,
        "resources": {
            "keyname": {"values": [EZ_KEY], "isExcludes": False, "isRecursive": False}
        },
        "policyItems": [
            {
                "accesses": [{"type": "decrypteek", "isAllowed": True}],
                "users": [HRT21_USER],
                "delegateAdmin": False,
            }
        ],
        "denyPolicyItems": [
            {
                "accesses": [{"type": "decrypteek", "isAllowed": True}],
                "users": [HDFS_USER],
                "delegateAdmin": False,
            }
        ],
    }
    resp = requests.post(RANGER_POLICY_URL, auth=RANGER_AUTH, json=policy, timeout=30)
    assert resp.status_code in (200, 201), (
        f"Failed to create KMS policy: {resp.status_code} {resp.text}"
    )


def _wait_for_deny_enforced(container):
    probe = f"/{EZ_NAME}/_probe_{TEST_FILE}"
    deadline = time.time() + KMS_SYNC_TIMEOUT
    while True:
        _, rc = run_command(
            container,
            f'hdfs dfs -cp "{PLAIN_HDFS_FILE}" "{probe}"',
            HDFS_USER,
            fail_on_error=False,
            return_exit_code=True,
        )
        if rc != 0:
            return
        run_command(
            container,
            f"hdfs dfs -rm -f -skipTrash {probe}",
            HDFS_USER,
            fail_on_error=False,
        )
        if time.time() > deadline:
            pytest.fail(
                f"KMS plugin did not enforce the DECRYPT_EEK deny for hdfs "
                f"within {KMS_SYNC_TIMEOUT}s"
            )
        time.sleep(KMS_SYNC_INTERVAL)


def _provision_decrypt_eek_environment(container):
    _ensure_os_user(container, HRT21_USER)
    ensure_hadoop_user_keytab(HRT21_USER)
    ensure_ranger_user(HRT21_USER)

    run_command(container, f'echo "{PLAIN_TEXT}" > {PLAIN_LOCAL_FILE}', HDFS_USER)
    run_command(
        container,
        f'hdfs dfs -put -f "{PLAIN_LOCAL_FILE}" "{PLAIN_HDFS_FILE}"',
        HDFS_USER,
    )

    delete_kms_key(EZ_KEY)
    admin = TEST_CASE.hadoopAdminUser
    TEST_CASE.createKmsKey(
        "setup_decrypt_eek",
        admin,
        KEY_PAYLOAD,
        expected_status=201,
        test_desc="create_decrypt_eek_key",
    )

    create_encryption_zone(container, EZ_NAME, EZ_KEY)
    run_command(container, f"hdfs dfs -chmod 777 /{EZ_NAME}", HDFS_USER)

    _create_test_policy()
    _wait_for_deny_enforced(container)


def _teardown_decrypt_eek_environment(container):
    _delete_test_policy()
    run_command(
        container,
        f"hdfs dfs -rm -f -skipTrash {PLAIN_HDFS_FILE}",
        HDFS_USER,
        fail_on_error=False,
    )
    cleanup_encryption_zone(container, f"/{EZ_NAME}")


def setup_module():
    global TEST_CASE, list_of_keys_before_test, _hadoop_container
    init_kms_test_module(module_name=__name__)

    ensure_keyadmin_can_list_kms_keys()
    list_of_keys_before_test = fetch_kms_key_names()
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)

    TEST_CASE = KmsXaTestCase()
    TEST_CASE.setUpModule()

    _hadoop_container = docker.from_env().containers.get(HADOOP_CONTAINER)
    _provision_decrypt_eek_environment(_hadoop_container)


def teardown_module():
    global TEST_CASE, _hadoop_container
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)
    if _hadoop_container is not None:
        _teardown_decrypt_eek_environment(_hadoop_container)
    if TEST_CASE is not None:
        TEST_CASE.deleteKeysForThisTest()
        TEST_CASE.tearDownModule()
        TEST_CASE = None
    teardown_kms_keys_created_since(list_of_keys_before_test)
    _hadoop_container = None


def test_01_EncryptFile_case1A(hadoop_container):
    encrypted_file = f"/{EZ_NAME}/{TEST_FILE}"

    output, rc = run_command(
        hadoop_container,
        f'hdfs dfs -cp "{PLAIN_HDFS_FILE}" "{encrypted_file}"',
        HDFS_USER,
        fail_on_error=False,
        return_exit_code=True,
    )
    assert rc != 0, (
        "Admin should not be able to cp into EZ (denied DECRYPT_EEK) but succeeded"
    )
    assert DENIED_MSG in output, f"Expected '{DENIED_MSG}' in output but got: {output}"

    output, rc = run_kerberos_command(
        hadoop_container,
        f'hdfs dfs -cp "{PLAIN_HDFS_FILE}" "{encrypted_file}"',
        HRT21_USER,
        HRT21_PRINCIPAL,
        HRT21_KEYTAB,
        fail_on_error=False,
        return_exit_code=True,
    )
    assert DENIED_MSG not in output, f"hrt21 unexpectedly denied DECRYPT_EEK: {output}"
    assert rc == 0, (
        f"hrt21 should be able to cp into EZ (granted DECRYPT_EEK) but failed: {output}"
    )

    output, rc = run_command(
        hadoop_container,
        f'hdfs dfs -cat "{encrypted_file}"',
        HDFS_USER,
        fail_on_error=False,
        return_exit_code=True,
    )
    assert rc != 0, (
        "Admin should not be able to cat from EZ (denied DECRYPT_EEK) but succeeded"
    )
    assert DENIED_MSG in output, f"Expected '{DENIED_MSG}' in output but got: {output}"

    output, rc = run_kerberos_command(
        hadoop_container,
        f'hdfs dfs -cat "{encrypted_file}"',
        HRT21_USER,
        HRT21_PRINCIPAL,
        HRT21_KEYTAB,
        fail_on_error=False,
        return_exit_code=True,
    )
    assert rc == 0, (
        f"hrt21 should be able to cat from EZ (granted DECRYPT_EEK) but failed: {output}"
    )
    assert PLAIN_TEXT in output, (
        f"Expected plain text '{PLAIN_TEXT}' in cat output but got: {output}"
    )
