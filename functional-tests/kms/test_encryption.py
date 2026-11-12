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

"""HDFS encryption zone lifecycle: KMS key, EZ create, hive access, hbase denied, cleanup."""

import pytest

from ranger_test_utils.kms_hdfs_test_config import (
    ACTIONS_COMMANDS,
    CLEANUP_COMMANDS,
    CREATE_FILE_COMMAND,
    GRANT_PERMISSIONS_COMMANDS,
    HBASE_USER,
    HDFS_USER,
    HIVE_USER,
    UNAUTHORIZED_READ_COMMAND,
    UNAUTHORIZED_WRITE_COMMAND,
)
from hdfs.utils import (
    HIVE_KEYTAB,
    HIVE_PRINCIPAL,
    create_encryption_zone,
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

KEY_NAME = "hdfs-key"
EZ_NAME = "secure_zone"
FILENAME = "hdfs-test-file"
FILECONTENT = "Welcome to hdfs encryption"

KEY_PAYLOAD = {
    "name": KEY_NAME,
    "cipher": "AES/CTR/NoPadding",
    "length": 128,
}


def setup_module():
    global TEST_CASE, list_of_keys_before_test
    init_kms_test_module(module_name=__name__)

    ensure_keyadmin_can_list_kms_keys()
    list_of_keys_before_test = fetch_kms_key_names()
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)

    TEST_CASE = KmsXaTestCase()
    TEST_CASE.setUpModule()


def teardown_module():
    global TEST_CASE
    logger.info("The list of keys before the test : %s", list_of_keys_before_test)
    if TEST_CASE is not None:
        TEST_CASE.deleteKeysForThisTest()
        TEST_CASE.tearDownModule()
        TEST_CASE = None
    teardown_kms_keys_created_since(list_of_keys_before_test)


def test_create_key(hadoop_container):
    del hadoop_container
    admin = TEST_CASE.hadoopAdminUser
    TEST_CASE.createKmsKey(
        "test_create_key",
        admin,
        KEY_PAYLOAD,
        expected_status=201,
        test_desc="test_create_key",
    )
    names = fetch_kms_key_names()
    assert KEY_NAME in names, f"Key not found in: {names}"
    logger.info("Key names after create: %s", names)


@pytest.mark.createEZ
def test_create_encryption_zone(hadoop_container):
    create_encryption_zone(hadoop_container, EZ_NAME, KEY_NAME)


def test_grant_permissions(hadoop_container):
    grant_permission_commands = [
        cmd.format(ez_name=EZ_NAME, user=HIVE_USER) for cmd in GRANT_PERMISSIONS_COMMANDS
    ]

    for cmd in grant_permission_commands:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        logger.info("grant_permissions output: %s", output)


def test_hive_user_write_read(hadoop_container):
    create_file_cmd = [
        cmd.format(filename=FILENAME, filecontent=FILECONTENT, user=HIVE_USER)
        for cmd in CREATE_FILE_COMMAND
    ]

    run_command(hadoop_container, ["bash", "-c", create_file_cmd[0]], HIVE_USER)

    read_write_cmd = [
        cmd.format(filename=FILENAME, ez_name=EZ_NAME, user=HIVE_USER)
        for cmd in ACTIONS_COMMANDS
    ]
    for cmd in read_write_cmd:
        run_kerberos_command(
            hadoop_container, cmd, HIVE_USER, HIVE_PRINCIPAL, HIVE_KEYTAB
        )


def test_unauthorized_write(hadoop_container):
    filename2 = "hdfs-test-file2"

    create_file_cmd = CREATE_FILE_COMMAND[0].format(
        filename=filename2,
        filecontent="unauthorized write attempt",
        user=HBASE_USER,
    )
    run_command(hadoop_container, ["bash", "-c", create_file_cmd], HBASE_USER)

    unauth_write_cmd = UNAUTHORIZED_WRITE_COMMAND.format(
        filename=filename2, user=HBASE_USER, ez_name=EZ_NAME
    )
    output, exit_code = run_command(
        hadoop_container,
        unauth_write_cmd,
        HBASE_USER,
        fail_on_error=False,
        return_exit_code=True,
    )

    logger.info("unauthorized_write output (exit %s): %s", exit_code, output)

    assert exit_code != 0, (
        "Expected failure: hbase should not have write permission on EZ, "
        "but command succeeded."
    )
    assert any(
        p in output.lower()
        for p in ["permission denied", "access denied", "accesscontrolexception"]
    ), f"Expected HDFS Permission denied error but got: {output}"


def test_unauthorized_read(hadoop_container):
    unauth_read = UNAUTHORIZED_READ_COMMAND.format(
        filename=FILENAME, ez_name=EZ_NAME, user=HBASE_USER
    )
    output, exit_code = run_command(
        hadoop_container,
        unauth_read,
        HBASE_USER,
        fail_on_error=False,
        return_exit_code=True,
    )

    logger.info("unauthorized_read output (exit %s): %s", exit_code, output)

    assert exit_code != 0, (
        "Expected failure: hbase should not have read permission on EZ, "
        "but command succeeded."
    )
    assert any(
        p in output.lower()
        for p in ["permission denied", "access denied", "accesscontrolexception"]
    ), f"Expected HDFS Permission denied error but got: {output}"


@pytest.mark.cleanEZ
def test_cleanup(hadoop_container):
    cleanup_cmd = [
        cmd.format(filename=FILENAME, ez_name=EZ_NAME) for cmd in CLEANUP_COMMANDS
    ]
    for i, cmd in enumerate(cleanup_cmd):
        output = run_command(
            hadoop_container,
            cmd,
            HDFS_USER,
            fail_on_error=(i == len(cleanup_cmd) - 1),
        )
        logger.info("cleanup output: %s", output)
