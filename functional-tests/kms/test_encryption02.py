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

"""HDFS encryption zones after KMS key rollover and key deletion."""

from ranger_test_utils.kms_hdfs_test_config import (
    ACTIONS_COMMANDS,
    CLEANUP_COMMANDS,
    CREATE_FILE_COMMAND,
    GRANT_PERMISSIONS_COMMANDS,
    HDFS_USER,
    HIVE_USER,
    READ_EZ_FILE,
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

EZ_NAME = "secure_zone1"

KEY_PAYLOAD_TEMPLATE = {
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


def _create_kms_key(key_name: str) -> None:
    admin = TEST_CASE.hadoopAdminUser
    payload = {**KEY_PAYLOAD_TEMPLATE, "name": key_name}
    TEST_CASE.createKmsKey(
        "_create_kms_key",
        admin,
        payload,
        expected_status=201,
        test_desc=f"create_{key_name}",
    )


def _rollover_kms_key(key_name: str) -> None:
    admin = TEST_CASE.hadoopAdminUser
    TEST_CASE.rolloverEncryptionZoneKey(
        "_rollover_kms_key",
        admin,
        key_name,
        expected_status=200,
        test_desc=f"rollover_{key_name}",
    )


def _delete_kms_key(key_name: str) -> None:
    admin = TEST_CASE.hadoopAdminUser
    TEST_CASE.deleteEncryptionZoneKey(
        "_delete_kms_key",
        admin,
        key_name,
        expected_status=200,
        test_desc=f"delete_{key_name}",
    )


def _grant_hive_on_ez(hadoop_container, ez_name: str) -> None:
    for cmd in GRANT_PERMISSIONS_COMMANDS:
        formatted = cmd.format(ez_name=ez_name, user=HIVE_USER)
        output = run_command(hadoop_container, formatted, HDFS_USER)
        logger.info("grant_permissions: %s", output)


def _hive_create_local_file(hadoop_container, filename: str, filecontent: str) -> None:
    create_file_cmd = CREATE_FILE_COMMAND[0].format(
        filename=filename,
        filecontent=filecontent,
        user=HIVE_USER,
    )
    run_command(hadoop_container, ["bash", "-c", create_file_cmd], HIVE_USER)


def _hive_put_and_read(hadoop_container, ez_name: str, filename: str) -> None:
    for cmd in ACTIONS_COMMANDS:
        formatted = cmd.format(filename=filename, ez_name=ez_name, user=HIVE_USER)
        run_kerberos_command(
            hadoop_container, formatted, HIVE_USER, HIVE_PRINCIPAL, HIVE_KEYTAB
        )


def _hive_read_ez_file(hadoop_container, ez_name: str, filename: str) -> None:
    for cmd in READ_EZ_FILE:
        formatted = cmd.format(filename=filename, ez_name=ez_name)
        run_kerberos_command(
            hadoop_container, formatted, HIVE_USER, HIVE_PRINCIPAL, HIVE_KEYTAB
        )


def _cleanup_ez(hadoop_container, ez_name: str, filename: str, *, strict_last: bool = False) -> None:
    cleanup_cmd = [cmd.format(filename=filename, ez_name=ez_name) for cmd in CLEANUP_COMMANDS]
    for i, cmd in enumerate(cleanup_cmd):
        run_command(
            hadoop_container,
            cmd,
            HDFS_USER,
            fail_on_error=(strict_last and i == len(cleanup_cmd) - 1),
        )


def test_read_old_file_after_rollover(hadoop_container):
    key_name = "test-key1"
    filename = "testfile1"
    filecontent = "Hello Human"

    _create_kms_key(key_name)
    create_encryption_zone(hadoop_container, EZ_NAME, key_name)
    _grant_hive_on_ez(hadoop_container, EZ_NAME)

    _hive_create_local_file(hadoop_container, filename, filecontent)
    _hive_put_and_read(hadoop_container, EZ_NAME, filename)

    _rollover_kms_key(key_name)
    _hive_read_ez_file(hadoop_container, EZ_NAME, filename)

    _cleanup_ez(hadoop_container, EZ_NAME, filename)
    _delete_kms_key(key_name)


def test_writeAndRead_Newfile_after_rollover(hadoop_container):
    key_name = "test-key2"
    filename = "testfile2"
    filename2 = "testfile3"
    filecontent = "Hello First"
    filecontent2 = "Hello Second"

    _create_kms_key(key_name)
    create_encryption_zone(hadoop_container, EZ_NAME, key_name)
    _grant_hive_on_ez(hadoop_container, EZ_NAME)

    _hive_create_local_file(hadoop_container, filename, filecontent)
    _hive_put_and_read(hadoop_container, EZ_NAME, filename)

    _rollover_kms_key(key_name)

    _hive_create_local_file(hadoop_container, filename2, filecontent2)
    _hive_put_and_read(hadoop_container, EZ_NAME, filename2)

    _cleanup_ez(hadoop_container, EZ_NAME, filename, strict_last=True)
    _delete_kms_key(key_name)


def test_Readfile_after_keyDeletion(hadoop_container):
    key_name = "test-key3"
    filename = "testfile4"
    filecontent = "You are reading it before key deletion"

    _create_kms_key(key_name)
    create_encryption_zone(hadoop_container, EZ_NAME, key_name)
    _grant_hive_on_ez(hadoop_container, EZ_NAME)

    _hive_create_local_file(hadoop_container, filename, filecontent)
    _hive_put_and_read(hadoop_container, EZ_NAME, filename)

    _delete_kms_key(key_name)

    failure_detected = False
    for cmd in READ_EZ_FILE:
        formatted = cmd.format(filename=filename, ez_name=EZ_NAME)
        output, exit_code = run_command(
            hadoop_container,
            formatted,
            HIVE_USER,
            fail_on_error=False,
            return_exit_code=True,
        )
        error_lines = [line for line in output.splitlines() if line.startswith("-cat:")]
        logger.info("read after key delete (exit %s):\n%s", exit_code, "\n".join(error_lines))
        if exit_code != 0:
            failure_detected = True

    assert failure_detected, "Expected failure due to deleted EZ key, but command succeeded."

    _cleanup_ez(hadoop_container, EZ_NAME, filename)
