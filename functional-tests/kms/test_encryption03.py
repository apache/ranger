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

"""Cross encryption zone access: hive allowed on one EZ, denied on another."""

from ranger_test_utils.kms_hdfs_test_config import (
    ACTIONS_COMMANDS,
    CLEANUP_EZ,
    CREATE_FILE_COMMAND,
    CROSS_EZ_ACTION_COMMANDS,
    GRANT_PERMISSIONS_COMMANDS,
    HDFS_USER,
    HIVE_USER,
)
from hdfs.utils import create_encryption_zone, run_command
from ranger_test_utils.kms_test_utils import init_kms_test_module, logger
from ranger_test_utils.kms_utils import (
    KmsXaTestCase,
    ensure_keyadmin_can_list_kms_keys,
    fetch_kms_key_names,
    teardown_kms_keys_created_since,
)

TEST_CASE = None
list_of_keys_before_test = None

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


def _delete_kms_key(key_name: str) -> None:
    admin = TEST_CASE.hadoopAdminUser
    TEST_CASE.deleteEncryptionZoneKey(
        "_delete_kms_key",
        admin,
        key_name,
        expected_status=200,
        test_desc=f"delete_{key_name}",
    )


def test_cross_EZ_operations(hadoop_container):
    key_name = "cross-key"
    key_name2 = "cross-key2"

    ez_name = "secure_zone1"
    ez_name2 = "secure_zone2"

    filename = "testfile1"
    filecontent = "Cross operation on Encryption zone"

    dirname = "dir1"
    dirname2 = "dir2"

    _create_kms_key(key_name)
    _create_kms_key(key_name2)

    create_encryption_zone(hadoop_container, ez_name, key_name)
    create_encryption_zone(hadoop_container, ez_name2, key_name2)

    create_dirs_cmds = [
        f"hdfs dfs -mkdir -p /{ez_name}/{dirname}",
        f"hdfs dfs -mkdir -p /{ez_name}/{dirname2}",
    ]
    for cmd in create_dirs_cmds:
        run_command(hadoop_container, cmd, HDFS_USER)

    grant_permission_commands = [
        cmd.format(ez_name=ez_name, user=HIVE_USER) for cmd in GRANT_PERMISSIONS_COMMANDS
    ]
    for cmd in grant_permission_commands:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        logger.info("grant_permissions: %s", output)

    create_file_cmd = CREATE_FILE_COMMAND[0].format(
        filename=filename,
        filecontent=filecontent,
        user=HIVE_USER,
    )
    run_command(hadoop_container, ["bash", "-c", create_file_cmd], HIVE_USER)

    for dirname_target in (dirname, dirname2):
        read_write_cmd = [
            cmd.format(
                filename=filename,
                ez_name=ez_name,
                dirname=dirname_target,
                user=HIVE_USER,
            )
            for cmd in CROSS_EZ_ACTION_COMMANDS
        ]
        for cmd in read_write_cmd:
            run_command(hadoop_container, cmd, HIVE_USER)

    failure_detected = False
    read_write_cmd = [
        cmd.format(filename=filename, ez_name=ez_name2, user=HIVE_USER)
        for cmd in ACTIONS_COMMANDS
    ]
    for cmd in read_write_cmd:
        output, exit_code = run_command(
            hadoop_container,
            cmd,
            HIVE_USER,
            fail_on_error=False,
            return_exit_code=True,
        )
        logger.info("cross_ez write attempt (exit %s): %s", exit_code, output)
        if exit_code != 0:
            failure_detected = True
            break

    assert failure_detected, (
        "Expected failure due to no permission on EZ, but command succeeded."
    )

    for ez in (ez_name, ez_name2):
        for cmd in [c.format(ez_name=ez) for c in CLEANUP_EZ]:
            run_command(hadoop_container, cmd, HDFS_USER)

    _delete_kms_key(key_name)
    _delete_kms_key(key_name2)
