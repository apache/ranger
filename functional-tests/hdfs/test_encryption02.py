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

import pytest
from kms.utils import krb_requests
from hdfs.utils import (
    run_command,
    ensure_kms_key,
    create_encryption_zone,
    delete_kms_key,
    run_kerberos_command,
    HIVE_PRINCIPAL,
    HIVE_KEYTAB
)
from hdfs.test_config import (
    HDFS_USER, HIVE_USER, HEADERS, PARAMS, BASE_URL,
    GRANT_PERMISSIONS_COMMANDS,
    CREATE_FILE_COMMAND, ACTIONS_COMMANDS, READ_EZ_FILE,
    CLEANUP_COMMANDS,
)

# ****** ********************Test Case 01 ********************************************
# ***** Check if after key roll over old files can be read or not
# ***********************************************************************************
def test_read_old_file_after_rollover(hadoop_container):
    key_name="test-key1"
    ez_name = "secure_zone1"
    filename="testfile1"
    filecontent="Hello Human"

    #create EZ key-------
    ensure_kms_key(key_name)
    create_encryption_zone(hadoop_container, ez_name, key_name)
    #grant permissions for 'hive' user------------
    grant_permission_commands= [cmd.format(ez_name=ez_name, user=HIVE_USER) for cmd in GRANT_PERMISSIONS_COMMANDS]

    for cmd in grant_permission_commands:
        output = run_command(hadoop_container,cmd,HDFS_USER)
        print(output)

    #create file as 'hive' user-------
    create_file_cmd = [cmd.format(
        filename=filename,
        filecontent=filecontent,
        user=HIVE_USER
    ) for cmd in CREATE_FILE_COMMAND]

    run_command(hadoop_container, ["bash", "-c", create_file_cmd[0]], HIVE_USER)

    #read-write using 'hive' user-------
    read_write_cmd= [cmd.format(filename=filename, ez_name=ez_name, user=HIVE_USER) for cmd in ACTIONS_COMMANDS]
    for cmd in read_write_cmd:
        run_kerberos_command(hadoop_container, cmd, HIVE_USER, HIVE_PRINCIPAL, HIVE_KEYTAB)

    #roll-over of key---------
    response=krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=HEADERS, params=PARAMS)
    assert response.status_code == 200, f"Key roll over failed: {response.text}"

    #read same file after roll over---------
    read_ez_file=[cmd.format(filename=filename, ez_name=ez_name) for cmd in READ_EZ_FILE]
    for cmd in read_ez_file:
        run_kerberos_command(hadoop_container, cmd, HIVE_USER, HIVE_PRINCIPAL, HIVE_KEYTAB)

    #cleanup EZ and EZ file--------
    cleanup_cmd=[cmd.format(filename=filename, ez_name=ez_name) for cmd in CLEANUP_COMMANDS]
    for cmd in cleanup_cmd:
        run_command(hadoop_container,cmd,HDFS_USER)

    #delete EZ key ----------
    delete_kms_key(key_name)


# ****** ********************Test Case 02 ********************************************
# ***** Check if after key roll over new files can be written and read too
# ***********************************************************************************
def test_writeAndRead_Newfile_after_rollover(hadoop_container):
    key_name="test-key2"
    ez_name = "secure_zone1"
    filename="testfile2"
    filename2="testfile3"
    filecontent="Hello First"
    filecontent2="Hello Second"

    #create EZ key-------
    #grant permissions for 'hive' user------------
    ensure_kms_key(key_name)
    create_encryption_zone(hadoop_container, ez_name, key_name)

    grant_permission_commands = [
        cmd.format(ez_name=ez_name, user=HIVE_USER) for cmd in GRANT_PERMISSIONS_COMMANDS
    ]
    for cmd in grant_permission_commands:
        run_command(hadoop_container, cmd, HDFS_USER)

    #create file in EZ as 'hive' user-------
    create_file_cmd = [cmd.format(
        filename=filename,
        filecontent=filecontent,
        user=HIVE_USER
    ) for cmd in CREATE_FILE_COMMAND]

    run_command(hadoop_container, ["bash", "-c", create_file_cmd[0]], HIVE_USER)

    #read-write using 'hive' user-------
    read_write_cmd= [cmd.format(filename=filename, ez_name=ez_name, user=HIVE_USER) for cmd in ACTIONS_COMMANDS]
    for cmd in read_write_cmd:
        output=run_command(hadoop_container,cmd,HIVE_USER)
        print(output)

    #roll-over of key---------
    response=krb_requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=HEADERS, params=PARAMS)
    assert response.status_code == 200, f"Key roll over failed: {response.text}"

    #write new file after rollover
    create_file_cmd = [cmd.format(
        filename=filename2,
        filecontent=filecontent2,
        user=HIVE_USER
    ) for cmd in CREATE_FILE_COMMAND]

    run_command(hadoop_container, ["bash", "-c", create_file_cmd[0]], HIVE_USER)

    #read-write new file now
    read_write_cmd= [cmd.format(filename=filename2, ez_name=ez_name, user=HIVE_USER) for cmd in ACTIONS_COMMANDS]
    for cmd in read_write_cmd:
        output=run_command(hadoop_container,cmd,HIVE_USER)
        print(output)

    #cleanup EZ and EZ file--------
    cleanup_cmd=[cmd.format(filename=filename, ez_name=ez_name) for cmd in CLEANUP_COMMANDS]
    for cmd in cleanup_cmd:
        run_command(hadoop_container,cmd,HDFS_USER)

    #delete EZ key ----------
    delete_kms_key(key_name)


# ****** ********************Test Case 03 ********************************************
# ***** Check read operation on file after key deletion
# ***********************************************************************************
def test_Readfile_after_keyDeletion(hadoop_container):
    key_name="test-key3"
    ez_name = "secure_zone1"
    filename="testfile4"
    filecontent="You are reading it before key deletion"

    #create EZ key-------
    ensure_kms_key(key_name)
    # create EZ ------------
    create_encryption_zone(hadoop_container, ez_name, key_name)


    #grant permissions for 'hive' user------------
    grant_permission_commands= [cmd.format(ez_name=ez_name, user=HIVE_USER) for cmd in GRANT_PERMISSIONS_COMMANDS]

    for cmd in grant_permission_commands:
        output = run_command(hadoop_container,cmd,HDFS_USER)
        print(output)

    #create file in EZ as 'hive' user-------
    create_file_cmd = [cmd.format(
        filename=filename,
        filecontent=filecontent,
        user=HIVE_USER
    ) for cmd in CREATE_FILE_COMMAND]

    run_command(hadoop_container, ["bash", "-c", create_file_cmd[0]], HIVE_USER)

    #read-write using 'hive' user-------
    read_write_cmd= [cmd.format(filename=filename, ez_name=ez_name, user=HIVE_USER) for cmd in ACTIONS_COMMANDS]
    for cmd in read_write_cmd:
        output=run_command(hadoop_container,cmd,HIVE_USER)
        print(output)


    #delete EZ key ----------
    delete_kms_key(key_name)


    #read-write file after key deletion --------------
    read_write_cmd= [cmd.format(filename=filename, ez_name=ez_name, user=HIVE_USER) for cmd in READ_EZ_FILE]
    failure_detected = False

    for cmd in read_write_cmd:
        output = run_command(hadoop_container, cmd, HIVE_USER, fail_on_error=False)
        print(f"Command Output:\n{output}")

        # Check for known failure indicators in output
        if any(err in output.lower() for err in ["error", "exception", "failed", "not found"]):
            failure_detected = True

        #assert that failure was detected as expected
    assert failure_detected, "Expected failure due to deleted EZ key, but command succeeded."


    #cleanup EZ and EZ file--------
    cleanup_cmd=[cmd.format(filename=filename, ez_name=ez_name) for cmd in CLEANUP_COMMANDS]
    for cmd in cleanup_cmd:
        run_command(hadoop_container,cmd,HDFS_USER)


