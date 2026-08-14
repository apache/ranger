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
from hdfs.utils import (
    run_command,
    run_kerberos_command,
    ensure_kms_key,
    create_encryption_zone,
    delete_kms_key,
    HIVE_PRINCIPAL,
    HIVE_KEYTAB,
)
from kms.utils import krb_requests, BASE_URL, PARAMS
from hdfs.test_config import (
    HDFS_USER, HIVE_USER, HBASE_USER,
    GRANT_PERMISSIONS_COMMANDS, UNAUTHORIZED_WRITE_COMMAND,
    ACTIONS_COMMANDS, UNAUTHORIZED_READ_COMMAND,
    CLEANUP_COMMANDS, CREATE_FILE_COMMAND,
)

key_name="hdfs-key"
ez_name="secure_zone"
filename="hdfs-test-file"
filecontent="Welcome to hdfs encryption"

# EZ key creation before creating an EZ---------------------------------------------
def test_create_key(hadoop_container):
    ensure_kms_key(key_name)
    names = krb_requests.get(f"{BASE_URL}/keys/names", params=PARAMS)
    assert key_name in names.text, f"Key not found in: {names.text}"
    print("Key List Output:", names.text)


# Create Encryption Zone -----------------------------------------------------------
@pytest.mark.createEZ
def test_create_encryption_zone(hadoop_container):
    create_encryption_zone(hadoop_container, ez_name, key_name)


# Grant Permissions to 'Hive' User to above EZ----------------------------------------
def test_grant_permissions(hadoop_container):
    grant_permission_commands= [cmd.format(ez_name=ez_name, user=HIVE_USER) for cmd in GRANT_PERMISSIONS_COMMANDS]

    for cmd in grant_permission_commands:
        output = run_command(hadoop_container,cmd,HDFS_USER)
        print(output)

# Testing read write permission for hive user-----------------------------------------
def test_hive_user_write_read(hadoop_container):
    #create file as 'hive' user
    create_file_cmd = [cmd.format(
        filename=filename,
        filecontent=filecontent,
        user=HIVE_USER
    ) for cmd in CREATE_FILE_COMMAND]

    run_command(hadoop_container, ["bash", "-c", create_file_cmd[0]], HIVE_USER)

    #read-write using 'hive' user
    read_write_cmd= [cmd.format(filename=filename, ez_name=ez_name, user=HIVE_USER) for cmd in ACTIONS_COMMANDS]
    for cmd in read_write_cmd:
        run_kerberos_command(hadoop_container, cmd, HIVE_USER, HIVE_PRINCIPAL, HIVE_KEYTAB)


# Negative Test - Unauthorized User Cannot Write i.e 'HBASE' in this case-------------
def test_unauthorized_write(hadoop_container):
    filename2="hdfs-test-file2" #writing new file into EZ
    failure_detected = False

    unauth_write_cmd= UNAUTHORIZED_WRITE_COMMAND.format(filename=filename2,user=HBASE_USER,ez_name=ez_name)
    output,exit_code= run_command(hadoop_container,unauth_write_cmd,HBASE_USER,fail_on_error=False,return_exit_code=True)

    print(f"Command Output:\n{output}")

    # Check for known failure indicators in output
    if exit_code != 0:
        failure_detected = True

    #assert that failure was detected as expected
    assert failure_detected, "Expected failure due to no permission on EZ, but command succeeded."


# Negative Test - Unauthorized User 'HBASE' Cannot Read ------------------------------
def test_unauthorized_read(hadoop_container):
    unauth_read= UNAUTHORIZED_READ_COMMAND.format(filename=filename, ez_name=ez_name, user=HBASE_USER)
    output,exit_code = run_command(hadoop_container,unauth_read,HBASE_USER,fail_on_error=False,return_exit_code=True)

    print(f"Command Output:\n{output}")

    assert exit_code != 0, "Expected failure due to no permission on EZ, but command succeeded."


# Clean Up - Remove Test file and EZ -------------------------------------------------
@pytest.mark.cleanEZ
def test_cleanup(hadoop_container):
    cleanup_cmd = [cmd.format(filename=filename, ez_name=ez_name) for cmd in CLEANUP_COMMANDS]
    for i, cmd in enumerate(cleanup_cmd):
        output = run_command(
            hadoop_container, cmd, HDFS_USER,
            fail_on_error=(i == len(cleanup_cmd) - 1),
        )
        print(output)

    delete_kms_key(key_name)







