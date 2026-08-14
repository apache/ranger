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
    ensure_kms_key,
    create_encryption_zone,
    delete_kms_key,
    run_kerberos_command,
)
from hdfs.test_config import (
    HDFS_USER, HIVE_USER,
    GRANT_PERMISSIONS_COMMANDS,
    CREATE_FILE_COMMAND, ACTIONS_COMMANDS,
    CROSS_EZ_ACTION_COMMANDS, CLEANUP_EZ,
)

# ****** ********************Test Case 01 ********************************************
# ***** Cross EZ operation where one user has given access to one EZ and does operation on that zone and another second zone where he has no permission
# ***********************************************************************************
def test_cross_EZ_operations(hadoop_container):
    key_name="cross-key"
    key_name2="cross-key2"

    ez_name = "secure_zone1"
    ez_name2 = "secure_zone2"

    filename="testfile1"
    filecontent="Cross operation on Encryption zone"

    dirname="dir1"
    dirname2="dir2"

    #create 2 EZ key-------
    ensure_kms_key(key_name)
    ensure_kms_key(key_name2)

    # create 2 EZ ------------
    create_encryption_zone(hadoop_container, ez_name, key_name)
    create_encryption_zone(hadoop_container, ez_name2, key_name2)

    # Create the subdirectories inside the encryption zone as HDFS user
    create_dirs_cmds = [
        f"hdfs dfs -mkdir -p /{ez_name}/{dirname}",
        f"hdfs dfs -mkdir -p /{ez_name}/{dirname2}"
    ]
    for cmd in create_dirs_cmds:
        run_command(hadoop_container, cmd, HDFS_USER)

    #grant permissions for 'hive' user on 1st EZ------------
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

    #write it to dir1 in EZ1 using 'hive' user and read it -------
    read_write_cmd= [cmd.format(filename=filename, ez_name=ez_name,dirname=dirname, user=HIVE_USER) for cmd in CROSS_EZ_ACTION_COMMANDS]
    for cmd in read_write_cmd:
        run_command(hadoop_container,cmd,HIVE_USER)

    #write it to dir2 in EZ1 using 'hive' user and read it -------
    read_write_cmd= [cmd.format(filename=filename, ez_name=ez_name,dirname=dirname2, user=HIVE_USER) for cmd in CROSS_EZ_ACTION_COMMANDS]
    for cmd in read_write_cmd:
        run_command(hadoop_container,cmd,HIVE_USER)

    #try to write in EZ2 now as HIVE user- should fail as has no permission on EZ2-----------------------
    failure_detected = False
    read_write_cmd= [cmd.format(filename=filename, ez_name=ez_name2, user=HIVE_USER) for cmd in ACTIONS_COMMANDS]

    for cmd in read_write_cmd:
        output,exit_code=run_command(hadoop_container,cmd,HIVE_USER, fail_on_error=False,return_exit_code=True)
        print(f"Command Output:\n{output}")

        # Check for known failure indicators in output
        if exit_code != 0:
            failure_detected = True
            break

    #assert that failure was detected as expected
    assert failure_detected, "Expected failure due to no permission on EZ, but command succeeded."

    #cleanup EZ and EZ file------------------------------------------------------------------------------
    cleanup_cmd=[cmd.format(ez_name=ez_name) for cmd in CLEANUP_EZ]
    for cmd in cleanup_cmd:
        run_command(hadoop_container,cmd,HDFS_USER)

    cleanup_cmd=[cmd.format(ez_name=ez_name2) for cmd in CLEANUP_EZ]
    for cmd in cleanup_cmd:
        run_command(hadoop_container,cmd,HDFS_USER)

    #delete EZ key ----------
    delete_kms_key(key_name)
    delete_kms_key(key_name2)

