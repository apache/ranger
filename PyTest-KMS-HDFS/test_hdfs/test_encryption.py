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



import pytest
from utils import run_command,get_error_logs 
from test_config import (HDFS_USER,HIVE_USER,HBASE_USER,KEY_ADMIN,
                         CREATE_KEY_COMMAND, VALIDATE_KEY_COMMAND, CREATE_EZ_COMMANDS,GRANT_PERMISSIONS_COMMANDS, 
                         UNAUTHORIZED_WRITE_COMMAND, ACTIONS_COMMANDS,
                         UNAUTHORIZED_READ_COMMAND,KEY_DELETION_CMD,
                         CLEANUP_COMMANDS,CREATE_FILE_COMMAND)
                        
key_name="hdfs-key"
ez_name="secure_zone"
filename="hdfs-test-file"
filecontent="Welcome to hdfs encryption"

#EZ key creation before creating an EZ---------------------
def test_create_key(hadoop_container):

    create_key_cmd= CREATE_KEY_COMMAND.format(key_name=key_name)
    # Run the command as keyadmin user
    output = run_command(hadoop_container,create_key_cmd, KEY_ADMIN)
    print("Key Creation Output:", output)

    # Validate if the key was created successfully
    validation_output = run_command(hadoop_container, VALIDATE_KEY_COMMAND, KEY_ADMIN)

    print("Key List Output:", validation_output)

    # Check if key is present
    if key_name not in validation_output:
        error_logs = get_error_logs()                           # Fetch logs on failure
        pytest.fail(f"Key creation failed. Logs:\n{error_logs}")



# Create Encryption Zone ----------------------------------------------------
@pytest.mark.createEZ
def test_create_encryption_zone(hadoop_container):
    
    create_ez_commands = [cmd.format(ez_name=ez_name, key_name=key_name) for cmd in CREATE_EZ_COMMANDS]

    for cmd in create_ez_commands:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        print(output)
        

#Grant Permissions to 'Hive' User --------------------------------------------
def test_grant_permissions(hadoop_container):
    grant_permission_commands= [cmd.format(ez_name=ez_name, user=HIVE_USER) for cmd in GRANT_PERMISSIONS_COMMANDS]

    for cmd in grant_permission_commands:
        output = run_command(hadoop_container,cmd,HDFS_USER)
        print(output)

#testing read write permission for hive user-----------------------------------------
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
       run_command(hadoop_container,cmd,HIVE_USER)
                  

#Negative Test - Unauthorized User Cannot Write 'HBASE'------------------------------
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
    
    #if want to fail for report purpose run_command(hadoop_container,unauth_write_cmd,HBASE_USER)
    
# Negative Test - Unauthorized User 'HBASE' Cannot Read ----------------------------------
def test_unauthorized_read(hadoop_container):

    unauth_read= UNAUTHORIZED_READ_COMMAND.format(filename=filename, ez_name=ez_name, user=HBASE_USER)
    output,exit_code = run_command(hadoop_container,unauth_read,HBASE_USER,fail_on_error=False,return_exit_code=True)
    
    print(f"Command Output:\n{output}")
        
    # Check for known failure indicators in output
    if exit_code != 0:
     failure_detected = True

    #assert that failure was detected as expected
    assert failure_detected, "Expected failure due to no permission on EZ, but command succeeded."
    
    #run_command(hadoop_container,unauth_read,HBASE_USER)

    
# Clean Up - Remove Test file and EZ
@pytest.mark.cleanEZ
def test_cleanup(hadoop_container):
   
    cleanup_cmd=[cmd.format(filename=filename, ez_name=ez_name) for cmd in CLEANUP_COMMANDS]
    for cmd in cleanup_cmd:
       output=run_command(hadoop_container,cmd,HDFS_USER)
    
    print(output)
    
    #clean EZ key
    key_deletion_cmd=KEY_DELETION_CMD.format(key_name=key_name)
    output=run_command(hadoop_container,key_deletion_cmd,KEY_ADMIN)
    print(output)

       

    



