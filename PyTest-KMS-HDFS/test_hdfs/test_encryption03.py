import pytest
import requests
from utils import run_command,get_error_logs 
from test_config import (HDFS_USER,HIVE_USER,HEADERS,PARAMS,BASE_URL,
                         CREATE_EZ_COMMANDS ,GRANT_PERMISSIONS_COMMANDS, 
                         CREATE_FILE_COMMAND, ACTIONS_COMMANDS,READ_EZ_FILE,
                         CLEANUP_COMMANDS,CROSS_EZ_ACTION_COMMANDS,CLEANUP_EZ)




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
    key_data1={
        "name":key_name
    }
    key_data2={
        "name":key_name2
    }
    response=requests.post(f"{BASE_URL}/keys",json=key_data1,params=PARAMS,headers=HEADERS)
    assert response.status_code == 201, f"Key creation failed: {response.text}"

    response2=requests.post(f"{BASE_URL}/keys",json=key_data2,params=PARAMS,headers=HEADERS)
    assert response2.status_code == 201, f"Key creation failed: {response2.text}"
    
    # create 2 EZ ------------
    create_ez_commands = [cmd.format(ez_name=ez_name, key_name=key_name) for cmd in CREATE_EZ_COMMANDS]

    for cmd in create_ez_commands:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        print(output)

    create_ez_commands = [cmd.format(ez_name=ez_name2, key_name=key_name2) for cmd in CREATE_EZ_COMMANDS]

    for cmd in create_ez_commands:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        print(output)
    
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
    delete_output2=requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    print(delete_output2)

    delete_output2=requests.delete(f"{BASE_URL}/key/{key_name2}", params=PARAMS)
    print(delete_output2)

