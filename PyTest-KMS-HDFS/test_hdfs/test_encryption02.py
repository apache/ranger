import pytest
import requests
from utils import run_command,get_error_logs 
from test_config import (HDFS_USER,HIVE_USER,HEADERS,PARAMS,BASE_URL,
                         CREATE_EZ_COMMANDS ,GRANT_PERMISSIONS_COMMANDS, 
                         CREATE_FILE_COMMAND, ACTIONS_COMMANDS,READ_EZ_FILE,
                         CLEANUP_COMMANDS)

# ****** ********************Test Case 01 ********************************************
# ***** Check if after key roll over old files can be read or not
# ***********************************************************************************
def test_read_old_file_after_rollover(hadoop_container):
    
    key_name="test-key1"
    ez_name = "secure_zone1"
    filename="testfile1"
    filecontent="Hello Human"


    #create EZ key-------
    key_data={
        "name":key_name
    }
    response=requests.post(f"{BASE_URL}/keys",json=key_data,params=PARAMS,headers=HEADERS)
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    
    # create EZ ------------
    create_ez_commands = [cmd.format(ez_name=ez_name, key_name=key_name) for cmd in CREATE_EZ_COMMANDS]

    for cmd in create_ez_commands:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        print(output)
    
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
       run_command(hadoop_container,cmd,HIVE_USER)
        

    #roll-over of key---------
    response=requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=HEADERS, params=PARAMS)
    assert response.status_code == 200, f"Key roll over failed: {response.text}"


    #read same file after roll over---------
    read_ez_file=[cmd.format(filename=filename, ez_name=ez_name) for cmd in READ_EZ_FILE]
    for cmd in read_ez_file:
        run_command(hadoop_container,cmd,HIVE_USER)

    #cleanup EZ and EZ file--------
    cleanup_cmd=[cmd.format(filename=filename, ez_name=ez_name) for cmd in CLEANUP_COMMANDS]
    for cmd in cleanup_cmd:
       run_command(hadoop_container,cmd,HDFS_USER)

    #delete EZ key ----------
    delete_output2=requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    print(delete_output2)



    # #key creation through hadoop command
    # output = run_command(hadoop_container,CREATE_KEY_COMMAND, KEY_ADMIN)
    # print("Key Creation Output:", output)

    # delete_output=run_command(hadoop_container,KEY_DELETION_CMD,KEY_ADMIN)
    # print(delete_output)


# ****** ********************Test Case 02 ********************************************
# ***** Check if after key roll over new files can be written and read too
# ***********************************************************************************
def test_writeAndRead_Newfile_after_rollover(hadoop_container):
    
    key_name="test-key2"
    ez_name = "secure_zone1"
    filename="testfile2"
    filename2="testfile3"
    filecontent="Hello Robot"
    filecontent2="Hello Second Robo"


    #create EZ key-------
    key_data={
        "name":key_name
    }
    response=requests.post(f"{BASE_URL}/keys",json=key_data,params=PARAMS,headers=HEADERS)
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    
    # create EZ ------------
    create_ez_commands = [cmd.format(ez_name=ez_name, key_name=key_name) for cmd in CREATE_EZ_COMMANDS]

    for cmd in create_ez_commands:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        print(output)
    
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
        

    #roll-over of key---------
    response=requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=HEADERS, params=PARAMS)
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
    delete_output2=requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    print(delete_output2)


# ****** ********************Test Case 03 ********************************************
# ***** Check read operation on file after key deletion 
# ***********************************************************************************
def test_Readfile_after_keyDeletion(hadoop_container):
    
    key_name="test-key3"
    ez_name = "secure_zone1"
    filename="testfile4"
    filename2="testfile5"
    filecontent="You are reading it before key deletion"
    filecontent2="You can't read me"


    #create EZ key-------
    key_data={
        "name":key_name
    }
    response=requests.post(f"{BASE_URL}/keys",json=key_data,params=PARAMS,headers=HEADERS)
    assert response.status_code == 201, f"Key creation failed: {response.text}"
    
    # create EZ ------------
    create_ez_commands = [cmd.format(ez_name=ez_name, key_name=key_name) for cmd in CREATE_EZ_COMMANDS]

    for cmd in create_ez_commands:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        print(output)
    
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
    delete_output2=requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    print(delete_output2)
    

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

