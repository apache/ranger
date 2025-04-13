import pytest
from utils import run_command,get_error_logs 
from test_config import (HDFS_USER,HIVE_USER,KEY_ADMIN,HEADERS,PARAMS,BASE_URL,
                         CREATE_KEY_COMMAND, VALIDATE_KEY_COMMAND, CREATE_EZ_COMMANDS,GRANT_PERMISSIONS_COMMANDS, 
                         HIVE_CREATE_FILE_COMMAND, HIVE_ACTIONS_COMMANDS,UNAUTHORIZED_WRITE_COMMAND, 
                         UNAUTHORIZED_READ_COMMAND,KEY_DELETION_CMD,
                         CLEANUP_COMMANDS)
                        

#Zone key creation before creating an EZ
def test_create_key(hadoop_container):

    # Run the command as keyadmin user
    output = run_command(hadoop_container,CREATE_KEY_COMMAND, KEY_ADMIN)
    print("Key Creation Output:", output)

    # Validate if the key was created successfully
    validation_output = run_command(hadoop_container, VALIDATE_KEY_COMMAND, KEY_ADMIN)

    print("Key List Output:", validation_output)

    # Check if key is present
    if "my_key" not in validation_output:
        error_logs = get_error_logs()                           # Fetch logs on failure
        pytest.fail(f"Key creation failed. Logs:\n{error_logs}")



# Create Encryption Zone Test Case -----------------------
@pytest.mark.createEZ
def test_create_encryption_zone(hadoop_container):

    # Commands to run
    for cmd in CREATE_EZ_COMMANDS:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        print(output)
        

#Grant Permissions to Hive User-----------
def test_grant_permissions(hadoop_container):
    
    for cmd in GRANT_PERMISSIONS_COMMANDS:
        output = run_command(hadoop_container,cmd,HDFS_USER)
        print(output)

#testing read write permission for hive user
def test_hive_user_write_read(hadoop_container):

    output = run_command(hadoop_container,HIVE_CREATE_FILE_COMMAND,HIVE_USER)
    print(output)

    for cmd in HIVE_ACTIONS_COMMANDS:
        output = run_command(hadoop_container,cmd,HIVE_USER)
        print(output)
                  

#Negative Test - Unauthorized User Cannot Write
def test_unauthorized_write(hadoop_container):
    output = run_command(hadoop_container,UNAUTHORIZED_WRITE_COMMAND,"hbase")
    print(output)
    
    
# Negative Test - Unauthorized User Cannot Read
def test_unauthorized_read(hadoop_container):

    output = run_command(hadoop_container,UNAUTHORIZED_READ_COMMAND,"hbase")
    print(output)


# Clean Up - Remove Test file and EZ
@pytest.mark.cleanEZ
def test_cleanup(hadoop_container):
   
    for cmd in CLEANUP_COMMANDS:
        output = run_command(hadoop_container,cmd,HDFS_USER)
        print(output)

    
    output=run_command(hadoop_container,KEY_DELETION_CMD,KEY_ADMIN)
    print(output)

       

    



