import pytest
import docker
import requests
from utils import fetch_logs 

import time
from docker.errors import APIError

# Setup Docker Client
client = docker.from_env()

HADOOP_CONTAINER = "ranger-hadoop"
KMS_CONTAINER = "ranger-kms"
HDFS_USER = "hdfs"
HIVE_USER = "hive"
HEADERS={"Content-Type": "application/json","Accept":"application/json"}
PARAMS={"user.name":"keyadmin"}
BASE_URL="http://localhost:9292/kms/v1"

# Constants
KMS_PROPERTY = """<property><name>hadoop.security.key.provider.path</name><value>kms://http@host.docker.internal:9292/kms</value></property>"""

CORE_SITE_XML_PATH = "/opt/hadoop/etc/hadoop/core-site.xml"

# Automatically setup environment before tests run
@pytest.fixture(scope="module", autouse=True)
def setup_environment():
    hadoop_container = client.containers.get(HADOOP_CONTAINER)

     # Ensure PATH is set for /opt/hadoop/bin
    set_path_cmd = "echo 'export PATH=/opt/hadoop/bin:$PATH' >> /etc/profile && export PATH=/opt/hadoop/bin:$PATH"
    hadoop_container.exec_run(set_path_cmd, user='root')

    # Check if KMS property already exists
    check_cmd = f"grep 'hadoop.security.key.provider.path' {CORE_SITE_XML_PATH}"
    exit_code, _ = hadoop_container.exec_run(check_cmd, user='root')

    if exit_code != 0:
        # Insert KMS property
        insert_cmd = f"sed -i '/<\\/configuration>/i {KMS_PROPERTY}' {CORE_SITE_XML_PATH}"
        exit_code, output = hadoop_container.exec_run(insert_cmd, user='root')
        print(f"KMS property inserted. Exit code: {exit_code}")

        # Debug: Show updated file
        cat_cmd = f"cat {CORE_SITE_XML_PATH}"
        _, file_content = hadoop_container.exec_run(cat_cmd, user='root')
        print("Updated core-site.xml:\n", file_content.decode())

        # Restart the container to apply the config changes
        print("Restarting Hadoop container to apply changes...")
        hadoop_container.restart()
        time.sleep(10)  # Wait for container to fully restart
        print("Hadoop container restarted and ready.")

    else:
        print("KMS provider already present. No need to update config.")

    # Leave safe mode if active
    print("Exiting safe mode (if active)...")
    leave_safe_mode_cmd = "hdfs dfsadmin -safemode leave"
    exit_code, output = hadoop_container.exec_run(leave_safe_mode_cmd, user=HDFS_USER)
    print(output.decode())  # For debugging

    yield  # Run tests

    # Post-test cleanup
    print("Tests completed.")

@pytest.fixture(scope="module")
def hadoop_container():
    container = client.containers.get(HADOOP_CONTAINER)      #to get hadoop container instance
    return container

def get_error_logs(hadoop_container, kms_container):

    # Get Hadoop NameNode logs
    hadoop_log_cmd = "tail -n 50 /opt/hadoop/logs/hadoop-hdfs-namenode-ranger-hadoop.example.com.log"
    hadoop_exit, hadoop_logs = hadoop_container.exec_run(hadoop_log_cmd, user='hdfs')
    hadoop_logs_decoded = hadoop_logs.decode()
    hadoop_error_lines = [line for line in hadoop_logs_decoded.split("\n") if "ERROR" in line or "Exception" in line or "WARN" in line]
    hadoop_error_text = "\n".join(hadoop_error_lines) if hadoop_error_lines else "No recent errors in Hadoop Namenode logs."

    # Get KMS logs
    kms_log_cmd = "tail -n 50 /var/log/ranger/kms/ranger-kms-ranger-kms.example.com-root.log"
    kms_exit, kms_logs = kms_container.exec_run(kms_log_cmd, user='root')
    kms_logs_decoded = kms_logs.decode()
    kms_error_lines = [line for line in kms_logs_decoded.split("\n") if "ERROR" in line or "Exception" in line or "WARN" in line]
    kms_error_text = "\n".join(kms_error_lines) if kms_error_lines else "No recent errors in KMS logs."

    return hadoop_error_text, kms_error_text

def run_command(container, cmd, user):
        exit_code, output = container.exec_run(cmd, user=user)
        output_response = output.decode()

        if exit_code != 0:
            kms_container = client.containers.get(KMS_CONTAINER)
            hadoop_logs, kms_logs = get_error_logs(container, kms_container)

            pytest.fail(f"""
            Command failed: {cmd}
            Exit Code: {exit_code}

            Output:
            {output_response}

            Hadoop Container Logs:
            {hadoop_logs}

            KMS Container Logs:
            {kms_logs}
            """)
        return output_response

def test_create_key():
        key_data = {
            "name": "my_key",
            "cipher": "AES/CTR/NoPadding",
            "length": 128,
            "description": "test key for hdfs encryption in pytest"
        }
      
        response = requests.post(f"{BASE_URL}/keys",headers=HEADERS, json=key_data,params=PARAMS)
        print(response.json())

        if response.status_code != 201:
            error_logs = fetch_logs()  # Fetch logs on failure
            pytest.fail(f"Key creation failed. API Response: {response.text}\nLogs:\n{error_logs}")



# Create Encryption Zone Test Case -----------------------
@pytest.mark.createEZ
def test_create_encryption_zone(hadoop_container):

    # Commands to run
    commands = [
        "hdfs dfs -mkdir /secure_zone2",
        "hdfs crypto -createZone -keyName my_key -path /secure_zone2",
        "hdfs crypto -listZones"
    ]

    for cmd in commands:
        output = run_command(hadoop_container, cmd, HDFS_USER)
        print(output)
        

#Grant Permissions to Hive User-----------
def test_grant_permissions(hadoop_container):
    commands = [
        "hdfs dfs -chmod 700 /secure_zone2",
        "hdfs dfs -chown hive:hive /secure_zone2"
    ]
    for cmd in commands:
        output = run_command(hadoop_container,cmd,HDFS_USER)
        print(output)

#testing read write permission for hive user
def test_hive_user_write_read(hadoop_container):

    create_file = 'bash -c \'echo "Hello, this is a third file!" > /home/hive/testfile2.txt && ls -l /home/hive/testfile2.txt\''

    output = run_command(hadoop_container,create_file,HIVE_USER)
    print(output)

    commands = [
        "hdfs dfs -put /home/hive/testfile2.txt /secure_zone2/",
        "hdfs dfs -ls /secure_zone2/",
        "hdfs dfs -cat /secure_zone2/testfile2.txt"
    ]
    for cmd in commands:
        output = run_command(hadoop_container,cmd,HIVE_USER)
        print(output)
                  


#Negative Test - Unauthorized User Cannot Write
def test_unauthorized_write(hadoop_container):
    unauth_write='hdfs dfs -put /home/hbase/hack.txt /secure_zone2/'
    output = run_command(hadoop_container,unauth_write,"hbase")
    print(output)
    
    
# Negative Test - Unauthorized User Cannot Read
def test_unauthorized_read(hadoop_container):
    unauth_read="hdfs dfs -cat /secure_zone2/testfile2.txt"
    output = run_command(hadoop_container,unauth_read,"hbase")
    print(output)


# Clean Up - Remove Test file and EZ
@pytest.mark.cleanEZ
def test_cleanup(hadoop_container):
    commands = [
        "hdfs dfs -rm /secure_zone2/testfile2.txt",
        "hdfs dfs -rm -R /secure_zone2"
    ]
    for cmd in commands:
        output = run_command(hadoop_container,cmd,HDFS_USER)
        print(output)

    requests.delete(f"{BASE_URL}/key/my_key",params=PARAMS)               #cleanup my_key


