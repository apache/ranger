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


import docker
import pytest
import time
from test_config import (HADOOP_CONTAINER, HDFS_USER,KMS_PROPERTY,CORE_SITE_XML_PATH,SET_PATH_CMD)

# Setup Docker Client
client = docker.from_env()

@pytest.fixture(scope="module")
def hadoop_container():
    container = client.containers.get(HADOOP_CONTAINER)      #to get hadoop container instance
    return container

# polling method to wait until container gets restarted
def wait_for_hdfs(container, user='hdfs', timeout=60, interval=2):

    print("Waiting for HDFS to become available...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        container.reload()  # 🔁 refresh state

        if container.status != "running":
            print("❌ Container is not running yet.")
            time.sleep(interval)
            continue

        try:
            exit_code, _ = container.exec_run("hdfs dfs -ls /", user=user)
            if exit_code == 0:
                print("✅ HDFS is ready.")
                return True
            else:
                print("⏳ HDFS not ready yet, retrying...")
        except docker.errors.APIError as e:
            print(f"⚠️ APIError while checking HDFS: {e}")
            print("Retrying after brief wait...")

        time.sleep(interval)

    raise TimeoutError("HDFS did not become ready within the timeout period.")


def configure_kms_property(hadoop_container):

    global client
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
        time.sleep(5)

        #Re-fetch container after restart
        hadoop_container = client.containers.get(HADOOP_CONTAINER)

        wait_for_hdfs(hadoop_container, user=HDFS_USER)  # Wait for container to fully restart
        # time.sleep(10)
        print("Hadoop container restarted and ready.")

    else:
        print("KMS provider already present. No need to update config.")

    # # Leave safe mode if active
    # print("Exiting safe mode (if active)...")
    # leave_safe_mode_cmd = "hdfs dfsadmin -safemode leave"
    # exit_code, output = hadoop_container.exec_run(leave_safe_mode_cmd, user=HDFS_USER)
    # print(output.decode())  # For debugging


def ensure_user_exists(hadoop_container, username):
    # Ensure keyadmin user exists
    print("Ensuring keyadmin user exists...")
    hadoop_container.reload()
    user_check_cmd = f"id -u {username}"
    exit_code, _ = hadoop_container.exec_run(user_check_cmd, user='root')

    if exit_code != 0:
        # Create the keyadmin user if not already present
        create_user_cmd = f"useradd {username}"
        exit_code, output = hadoop_container.exec_run(create_user_cmd, user='root')
        print(f"keyadmin user created. Exit code: {exit_code}")

        # Assign necessary permissions to the user
        assign_permissions_cmd = f"usermod -aG hadoop {username}"
        exit_code, output = hadoop_container.exec_run(assign_permissions_cmd, user='root')
        print(f"Permissions assigned to keyadmin. Exit code: {exit_code}")
    else:
        print("keyadmin user already exists. No need to create.")



# Automatically setup environment before tests run
@pytest.fixture(scope="module", autouse=True)
def setup_environment(hadoop_container):
    
    set_path_cmd = SET_PATH_CMD
    hadoop_container.exec_run(set_path_cmd, user='root')

    configure_kms_property(hadoop_container)
    ensure_user_exists(hadoop_container,"keyadmin")

    # Exit Safe Mode
    print("Exiting HDFS Safe Mode...")
    hadoop_container.exec_run("hdfs dfsadmin -safemode leave", user=HDFS_USER)

    yield  # Run tests

    # Post-test cleanup
    print("Tests completed.")
