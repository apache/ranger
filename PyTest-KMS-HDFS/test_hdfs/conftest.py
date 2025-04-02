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


def configure_kms_property(hadoop_container):
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



def ensure_user_exists(hadoop_container, username):
    # Ensure keyadmin user exists
    print("Ensuring keyadmin user exists...")
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
