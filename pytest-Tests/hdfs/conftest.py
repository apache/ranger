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
from test_config import (HADOOP_CONTAINER, HDFS_USER,KMS_PROPERTY,CORE_SITE_XML_PATH)

# Setup Docker Client
client = docker.from_env()

@pytest.fixture(scope="session")
def hadoop_container():
    container = client.containers.get(HADOOP_CONTAINER)      #to get hadoop container instance
    return container

def ensure_key_provider_and_simple_auth(container) -> bool:
    """
    Ensures:
      1) KMS provider property exists
      2) hadoop.security.authentication = simple
    Returns True if the file was modified.
    """
    changed = False

    # 1) Ensure KMS provider property exists
    exit_code, _ = container.exec_run(
        f"grep -q 'hadoop.security.key.provider.path' {CORE_SITE_XML_PATH}",
        user="root",
    )
    if exit_code != 0:
        container.exec_run(
            f"sed -i '/<\\/configuration>/i {KMS_PROPERTY}' {CORE_SITE_XML_PATH}",
            user="root",
        )
        changed = True

    # 2) Force auth to simple (replace value if property exists, else insert new property)
    exit_code, _ = container.exec_run(
        f"grep -q '<name>hadoop.security.authentication</name>' {CORE_SITE_XML_PATH}",
        user="root",
    )
    if exit_code == 0:
        container.exec_run(
            "sed -i "
            "'/<name>hadoop.security.authentication<\\/name>/,/<\\/property>/ "
            "s/<value>[^<]*<\\/value>/<value>simple<\\/value>/' "
            f"{CORE_SITE_XML_PATH}",
            user="root",
        )
        changed = True
    else:
        simple_prop = (
            "<property><name>hadoop.security.authentication</name>"
            "<value>simple</value></property>"
        )
        container.exec_run(
            f"sed -i '/<\\/configuration>/i {simple_prop}' {CORE_SITE_XML_PATH}",
            user="root",
        )
        changed = True

    return changed

def ensure_user_exists(container, username: str) -> None:
    exit_code, _ = container.exec_run(f"id -u {username}", user="root")
    if exit_code == 0:
        return

    container.exec_run(f"useradd -m -s /bin/bash {username}", user="root")
    container.exec_run(f"usermod -aG hadoop {username}", user="root")


@pytest.fixture(scope="session", autouse=True)
def setup_environment(hadoop_container):
    changed = ensure_key_provider_and_simple_auth(hadoop_container)
    if changed:
        hadoop_container.restart()

    time.sleep(30)  # Wait for container to restart and services to come up

    ensure_user_exists(hadoop_container, "keyadmin")
    hadoop_container.exec_run("hdfs dfsadmin -safemode leave", user=HDFS_USER)

    yield
