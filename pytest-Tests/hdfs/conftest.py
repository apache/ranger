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
from hdfs.test_config import (HADOOP_CONTAINER, HDFS_USER, KMS_PROPERTY, CORE_SITE_XML_PATH, KMS_CONTAINER)
from hdfs.test_config import TEST_KMS_KEYS, TEST_EZ_PATHS
from hdfs.utils import (
    cleanup_test_artifacts,
    ensure_kms_hdfs_policy,
    ensure_kms_kerberos_rules,
    ensure_hadoop_user_keytab,
)
from kms.utils import ensure_keyadmin_keytab, ensure_ticket

# Setup Docker Client
client = docker.from_env()
def ensure_kms_provider_configured(container) -> bool:
    """
    Ensures KMS provider property exists in core-site.xml.
    Returns True if the file was modified (restart needed).
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

    # 2) Fix stale host.docker.internal from previous runs
    exit_code, _ = container.exec_run(
        f"grep -q 'host.docker.internal' {CORE_SITE_XML_PATH}",
        user="root",
    )
    if exit_code == 0:
        container.exec_run(
            f"sed -i 's|host.docker.internal|ranger-kms.rangernw|g' {CORE_SITE_XML_PATH}",
            user="root",
        )
        changed = True

    return changed
@pytest.fixture(scope="session", autouse=True)
def setup_kerberos():
    ensure_keyadmin_keytab()
    ensure_ticket()

@pytest.fixture(scope="session", autouse=True)
def setup_environment(hadoop_container):
    changed = ensure_kms_provider_configured(hadoop_container)
    if changed:
        hadoop_container.restart()
        time.sleep(30)

    ensure_user_exists(hadoop_container, "hive")
    ensure_user_exists(hadoop_container, "hbase")
    ensure_hadoop_user_keytab("hive")

    # safemode leave with kerberos (hdfs principal already has keytab in container)
    hadoop_container.exec_run(
        ["bash", "-c",
         "kinit -kt /etc/keytabs/hdfs.keytab hdfs/ranger-hadoop.rangernw@EXAMPLE.COM 2>/dev/null;"
         "hdfs dfsadmin -safemode leave"],
        user=HDFS_USER,
    )

    kms_container = client.containers.get(KMS_CONTAINER)
    if ensure_kms_kerberos_rules(kms_container):
        time.sleep(5)

    ensure_keyadmin_keytab()
    ensure_ticket()
    ensure_kms_hdfs_policy()
    ensure_ticket()

    cleanup_test_artifacts(hadoop_container, TEST_KMS_KEYS, TEST_EZ_PATHS)
    yield
    cleanup_test_artifacts(hadoop_container, TEST_KMS_KEYS, TEST_EZ_PATHS)

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

    # 2) Fix stale host.docker.internal from previous runs
    exit_code, _ = container.exec_run(
        f"grep -q 'host.docker.internal' {CORE_SITE_XML_PATH}",
        user="root",
    )
    if exit_code == 0:
        container.exec_run(
            f"sed -i 's|host.docker.internal|ranger-kms.rangernw|g' {CORE_SITE_XML_PATH}",
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


