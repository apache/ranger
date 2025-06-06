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
import time
import docker
import requests
from test_config import (KMS_CONTAINER,BASE_URL,PARAMS,HEADERS,CREATE_EZ_COMMANDS,HDFS_USER,HIVE_USER,READ_EZ,CLEANUP_EZ)
from utils import run_command 

AUDIT_XML_PATH = "/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp/WEB-INF/classes/conf/ranger-kms-audit.xml"

# Setup Docker Client
client = docker.from_env()
kms_container= client.containers.get(KMS_CONTAINER)

SPOOL_PROPS = """\
<property>
  <name>xasecure.audit.destination.hdfs.batch.queuetype</name>
  <value>filequeue</value>
</property>
<property>
  <name>xasecure.audit.destination.hdfs.batch.filequeue.filespool.dir</name>
  <value>/var/log/kms/audit/hdfs/spool</value>
</property>"""

@pytest.fixture(scope="function")
def configure_kms_audit_xml(hadoop_container):

    # Check if both new properties exist
    grep_queue_cmd = f"grep 'xasecure.audit.destination.hdfs.batch.queuetype' {AUDIT_XML_PATH}"
    grep_spool_cmd = f"grep 'xasecure.audit.destination.hdfs.batch.filequeue.filespool.dir' {AUDIT_XML_PATH}"

    queue_code, _ = kms_container.exec_run(grep_queue_cmd, user='root')
    spool_code, _ = kms_container.exec_run(grep_spool_cmd, user='root')

    added = False
    
    if queue_code != 0 or spool_code != 0:
        # Inject both properties just before </configuration>
        flat_props = SPOOL_PROPS.replace('\n', '').replace('"', '\\"')
        inject_cmd = f"""sed -i '/<\\/configuration>/e echo "{flat_props}"' {AUDIT_XML_PATH}"""

        # inject_cmd = f"sed -i '/<\\/configuration>/i {SPOOL_PROPS}' {AUDIT_XML_PATH}"
        kms_container.exec_run(inject_cmd, user='root')
        added = True
        print("Injected audit spool properties in ranger-kms-audit.xml")

    if added:
        # Restart KMS to apply changes
        kms_container.restart()
        time.sleep(30)

    yield  # Run the test

    if added:
        cleanup_cmd = (
            f"sed -i '/<property>/,/<\\/property>/{{/"
            f"xasecure.audit.destination.hdfs.batch.queuetype\\|"
            f"xasecure.audit.destination.hdfs.batch.filequeue.filespool.dir/d;}}' {AUDIT_XML_PATH}"
        )
        kms_container.exec_run(cleanup_cmd, user='root')
        print("🧹 Cleaned up audit spool properties in ranger-kms-audit.xml")

        # Restart KMS
        kms_container.restart()
        time.sleep(5)


#TEST CASE: Verify the audit spool directory contains local logs
def test_audit_spooling(hadoop_container, configure_kms_audit_xml):

    key_name = "spool_key"
    ez_name= "spool_zone"

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
    
    
    # read using unauth user
    read_cmd= [cmd.format(ez_name=ez_name) for cmd in READ_EZ]
    failure_detected = False

    # Try unauthorized access to trigger audit logging
    for cmd in read_cmd:
      output, exit_code = run_command(hadoop_container, cmd, HIVE_USER, fail_on_error=False, return_exit_code=True)
      if exit_code != 0:
        failure_detected = True  

    # Assert that it failed (unauthorized access expected)
    assert failure_detected, "Expected failure due to no permission on EZ, but all commands succeeded."

    time.sleep(120)
    
    # Check that spool file exists
    check_cmd = f"ls /var/log/kms/audit/hdfs/spool/"
    exit_code,output= kms_container.exec_run(check_cmd,user="root")
    output = output.decode()
    assert ".log" in output, f"No audit log file found in spool directory: /var/log/kms/audit/hdfs/spool/"

    check_dir_cmd = "ls -l /var/log/kms/audit/hdfs/spool/"
    exit_code, dir_output = kms_container.exec_run(check_dir_cmd,user="root")
    print("📁 Spool directory contents:\n", dir_output.decode())
    
    read_log_cmd = f"sh -c 'cat /var/log/kms/audit/hdfs/spool/*.log'"
    exit_code,log_content = kms_container.exec_run(read_log_cmd,user="root")
    print("📝 Log file contents:\n", log_content.decode())



     #cleanup EZ and EZ file--------
    cleanup_cmd=[cmd.format(ez_name=ez_name) for cmd in CLEANUP_EZ]
    for cmd in cleanup_cmd:
       run_command(hadoop_container,cmd,HDFS_USER)

    #cleanup spool file
    cleanup_cmd = "rm -rf /var/log/kms/audit/hdfs/spool"
    kms_container.exec_run(cleanup_cmd,user="root")
    

    #delete EZ key ----------
    delete_output2=requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)
    print(delete_output2)