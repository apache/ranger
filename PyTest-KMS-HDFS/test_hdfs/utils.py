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
import docker
from test_config import (KMS_CONTAINER,HADOOP_NAMENODE_LOG_PATH,KMS_LOG_PATH)

# Setup Docker Client
client = docker.from_env()

#to run all HDFS commands
def run_command(container, cmd, user, fail_on_error=True,return_exit_code=False):
        exit_code, output = container.exec_run(cmd, user=user)
        output_response = output.decode()

        if exit_code != 0 and fail_on_error:
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
        if return_exit_code:
          return output_response, exit_code
        
        return output_response


#fetch logs from hadoop and KMS file
def get_error_logs(hadoop_container, kms_container):

    # Get Hadoop NameNode logs
    hadoop_log_cmd = f"tail -n 50 {HADOOP_NAMENODE_LOG_PATH}"
    _, hadoop_logs = hadoop_container.exec_run(hadoop_log_cmd, user='hdfs')
    hadoop_logs_decoded = hadoop_logs.decode()
    hadoop_error_lines = [line for line in hadoop_logs_decoded.split("\n") if "ERROR" in line or "Exception" in line or "WARN" in line]
    hadoop_error_text = "\n".join(hadoop_error_lines) if hadoop_error_lines else "No recent errors in Hadoop Namenode logs."

    # Get KMS logs
    kms_log_cmd = f"tail -n 50 {KMS_LOG_PATH}"
    _, kms_logs = kms_container.exec_run(kms_log_cmd, user='root')
    kms_logs_decoded = kms_logs.decode()
    kms_error_lines = [line for line in kms_logs_decoded.split("\n") if "ERROR" in line or "Exception" in line or "WARN" in line]
    kms_error_text = "\n".join(kms_error_lines) if kms_error_lines else "No recent errors in KMS logs."

    return hadoop_error_text, kms_error_text
