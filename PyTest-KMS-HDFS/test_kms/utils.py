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





#no scope mismatch due to utils
import subprocess

KMS_CONTAINER_NAME = "ranger-kms"             # Replace with your actual KMS container name
KMS_LOG_FILE = "/var/log/ranger/kms/ranger-kms-ranger-kms.example.com-root.log"

def fetch_logs():
    try:
        cmd = f"docker exec {KMS_CONTAINER_NAME} tail -n 100 {KMS_LOG_FILE}"
        logs = subprocess.check_output(cmd, shell=True, text=True)
        error_logs = [line for line in logs.split("\n") if "ERROR" in line or "Exception" in line]
        return "\n".join(error_logs) if error_logs else "No recent errors in logs."
    except Exception as e:
        return f"Failed to fetch logs from container: {str(e)}"
