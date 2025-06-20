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



#    KMS API & HDFS Encryption Pytest Suite


This test suite validates REST API endpoints for KMS (Key Management Service) and tests HDFS encryption functionalities including key management and file operations within encryption zones.

**test_kms  :** contains test cases for checking KMS API functionality  

**test_hdfs :** contains test cases for checking KMS functionality through hdfs encryption lifecycle

## 📂 Directory Structure

```
test_directory/
├── test_kms/                # Tests on KMS API
  ├── test_keys.py           # Key creation and key name validation
  ├── test_keys_02.py        # Extra test cases on key operation
  ├── test_keyDetails.py     # getKeyName, getKeyMetadata, getKeyVersion checks
  ├── test_keyOps.py         # Key operations: Roll-over, generate DEK, Decrypt EDEK
  ├── test_keyOps_policy.py  # validate key operation based on policy enforcement
  ├── test_blacklisting.py   # check key operation through blacklisted user
  ├── conftest.py            # Reusable fixtures and setup
  ├── utils.py               # Utility methods
  ├── readme.md
├── test_hdfs/               # Tests on HDFS encryption cycle
  ├── test_encryption.py     # test file 1
  ├── test_encryption02.py   # test file 2
  ├── test_encryption03.py   # test file 3
  ├── test_AuditSpooling.py  # test to check audit spooling operation
  ├── test_config.py         # stores all constants and HDFS commands
  ├── conftest.py            # sets up the environment
  ├── readme.md
  ├── utils.py               # Utility methods

├── pytest.ini               # Registers custom pytest markers
├── requirements.txt
├── README.md                # This file
```

## ⚙️ Setup Instructions
Bring up KMS container and any dependent containers using Docker.

Create a virtual environment and install the necessary packages through requirements.txt

## Run test cases

**Navigate to PyTest-KMS-HDFS directory**

**to run tests in test_kms folder**
> pytest -vs test_kms/

to run with report included
> pytest -vs test_kms/ --html=kms-report.html


**to run tests in test_hdfs folder**

> pytest -vs -k "test_encryption"
or
>pytest -vs test_hdfs/

to run with report included
>pytest -vs test_hdfs/ --html=hdfs-report.html

📌 Notes

Ensure Docker containers for KMS and HDFS are running before executing tests.

Reports generated using --html can be viewed in any browser for detailed test results.
