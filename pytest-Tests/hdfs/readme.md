<!---
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


This workflow will build a Java project with Maven, and cache/restore any dependencies to improve the workflow execution time
For more information see: https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-java-with-maven

This workflow uses actions that are not certified by GitHub.
They are provided by a third-party and are governed by
separate terms of service, privacy policy, and support
documentation.
-->

# This is the main directory for testing HDFS encryption cycle

## Structure
```
test_hdfs/
├── test_encryption.py
├── test_encryption02.py  
├── test_encryption03.py
├── test_config.py        #stores all constants and HDFS commands
├── conftest.py           #sets up the environment
├── utils.py              #utility methods

```

---

## Features

- **Markers:**  
  Markers can be used to selectively run specific test cases, improving test efficiency and organization.

---

### `setup_environment`

Handled in `conftest.py` file
Before running the test cases, some environment configurations are needed:
- HDFS must communicate with KMS to fetch key details.
- Specific KMS properties are added to the `core-site.xml` file.
- Containers are restarted to apply the changes effectively.

---

### Utility Methods

- **get_error_logs:**  
  Fetches logs from both KMS and HDFS containers. Helps in identifying issues when errors or exceptions occur during testing.

- **run_command:**  
  Executes all necessary HDFS commands inside the containers.

---

## `test_encryption.py`

Handles the **full HDFS encryption cycle**, including setup, positive and negative test scenarios, and cleanup.

### Main Highlights:
- Encryption Zone (EZ) creation in HDFS.
- Granting permissions to specific users for read/write operations within the EZ.
- Validating read/write attempts by unauthorized users inside the EZ.


## `test_encryption02.py`

Handles the **Check if after key roll over old files can be read or not**
**Check if after key roll over new files can be written and read too**
**Check read operation on file after key deletion**
         
---

## `test_encryption03.py`

Handles the **Test case on cross Encryption zone operations**


## Summary

This test suite ensures that **HDFS encryption and access control mechanisms** function as expected, validating both authorized and unauthorized access scenarios.
