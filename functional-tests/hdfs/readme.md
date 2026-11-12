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
-->

# This is the main directory for testing HDFS encryption cycle

## Structure
```
hdfs/
├── utils.py              #utility methods
```

HDFS encryption zone constants and command templates:
**`ranger_test_utils/kms_hdfs_test_config.py`**. Session fixtures (``hadoop_container``, encryption setup) live in **`kms/conftest.py`**; the root **`functional-tests/conftest.py`** loads that plugin when you run ``pytest hdfs/``.

---

## Features

- **Markers:**  
  Markers can be used to selectively run specific test cases, improving test efficiency and organization.

---

### `setup_environment`

Handled in **`kms/conftest.py`** (loaded for ``pytest hdfs/`` via root **`functional-tests/conftest.py`**).

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

HDFS encryption zone lifecycle tests live under **`kms/test_encryption.py`**,
**`kms/test_encryption02.py`**, **`kms/test_encryption03.py`**, and **`kms/test_encryption04.py`**
(DECRYPT_EEK policy; require ``ranger-hadoop``).

## Summary

This test suite ensures that **HDFS encryption and access control mechanisms** function as expected, validating both authorized and unauthorized access scenarios.
