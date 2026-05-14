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

## Pytest Functional Test Suite

This test suite validates REST API endpoints for Apache Ranger services,Admin (xuserrest, servicerest), KMS (Key Management Service), and tests HDFS encryption functionalities including key management and file operations within encryption zones.

### Available Test Suites

| Suite | Description |
|---|---|
| **hdfs** | Test cases for HDFS encryption lifecycle using KMS |
| **kms** | Test cases for KMS REST API functionality |
| **xuserrest** | Test cases for Ranger Admin User/Group/Role REST APIs |
| **servicerest** | Test cases for Ranger Admin Service REST APIs |

---

### Directory Structure

```text
pytest-Tests/
├── hdfs/                        # Tests on HDFS encryption cycle
│   ├── conftest.py              # Fixtures and setup for HDFS tests
│   └── test_file.py             # HDFS encryption test cases
│
├── kms/                         # Tests on KMS REST API
│   ├── conftest.py              # Fixtures and setup for KMS tests
│   └── test_file.py              # KMS API test cases
│
├── xuserrest/                   # Tests on Ranger User/Group/Role REST APIs
│   ├──utility/                  # Utility Folder contains helper functions
│   |     ├── utils.py   
│   ├── conftest.py              # Fixtures and setup for user REST tests
│   └── test_file.py             # User REST API test cases
│
├── servicerest/                 # Tests on Ranger Service REST APIs
│   ├──utility/                  # Utility Folder contains helper functions
│   |     ├── utils.py   
│   ├── conftest.py              # Fixtures and setup for service REST tests
│   ├── test_file.py             # Service REST API test cases
│   └── automation.log           # logs related to the tests and the conftest files for servicerest
│
├── pytest.ini                   # Registers custom pytest markers
├── run_tests.sh                 # Script to automate setup and test execution
├── requirements.txt             # Python dependencies
└── readme.md                    # This documentation

```
> **Note:** A Python virtual environment folder named `myenv` will be automatically
> generated upon running the tests for the first time.


## Prerequisites
1. Docker & Docker Compose installed and running
2. Python 3.10 or higher
3. Change the working directory to pytest-Tests
```text
cd pytest-Tests/
```
4. Make the shell script executable

```text
chmod +x run_tests.sh
```


## Environment Variables

Configure container behavior before running the script using the following environment variables:

1. Fresh Setup & Cleanup:

Force a clean environment & helps building binaries with local changes (removes old Ranger containers, prunes Docker space, builds fresh, and cleans up after tests):
```text
export CLEAN_CONTAINERS=1
./run_tests.sh
```

After initial setup, disable fresh container creation to speed up subsequent runs:

```text
export CLEAN_CONTAINERS=0
./run_tests.sh
```

2. Infrastructure Only (Skip Tests)

Start Docker infrastructure without executing Pytest suites:

```text
export RUN_TESTS=0
./run_tests.sh
```
This is useful when tests fail due to slow container startup. Once all containers are healthy, re-enable tests:

```text
export RUN_TESTS=1
./run_tests.sh
```
 

## Running Tests
The run_tests.sh script manages Docker container setup, dependency installation, and test execution. It supports both interactive and argument-based modes.

1. Interactive Mode:

Run the script without arguments to be prompted for inputs:
```text
./run_tests.sh
```
> DB Type: Enter one of postgres, mysql, oracle, mssql. Defaults to postgres.

> Test Suites: Enter space-separated suite names. Defaults to ALL suites. 

example:
```text

Available DB types: postgres, mysql, oracle, mssql
Enter DB type (press Enter to default to postgres): postgres

Available test suites: xuserrest servicerest hdfs kms
Enter test suites space-separated (press Enter to run ALL): kms hdfs
```

2. Command-Line Arguments Mode:

Pass arguments directly to skip prompts:

```text
./run_tests.sh [db-type] [test-suites...]
```
db-type — Must be the first argument. Valid values: postgres, mysql, oracle, mssql.

test-suites — Space-separated list: hdfs, kms, xuserrest, servicerest.

Examples:

```text
# Run all suites with Postgres (default)
./run_tests.sh postgres

# Run specific suites with Postgres
./run_tests.sh postgres kms hdfs

# Run only user REST tests with MySQL
./run_tests.sh mysql xuserrest

# Run service REST tests with Oracle
./run_tests.sh oracle servicerest
 ```

## Test Reports
After execution, HTML reports are automatically generated for each suite. Open the corresponding file in any browser to view detailed results:

| Suite       | Report File             |
|:------------|:------------------------|
| hdfs        | report_hdfs.html        |
| kms         | report_kms.html         |
| xuserrest   | report_xuserrest.html   |
| servicerest | report_servicerest.html |