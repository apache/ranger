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


##    Pytest Functional Test-suite


This test suite validates REST API endpoints for KMS (Key Management Service) and tests HDFS encryption functionalities including key management and file operations within encryption zones.

**hdfs :** contains test cases for checking KMS functionality through hdfs encryption lifecycle

**kms  :** contains test cases for checking KMS API functionality


### Directory Structure

```
pytest-Tests/
├── hdfs/                    # Tests on HDFS encryption cycle
├── kms/                     # Tests on KMS API
├── pytest.ini               # Registers custom pytest markers
├── run_tests.sh             # Script to automate test execution
├── requirements.txt
├── readme.md

```

### Running Tests

#### Container Setup

Before running the tests, configure container behavior using the following environment variable:
~~~
  # To force a fresh container setup:
  export CLEAN_CONTAINERS=1
~~~

After the initial setup, you can disable fresh container creation by setting below for the next re-runs:
~~~
  export CLEAN_CONTAINERS=0
~~~

#### Executing Tests

Run the test script using:
~~~
  ./run-tests.sh [db-type] [additional-services]
  
  # valid values for db-type: mysql/postgres/oracle , postgres is the default
  # additional-services: multiple services can be specified separated by space
  
  # e.g for running tests within kms and hdfs use below command:
  
  ./run-tests.sh postgres hadoop
  
  # Note: If additional-services are specified, db-type must also be explicitly specified

~~~

#### Note (Optional)

If you only need to start the infrastructure i.e containers (without running tests):
~~~
  export RUN_TESTS=0
~~~
This is useful when tests are failing due to incomplete container setup.

After the infrastructure is successfully up, set RUN_TESTS=1 and rerun the script to execute the tests without setup issues.

#### Note

Reports generated after tests execution in html can be viewed in any browser for detailed test results.
