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

This test suite validates REST API endpoints for Apache Ranger services — Admin (rolerest, xuserrest, servicerest,tagrest), KMS (Key Management Service), and HDFS encryption functionalities including key management and file operations within encryption zones.

### Available Test Suites

| Suite | Description |
|---|---|
| **hdfs** | Test cases for HDFS encryption lifecycle using KMS |
| **kms** | Test cases for KMS REST API functionality |
| **xuserrest** | Test cases for Ranger Admin User/Group REST APIs |
| **rolerest** | Test cases for Ranger Admin Role REST APIs |
| **servicerest** | Test cases for Ranger Admin Service REST APIs |
| **tagrest** | Test cases for Ranger Admin Tag REST APIs |
| **api** | Public API v2 service and policy CRUD (admin Docker only) |
| **hive** | Hive Ranger plugin enforcement via HS2/beeline (requires `ranger-hadoop` + `ranger-hive`) |

---

### Directory Structure

```text
functional-tests/
├── ranger_test_utils/utils.py   # Shared logging, HTTP asserts, Public API v2 helpers
├── hdfs/                        # Tests on HDFS encryption cycle
├── kms/                         # Tests on KMS REST API
├── xuserrest/                   # Tests on Ranger User/Group/Role REST APIs
├── rolerest/                    # Tests on Ranger Role REST APIs
├── servicerest/                 # Tests on Ranger Service REST APIs
├── tagrest/                     # Tests on Ranger Tag REST APIs
├── api/                         # Public API v2 (service / policy) tests
├── hive/                        # Hive plugin enforcement tests
├── ranger_test_utils/hive_utils.py
├── ranger_test_utils/access_audit_utils.py  # For future access-audit asserts
│
├── pytest.ini                   # Registers custom pytest markers
├── run-tests.sh                 # Script to automate setup and test execution
├── requirements.txt             # Python dependencies
└── readme.md                    # This documentation
```

> **Note:** A Python virtual environment folder named `myenv` is created automatically
> on the first run and **reused on all subsequent runs** — no reinstallation overhead.

---

## Prerequisites

1. Docker & Docker Compose installed and running
2. Python 3.10 or higher
3. Change the working directory to `functional-tests/`
```text
cd functional-tests/
```
4. Make the shell script executable
```text
chmod +x run-tests.sh
```

---

## Environment Variables

These can be exported before running, or passed inline. All have sensible defaults

| Variable | Default | Description |
|---|---|---|
| `CLEAN_CONTAINERS` | `0` | Set to `1` to wipe all containers and force a full rebuild from scratch |
| `RUN_TESTS` | `1` | Set to `0` to bring up infrastructure only without running any tests |
| `AUDIT_INDEX_STORE` | `opensearch` | Audit backend: `opensearch`, `solr`, or `none` to skip the audit pipeline entirely |

### `CLEAN_CONTAINERS`

By default containers **persist between runs** for fast re-execution. Use `CLEAN_CONTAINERS=1` only when you want a completely clean rebuild (e.g. for rebuilding the Docker image via Maven against your local source):

```text
export CLEAN_CONTAINERS=1
./run-tests.sh
```

On all subsequent re-runs (no rebuild needed):
```text
./run-tests.sh
```

### `RUN_TESTS`

Bring up the full infrastructure without executing any tests. Useful when containers are still initializing or you want to inspect the environment first:

```text
export RUN_TESTS=0
./run-tests.sh
```

Once containers are healthy, run tests normally (default):
```text
./run-tests.sh
```

### `AUDIT_INDEX_STORE`

Controls whether the audit pipeline (Kafka + ingestor + OpenSearch/Solr) is started. Defaults to `opensearch`. Allowed values for export AUDIT_INDEX_STORE= (solr/opensearch/none)

```text
export AUDIT_INDEX_STORE=none
./run-tests.sh
```

---

## Running Tests
The `run-tests.sh` script manages Docker container setup, dependency installation, and test execution. It supports both interactive and argument-based modes.

1. Interactive Mode:

### 1. Interactive Mode

Run the script without arguments to be prompted for each input:
```text
./run-tests.sh
```

You will be asked three questions in sequence:

```text
Available DB types: postgres, mysql, oracle
Enter DB type (press Enter to default to postgres): postgres

Available audit stores: opensearch, solr, none
Enter audit store (press Enter to default to opensearch): none

Available test suites: rolerest xuserrest servicerest tagrest hdfs kms
Enter test suites space-separated (press Enter to run ALL): kms hdfs
```

> Press **Enter** at any prompt to accept the default value.

### 2. Command-Line Arguments Mode

Pass `db-type` and `test-suites` directly to skip prompts. Use env vars for the remaining options:

```text
./run-tests.sh [db-type] [test-suites...]
```

- `db-type` — First argument. Valid values: `postgres`, `mysql`, `oracle`.
- `test-suites` — Space-separated list: `hdfs`, `hive`, `kms`, `rolerest`, `xuserrest`, `servicerest`, `tagrest`, `api`.

Examples:

```text
# Run kms and hdfs tests with postgres, no audit pipeline
export AUDIT_INDEX_STORE=none 
./run-tests.sh postgres kms hdfs

# Run all suites with mysql and opensearch audit
./run-tests.sh mysql

# Full clean rebuild with all suites
CLEAN_CONTAINERS=1 ./run-tests.sh postgres
```

---

## Containers Brought Up

### Always (Base Services)

Regardless of which test suites you choose, these containers always start:

| Container | Role |
|---|---|
| `ranger` | Ranger Admin (policies, users, REST APIs) |
| `ranger-kdc` | Kerberos KDC |
| `ranger-<db>` | Database (`ranger-postgres`, `ranger-mysql`, `ranger-oracle`) |
| `ranger-zk` | ZooKeeper |
| `ranger-kms` | Ranger KMS |

### Audit Pipeline (when `AUDIT_INDEX_STORE != none`)

| Container | Role |
|---|---|
| `ranger-kafka` | Kafka broker |
| `ranger-audit-ingestor` | Reads from Kafka, writes to audit store |
| `ranger-opensearch` / `ranger-solr` | Audit index store |
| `ranger-audit-dispatcher-<store>` | Dispatches audit events |

### Suite-Specific

| Suite | Extra Container |
|---|---|
| `hdfs` | `ranger-hadoop` |
| `hive` | `ranger-hadoop`, `ranger-hive` |
| `kms` | _(already in base)_ |
| `rolerest`, `xuserrest`, `servicerest`, `tagrest`| _(none — use Ranger Admin API only)_ |

---


### Hive suite notes

- Service repo name in Docker is `dev_hive` (not QE `cm_hive`).
- Tests currently assert **beeline authorization output only**. Hive access audits helpers live in `ranger_test_utils/access_audit_utils.py` for a follow-up once auditing works.
- Example: `./run-tests.sh postgres hive`

## Test Reports
After execution, an HTML report is automatically generated for each suite in the `functional-tests/` directory:
