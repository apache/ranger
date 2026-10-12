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

A Docker-based test execution framework that validates Apache Ranger service integrations through pytest. Spins up the required containers, runs the test suites, and tears down — all from a single script. New service modules can be added as pytest folders without changing the script.

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

### Directory Structure

```text
functional-tests/
├── multiple modules containing test cases
├── ...
│
├── ranger_test_utils/utils.py   # Shared logging, HTTP asserts, Public API v2 helpers
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

## Running Tests

The `run-tests.sh` script manages Docker container setup, dependency installation, and test execution. It supports both interactive and argument-based modes.

### 1. Interactive Mode (Preferred)

Run the script without arguments to be prompted for each input:
```text
./run-tests.sh
```

You will be asked **four** questions in sequence:

```text
Available DB types: postgres, mysql, oracle
Enter DB type (press Enter to default to postgres): postgres

Available audit stores: opensearch, solr, none
Enter audit store (press Enter to default to opensearch): opensearch

Available test suites: rolerest xuserrest servicerest tagrest hdfs kms api hive
Enter test suites space-separated (press Enter to run ALL): hdfs kms

Available containers : hadoop hive hbase knox trino ozone kafka
  (base always starts: ranger, kdc, db, zk, kms, pdp, usersync, tagsync + audit pipeline)
Enter containers space-separated (press Enter or 'all' for ALL): hadoop
```

> Press **Enter** at any prompt to accept the default. Type `all` at the containers prompt to bring up every optional container.

### 2. Command-Line Arguments Mode

Pass `db-type` and `test-suites` as arguments to skip those prompts. Use env vars for the remaining options:

```text
./run-tests.sh [db-type] [test-suites...]
```

- `db-type` — First positional argument. Valid values: `postgres`, `mysql`, `oracle`.
- `test-suites` — Remaining arguments, space-separated.

The containers prompt always appears unless `SELECTED_CONTAINERS` is exported.

**Examples:**

```text
# Run hdfs and kms tests — postgres db, no audit pipeline, hadoop container only
export AUDIT_INDEX_STORE=none
export SELECTED_CONTAINERS=hadoop
./run-tests.sh postgres hdfs kms

# Run all test suites — mysql db, opensearch audit, all optional containers
export SELECTED_CONTAINERS=all
./run-tests.sh mysql

# Full clean rebuild — all suites, all containers
export CLEAN_CONTAINERS=1 
export SELECTED_CONTAINERS=all 
./run-tests.sh postgres
```

---

## Environment Variables

All variables have sensible defaults and can be exported before running or passed inline.

| Variable | Default | Description |
|---|---|---|
| `CLEAN_CONTAINERS` | `0` | Set to `1` to wipe all containers and force a full rebuild from scratch |
| `RUN_TESTS` | `1` | Set to `0` to bring up infrastructure only without running any tests |
| `AUDIT_INDEX_STORE` | `opensearch` | Audit backend: `opensearch`, `solr`, or `none` to skip the audit pipeline entirely |
| `SELECTED_CONTAINERS` | _(prompted)_ | Space-separated optional containers to bring up, e.g. `"hadoop hive"`. Set to `"all"` to bring up every optional container. |

### `CLEAN_CONTAINERS`

By default containers **persist between runs** for fast re-execution. Set to `1` only when you need a completely clean rebuild — e.g. after a Maven source change or to reset all data. On all subsequent re-runs, leave it unset to reuse existing containers.

### `RUN_TESTS`

Set to `0` to bring up the full infrastructure without executing any tests. Useful when containers are still initializing or you want to inspect the environment first before running tests.

### `AUDIT_INDEX_STORE`

Controls whether the audit pipeline (Kafka + ingestor + OpenSearch/Solr) is started. Defaults to `opensearch`. Set to `none` to skip the entire audit pipeline — no Kafka download, no audit containers.

### `SELECTED_CONTAINERS`

Pre-select which optional containers to bring up without being prompted interactively. Accepts a space-separated list of container names (e.g. `"hadoop hive"`) or `all` to bring up every optional container. When not set, the script prompts at runtime.

---

## Containers Brought Up

### Always (Base Services)

Regardless of which test suites or containers you choose, these always start. This matches the `ranger-core` command in `dev-support/ranger-docker/README.md`.

| Container | Role |
|---|---|
| `ranger` | Ranger Admin (policies, users, REST APIs) |
| `ranger-kdc` | Kerberos KDC |
| `ranger-<db>` | Database (`ranger-postgres`, `ranger-mysql`, `ranger-oracle`) |
| `ranger-zk` | ZooKeeper |
| `ranger-kms` | Ranger KMS |
| `ranger-pdp` | Ranger PDP (Policy Decision Point) |
| `ranger-usersync` | User/group sync from LDAP or Unix |
| `ranger-tagsync` | Tag-based policy sync |

### Audit Pipeline (when `AUDIT_INDEX_STORE != none`)

| Container | Role |
|---|---|
| `ranger-kafka` | Kafka broker |
| `ranger-audit-ingestor` | Reads from Kafka, writes to audit store |
| `ranger-opensearch` / `ranger-solr` | Audit index store |
| `ranger-audit-dispatcher-<store>` | Dispatches audit events |

### Optional Containers (selected at runtime)

Brought up based on what you enter at the containers prompt (or `SELECTED_CONTAINERS` env var):

| Container name entered | Containers started | Notes |
|---|---|---|
| `hadoop` | `ranger-hadoop` | HDFS + YARN |
| `hive` | `ranger-hadoop`, `ranger-hive` | hadoop auto-included |
| `hbase` | `ranger-hadoop`, `ranger-hbase` | hadoop auto-included |
| `knox` | `ranger-knox` | |
| `trino` | `ranger-trino` | |
| `ozone` | `ozone-om`, `ozone-scm`, `ozone-datanode` | pre-setup script runs automatically |
| `kafka` | `ranger-kafka` | |
| `all` / _(Enter)_ | All of the above | |

---

### Hive suite notes

- Service repo name in Docker is `dev_hive` (not QE `cm_hive`).
- Tests currently assert **beeline authorization output only**. Hive access audits helpers live in `ranger_test_utils/access_audit_utils.py` for a follow-up once auditing works.
- Example: `./run-tests.sh postgres hive`

## Test Reports

After execution, an HTML report is automatically generated for each suite in the `functional-tests/` directory:
