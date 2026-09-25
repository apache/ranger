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

# This is the main directory for running XUSERREST API functionality tests

This directory contains automated functional tests for the XUSERREST API, covering user management, group management, permissions, secure user operations, and UGSync functionality.

## Structure
```
test_xuserrest/
├── utility/                        
│   ├── __init__.py
│   ├── utils.py              
├── __init__.py 
├── conftest.py 
├── test_groups.py 
├── test_permission.py  
├── test_secure_user.py
├── test_ugsync.py
├── test_user.py       
├── test_utility_fun.py
```


## Features and Functionalities Used:

- **Parametrization:** For running multiple test cases handling the same functionality in a single method.

- **fetch_logs:** Fetches errors or exceptions from logs when something goes wrong.

- **cleanup:** Cleans up all resources used while testing, ensuring re-runs of test cases.

---

## `utils.py`

- Shared utility module for all xuser REST test files — provides reusable helpers, schema validators, and API interaction functions
- Defines global constants: BIGINT_MIN/BIGINT_MAX for boundary checks, RANGER_CONTAINER_NAME/RANGER_LOG_FILE for Docker log access
init_configs() sets global auth configs for all four roles (admin, keyadmin, auditor, user)
assert_response() validates HTTP status codes (single or list) and on failure fetches Ranger logs via fetch_ranger_logs()
fetch_ranger_logs() runs docker exec to pull categorized log sections — recent activity, errors/warns, API calls, and HTTP-code-specific patterns using keyword_map
- Schema validators: validate_user_schema(), validate_secure_user_schema(), validate_xgroup_schema(), validate_external_user_schema(), group_permission_schema() — enforce field types, lengths, and value constraints
CRUD helpers: user_exists(), delete_user(), group_exists(), delete_group(), groupuser_exists(), delete_groupuser() — used for test setup/teardown
- UGSync helpers: validate_auditinfo_schema(), validate_sync_source_info() — validate sync audit responses across unix, file, and ldap sources with source-specific field schemas
- Permission helpers: build_user_permission(), build_group_permission(), build_permission_payload(), user_permission_exists() — construct and verify module permission payloads return_value_ugsync_groupusers() computes expected count of group-user sync operations for assertion in UGSync tests


---

## `conftest.py`

- Central pytest configuration file providing shared fixtures and helpers for the entire test suite
- Defines module-level constants: CREDENTIALS, DEFAULT_HEADERS, KEYADMIN_CREDENTIALS
- Includes create_user_with_retry() — retries user POST creation up to 5 times with incremental sleep on failure
- Provides session/function/class scoped fixtures:
   1. Session: credentials, default_headers, keyadmin_credentials, ranger_config, ranger_key_admin_config, client_roles
   2. Function: ranger_session, all_users, all_schema_following_users, get_user_by_id
   3. Class: temp_secure_user, temp_keyadmin_user, temp_permission_module, temp_group, temp_permission_group, temp_permission_user, temp_role — all auto-cleanup after test class

---

## `test_groups.py`

- Tests for Group management operations across three test classes: TestSecureGroups, TestGroups, and TestGroupUsers
- Each class uses a _setup fixture (class-scoped) that provisions primary & secondary users across all roles (admin, key admin, auditor, user) and two temporary groups with auto-cleanup
- Default response code for auth failure cases is set to 404 to prevent API endpoint discovery due to Spring silent failures

---

## `test_secure_user.py`

- Tests Secure User CRUD operations via /xusers/secure/users/* endpoints within TestSecureUserEndpoint
- Class-scoped _setup provisions primary & secondary users across all roles (admin, keyadmin, auditor, user) with auto-cleanup
- Defines BIGINT_MIN / BIGINT_MAX constants for boundary value testing
- Covers role-based access control across all operations — verifying both allowed and restricted role/target combinations
- Validates immutability of fields (name, id, createDate) on PUT and silent 204 behavior on malformed bulk DELETE payloads
Uses forceDelete=true and X-Requested-By: ranger headers where required by the API

---

## `test_ugsync.py`

- Covers userinfo endpoint for user-group association creation with schema validation
- Tests UGSync (User-Group Sync) operations within TestUgsync for syncing users/groups from external sources (LDAP, UNIX, FILE, MULTI_SOURCES)
- Class-scoped _setup provisions primary & secondary users across all roles (admin, keyadmin, auditor, user) and two temp groups with auto-cleanup
- All endpoints are admin-only; unauthorized roles (keyadmin, auditor, user) consistently return 404 due to Spring's silent failure instead of 403
- Invalid payloads (bad group/user names, missing fields) often return 200 as silent failures — response body is validated instead of status code
- Validates addUsers/delUsers group membership changes are reflected via follow-up GET calls
- Covers sync source audit logging for all source types with schema validation via validate_auditinfo_schema() and validate_sync_source_info()
- Tests group/user visibility toggling (sets isVisible=0) and verifies persistence via follow-up GET
- Handles semi-failure scenarios (mixed valid/invalid users in one payload) and asserts partial success count in response

---

## `test_user.py`

- Tests User CRUD operations via /xusers/users/* endpoints within TestUsers
- Class-scoped _setup provisions primary & secondary users across all roles (admin, keyadmin, auditor, user) with auto-cleanup
- Covers role-based access control across all operations — verifying both allowed and restricted role/target combinations
- Tests role assignment logic via /roleassignments endpoint — validating priority order: whiteListUser > whiteListGroup > userMap > groupMap > default
- Validates Spring's silent 404 failure pattern for unauthorized POST/DELETE operations (non-admin roles)
- Tests external user creation flow — 204 on first call (new user), 200 on second call (existing user)
- Covers userinfo endpoint for user-group association creation with schema validation
Validates immutability enforcement and invalid role assignment (ROLE_KEY_ADMIN, ROLE_NON_EXISTEN) returning 400/403

---

## `test_utility_fun.py`

- Tests Lookup and AuthSession utility endpoints within TestLookup and AuthSession classes
- Class-scoped _setup provisions users across all roles (admin, keyadmin, auditor, user) with auto-cleanup
- All four roles are permitted for every endpoint — no role-based restriction tests
- TestLookup covers /xusers/lookup/users, /xusers/lookup/groups, and /xusers/lookup/principals — validating vXStrings schema when response is non-empty
- AuthSession covers /xusers/authSessions with query param variations  — validates vXAuthSessions or totalCount in response
