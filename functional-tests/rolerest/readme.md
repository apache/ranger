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

# This is the main directory for running ROLEREST API functionality tests

This directory contains automated functional tests for the ROLEREST API, covering roles functionality.

## Structure
```
rolerrest/
├── utility/                        
│   ├── __init__.py
│   ├── utils.py              
├── __init__.py 
├── conftest.py 
├── test_role_management.py 
├── test_role_utility_fun.py  
```


## Features and Functionalities Used:

- **Parametrization:** For running multiple test cases handling the same functionality in a single method.

- **fetch_logs:** Fetches errors or exceptions from logs when something goes wrong.

- **cleanup:** Cleans up all resources used while testing, ensuring re-runs of test cases.

---

## `utils.py`

## `utils.py`
- Shared utility module for all role REST test files — provides reusable helpers and API interaction functions
- Defines global constants: BIGINT_MIN/BIGINT_MAX for boundary checks, RANGER_CONTAINER_NAME/RANGER_LOG_FILE for Docker log access
- init_configs() sets global auth configs for all four roles (admin, keyadmin, auditor, user)
- assert_response() delegates to xuserrest — validates HTTP status codes and fetches Ranger logs on failure
- Service helpers: create_service(), assign_service_admin(), assign_service_admin_group(), delete_service() — provision HDFS services and service-admin access for authorization tests
- Role helpers: get_role_by_name(), delete_role() — lookup and teardown of roles
- ensureRoleAccess() provisions users, groups, roles, and services based on test case (admin, service admin, group admin, role membership) and returns auth, query params, and cleanup items
---
## `conftest.py`
- Central pytest configuration file providing shared fixtures and helpers for the test suite
- Defines module-level constants: CREDENTIALS, DEFAULT_HEADERS, KEYADMIN_CREDENTIALS
- Includes create_user_with_retry() — retries secure user POST creation up to 5 times with incremental sleep on failure
- Provides session/function/class scoped fixtures:
  1. Session: credentials, default_headers, keyadmin_credentials, ranger_config, ranger_key_admin_config
  2. Function: ranger_session
  3. Class: temp_secure_user, temp_keyadmin_user, temp_group, temp_role — all auto-cleanup after test class
---
## `test_role_management.py`
- Tests Role CRUD and lookup operations within TestRoleCRUD
- Class-scoped _setup provisions primary and secondary users across all roles (admin, keyadmin, auditor, user), groups, and roles with auto-cleanup
- GET: /roles/roles, /roles/roles/{id}, /roles/roles/names, /roles/roles/name/{name}, /roles/roles/user/{userName}, /roles/lookup/roles
- POST/PUT/DELETE: create, update, and delete roles by ID and name
- Covers role-based access control including service admin and service-admin-group paths via ensureRoleAccess()
- Negative tests validate unauthorized access, invalid IDs/names, and malformed payloads
---
## `test_role_utility_fun.py`
- Tests role utility and advanced operations within TestRoleUtilityFun
- Class-scoped _setup mirrors TestRoleCRUD — users, groups, roles, and init_configs()
- Export/import: GET /roles/roles/exportJson, POST /roles/roles/importRolesFromFile
- Download: GET /roles/secure/download/{serviceName} with version-based 200/304 responses
- Membership: PUT addUsersAndGroups, removeUsersAndGroups, removeAdminFromUsersAndGroups
- Grant/revoke: PUT /roles/roles/grant/{serviceName} and /roles/roles/revoke/{serviceName}
- Negative and unauthorized variants for all utility endpoints
- Note: /roles/download/{serviceName} tests are currently skipped pending rolerest changes