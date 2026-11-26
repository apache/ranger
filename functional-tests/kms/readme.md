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

# KMS REST API Functional Tests

Comprehensive test suite for Ranger KMS REST API functionality with 45 tests across 8 files, following the established Hive/HBase pattern.

## Structure

```
kms/
├── test_keys.py             # Key creation, validation, duplicates (6 tests)
├── test_keys_02.py          # Key lifecycle operations (4 tests)
├── test_keys_03.py          # Material visibility with policies (3 tests)
├── test_keys_04.py          # Case normalization policies (6 tests)
├── test_blacklisting.py     # Operation blacklisting (4 tests)
├── test_keyDetails.py       # Key details retrieval (7 tests)
├── test_keyOps.py           # Key operations, DEK/EDEK, cache (7 tests)
└── test_keyOps_policy.py    # Progressive policy enforcement (8 tests)
```

**Total: 45 tests** covering key lifecycle, operations, policy enforcement, blacklisting, and validation.

## Pattern

All tests follow the unified pattern established for plugin enforcement tests:

- **Session-level container validation:** `pytest.mark.kms` marker triggers `conftest.py` session fixture
- **Module-level setup/teardown:** Each test file has `setup_module()` and `teardown_module()`
- **No conftest dependencies:** Tests are self-contained (no fixtures)
- **Direct resource tracking:** Keys and policies tracked in module-level lists
- **Two-layer user setup:** Ranger Admin user (REST API) + Kerberos principal (KDC)
- **Consolidated utilities:** All helpers in `ranger_test_utils/kms_utils.py` (475 lines)

## Utilities

**All KMS utilities are consolidated in:**
- `ranger_test_utils/kms_utils.py` — KMS REST, Kerberos, policies, blacklisting, user management (475 lines)

**Key functions:**
- `preflight_kms_stack()` — Container validation (skips tests if containers unavailable)
- `ensure_keyadmin_ticket()`, `ensure_testuser_ticket()` — Kerberos authentication
- `ensure_test_user_exists()`, `delete_test_user()` — Ranger user management (REST)
- `krb_requests` — Kerberos-authenticated HTTP client
- `find_conflicting_policies()`, `update_kms_policy()` — Policy management
- `blacklist_op_users()`, `unblacklist_op_users()` — Blacklist operations

## Features

- **Parametrization:** Multiple test cases with different inputs in a single test function
- **Logging:** Comprehensive execution flow tracking via `plugin_enforcement_utils.logger`
- **Error handling:** Fetches KMS logs on failures for debugging
- **Cleanup:** All tests clean up resources (keys, policies, users) in `teardown_module()`
- **Kerberos authentication:** Docker-based Kerberos using `krb_requests`

---

## Test Files

### `test_keys.py` — Key Creation and Validation (6 tests)

**Purpose:** Basic key management operations with validation.

**Tests:**
1. `test_create_key()` — Creates key2, verifies 201 response, cleans up inline
2. `test_key_name_validation()` — **Parametrized (4 cases):** Tests valid and invalid key name patterns
   - `"valid-key"` → 201 (success)
   - `""` (empty) → 400 (validation error)
   - `"@invalid!"` (special chars) → 400 (validation error)
   - `"invalid--key"` (double dash) → 400 (validation error)
3. `test_duplicate_key_creation()` — Negative test: Attempts duplicate key creation, expects 500 error

**Pattern:** Simple keyadmin-only tests with inline cleanup (no persistent keys).

---

### `test_keys_02.py` — Key Lifecycle Operations (4 tests)

**Purpose:** Key rollover, material verification, deletion, and bulk operations.

**Tests:**
1. `test_versionIncrement_after_rollover()` — Verifies version increments after key rollover
2. `test_newMaterial_after_rollover()` — Verifies new key material generated after rollover
3. `test_delete_key()` — Tests key deletion and verifies 404 on subsequent access
4. `test_bulk_delete_keys()` — Creates and deletes multiple keys in bulk

**Pattern:** Uses test key (key1) from `setup_module()`, tracks created keys in `_created_keys` list.

---

### `test_keys_03.py` — Material Visibility with Policies (3 tests)

**Purpose:** Tests GET policy enforcement for key material visibility.

**Tests:**
1. `test_get_key_material_with_policy()` — testuser with GET policy can retrieve material
2. `test_get_key_material_without_policy()` — testuser without GET policy gets 403
3. `test_get_key_material_keyadmin()` — keyadmin always has access

**Pattern:** Dual-user (testuser + keyadmin), creates Ranger policies, two-layer user setup (Ranger REST + Kerberos).

**Users:** `testuser`, `keyadmin`

---

### `test_keys_04.py` — Key Name Case Normalization (6 tests)

**Purpose:** Tests case-insensitive key name handling with allow + deny policies.

**Tests:**
1. `test_case_normalization_allow_policy()` — Allow policy on `testkey*` works with `testkey04`
2. `test_case_normalization_mixed_case()` — Mixed case key names normalized
3. `test_deny_policy_blocks_access()` — Deny policy on `testkey04` blocks access
4. `test_allow_wildcard_deny_specific()` — Allow wildcard + deny specific key
5. `test_multiple_case_variations()` — Multiple case variations (TESTkey04, TestKey04, etc.)
6. `test_policy_cleanup()` — Verifies policy cleanup

**Pattern:** Dual-user with complex policy combinations (allow wildcard + deny specific), tracks policies and keys.

**Users:** `testuser`, `keyadmin`

---

### `test_blacklisting.py` — Operation Blacklisting (4 tests)

**Purpose:** Tests operation-level blacklisting (CREATE, ROLLOVER, DELETE) with container restarts.

**Tests:**
1. `test_blacklist_create()` — Blacklists CREATE, restarts KMS, verifies 403, unblacklists, verifies 200
2. `test_blacklist_rollover()` — Blacklists ROLLOVER, restarts KMS, verifies 403, unblacklists, verifies 200
3. `test_blacklist_delete()` — Blacklists DELETE, restarts KMS, verifies 403, unblacklists, verifies 200
4. `test_blacklist_all_operations()` — Blacklists all operations, verifies all return 403

**Pattern:** Unique - modifies `dbks-site.xml`, requires KMS container restarts, re-authenticates Kerberos after restart.

**Users:** `keyadmin` (blacklisted user)

---

### `test_keyDetails.py` — Key Details Retrieval (7 tests)

**Purpose:** Tests endpoints for retrieving key information (names, metadata, versions).

**Tests:**
1. `test_get_key_names()` — Retrieves list of all key names
2. `test_get_key_metadata()` — **Parametrized (2 cases):** Gets metadata for existent/non-existent keys
3. `test_get_key_versions()` — **Parametrized (2 cases):** Gets versions for existent/non-existent keys
4. `test_get_keys_metadata()` — Batch metadata retrieval for multiple keys
5. `test_get_key_version()` — **Parametrized (2 cases):** Gets specific version details (format: `key1@0`)

**Pattern:** Was class-based, now function-based with parametrized tests. Creates test key in `setup_module()`.

**Users:** `keyadmin`

---

### `test_keyOps.py` — Key Operations and Cache (7 tests)

**Purpose:** Advanced key operations including DEK/EDEK encryption/decryption and cache invalidation.

**Tests:**
1. `test_temp_key()` — Creates and deletes temporary rollover-key
2. `test_roll_over_key()` — **Parametrized (2 cases):** Tests rollover for existent/non-existent keys
3. `test_roll_over_new_material()` — Verifies rollover creates new material (metadata comparison)
4. `test_generate_data_key_and_decrypt()` — Full DEK/EDEK cycle:
   - Generates DEK (Data Encryption Key) and EDEK (Encrypted DEK)
   - Extracts encryption details from EDEK
   - Decrypts EDEK back to DEK
   - Verifies decrypted DEK matches original
5. `test_reencrypt_encrypted_keys()` — Tests `_reencryptbatch` endpoint:
   - Creates key, generates EDEK
   - Rotates key (new version)
   - Re-encrypts EDEK with new version
   - Verifies EDEK versionName changed
6. `test_generate_data_key_after_invalidate_cache()` — Complex cache scenario:
   - Creates key (version @0)
   - Rolls over (creates @1, cached)
   - Deletes key (DB clean, cache stale)
   - Recreates key (only @0 in DB)
   - Invalidates cache (forces reload)
   - Generates DEK (should succeed with correct version)

**Pattern:** Mixed scenarios from simple to complex, uses test key from `setup_module()` for some tests.

**Users:** `keyadmin`

---

### `test_keyOps_policy.py` — Progressive Policy Enforcement (8 tests)

**Purpose:** Tests KMS policy enforcement with progressive permission combinations.

**Tests:**
1. `test_policy_01()` — CREATE only → 1 allowed, 5 denied (403)
2. `test_policy_02()` — CREATE + DELETE → 2 allowed, 4 denied
3. `test_policy_03()` — CREATE + DELETE + ROLLOVER → 3 allowed, 3 denied
4. `test_policy_04()` — CREATE + DELETE + ROLLOVER + GET → 4 allowed, 2 denied
5. `test_policy_05()` — CREATE + DELETE + ROLLOVER + GET + GETMETADATA → 5 allowed, 1 denied
6. `test_policy_06()` — + GENERATEEEK → 6 allowed, 0 denied
7. `test_policy_07()` — Full access (all permissions including DECRYPTEEK) → All allowed
8. `test_policy_08()` — No access → 0 allowed, 6 denied (403)

**Pattern:** Shared policy pattern - one policy (pytest-policy on pytest-* keys) updated by each test.

**Users:** `testuser` (policy enforcement), `keyadmin` (cleanup)

**Unique:** Module-level global `_policy_id` for shared policy across all tests.

---

## Running Tests

### Run All KMS Tests (45 tests)

```bash
cd functional-tests/
./run-tests.sh postgres kms
```

### Run Specific Test File

```bash
# Single file (7 tests)
pytest kms/test_keyOps_policy.py -v

# Multiple files
pytest kms/test_keys.py kms/test_keys_02.py -v
```

### Run Single Test

```bash
# Specific test
pytest kms/test_keys_02.py::test_versionIncrement_after_rollover -v

# Specific parametrized test case
pytest kms/test_keyDetails.py::test_get_key_metadata[key1-200-valid] -v
```

### Run with Marker

```bash
# All KMS tests (using marker)
pytest -m kms -v

# KMS tests with live output
pytest -m kms -v -s
```

### View Logs

```bash
# After test execution
cat logs/test_keyOps_policy.log
cat logs/test_keys_02.log
```

## Prerequisites

**Required containers:**
- `ranger` — Ranger Admin (policies, users, REST APIs)
- `ranger-kms` — Ranger KMS
- `ranger-hadoop` — HDFS (for Kerberos)
- `ranger-kdc` — Kerberos KDC
- `ranger-<db>` — Database (postgres/mysql/oracle)

**No optional containers needed** — All tests use KMS REST API directly.

**Authentication:**
- Docker-based Kerberos via `krb_requests`
- Two-layer user setup (Ranger Admin user + Kerberos principal)


## Test Counts Summary

| Test File | Tests | Type |
|-----------|-------|------|
| test_keys.py | 6 | Basic key operations |
| test_keys_02.py | 4 | Key lifecycle |
| test_keys_03.py | 3 | Policy-based visibility |
| test_keys_04.py | 6 | Case normalization |
| test_blacklisting.py | 4 | Operation blacklisting |
| test_keyDetails.py | 7 | Details retrieval (parametrized) |
| test_keyOps.py | 7 | DEK/EDEK, cache |
| test_keyOps_policy.py | 8 | Progressive permissions |
| **TOTAL** | **45** | **8 files** |



