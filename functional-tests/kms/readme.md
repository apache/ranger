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

# This is the main directory for running KMS API functionality tests

Guides: **[AGENTS.md](./AGENTS.md)** · **[skills.md](./skills.md)**  
Repo-wide: [../../AGENTS.md](../../AGENTS.md) · [../../skills.md](../../skills.md)

## Structure
```
kms/
├── test_kms_encryption_zone_key_ops.py
├── test_keys.py
├── test_keyDetails.py 
├── test_keyOps.py  
├── test_kms_04.py
├── test_blacklisting.py
├── test_encryption.py
├── test_encryption02.py
├── test_encryption03.py
├── test_encryption04.py
```

Shared helpers: **`ranger_test_utils/kms_utils.py`** (Docker curl, Kerberos, REST, ``KmsXaTestCase``);
**`ranger_test_utils/kms_hdfs_test_config.py`** (HDFS encryption zone command templates and Docker paths).

Session Kerberos for KMS tests: **`functional-tests/conftest.py`**.

HDFS encryption zone tests (``test_encryption.py``) need the **``ranger-hadoop``** container:
``export SELECTED_CONTAINERS=hadoop`` before ``./run-tests.sh postgres kms``.
``kms/conftest.py`` provides ``hadoop_container`` and encryption session setup (``kms/`` and ``hdfs/`` via root ``conftest.py`` plugin).

## Features and Functionalities Used:

- **Parametrization:** For running multiple test cases handling the same functionality in a single method.

- **fetch_logs:** Fetches errors or exceptions from logs when something goes wrong.

- **cleanup:** Cleans up all resources used while testing, ensuring re-runs of test cases.

---

## `test_kms_encryption_zone_key_ops.py`

Encryption zone key scenarios: create, fetch, rollover, delete, generate EDEK/DEK and verify decrypt.

Uses ``TEST_CASE`` / ``KmsXaTestCase``, key snapshot, ``ensure_keyadmin_can_list_kms_keys``,
and ``deleteKeysForThisTest``.

---

## `test_keys.py`

Key creation REST checks:

1. **test_create_key** — create with full payload  
2. **test_key_name_validation** — parametrized valid/invalid names  
3. **test_duplicate_key_creation** — duplicate create expects HTTP 500  

Uses keyadmin and the seed ``dev_kms`` policy (not ``test_kms_04`` policy hijack).

---

## `test_keyDetails.py`

Retrieval of key-related data (names, per-key metadata/versions, bulk metadata, keyversion).

Module setup creates ``key1`` for metadata tests.

1. **test_get_key_names** — list contains ``key1``  
2. **test_get_key_metadata** — parametrized existent / non-existent key  
3. **test_get_key_versions** — parametrized existent / non-existent key  
4. **test_get_keys_metadata** — bulk ``/keys/metadata`` for ``key1`` + ``key2``  
5. **test_get_key_version** — ``/keyversion/{name}@0``  

---

## `test_keyOps.py`

Operations on keys (rollover, DEK/EDEK, re-encrypt batch, cache invalidation).

Module setup creates ``key1`` for metadata/DEK tests.

1. **test_temp_key** — create/delete rollover key  
2. **test_roll_over_key** — parametrized rollover (valid / missing key)  
3. **test_roll_over_new_material** — metadata changes after rollover  
4. **test_generate_data_key_and_decrypt** — ``/_dek`` + decrypt EEK  
5. **test_reencrypt_encrypted_keys** — ``/_reencryptbatch`` after rollover  
6. **test_generate_data_key_after_invalidate_cache** — invalidateCache + DEK  

---

## `test_kms_04.py`

Incremental KMS Ranger policy enforcement for ``hrt_21`` via ``KmsXaTestCase`` /
``createEncryptionZoneKey``, ``setEncryptionZoneKeyMaterial``, ``generateEncryptedDEKs``, etc.

Tests must run in module order; policy is restored on teardown.

---

## `test_blacklisting.py`

KMS **dbks-site.xml blacklist** for **keyadmin**: CREATE, ROLLOVER, DELETE (container restart
after each change). Uses ``blacklist_op_users`` in ``ranger_test_utils/kms_utils.py``.

1. **test_user_keyOperation_before_blacklist** — create / rollover / delete succeed  
2. **test_blacklist_create** — 403 then 201 after unblacklist  
3. **test_blacklist_rollOver** — rollover blocked then allowed  
4. **test_blacklist_delete** — delete blocked then allowed  

---

## `test_encryption.py`

Full HDFS encryption zone cycle: create KMS key, create EZ, grant hive access,
hive read/write, hbase write/read denied, cleanup EZ and key. Uses ``hdfs.utils`` and
``kms_hdfs_test_config`` command templates; requires ``ranger-hadoop``.

---

## `test_encryption02.py`

KMS key rollover and deletion with HDFS encryption zone file access:

1. **test_read_old_file_after_rollover** — read existing EZ file after key rollover  
2. **test_writeAndRead_Newfile_after_rollover** — write/read a new file after rollover  
3. **test_Readfile_after_keyDeletion** — read fails after KMS key delete  

Uses ``TEST_CASE`` / ``KmsXaTestCase`` for key create, rollover, and delete; requires ``ranger-hadoop``.

---

## `test_encryption03.py`

Cross encryption zone access for hive: read/write under subdirs on EZ1, put to EZ2 denied.

1. **test_cross_EZ_operations** — two keys/zones; hive granted on ``secure_zone1`` only  

Requires ``ranger-hadoop``.

---

## `test_encryption04.py`

``DECRYPT_EEK`` Ranger policy on ``decrypt-eek-key``: deny ``hdfs``, allow ``hrt21`` for copy/read in the EZ.

1. **test_01_EncryptFile_case1A** — hdfs denied, hrt21 reads plain text after cp into EZ  

Module setup creates the scoped policy and waits for KMS plugin sync (up to ~120s). Requires ``ranger-hadoop``.
