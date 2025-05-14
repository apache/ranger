# This is the main directory for running KMS API functionality tests

## Structure
```
test_kms/
├── test_keys.py 
├── test_keys_02.py 
├── test_keyDetails.py 
├── test_keyOps.py  
├── test_keyOps_policy.py   
├── conftest.py       
├── utils.py
```


## Extra Features and Functionalities Used:

- **Parametrization:** For running multiple test cases handling the same functionality in a single method.

- **fetch_logs:** Fetches errors or exceptions from logs when something goes wrong.

- **cleanup:** Cleans up all resources used while testing, ensuring re-runs of test cases.

---

## `conftest.py`

Special file used to define fixtures and shared configurations that pytest can automatically discover and use across tests.  
Pytest automatically loads this file, aiding code reusability.

---

## `utils.py`

Consists of helper functions or classes used in tests.  
You need to import it wherever required.

---

## `test_keys.py`

Handles **key creation operations**.  
Contains a class `TestKeyManagement` with two methods:

1. **test_create_key:**  
   Used to create a key with the necessary payload, checks for errors, and cleans up the created key.

2. **test_key_name_validation:**  
   Validates creation of a key with different valid and invalid name formats.

3. **test_duplicate_key_creation:**  
   Checks for creation of duplicate EZ key and checks if it's failing or not.

> Similarly, other validations can be implemented on keys.

---

## `test_keys_02.py`

Handles **Bulk key opeartions and other extra cases**.  

---

## `test_keyDetails.py`

Handles **retrieval of key-related data**.  
Contains a class `TestKeyDetails` with three methods:

1. **test_get_key_names:**  
   Fetches all created keys and checks the presence of a specific key.

2. **test_get_key_metadata:**  
   Checks metadata of existing and non-existing keys and validates the response.

3. **test_get_key_versions:**  
   Checks key versions for existing and non-existing keys.

---

## `test_keyOps.py`

Handles **operations on keys**.  
Contains a class `TestKeyOperations` with four methods:

1. **test_temp_key:**  
   Creates a temporary key used for further roll-over functionality.

2. **test_roll_over_key:**  
   Handles proper roll-over of the key.

3. **test_roll_over_new_material:**  
   Checks whether the rolled-over key has new material.

4. **test_generate_data_key_and_decrypt:**  
   - Generation of data key from EZ key and checks for presence of EDEK and DEK.  
   - Decryption of EDEK to get back DEK.

---

## `test_keyOps_policy.py`

Handles **operations on keys based on policy enforcement**.  
Checks Key operation by giving incremental access to each opeartion one by one
i.e `create, rollover, getKeyVersion, getMetadata, generateeek, decrypteek, delete`

## `test_blacklisting.py`

Handles **operations on keys before and after blacklisting a user**.  
Checks Key operation by blacklisting a specific user and checks again after unblacklisting
i.e `create, rollover,delete` key operation



