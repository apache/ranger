# This is the main directory for testing HDFS encryption cycle 

## Structure
```
test_hdfs/
├── test_encryption.py
├── utils.py              #Utility methods
```

---

## Extra Features

- **Markers:**  
  Markers have been used to selectively run specific test cases, improving test efficiency and organization.

---

## `test_encryption.py`

Handles the **full HDFS encryption cycle**, including setup, positive and negative test scenarios, and cleanup.

### Main Highlights:
- Encryption Zone (EZ) creation in HDFS.
- Granting permissions to specific users for read/write operations within the EZ.
- Validating read/write attempts by unauthorized users inside the EZ.

---

### `setup_environment`

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

## Test Cases

### ✅ Positive Test Cases

1. **test_create_encryption_zone:**  
   Creates an Encryption Zone (EZ) using an existing EZ key.

2. **test_grant_permissions:**  
   Grants read-write permissions to a specific user (e.g., HIVE) within the EZ.

3. **test_hive_user_write_read:**  
   Performs write and read operations inside the EZ using the authorized HIVE user.

---

### ❌ Negative Test Cases

1. **test_unauthorized_write:**  
   Attempts to write inside the EZ using an unauthorized user (e.g., HBASE). Validates expected denial of access.

2. **test_unauthorized_read:**  
   Attempts to read inside the EZ using an unauthorized user. Validates expected denial of access.

---

### 🧹 Cleanup

- **test_cleanup:**  
  Cleans up the Encryption Zone and all files created during testing.  
  Ensures the test environment is reset for clean re-runs.

---

## Summary

This test suite ensures that **HDFS encryption and access control mechanisms** function as expected, validating both authorized and unauthorized access scenarios while maintaining a clean and reusable test environment.





