import requests
import pytest
from utils import fetch_logs

BASE_URL = "http://localhost:9292/kms/v1"
PARAMS = {"user.name": "keyadmin"}

@pytest.mark.usefixtures("create_test_key")
class TestKeyOperations:
    
    def test_temp_key(self, headers):             #temporary key for testing roll over 
        data = {
            "name": "rollover-key",
            "cipher": "AES/CTR/NoPadding",
            "length": 128,
            "description": "Key to check roll over functionality"
        }
        key_creation_response = requests.post(f"{BASE_URL}/keys", headers=headers, json=data, params=PARAMS)

        if key_creation_response.status_code != 201:        #log check
            logs=fetch_logs()
            pytest.fail(f"Create key operation failed. API Response: {key_creation_response.text}\nLogs:\n{logs}")
       


    @pytest.mark.parametrize("key_name, expected_status", [
        ("rollover-key", 200),             # Valid key rollover
        ("non-existent-key", 500)          # Rollover on a non-existent key
    ])

    def test_roll_over_key(self, headers, key_name, expected_status):
        response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=headers, params=PARAMS)

        if response.status_code != expected_status:      #log check
           logs=fetch_logs()
           pytest.fail(f"Rollover key operation failed. API Response: {response.text}\nLogs:\n{logs}")

        # Cleanup after test
        requests.delete(f"{BASE_URL}/key/rollover-key", params=PARAMS)

    #test for checking roll overed key has new material-----------------------------
    def test_roll_over_new_material(self, headers):
        old_metadata = requests.get(f"{BASE_URL}/key/key1/_metadata", headers=headers, params=PARAMS)
        print("Old Metadata:", old_metadata.json())

        requests.post(f"{BASE_URL}/key/key1", json={}, headers=headers, params=PARAMS)      #roll-over here

        new_metadata = requests.get(f"{BASE_URL}/key/key1/_metadata", headers=headers, params=PARAMS)
        print("New Metadata:", new_metadata.json())

        assert old_metadata.json() != new_metadata.json(), "Key rollover should create new key material."

    #data key generation and decrypting EDEK to get DEK---------------------------------
    def test_generate_data_key_and_decrypt(self, headers, create_test_key):
        # Generate Data Key
        key_name=create_test_key["name"]
        response = requests.get(f"{BASE_URL}/key/{key_name}/_dek", headers=headers, params=PARAMS)

        if response.status_code != 200:         #log check
            logs=fetch_logs()
            pytest.fail(f"generation of data key operation failed. API Response: {response.text}\nLogs:\n{logs}")


        data_key_response = response.json()
        dek = data_key_response.get("dek")
        edek = data_key_response.get("edek")

        print(dek)
        print(edek)

        assert dek is not None, "Generated DEK should not be None"
        assert edek is not None, "Generated EDEK should not be None"

         # Extracting details for decryption from EDEK
        encrypted_key_version = edek.get("encryptedKeyVersion")  
        encrypted_material = encrypted_key_version.get("material")  
        name = encrypted_key_version.get("name") 
        version_name = edek.get("versionName")
        iv = edek.get("iv")
        
        decrypt_payload = {
                  
                    "name":name,
                    "iv": iv,
                    "material": encrypted_material,
        }

        DECRYPT_PARAMS = {"user.name": "keyadmin","eek_op":"decrypt"}
        decrypt_response = requests.post(f"{BASE_URL}/keyversion/{version_name}/_eek", json=decrypt_payload, headers=headers, params=DECRYPT_PARAMS)

        if decrypt_response.status_code != 200:       #log check
           logs=fetch_logs()
           pytest.fail(f"Decryption of key operation failed. API Response: {response.text}\nLogs:\n{logs}")

        decrypted_data = decrypt_response.json()
        print("Decrypted Data:", decrypted_data)  # check decrypted data

        # checking the decrypted key matches the original DEK
        assert decrypted_data == dek, "Decrypted DEK should match the original DEK"


    #invalidate cache use -----optional maybe
    # def test_generate_data_key_after_invalidate_cache(self, headers, create_test_key):
        
    #     key_name=create_test_key["name"]
    #     requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)


    #     data = {
    #         "name": "key1",
    #         "cipher": "AES/CTR/NoPadding",
    #         "length": 128,
    #         "description": "Test key"
    #     }
    #     key_creation_response = requests.post(f"{BASE_URL}/keys", headers=headers, json=data, params=PARAMS)
    #     assert key_creation_response.status_code == 201, "Key already exists"

        
    #     response = requests.get(f"{BASE_URL}/key/key1/_dek", headers=headers, params=PARAMS)    #hit dek but there is no @1 version
    #     assert response.status_code == 500, "version @1 found"                                  #gives 500 error bcz of above reason 
        
    #     invalidate_cache_param={"user.name": "keyadmin","action":"invalidateCache"}             #invalidateCache
    #     invalidate_response = requests.post(f"{BASE_URL}/key/key1", json={}, headers=headers, params=invalidate_cache_param)


    #     response_after_invalidateCache = requests.get(f"{BASE_URL}/key/key1/_dek", headers=headers, params=PARAMS)      #now it works
    #     assert response_after_invalidateCache.status_code == 200, "version @1 not found"
