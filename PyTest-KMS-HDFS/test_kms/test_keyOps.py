# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This workflow will build a Java project with Maven, and cache/restore any dependencies to improve the workflow execution time
# For more information see: https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-java-with-maven

# This workflow uses actions that are not certified by GitHub.
# They are provided by a third-party and are governed by
# separate terms of service, privacy policy, and support
# documentation.



import requests
import pytest
import time
from utils import fetch_logs

BASE_URL = "http://localhost:9292/kms/v1"
PARAMS = {"user.name": "keyadmin"}

@pytest.mark.usefixtures("create_test_key")
class TestKeyOperations:

    # Temporary key for testing roll over
    def test_temp_key(self, headers):            
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
       

    # ***********************************************************************************
    #  Parametrized Roll over of key
    # ***********************************************************************************
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

    
    # ***********************************************************************************
    # Test for checking roll overed key has new material
    # ***********************************************************************************
    def test_roll_over_new_material(self, headers):
        old_metadata = requests.get(f"{BASE_URL}/key/key1/_metadata", headers=headers, params=PARAMS)
        print("Old Metadata:", old_metadata.json())

        requests.post(f"{BASE_URL}/key/key1", json={}, headers=headers, params=PARAMS)      #roll-over here

        new_metadata = requests.get(f"{BASE_URL}/key/key1/_metadata", headers=headers, params=PARAMS)
        print("New Metadata:", new_metadata.json())

        assert old_metadata.json() != new_metadata.json(), "Key rollover should create new key material."

    
    # ***********************************************************************************
    #  Data key generation and decrypting EDEK to get DEK
    # ***********************************************************************************
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


    # ***********************************************************************************
    #   re encryption of encrypted keys---------------------------------
    #   verifies: that the EDEK is updated (i.e., versionName changes) after key rotation
    # ***********************************************************************************
    def test_reencrypt_encrypted_keys(self, headers):
        # Step 1: Create the key
        key_name = "reencrypt-key"
        data = {"name": key_name}
        create_response = requests.post(f"{BASE_URL}/keys", headers=headers, json=data, params=PARAMS)
        logs=fetch_logs()
        assert create_response.status_code == 201, f"Key creation failed: {create_response.text}\nLogs:\n{logs}"

        try:
            # Step 2: Generate an Encrypted DEK (EDEK) using the key
            generate_response = requests.get(f"{BASE_URL}/key/{key_name}/_dek", headers=headers, params=PARAMS)
            logs=fetch_logs()
            assert generate_response.status_code == 200, f"EEK generation failed: {generate_response.text}\nLogs:\n{logs}"

            print(generate_response.json())

            edek = generate_response.json()["edek"]
            encrypted_key_version = edek["encryptedKeyVersion"]

            edek_payload = [
                {
                    "versionName": edek["versionName"],
                    "iv": edek["iv"],
                    "encryptedKeyVersion": {
                        "versionName": "EEK",
                        "material": encrypted_key_version["material"]
                    }
                }
            ]


            # Step 3: Rotate the key (to create a new version)
            rollover_response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=headers, params=PARAMS)
            assert rollover_response.status_code == 200, f"Key rollover failed: {rollover_response.text}"

            # Step 4: Call the reencryptEncryptedKeys API
            reencrypt_url = f"{BASE_URL}/key/{key_name}/_reencryptbatch"
            reencrypt_response = requests.post(reencrypt_url, headers=headers, json=edek_payload, params=PARAMS)
            assert reencrypt_response.status_code == 200, f"Re-encrypt call failed: {reencrypt_response.text}"

            # Step 5: Validate the response EDEKs
            reencrypted_edeks = reencrypt_response.json()
            print(reencrypted_edeks)

            assert isinstance(reencrypted_edeks, list), "Expected list of re-encrypted EDEKs"
            assert len(reencrypted_edeks) == 1, "Expected exactly one re-encrypted EDEK"
            assert reencrypted_edeks[0]["versionName"] != edek["versionName"], \
                "Expected EDEK version to change after re-encryption"

        finally:
            # Cleanup key
            requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)





    # ***********************************************************************************
    # invalidate cache use 
    # ***********************************************************************************
    def test_generate_data_key_after_invalidate_cache(self, headers):
        key_name = "cache_key"

        data = {"name": key_name}

       # Step 1: Create a key
        create_response = requests.post(f"{BASE_URL}/keys", headers=headers, json=data, params=PARAMS)
        assert create_response.status_code == 201, "Key creation failed"

        # Step 2: Roll over (creates @1, cached)
        roll_response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=headers, params=PARAMS)
        assert roll_response.status_code == 200, "Rollover failed"


        # Step 3: Delete the key (DB is clean, cache still references @1)
        delete_response = requests.delete(f"{BASE_URL}/key/{key_name}", headers=headers, params=PARAMS)
        assert delete_response.status_code == 200, "Key deletion failed"

        time.sleep(5)

        # Step 4: Recreate the key (creates only @0 in DB, cache still stale @1)
        recreate_response = requests.post(f"{BASE_URL}/keys", headers=headers, json=data, params=PARAMS)
        assert recreate_response.status_code == 201, "Key recreation failed"

        # Step 5: Try DEK – fails due to stale cache (refers to non-existent @1)
        dek_response_before = requests.get(f"{BASE_URL}/key/{key_name}/_dek", headers=headers, params=PARAMS)
        assert dek_response_before.status_code == 200, "Expected failure due to stale cache referencing @1"

        # Step 6: Invalidate cache – forces KMS to reload latest version from DB
        invalidate_params = {"user.name": "keyadmin", "action": "invalidateCache"}
        invalidate_response = requests.post(f"{BASE_URL}/key/{key_name}", json={}, headers=headers, params=invalidate_params)
        assert invalidate_response.status_code == 200, "Invalidate cache failed"

        # Step 7: Retry DEK – should now succeed (correct version @0 loaded)
        dek_response_after = requests.get(f"{BASE_URL}/key/{key_name}/_dek", headers=headers, params=PARAMS)
        assert dek_response_after.status_code == 200, "DEK generation should succeed after cache invalidation"

        requests.delete(f"{BASE_URL}/key/{key_name}", headers=headers, params=PARAMS)
