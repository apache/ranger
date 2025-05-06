import requests     
import pytest  
from utils import fetch_logs     

BASE_URL = "http://localhost:9292/kms/v1"  
PARAMS={"user.name":"keyadmin"}            

class TestKeyManagement:

    @pytest.fixture(autouse=True)
    def setup_class(self, create_test_key):
        self.test_key = create_test_key
    
    def test_create_key(self,headers):
        key_data = {
            "name": "key2",
            "cipher": "AES/CTR/NoPadding",
            "length": 128,
            "description": "New key for checking key creation functionality"
        }
        response = requests.post(f"{BASE_URL}/keys",headers=headers, json=key_data,params=PARAMS)

        if response.status_code != 201:
            error_logs = fetch_logs()  # Fetch logs on failure
            pytest.fail(f"Key creation failed. API Response: {response.text}\nLogs:\n{error_logs}")

        requests.delete(f"{BASE_URL}/key/key2",params=PARAMS)               #cleanup key2
    
    #---------------------------------creation key validation------------------------------
    @pytest.mark.parametrize("name, expected_status", [
        ("valid-key", 201),
        ("", 400),                                     # Invalid case: Empty name
        ("@invalid!", 400),                            # Invalid case: Special characters
        ("invalid--key",400)                           #-- or __ or _- -_ not allowed        
    ])
    def test_key_name_validation(self, headers, name, expected_status):
        key_data = {
            "name": name,
            "cipher": "AES/CTR/NoPadding",
            "length": 128,
            "description": "Validation test"
        }
        response = requests.post(f"{BASE_URL}/keys", json=key_data, headers=headers,params=PARAMS)

        if response.status_code != expected_status:
            error_logs = fetch_logs()  # Fetch logs on failure
            pytest.fail(f"Key validation failed. API Response: {response.text}\nLogs:\n{error_logs}")

        if expected_status == 201:
         requests.delete(f"{BASE_URL}/key/{name}", params=PARAMS)

    # Negative test----duplicate key creation test ----------------------------------------------
    def test_duplicate_key_creation(self, headers):
        key_name = "duplicate-key"
        key_data = {
            "name": key_name,
            "cipher": "AES/CTR/NoPadding",
            "length": 128,
            "description": "Testing duplicate key creation"
        }

        response1 = requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)
        assert response1.status_code == 201, f"Initial key creation failed: {response1.text}"

        # creating the same key again
        response2 = requests.post(f"{BASE_URL}/keys", headers=headers, json=key_data, params=PARAMS)

        assert response2.status_code == 500, f"Duplicate key got created, expected to fail"
    
        # Cleanup
        requests.delete(f"{BASE_URL}/key/{key_name}", params=PARAMS)


    
    
