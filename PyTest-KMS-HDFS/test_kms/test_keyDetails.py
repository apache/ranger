import requests
import pytest
from utils import fetch_logs

BASE_URL = "http://localhost:9292/kms/v1"
PARAMS = {"user.name": "keyadmin"}

class TestKeyDetails:

    @pytest.fixture(autouse=True)
    def setup_class(self, create_test_key):
        self.test_key = create_test_key

    def test_get_key_names(self):
        response = requests.get(f"{BASE_URL}/keys/names",params=PARAMS)
        
        if response.status_code!=200:                        #log check
            logs=fetch_logs()
            pytest.fail(f"Get key operation failed. API Response: {response.text}\nLogs:\n{logs}")

        print(response.json())
        assert self.test_key["name"] in response.json()



    @pytest.mark.parametrize("key_name, expected_status, expected_response", [
    ("my_key", 200, "valid"),                   # Key exists, should return valid metadata
    ("non-existent-key", 200, "invalid"),       # Key does not exist but returns 200 with [] should give 404
    ])
    def test_get_key_metadata(self, headers, key_name, expected_status, expected_response):
    
        response = requests.get(f"{BASE_URL}/key/{key_name}/_metadata", headers=headers, params=PARAMS)
 
        if response.status_code!=expected_status:                  #log check
            logs=fetch_logs()           
            pytest.fail(f"Get key metadata operation failed. API Response: {response.text}\nLogs:\n{logs}")

        if expected_response == "invalid":
         assert response.text.strip() in ["", "[ ]", "{ }"], f"Expected blank response for non-existent key, got: {response.text}"

        print(response.json())


    @pytest.mark.parametrize("key_name, expected_status, expected_response", [
        ("my_key", 200, "valid"),             # Key exists
        ("non-existent-key", 200,"invalid"),  # Misleading response for non-existent key gives 200 should've given 404
    ])
    def test_get_key_versions(self, headers, key_name, expected_status,expected_response):

        response = requests.get(f"{BASE_URL}/key/{key_name}/_versions", headers=headers, params=PARAMS)
         
        if response.status_code != expected_status:          #log check
            logs=fetch_logs()           
            pytest.fail(f"Get key version operation failed. API Response: {response.text}\nLogs:\n{logs}")
          
        if expected_response == "invalid":
         assert response.text.strip() in ["", "[ ]", "{ }"], f"Expected blank response for non-existent key, got: {response.text}"

















        # try:
        #  json_response = response.json()
        # except requests.exceptions.JSONDecodeError:
         
        #  json_response = response.text  # Store raw text response instead

        # if expected_response == "invalid":
        #     assert json_response =="key not found", f"Unexpected response: {json_response}"


   
        #this below was in testKeyMetadata-------------
         # response_data = response.text                      #The error occurs because the API returns plain text ("key not found") instead of a JSON response when querying a non-existent key.
                                                           #works fine if text is used instead json bcoz of my changes
        # if expected_response == "valid":
        #     assert response_data != "key not found"
        # elif expected_response == "invalid":
        #     assert response_data == "key not found"