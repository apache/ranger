
import pytest
import requests

from utils import fetch_logs

BASE_URL="http://localhost:9292/kms/v1"
PARAMS={"user.name":"keyadmin"}
HEADERS={"Content-Type": "application/json","Accept":"application/json"}


@pytest.fixture(scope="session")
def headers():
    return HEADERS


@pytest.fixture(scope="class")
def create_test_key(headers):
    data={
        "name":"key1",
        "cipher": "AES/CTR/NoPadding",      #material can be provided (optional)
        "length": 128,
        "description": "Test key"
    }

    key_creation_response=requests.post(f"{BASE_URL}/keys",headers=headers,json=data,params=PARAMS)

    if key_creation_response.status_code != 201:
            error_logs = fetch_logs()            # Fetch logs on failure
            pytest.fail(f"Key creation failed. API Response: {key_creation_response.text}\nLogs:\n{error_logs}")

    yield data
    requests.delete(f"{BASE_URL}/key/key1",params=PARAMS)



