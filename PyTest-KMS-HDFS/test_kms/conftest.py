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



