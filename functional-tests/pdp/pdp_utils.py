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

import os
import time

import pytest
import requests

from ranger_test_utils.utils import (
    DEFAULT_JSON_HEADERS,
    SERVICE_URL,
    assert_http_ok,
    create_ranger_admin_session,
    unique_suffix,
)

PDP_BASE_URL = os.environ.get(
    "RANGER_PDP_BASE_URL", "http://localhost:6500/authz/v1"
).rstrip("/")
SERVICE_NAME = "dev_hive"
SERVICE_ACCOUNT_USERS = "service.account.users"
REFRESH_TIMEOUT_SEC = int(
    os.environ.get("PDP_SERVICE_ACCOUNT_REFRESH_TIMEOUT_SEC", "90")
)


class PdpServiceAccountTestContext:
    def __init__(self):
        self.admin = create_ranger_admin_session()
        self.pdp = requests.Session()
        self.pdp.headers.update(DEFAULT_JSON_HEADERS)
        self.service = None
        self.original_value = None
        self.user = "pdp-functional-" + unique_suffix(8)
        self.unauthorized_user = "pdp-unauthorized-" + unique_suffix(8)

    def setup(self):
        service_url = SERVICE_URL + "name/" + SERVICE_NAME
        response = self.admin.get(service_url)
        assert_http_ok(response, 200, "Get service " + SERVICE_NAME, service_url)

        self.service = response.json()
        configs = self.service.setdefault("configs", {})
        self.original_value = configs.get(SERVICE_ACCOUNT_USERS)
        original_users = [
            user.strip()
            for user in (self.original_value or "").split(",")
            if user.strip()
        ]
        configs[SERVICE_ACCOUNT_USERS] = ",".join(original_users + [self.user])

        update_url = SERVICE_URL + str(self.service["id"])
        updated = self.admin.put(update_url, json=self.service)
        assert_http_ok(
            updated, 200, "Add dynamic service-account user", update_url
        )
        self._wait_for_status(200)

        return self

    def cleanup(self):
        if self.service is not None:
            configs = self.service["configs"]
            if self.original_value is None:
                configs.pop(SERVICE_ACCOUNT_USERS, None)
            else:
                configs[SERVICE_ACCOUNT_USERS] = self.original_value

            update_url = SERVICE_URL + str(self.service["id"])
            restored = self.admin.put(update_url, json=self.service)
            assert_http_ok(
                restored, 200, "Restore service-account users", update_url
            )
            self._wait_for_status(403)

        self.pdp.close()
        self.admin.close()

    def post(self, endpoint, payload, user=None):
        return self.pdp.post(
            PDP_BASE_URL + endpoint,
            headers={"X-Forwarded-User": user or self.user},
            json=payload,
            timeout=30,
        )

    @staticmethod
    def context(service_name=SERVICE_NAME, service_type="hive"):
        return {"serviceType": service_type, "serviceName": service_name}

    @staticmethod
    def access(resource):
        return {
            "resource": {"name": resource},
            "action": "QUERY",
            "permissions": ["select"],
        }

    def _wait_for_status(self, expected_status):
        deadline = time.monotonic() + REFRESH_TIMEOUT_SEC
        response = None

        while time.monotonic() < deadline:
            response = self.post(
                "/authorize",
                {
                    "requestId": "service-account-refresh-check",
                    "user": {"name": "hive"},
                    "access": self.access("table:default/sales"),
                    "context": self.context(),
                },
            )
            if response.status_code == expected_status:
                return
            time.sleep(2)

        pytest.fail(
            "PDP did not return HTTP "
            + str(expected_status)
            + " for the dynamic service-account user; last response was "
            + (
                str(response.status_code) + ": " + response.text
                if response is not None
                else "none"
            )
        )
