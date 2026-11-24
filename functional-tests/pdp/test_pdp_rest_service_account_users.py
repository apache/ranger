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

import pytest

from pdp.pdp_utils import PdpServiceAccountTestContext
from ranger_test_utils.utils import (
    assert_http_ok,
    get_test_logger,
    log_http_response,
)

pytestmark = pytest.mark.pdp
logger = get_test_logger(__name__)


@pytest.fixture(scope="module")
def pdp_context():
    context = PdpServiceAccountTestContext()

    try:
        yield context.setup()
    finally:
        context.cleanup()


def post(pdp_context, endpoint, payload):
    print("\n[PDP] Testing POST " + endpoint + " with a dynamic service-account user")
    response = pdp_context.post(endpoint, payload)
    print("[PDP] POST " + endpoint + " returned HTTP " + str(response.status_code))
    log_http_response(logger, "POST " + endpoint, response)
    assert_http_ok(response, 200, "POST " + endpoint)

    return response.json()


def test_dynamic_service_account_user_can_authorize(pdp_context):
    result = post(
        pdp_context,
        "/authorize",
        {
            "requestId": "pdp-functional-authorize",
            "user": {"name": "hive"},
            "access": pdp_context.access("table:default/sales"),
            "context": pdp_context.context(),
        },
    )

    assert result["requestId"] == "pdp-functional-authorize"
    assert result["decision"] in ("ALLOW", "DENY")


def test_dynamic_service_account_user_can_authorize_multi(pdp_context):
    result = post(
        pdp_context,
        "/authorizeMulti",
        {
            "requestId": "pdp-functional-authorize-multi",
            "user": {"name": "hive"},
            "accesses": [
                pdp_context.access("table:default/sales"),
                pdp_context.access("table:default/customers"),
            ],
            "context": pdp_context.context(),
        },
    )

    assert result["requestId"] == "pdp-functional-authorize-multi"
    assert len(result["accesses"]) == 2


def test_dynamic_service_account_user_can_get_permissions(pdp_context):
    result = post(
        pdp_context,
        "/permissions",
        {
            "requestId": "pdp-functional-permissions",
            "resource": {"name": "table:default/sales"},
            "context": pdp_context.context(),
        },
    )

    assert result["resource"]["name"] == "table:default/sales"


def test_dynamic_service_account_user_can_filter_resources(pdp_context):
    result = post(
        pdp_context,
        "/filterResources",
        {
            "requestId": "pdp-functional-filter-resources",
            "user": {"name": "hive"},
            "resources": [
                {"name": "table:default/sales"},
                {"name": "table:default/customers"},
            ],
            "action": "select",
            "permissions": ["select"],
            "context": pdp_context.context(),
        },
    )

    assert result["requestId"] == "pdp-functional-filter-resources"
    assert "resources" in result
