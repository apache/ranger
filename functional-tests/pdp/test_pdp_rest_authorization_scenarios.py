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
from ranger_test_utils.utils import assert_http_ok

pytestmark = pytest.mark.pdp


@pytest.fixture(scope="module")
def pdp_context():
    context = PdpServiceAccountTestContext()

    try:
        yield context.setup()
    finally:
        context.cleanup()


def test_unauthorized_user_cannot_authorize(pdp_context):
    response = pdp_context.post(
        "/authorize",
        {
            "requestId": "pdp-functional-unauthorized",
            "user": {"name": "hive"},
            "access": pdp_context.access("table:default/sales"),
            "context": pdp_context.context(),
        },
        pdp_context.unauthorized_user,
    )
    print("\n[PDP] Unauthorized /authorize returned HTTP "
          + str(response.status_code))
    assert_http_ok(response, 403, "Unauthorized POST /authorize")


def test_service_account_user_cannot_authorize_another_service(pdp_context):
    response = pdp_context.post(
        "/authorizeMulti",
        {
            "requestId": "pdp-functional-wrong-service",
            "user": {"name": "hdfs"},
            "accesses": [pdp_context.access("/tmp/test")],
            "context": pdp_context.context("dev_hdfs", "hdfs"),
        },
    )
    print("\n[PDP] Cross-service /authorizeMulti returned HTTP "
          + str(response.status_code))
    assert_http_ok(response, 403, "Cross-service POST /authorizeMulti")


def test_configured_user_can_get_permissions(pdp_context):
    response = pdp_context.post(
        "/permissions",
        {
            "requestId": "pdp-functional-configured-user",
            "resource": {"name": "table:default/sales"},
            "context": pdp_context.context(),
        },
        "ranger",
    )
    print("\n[PDP] Configured-user /permissions returned HTTP "
          + str(response.status_code))
    assert_http_ok(response, 200, "Configured-user POST /permissions")
