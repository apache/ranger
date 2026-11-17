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

from audit_ingestor.audit_ingestor_utils import (
    AuditIngestorServiceAccountTestContext,
)
from ranger_test_utils.utils import (
    assert_http_ok,
    get_test_logger,
    log_http_response,
)

pytestmark = pytest.mark.audit_ingestor
logger = get_test_logger(__name__)


@pytest.fixture(scope="module")
def audit_ingestor_context():
    context = AuditIngestorServiceAccountTestContext()

    try:
        yield context.setup()
    finally:
        context.cleanup()


def test_dynamic_service_account_user_can_post_audits(audit_ingestor_context):
    print(
        "\n[Audit Ingestor] Testing POST /access "
        "with a dynamic service-account user"
    )
    response = audit_ingestor_context.post_access()
    print(
        "[Audit Ingestor] POST /access returned HTTP "
        + str(response.status_code)
    )
    log_http_response(logger, "POST /access", response)
    assert_http_ok(response, 200, "POST /access")

    result = response.json()
    assert result["serviceName"] == "dev_hive"
    assert result["total"] == 1
