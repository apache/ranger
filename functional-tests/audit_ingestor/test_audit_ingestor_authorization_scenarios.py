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
from ranger_test_utils.utils import assert_http_ok

pytestmark = pytest.mark.audit_ingestor


@pytest.fixture(scope="module")
def audit_ingestor_context():
    context = AuditIngestorServiceAccountTestContext()

    try:
        yield context.setup()
    finally:
        context.cleanup()


def test_unauthorized_user_cannot_post_audits(audit_ingestor_context):
    response = audit_ingestor_context.post_access(
        user=audit_ingestor_context.unauthorized_user
    )
    print("\n[Audit Ingestor] Unauthorized /access returned HTTP "
          + str(response.status_code))
    assert_http_ok(response, 403, "Unauthorized POST /access")


def test_service_account_user_cannot_post_for_another_service(
    audit_ingestor_context,
):
    response = audit_ingestor_context.post_access(
        service_name="dev_hdfs", service_type="hdfs"
    )
    print("\n[Audit Ingestor] Cross-service /access returned HTTP "
          + str(response.status_code))
    assert_http_ok(response, 403, "Cross-service POST /access")


def test_configured_user_can_post_audits(audit_ingestor_context):
    response = audit_ingestor_context.post_access(user="hive")
    print("\n[Audit Ingestor] Configured-user /access returned HTTP "
          + str(response.status_code))
    assert_http_ok(response, 200, "Configured-user POST /access")
