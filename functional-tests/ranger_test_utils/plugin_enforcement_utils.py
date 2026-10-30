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

"""
Shared logging, access-audit checks, and pytest helpers for plugin enforcement suites.

Test modules should import ``logger`` from here (same pattern as ``api_test_utils``
for ``api_tests``).

Access audits (Solr/OpenSearch via Admin REST) for any service::

    from ranger_test_utils.plugin_enforcement_utils import (
        begin_access_audit_capture,
        logger,
    )

    capture = begin_access_audit_capture(admin_session, "dev_hbase", "hrt_21")
    ... hbase shell / kafka producer / hdfs dfs ...
    capture.verify({"accessType": "...", "accessResult": "0", ...})
"""

from ranger_test_utils.access_audit_utils import (
    AccessAuditCapture,
    access_audit_pipeline_enabled,
    assert_access_audits_found,
    begin_access_audit_capture,
    fetch_last_access_audit_event_time,
    verify_plugin_access_audit,
    wait_for_access_audits,
)
from ranger_test_utils.utils import (
    configure_test_logging,
    create_ranger_admin_session,
    get_test_logger,
    log_testcase_begin_for_pytest,
    unique_suffix,
)

logger = get_test_logger(__name__)


def normalize_audit_log_filter(audit_log_filter):
    """Map ranger-qe ``accessEnforcer`` to functional-tests ``aclEnforcer``."""
    if not audit_log_filter:
        return None
    normalized = dict(audit_log_filter)
    if "accessEnforcer" in normalized and "aclEnforcer" not in normalized:
        normalized["aclEnforcer"] = normalized.pop("accessEnforcer")
    return normalized
