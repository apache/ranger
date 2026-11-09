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
Session setup for all modules under ``api_tests/`` (asset, plugin, role, … ports).

Pytest loads this file for any test collected under ``api_tests/`` and subfolders.
"""

import pytest

from ranger_test_utils.api_test_utils import create_kmsauditor_test_user, create_required_test_users


@pytest.fixture(scope="session", autouse=True)
def api_test_standard_role_users():
    """Users for ``*_for_user_role_user`` and ``*_for_auditor_user`` modules."""
    create_required_test_users()


@pytest.fixture(scope="session")
def api_test_kms_auditor_user():
    """User for ``*_for_kmsauditor_user`` public API v2 modules."""
    create_kmsauditor_test_user()
