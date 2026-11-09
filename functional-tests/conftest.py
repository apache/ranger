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

"""Suite-wide pytest hooks (logging directory, per-module log files)."""

import logging
from pathlib import Path

import pytest

from ranger_test_utils.plugin_conftest import ensure_plugin_test_users_for_session
from ranger_test_utils.utils import LOGS_DIR, ensure_logs_directory

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def log_path_for_test_module(module_file):
    """``logs/<test_module_stem>.log`` under functional-tests/."""
    return LOGS_DIR / (Path(module_file).stem + ".log")


def pytest_configure(config):
    """Prepare logs/ and disable pytest.ini single shared log file (we use per-module files)."""
    ensure_logs_directory()
    if getattr(config.option, "log_file", None):
        config.option.log_file = None


@pytest.fixture(scope="session", autouse=True)
def _ensure_plugin_test_users_for_enforcement_suites(request):
    """
    Before plugin enforcement tests (markers: hive, hbase, kafka, hdfs_policy),
    create shared Ranger xusers and Kerberos keytabs via plugin_conftest.
    REST-only suites (api_test, kms, …) are unchanged.
    """
    if not request.session.items:
        yield
        return
    ensure_plugin_test_users_for_session(request.session)
    yield


@pytest.fixture(scope="module", autouse=True)
def _log_to_module_file(request):
    """Write logger.info (and above) to logs/<test_file_name>.log for each test module."""
    module = getattr(request, "module", None)
    if module is None or not getattr(module, "__file__", None):
        yield
        return

    log_path = log_path_for_test_module(module.__file__)
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    try:
        yield
    finally:
        root.removeHandler(handler)
        handler.close()
