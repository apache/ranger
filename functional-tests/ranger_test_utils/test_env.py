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

"""Environment defaults for ranger-qe-style API tests on Docker Ranger Admin."""

import os

from ranger_test_utils.utils import RANGER_ADMIN_BASE_URL

# Docker functional-tests defaults (override via env vars).
_ENV_DEFAULTS = {
    "XA_ADMIN_USER": "admin",
    "XA_ADMIN_PASSWORD": "rangerR0cks!",
    "XA_KEYADMIN_USER": "keyadmin",
    "XA_KEYADMIN_PASSWORD": "rangerR0cks!",
    "XA_HDFS_REPO_NAME": "dev_hdfs",
    "RANGER_ADMIN_BASE_URL": RANGER_ADMIN_BASE_URL,
}


def get_env(name, default=None):
    """
    Read a test env var (os.environ first, then functional-tests defaults).

    Matches ranger-qe ``getEnv`` call sites: ``get_env('XA_ADMIN_USER', 'admin')``.
    """
    if name in os.environ:
        return os.environ[name]
    if default is not None:
        return default
    return _ENV_DEFAULTS.get(name)


def ranger_service_base_url():
    """Ranger Admin REST base (``.../service``), same as ranger-qe ``base_url``."""
    return get_env("RANGER_ADMIN_BASE_URL", RANGER_ADMIN_BASE_URL).rstrip("/")


def hdfs_repo_name():
    """HDFS repository/service name seeded in Docker (``create-ranger-services.py``)."""
    return get_env("XA_HDFS_REPO_NAME", "dev_hdfs")
