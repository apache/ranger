#!/usr/bin/env python3
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

import logging
import os


DEFAULT_LOG_LEVEL = os.environ.get("RANGER_ADMIN_PY_LOG_LEVEL", "DEBUG").upper()
DEFAULT_LOG_FORMAT = os.environ.get("RANGER_ADMIN_PY_LOG_FORMAT", "%(asctime)-15s %(levelname)s %(message)s")
DEFAULT_LOGGER_LEVELS = {
    "apache_ranger": os.environ.get("RANGER_ADMIN_PY_APACHE_RANGER_LOG_LEVEL", "INFO"),
}

_LOGGING_CONFIGURED = False


def _parse_level(level_name):
    if isinstance(level_name, int):
        return level_name
    return getattr(logging, str(level_name).upper(), logging.INFO)


def configure_logging(default_level=DEFAULT_LOG_LEVEL, logger_levels=None):
    global _LOGGING_CONFIGURED

    if _LOGGING_CONFIGURED:
        return

    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=_parse_level(default_level))

    levels = dict(DEFAULT_LOGGER_LEVELS)
    if logger_levels:
        levels.update(logger_levels)

    for logger_name, logger_level in levels.items():
        logging.getLogger(logger_name).setLevel(_parse_level(logger_level))

    _LOGGING_CONFIGURED = True


def get_logger(name):
    return logging.getLogger(name)
