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

"""JSON test-data path layout for migrated ranger-qe api_tests suites."""

import os
from pathlib import Path


def api_data_paths(test_module_file, data_name, layout="suite"):
    """
    Resolve ``variable_jsons`` and ``test_jsons`` directories for a suite.

    layout ``suite`` (functional-tests convention)::

        <suite>/data/<data_name>/{variable_jsons,test_jsons}

    layout ``ranger_qe`` (``api_tests/<area>/`` tests + shared ``api_tests/data/``)::

        <area>/../data/<data_name>/{variable_jsons,test_jsons}
    """
    test_path = Path(test_module_file).resolve().parent
    if layout == "ranger_qe":
        data_folder = test_path.parent / "data" / data_name
    else:
        data_folder = test_path / "data" / data_name

    return {
        "data_folder_path": str(data_folder),
        "variables_data_path": str(data_folder / "variable_jsons"),
        "test_data_path": str(data_folder / "test_jsons"),
    }


def patch_json_file(json_path, updates):
    """Load JSON from disk, apply ``updates`` dict, write back (setup_module helper)."""
    import json

    with open(json_path, "r", encoding="utf-8") as json_file:
        data = json.load(json_file)
    data.update(updates)
    with open(json_path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4)


def join_under_variables(variables_data_path, filename):
    return os.path.join(variables_data_path, filename)
