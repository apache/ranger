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
Group write on /demo_<suffix>/data_<suffix> and read+execute on / and /demo_<suffix>
for group finance.
"""

import pytest

from ranger_test_utils.hdfs_utils import (
    GROUP_AUDIT,
    GROUP_FINANCE,
    HDFS_AUTHZ_DENIED_SUBSTR,
    HDFS_SERVICE_NAME,
    HDFS_SERVICE_USER,
    USER_AUDIT,
    USER_FINANCE,
    build_hdfs_policy_payload,
    cleanup_hdfs_paths,
    create_hdfs_policy,
    ensure_kerberos_user_on_hadoop,
    ensure_unix_user_with_primary_group,
    HADOOP_CONTAINER,
    get_docker_client,
    preflight_hdfs_plugin_stack,
    run_hdfs_dfs,
    wait_for_hdfs_policy_propagation,
)
from ranger_test_utils.hive_utils import delete_policies_by_id, set_all_policies_enabled
from ranger_test_utils.utils import (
    DOCKER_PLUGIN_TEST_USER_PASSWORD,
    configure_test_logging,
    create_ranger_admin_session,
    ensure_argus_policy_principals,
    ensure_user_assigned_to_groups,
    ensure_user_exists,
    get_test_logger,
    log_testcase_begin_for_pytest,
    unique_suffix,
)

pytestmark = pytest.mark.hdfs_policy

logger = get_test_logger(__name__)

_admin_session = None
_policy_ids = []
_hdfs_paths_to_cleanup = []
_run_suffix = None
_demo_base = None
_demo_data = None

# Container-local files for copyFromLocal (case 3B/3D).
_LOCAL_STAGING_ROOT = "/tmp/ranger-ft/demo/data"


def _demo_paths_for_suffix(run_suffix):
    """QE /demo/data layout with unique paths: /demo_<suffix>/data_<suffix>."""
    demo_base = "/demo_" + run_suffix
    return demo_base, demo_base + "/data_" + run_suffix


def _prepare_demo_data_hdfs_path(hdfs_data_path):
    """Recreate path with mode 000 — Ranger grants access, not POSIX."""
    run_hdfs_dfs(HDFS_SERVICE_USER, "-rm -r -f " + hdfs_data_path, service_user=True)
    run_hdfs_dfs(
        HDFS_SERVICE_USER,
        "-mkdir -p " + hdfs_data_path,
        service_user=True,
    )
    run_hdfs_dfs(
        HDFS_SERVICE_USER,
        "-chmod -R 000 " + hdfs_data_path,
        service_user=True,
    )


def _prepare_local_copy_sources(container):
    container.exec_run("rm -rf /tmp/ranger-ft", user="root")
    container.exec_run("mkdir -p " + _LOCAL_STAGING_ROOT, user="root")
    for filename in ("filefinance.txt", "fileaudit.txt"):
        path = _LOCAL_STAGING_ROOT + "/" + filename
        container.exec_run(
            'bash -c "echo test content > ' + path + '"',
            user="root",
        )
        container.exec_run("chmod a+r " + path, user="root")


def setup_module():
    configure_test_logging(module_name=__name__)
    logger.info("=== setup_module: start (HDFS group policy case 3) ===")
    preflight_hdfs_plugin_stack()

    global _admin_session, _policy_ids, _run_suffix, _demo_base, _demo_data
    _run_suffix = unique_suffix(4)
    _demo_base, _demo_data = _demo_paths_for_suffix(_run_suffix)
    logger.info("Test HDFS paths: demo_base=%s demo_data=%s", _demo_base, _demo_data)

    _admin_session = create_ranger_admin_session()
    hadoop = get_docker_client().containers.get(HADOOP_CONTAINER)

    logger.info("Ensuring Ranger users, groups, and group membership")
    ensure_argus_policy_principals(_admin_session)
    ensure_user_exists(
        _admin_session,
        USER_FINANCE,
        password=DOCKER_PLUGIN_TEST_USER_PASSWORD,
        last_name="hdfs_test",
    )
    ensure_user_exists(
        _admin_session,
        USER_AUDIT,
        password=DOCKER_PLUGIN_TEST_USER_PASSWORD,
        last_name="hdfs_test",
    )
    ensure_user_assigned_to_groups(_admin_session, USER_FINANCE, [GROUP_FINANCE])
    ensure_user_assigned_to_groups(_admin_session, USER_AUDIT, [GROUP_AUDIT])

    logger.info("Ensuring Unix users and Kerberos principals on ranger-hadoop")
    ensure_unix_user_with_primary_group(hadoop, USER_FINANCE, GROUP_FINANCE)
    ensure_unix_user_with_primary_group(hadoop, USER_AUDIT, GROUP_AUDIT)
    ensure_kerberos_user_on_hadoop(USER_FINANCE)
    ensure_kerberos_user_on_hadoop(USER_AUDIT)

    _prepare_local_copy_sources(hadoop)

    logger.info("Disabling existing policies on %s", HDFS_SERVICE_NAME)
    set_all_policies_enabled(_admin_session, False, HDFS_SERVICE_NAME)

    policy_data_write = build_hdfs_policy_payload(
        policy_name="gp3-data-write-" + _run_suffix,
        path_values=[_demo_data],
        perm_map_list=[
            {
                "groupList": [GROUP_FINANCE],
                "userList": [],
                "permList": ["write"],
            }
        ],
        description=_demo_data + " Write for Group:Finance",
        is_recursive=False,
    )
    policy_root_demo_rx = build_hdfs_policy_payload(
        policy_name="gp3-root-demo-rx-" + _run_suffix,
        path_values=["/", _demo_base],
        perm_map_list=[
            {
                "groupList": [GROUP_FINANCE],
                "userList": [],
                "permList": ["read", "execute"],
            }
        ],
        description="/, " + _demo_base + " RX for Group:Finance",
        is_recursive=False,
    )
    _policy_ids = [
        create_hdfs_policy(_admin_session, policy_data_write),
        create_hdfs_policy(_admin_session, policy_root_demo_rx),
    ]
    logger.info("Created HDFS policies: %s", _policy_ids)

    _prepare_demo_data_hdfs_path(_demo_data)
    wait_for_hdfs_policy_propagation()
    logger.info("=== setup_module: done ===")


def teardown_module():
    if _admin_session is None:
        return

    logger.info("=== teardown_module: start ===")
    if _hdfs_paths_to_cleanup:
        cleanup_hdfs_paths(_hdfs_paths_to_cleanup)
    if _demo_base is not None:
        cleanup_hdfs_paths([_demo_base])

    if _policy_ids:
        delete_policies_by_id(_admin_session, _policy_ids)

    logger.info("Re-enabling all policies on %s", HDFS_SERVICE_NAME)
    set_all_policies_enabled(_admin_session, True, HDFS_SERVICE_NAME)
    wait_for_hdfs_policy_propagation()
    logger.info("=== teardown_module: done ===")


def test_03_01_create_hdfs_directory_case3a(request):
    log_testcase_begin_for_pytest(logger, request.node.name, suite="HDFS")
    hdfsdir = _demo_data + "/findir_test_03_01"
    _hdfs_paths_to_cleanup.append(hdfsdir)

    exit_code, output = run_hdfs_dfs(
        USER_FINANCE,
        "-mkdir -p " + hdfsdir,
        fail_on_error=False,
    )
    assert HDFS_AUTHZ_DENIED_SUBSTR not in output, (
        "Expected mkdir to succeed for finance user; exit_code="
        + str(exit_code)
        + " output="
        + output
    )


def test_03_02_create_hdfs_file_case3b(request):
    log_testcase_begin_for_pytest(logger, request.node.name, suite="HDFS")
    localfile = _LOCAL_STAGING_ROOT + "/filefinance.txt"
    hdfsfile = _demo_data + "/filefinance_test_03_02.txt"
    _hdfs_paths_to_cleanup.append(hdfsfile)

    exit_code, output = run_hdfs_dfs(
        USER_FINANCE,
        "-copyFromLocal " + localfile + " " + hdfsfile,
        fail_on_error=False,
    )
    assert HDFS_AUTHZ_DENIED_SUBSTR not in output, (
        "Expected copyFromLocal to succeed for finance user; exit_code="
        + str(exit_code)
        + " output="
        + output
    )


def test_03_03_create_hdfs_directory_case3c(request):
    log_testcase_begin_for_pytest(logger, request.node.name, suite="HDFS")
    hdfsdir = _demo_data + "/auditdir_test_03_03"
    _hdfs_paths_to_cleanup.append(hdfsdir)

    exit_code, output = run_hdfs_dfs(
        USER_AUDIT,
        "-mkdir -p " + hdfsdir,
        fail_on_error=False,
    )
    assert HDFS_AUTHZ_DENIED_SUBSTR in output, (
        "Expected mkdir to be denied for audit user; exit_code="
        + str(exit_code)
        + " output="
        + output
    )


def test_03_04_create_hdfs_file_case3d(request):
    log_testcase_begin_for_pytest(logger, request.node.name, suite="HDFS")
    localfile = _LOCAL_STAGING_ROOT + "/fileaudit.txt"
    hdfsfile = _demo_data + "/fileaudit_test_03_04.txt"
    _hdfs_paths_to_cleanup.append(hdfsfile)

    exit_code, output = run_hdfs_dfs(
        USER_AUDIT,
        "-copyFromLocal " + localfile + " " + hdfsfile,
        fail_on_error=False,
    )
    assert HDFS_AUTHZ_DENIED_SUBSTR in output, (
        "Expected copyFromLocal to be denied for audit user; exit_code="
        + str(exit_code)
        + " output="
        + output
    )
