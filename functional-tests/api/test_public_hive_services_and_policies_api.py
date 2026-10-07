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

from ranger_test_utils.utils import (
    POLICY_URL,
    SERVICE_URL,
    assert_http_ok,
    build_hive_policy_payload,
    build_hive_service_payload,
    configure_test_logging,
    create_ranger_admin_session,
    ensure_argus_policy_principals,
    find_policy_by_id,
    find_service_by_id,
    get_test_logger,
    log_http_response,
    policy_matches,
    service_matches,
    unique_suffix,
)

pytestmark = pytest.mark.api

logger = get_test_logger(__name__)

http = None
run_id = None

repo_id = None
policy_id = None
service_payload = None
policy_payload = None

repo_id1 = None
repo_id2 = None
service_payload1 = None
service_payload2 = None
policy_id1 = None
policy_id2 = None


def setup_module():
    global http, run_id

    configure_test_logging(module_name=__name__)
    logger.info("=== setup_module: start ===")

    http = create_ranger_admin_session()
    run_id = unique_suffix()
    logger.info("Test run id: %s", run_id)

    logger.info("Creating policy users and groups in Ranger Admin")
    ensure_argus_policy_principals(http)

    logger.info("=== setup_module: done ===")


def teardown_module():
    if http is None:
        return

    logger.info("=== teardown_module: cleaning leftover policies and services ===")

    if policy_id is not None:
        logger.info("Deleting leftover policy id=%s", policy_id)
        http.delete(POLICY_URL + str(policy_id))
    if policy_id1 is not None:
        logger.info("Deleting leftover policy id=%s", policy_id1)
        http.delete(POLICY_URL + str(policy_id1))
    if policy_id2 is not None:
        logger.info("Deleting leftover policy id=%s", policy_id2)
        http.delete(POLICY_URL + str(policy_id2))

    if repo_id is not None:
        logger.info("Deleting leftover service id=%s", repo_id)
        http.delete(SERVICE_URL + str(repo_id))
    if repo_id1 is not None:
        logger.info("Deleting leftover service id=%s", repo_id1)
        http.delete(SERVICE_URL + str(repo_id1))
    if repo_id2 is not None:
        logger.info("Deleting leftover service id=%s", repo_id2)
        http.delete(SERVICE_URL + str(repo_id2))

    logger.info("=== teardown_module: done ===")


def test_create_repository_using_pub_api_02a():
    global repo_id, service_payload

    logger.info("--- test_create_repository_using_pub_api_02a ---")
    service_name = "test_hive_repo_" + run_id
    service_payload = build_hive_service_payload(
        name=service_name,
        description="hive repo using api",
    )
    logger.info("POST hive service name=%s", service_name)

    response = http.post(SERVICE_URL, json=service_payload)
    log_http_response(logger, "POST create service", response)
    assert_http_ok(response, (200, 201), "POST create hive service", SERVICE_URL)

    body = response.json()
    repo_id = body["id"]
    logger.info("Created service id=%s", repo_id)
    assert service_matches(service_payload, body)


def test_get_repo_using_pub_api_02b():
    logger.info("--- test_get_repo_using_pub_api_02b ---")
    url = SERVICE_URL + str(repo_id)
    logger.info("GET service id=%s", repo_id)

    response = http.get(url)
    log_http_response(logger, "GET service", response)
    assert_http_ok(response, 200, "GET hive service by id", url)

    body = response.json()
    assert body["id"] == repo_id
    assert service_matches(service_payload, body)
    logger.info("GET service OK")


def test_update_repo_using_pub_api_02d():
    global repo_id, service_payload

    logger.info("--- test_update_repo_using_pub_api_02d ---")
    service_name = "test_hive_repo_updating_" + run_id
    service_payload = build_hive_service_payload(
        name=service_name,
        description="hive repo using api updating",
    )
    url = SERVICE_URL + str(repo_id)
    logger.info("PUT service id=%s new name=%s", repo_id, service_name)

    response = http.put(url, json=service_payload)
    log_http_response(logger, "PUT service", response)
    assert_http_ok(response, 200, "PUT update hive service", url)

    body = response.json()
    repo_id = body["id"]
    assert service_matches(service_payload, body)
    logger.info("Updated service id=%s", repo_id)


def test_create_hive_policy_using_pub_api_02e():
    global policy_id, policy_payload

    logger.info("--- test_create_hive_policy_using_pub_api_02e ---")
    policy_name = "test_hive_api_" + run_id
    policy_payload = build_hive_policy_payload(
        service_name=service_payload["name"],
        policy_name=policy_name,
        database_list="default,xatest",
        table_list="sample01,sample02",
        column_list="*",
        perm_map_list=[
            {"userList": ["hrt_21"], "permList": ["select", "update"]},
            {"userList": ["hrt_22"], "permList": ["select"]},
        ],
    )
    logger.info("POST policy name=%s for service=%s", policy_name, service_payload["name"])

    response = http.post(POLICY_URL, json=policy_payload)
    log_http_response(logger, "POST create policy", response)
    assert_http_ok(response, (200, 201), "POST create hive policy", POLICY_URL)

    body = response.json()
    policy_id = body["id"]
    logger.info("Created policy id=%s", policy_id)
    assert policy_matches(policy_payload, body)


def test_get_hive_policy_using_public_api_02f():
    logger.info("--- test_get_hive_policy_using_public_api_02f ---")
    url = POLICY_URL + str(policy_id)
    logger.info("GET policy id=%s", policy_id)

    response = http.get(url)
    log_http_response(logger, "GET policy", response)
    assert_http_ok(response, 200, "GET hive policy by id", url)

    body = response.json()
    assert body["id"] == policy_id
    assert policy_matches(policy_payload, body)
    logger.info("GET policy OK")


def test_update_policy_using_pub_api_02h():
    global policy_id, policy_payload

    logger.info("--- test_update_policy_using_pub_api_02h ---")
    policy_name = "test_hive_api_updated_" + run_id
    policy_payload = build_hive_policy_payload(
        service_name=service_payload["name"],
        policy_name=policy_name,
        database_list="default,xatest",
        table_list="sample01,sample02",
        column_list="*",
        perm_map_list=[
            {"groupList": ["audit"], "permList": ["select", "update"]},
            {"userList": ["hrt_21"], "permList": ["select"]},
        ],
        policy_id=policy_id,
    )
    url = POLICY_URL + str(policy_id)
    logger.info("PUT policy id=%s", policy_id)

    response = http.put(url, json=policy_payload)
    log_http_response(logger, "PUT policy", response)
    assert_http_ok(response, 200, "PUT update hive policy", url)

    body = response.json()
    policy_id = body["id"]
    assert policy_matches(policy_payload, body)
    logger.info("Updated policy id=%s", policy_id)


def test_delete_policy_using_pub_api_02i():
    global policy_id

    logger.info("--- test_delete_policy_using_pub_api_02i ---")
    url = POLICY_URL + str(policy_id)
    logger.info("DELETE policy id=%s", policy_id)

    delete_response = http.delete(url)
    log_http_response(logger, "DELETE policy", delete_response)
    assert_http_ok(delete_response, (200, 204), "DELETE hive policy", url)

    get_response = http.get(url)
    log_http_response(logger, "GET policy after delete", get_response)
    assert_http_ok(get_response, (400, 404), "GET policy after delete (expect missing)", url)

    policy_id = None
    logger.info("Policy deleted")


def test_delete_repo_using_pub_api_02j():
    global repo_id

    logger.info("--- test_delete_repo_using_pub_api_02j ---")
    url = SERVICE_URL + str(repo_id)
    logger.info("DELETE service id=%s", repo_id)

    delete_response = http.delete(url)
    log_http_response(logger, "DELETE service", delete_response)
    assert_http_ok(delete_response, (200, 204), "DELETE hive service", url)

    get_response = http.get(url)
    log_http_response(logger, "GET service after delete", get_response)
    assert_http_ok(get_response, (400, 404), "GET service after delete (expect missing)", url)

    repo_id = None
    logger.info("Service deleted")


def test_01_09_get_all_repos_using_pub_api_case1i():
    global repo_id1, repo_id2, service_payload1, service_payload2

    logger.info("--- test_01_09_get_all_repos_using_pub_api_case1i ---")
    service_payload1 = build_hive_service_payload(
        name="Test_Repo_Hive_API_06_01_" + run_id,
        description="Testing Hive Repo",
        username="hive",
        password="hive",
    )
    service_payload2 = build_hive_service_payload(
        name="Test_Repo_Hive_API_06_02_" + run_id,
        description="Testing Hive Repo",
        username="hive123",
        password="hive123",
    )

    create1 = http.post(SERVICE_URL, json=service_payload1)
    log_http_response(logger, "POST service 1", create1)
    create2 = http.post(SERVICE_URL, json=service_payload2)
    log_http_response(logger, "POST service 2", create2)
    assert_http_ok(create1, (200, 201), "POST hive service 1", SERVICE_URL)
    assert_http_ok(create2, (200, 201), "POST hive service 2", SERVICE_URL)

    repo_id1 = create1.json()["id"]
    repo_id2 = create2.json()["id"]
    logger.info("Created services id=%s and id=%s", repo_id1, repo_id2)

    list_response = http.get(SERVICE_URL)
    log_http_response(logger, "GET all services", list_response)
    assert_http_ok(list_response, 200, "GET all services", SERVICE_URL)

    services = list_response.json()
    found1 = find_service_by_id(services, repo_id1)
    found2 = find_service_by_id(services, repo_id2)
    assert found1 is not None
    assert found2 is not None
    assert service_matches(service_payload1, found1)
    assert service_matches(service_payload2, found2)
    logger.info("Both services found in list")


def test_01_10_get_all_policies_case1j():
    global policy_id1, policy_id2

    logger.info("--- test_01_10_get_all_policies_case1j ---")
    policy1 = build_hive_policy_payload(
        service_name=service_payload1["name"],
        policy_name="test_hive_api_01_" + run_id,
        database_list="default01,xatest01",
        table_list="test01,test02",
        column_list="*",
        perm_map_list=[
            {"groupList": ["finance"], "permList": ["select", "update"]},
            {"userList": ["hrt_21"], "permList": ["select"]},
            {"userList": ["hrt_22"], "permList": ["update"]},
        ],
    )
    policy2 = build_hive_policy_payload(
        service_name=service_payload2["name"],
        policy_name="test_hive_api_02_" + run_id,
        database_list="xatestcw,iemployee",
        table_list="sample01,sample02",
        column_list="*",
        perm_map_list=[
            {"groupList": ["audit"], "permList": ["select", "update"]},
            {"userList": ["hrt_21"], "permList": ["update"]},
        ],
    )

    create1 = http.post(POLICY_URL, json=policy1)
    log_http_response(logger, "POST policy 1", create1)
    create2 = http.post(POLICY_URL, json=policy2)
    log_http_response(logger, "POST policy 2", create2)
    assert_http_ok(create1, (200, 201), "POST hive policy 1", POLICY_URL)
    assert_http_ok(create2, (200, 201), "POST hive policy 2", POLICY_URL)

    policy_id1 = create1.json()["id"]
    policy_id2 = create2.json()["id"]
    logger.info("Created policies id=%s and id=%s", policy_id1, policy_id2)

    list_response = http.get(POLICY_URL)
    log_http_response(logger, "GET all policies", list_response)
    assert_http_ok(list_response, 200, "GET all policies", POLICY_URL)

    policies = list_response.json()
    found1 = find_policy_by_id(policies, policy_id1)
    found2 = find_policy_by_id(policies, policy_id2)
    assert found1 is not None
    assert found2 is not None
    assert policy_matches(policy1, found1)
    assert policy_matches(policy2, found2)
    logger.info("Both policies found in list")


def test_01_11_delete_all_repo_case1k():
    global policy_id1, policy_id2, repo_id1, repo_id2

    logger.info("--- test_01_11_delete_all_repo_case1k ---")

    if policy_id1 is not None:
        logger.info("DELETE policy id=%s", policy_id1)
        http.delete(POLICY_URL + str(policy_id1))
        policy_id1 = None
    if policy_id2 is not None:
        logger.info("DELETE policy id=%s", policy_id2)
        http.delete(POLICY_URL + str(policy_id2))
        policy_id2 = None

    if repo_id1 is not None:
        url = SERVICE_URL + str(repo_id1)
        logger.info("DELETE service id=%s", repo_id1)
        response = http.delete(url)
        log_http_response(logger, "DELETE service 1", response)
        assert_http_ok(response, (200, 204), "DELETE hive service 1", url)
        repo_id1 = None

    if repo_id2 is not None:
        url = SERVICE_URL + str(repo_id2)
        logger.info("DELETE service id=%s", repo_id2)
        response = http.delete(url)
        log_http_response(logger, "DELETE service 2", response)
        assert_http_ok(response, (200, 204), "DELETE hive service 2", url)
        repo_id2 = None

    logger.info("Cleanup finished")
