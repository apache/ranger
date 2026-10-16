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

"""Kafka Docker helpers for Ranger functional-tests (console producer + plugin enforcement)."""

import json
import shlex
import time

import pytest

from ranger_test_utils.hive_utils import (
    ADMIN_CONTAINER,
    KDC_CONTAINER,
    KERBEROS_REALM,
    get_docker_client,
    list_policies_for_service,
)
from ranger_test_utils.utils import (
    LOGS_DIR,
    POLICY_URL,
    assert_http_ok,
    ensure_logs_directory,
    get_test_logger,
    unique_suffix,
)

logger = get_test_logger(__name__)

KAFKA_SERVICE_NAME = "dev_kafka"
KAFKA_CONTAINER = "ranger-kafka"
KAFKA_BOOTSTRAP_SERVER = "ranger-kafka.rangernw:9092"

KAFKA_ALLOWED_USER = "hrt_21"
KAFKA_DENIED_USER = "hrt_22"
KAFKA_ALLOWED_GROUP = "finance"

KAFKA_AUTHZ_DENIED_SUBSTR = "TopicAuthorizationException"

# publish to a not-yet-existing topic: broker auto-creates it, which needs create + describe
KAFKA_TOPIC_POLICY_ACCESS_TYPES = ["publish", "create", "describe"]

# Kafka plugin polls Ranger for policy changes every 30s by default.
KAFKA_POLICY_PROPAGATION_SLEEP_SEC = 40


def _container_running(name):
    try:
        container = get_docker_client().containers.get(name)
    except Exception:
        return False
    container.reload()
    return container.status == "running"


def preflight_kafka_stack():
    """Skip tests when required Docker services are unavailable."""
    required = [ADMIN_CONTAINER, KDC_CONTAINER, KAFKA_CONTAINER]
    missing = [name for name in required if not _container_running(name)]
    if missing:
        pytest.skip(
            "Kafka functional-tests need running containers: "
            + ", ".join(missing)
            + ". Run ./run-tests.sh postgres kafka from functional-tests/."
        )


def _user_principal(username):
    return username + "@" + KERBEROS_REALM


def _user_keytab_in_kafka_container(username):
    return "/etc/keytabs/" + username + ".keytab"


def ensure_kerberos_user(username):
    """Create a user Kerberos principal and keytab on KDC, visible in ranger-kafka."""
    kdc = get_docker_client().containers.get(KDC_CONTAINER)
    principal = _user_principal(username)
    keytab_in_kdc = "/etc/keytabs/ranger-kafka/" + username + ".keytab"

    logger.info("Kerberos setup on %s: principal=%s keytab=%s", KDC_CONTAINER, principal, keytab_in_kdc)
    kdc.exec_run('kadmin.local -q "addprinc -randkey ' + principal + '"', user="root")
    kdc.exec_run("rm -f " + keytab_in_kdc, user="root")
    # -norandkey keeps the key version, so keytabs this shared user already has in the
    # hive/hbase containers stay valid.
    exit_code, output = kdc.exec_run(
        'kadmin.local -q "ktadd -norandkey -k ' + keytab_in_kdc + " " + principal + '"',
        user="root",
        demux=True,
    )
    if exit_code != 0:
        raise RuntimeError(
            "ktadd failed for " + principal + ": " + ((output[0] or b"") + (output[1] or b"")).decode()
        )
    kdc.exec_run("chmod 444 " + keytab_in_kdc, user="root")


def ensure_os_group_membership(username, group):
    """
    Make username a member of group on the ranger-kafka OS.

    The Kafka plugin resolves a user's groups from the broker's OS (Hadoop group
    mapping), not from Ranger, so group-based policies need the membership here.
    """
    kafka = get_docker_client().containers.get(KAFKA_CONTAINER)
    shell = (
        "getent group " + group + " > /dev/null || groupadd " + group + "; "
        "id " + username + " > /dev/null 2>&1 || useradd -M -s /sbin/nologin " + username + "; "
        "usermod -aG " + group + " " + username
    )
    exit_code, output = kafka.exec_run(["bash", "-c", shell], user="root", demux=True)
    if exit_code != 0:
        raise RuntimeError(
            "Failed to add "
            + username
            + " to OS group "
            + group
            + " in "
            + KAFKA_CONTAINER
            + ": "
            + ((output[0] or b"") + (output[1] or b"")).decode()
        )
    logger.info("OS group membership in %s: %s -> %s", KAFKA_CONTAINER, username, group)


def run_kafka_producer(username, topic, message):
    """
    Publish one message to topic with kafka-console-producer.sh inside ranger-kafka,
    authenticating as username from its keytab.

    Returns (exit_code, combined_output).
    """
    kafka = get_docker_client().containers.get(KAFKA_CONTAINER)
    props_path = "/tmp/kafka-producer-" + username + "-" + unique_suffix(6) + ".properties"
    properties = "\n".join(
        [
            "security.protocol=SASL_PLAINTEXT",
            "sasl.mechanism=GSSAPI",
            "sasl.kerberos.service.name=kafka",
            "sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required"
            " useKeyTab=true storeKey=true doNotPrompt=true"
            ' keyTab="' + _user_keytab_in_kafka_container(username) + '"'
            ' principal="' + _user_principal(username) + '";',
            # Ranger's Kafka authorizer denies the idempotent producer's cluster-level
            # idempotent_write check, so publish with a plain producer.
            "enable.idempotence=false",
        ]
    )
    producer_cmd = (
        "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "
        + KAFKA_BOOTSTRAP_SERVER
        + " --topic "
        + shlex.quote(topic)
        + " --producer.config "
        + props_path
    )
    shell = (
        "printf '%s\\n' "
        + shlex.quote(properties)
        + " > "
        + props_path
        + "; printf '%s\\n' "
        + shlex.quote(message)
        + " | "
        + producer_cmd
        + '; rc=$?; rm -f '
        + props_path
        + '; exit "$rc"'
    )
    logger.info("Kafka producer: container=%s ranger_user=%s", KAFKA_CONTAINER, username)
    logger.info("RUNNING: %s", producer_cmd)

    exit_code, output = kafka.exec_run(["bash", "-c", shell], user="kafka", demux=True)
    combined = (output[0] or b"").decode() + (output[1] or b"").decode()
    logger.info("Kafka producer exit_code=%s output:\n%s", exit_code, combined)
    return exit_code, combined


def create_topic_allow_policy(session, topic, group, service_name=KAFKA_SERVICE_NAME):
    """Create a policy allowing group to publish to topic. Returns the policy id."""
    payload = {
        "service": service_name,
        "name": "allow publish on kafka topic " + topic + " - " + unique_suffix(),
        "description": "Kafka topic allow policy for functional-tests",
        "isEnabled": True,
        "isAuditEnabled": True,
        "resources": {
            "topic": {"values": [topic], "isExcludes": False, "isRecursive": False},
        },
        "policyItems": [
            {
                "users": [],
                "groups": [group],
                "accesses": [
                    {"type": perm, "isAllowed": True}
                    for perm in KAFKA_TOPIC_POLICY_ACCESS_TYPES
                ],
                "delegateAdmin": False,
            }
        ],
    }
    response = session.post(POLICY_URL, json=payload)
    assert_http_ok(response, (200, 201), "Create kafka topic allow policy", POLICY_URL)
    policy_id = response.json().get("id")
    assert policy_id is not None, "Create policy response missing id: " + response.text
    return policy_id


def snapshot_policies(session, service_name=KAFKA_SERVICE_NAME):
    """Return every policy in the service and save them to logs/ before anything changes."""
    policies = list_policies_for_service(session, service_name)
    ensure_logs_directory()
    path = LOGS_DIR / ("kafka_policies_snapshot_" + unique_suffix() + ".json")
    path.write_text(json.dumps(policies, indent=2))
    logger.info("Saved %s policies from %s to %s", len(policies), service_name, path)
    return policies


def restore_missing_policies(session, original_policies, service_name=KAFKA_SERVICE_NAME):
    """
    Re-create each original policy whose name is not currently in the service.

    Safe to call after a partial setup or teardown: policies still present are skipped,
    and a failed restore does not stop the others. Raises at the end if any failed.
    """
    current_names = {policy["name"] for policy in list_policies_for_service(session, service_name)}
    restored = 0
    failures = []
    for policy in original_policies:
        if policy["name"] in current_names:
            continue
        body = {key: value for key, value in policy.items() if key not in ("id", "guid", "version")}
        response = session.post(POLICY_URL, json=body)
        if response.status_code in (200, 201):
            restored += 1
        else:
            failures.append(policy["name"] + ": HTTP " + str(response.status_code) + " " + response.text)
    logger.info("Restored %s policies in %s", restored, service_name)
    if failures:
        raise AssertionError(
            "Failed to restore policies in " + service_name + ":\n" + "\n".join(failures)
        )


def wait_for_kafka_policy_propagation(seconds=KAFKA_POLICY_PROPAGATION_SLEEP_SEC):
    logger.info("Waiting %ss for Kafka plugin policy propagation", seconds)
    time.sleep(seconds)
