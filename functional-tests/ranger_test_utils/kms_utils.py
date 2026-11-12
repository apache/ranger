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
KMS Docker functional-test helpers: transport, REST client, Ranger policy, ``KmsXaTestCase``.

Pair with ``kms_test_utils.py`` for module logging and session Kerberos setup.
"""

import copy
import io
import json
import os
import subprocess
import tarfile
import tempfile
import time
import xml.etree.ElementTree as ET
from contextlib import contextmanager

import docker
import requests

from ranger_test_utils.plugin_test_users import PLUGIN_TEST_USER
from ranger_test_utils.utils import get_test_logger, log_http_response, log_testcase_begin

logger = get_test_logger(__name__)

# --- Docker / Kerberos transport -----------------------------------------------------------------

KMS_CONTAINER_NAME = "ranger-kms"
KMS_LOG_FILE = "/var/log/ranger/kms/ranger-kms-ranger-kms.rangernw-root.log"

BASE_URL_RANGER = "http://localhost:6080/service/public/v2/api/policy"
BASE_URL_RANGER_USERS = "http://localhost:6080/service/xusers/secure/users"
BASE_URL_RANGER_USERS_BY_NAME = "http://localhost:6080/service/xusers/users/userName"

RANGER_ADMIN_AUTH = ("admin", "rangerR0cks!")
RANGER_KMS_AUTH = ('keyadmin', 'rangerR0cks!')  # Ranger key admin user
KMS_SERVICE_NAME = "dev_kms"
TEST_USER = "testuser"

TESTUSER_PARAMS = {}
BASE_URL = "http://ranger-kms.rangernw:9292/kms/v1"
PARAMS = {"user.name": "keyadmin"}

_docker_client = None


def get_docker_client():
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


def get_kms_container():
    """Return ranger-kms container (lazy — no Docker call at import time)."""
    return get_docker_client().containers.get(KMS_CONTAINER_NAME)


class _LazyKmsContainer:
    """Backward-compatible alias; resolves ranger-kms on first use."""

    def __getattr__(self, name):
        return getattr(get_kms_container(), name)


container = _LazyKmsContainer()

KEYADMIN_PRINCIPAL = "keyadmin@EXAMPLE.COM"
KEYADMIN_KEYTAB = "/etc/keytabs/keyadmin.keytab"
KDC_CONTAINER = "ranger-kdc"
TESTUSER = "testuser"
TESTUSER_PRINCIPAL = f"{TESTUSER}@EXAMPLE.COM"
TESTUSER_KEYTAB = f"/etc/keytabs/{TESTUSER}.keytab"

HRT_21 = "hrt_21"
HRT_21_PRINCIPAL = f"{HRT_21}@EXAMPLE.COM"
HRT_21_KEYTAB = f"/etc/keytabs/{HRT_21}.keytab"


def fetch_logs():
    try:
        cmd = f"docker exec {KMS_CONTAINER_NAME} tail -n 100 {KMS_LOG_FILE}"
        logs = subprocess.check_output(cmd, shell=True, text=True)
        error_logs = [line for line in logs.split("\n") if "ERROR" in line or "Exception" in line]
        return "\n".join(error_logs) if error_logs else "No recent errors in logs."
    except subprocess.CalledProcessError as e:
        return f"Failed to fetch logs from container. Command failed with exit code {e.returncode}: {e.output}"

def find_conflicting_policies(service, keyname_pattern):
    # Find any existing policy on this service whose keyname resource overlaps with our pattern, regardless of policy name."""
    response = requests.get(
        BASE_URL_RANGER,
        auth=RANGER_KMS_AUTH,
        params={"serviceName": service},
    )
    if response.status_code != 200:
        return []

    results = response.json()
    if not isinstance(results, list):
        return []

    conflicting_ids = []
    for policy in results:
        resources = policy.get("resources", {})
        keyname_values = resources.get("keyname", {}).get("values", [])
        if keyname_pattern in keyname_values:
            conflicting_ids.append(policy["id"])

    return conflicting_ids

class KerberosRequests:
    def _curl(self, method, url, json_body=None, params=None):
        full_url = url
        if params:
            if isinstance(params, dict):
                items = params.items()
            else:
                items = params   # list of tuples — allows duplicate keys e.g. [("key","k1"),("key","k2")]
            qs = "&".join(f"{k}={v}" for k, v in items)
            full_url = f"{url}?{qs}"

        # Build curl args as a shell-safe string so we can append the body read
        # in the same exec_run call — avoids a second docker exec for "cat".
        curl_args = [
            "curl", "-s", "-o", "/tmp/curl_body.txt", "-w", "%{http_code}",
            "--negotiate", "-u", ":",
            "-X", method.upper(),
            "-H", "Content-Type: application/json",
        ]
        if json_body is not None:
            curl_args += ["-d", json.dumps(json_body)]
        curl_args.append(full_url)

        # Escape each argument for a POSIX shell one-liner
        def _sh_quote(s):
            return "'" + s.replace("'", "'\\''") + "'"

        curl_cmd = " ".join(_sh_quote(a) for a in curl_args)
        # Single exec: run curl then print a fixed separator then the body.
        # Output format:  <status_code>\n---BODY---\n<body text>
        shell_cmd = f'{curl_cmd}; echo "\\n---BODY---"; cat /tmp/curl_body.txt'

        exit_code, output = get_kms_container().exec_run(
            ["sh", "-c", shell_cmd], user="root"
        )
        raw = output.decode() if output else ""

        separator = "\n---BODY---\n"
        if separator in raw:
            status_part, body = raw.split(separator, 1)
        else:
            status_part, body = raw.strip(), ""

        try:
            status_code = int(status_part.strip())
        except ValueError:
            status_code = 0

        return _FakeResponse(status_code, body)

    def post(self, url, headers=None, json=None, params=None, **_):
        return self._curl("POST", url, json_body=json, params=params)

    def delete(self, url, params=None, **_):
        return self._curl("DELETE", url, params=params)

    def get(self, url, headers=None, params=None, **_):
        return self._curl("GET", url, params=params)

    def put(self, url, headers=None, json=None, params=None, **_):
        return self._curl("PUT", url, json_body=json, params=params)


class _FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = body        # for test_keyDetails.py compatibility

    def json(self):
        return json.loads(self._body)

    def __repr__(self):
        return f"<Response [{self.status_code}]>"


krb_requests = KerberosRequests()

def _ensure_kdc_principal_keytab(principal: str, dest_container_name: str, dest_keytab_path: str, kdc_tmp_path: str, ) -> None:
    # create principal in KDC, export keytab, copy to target container
    kdc = get_docker_client().containers.get(KDC_CONTAINER)
    dest = get_docker_client().containers.get(dest_container_name)

    exit_code, output = kdc.exec_run(f'kadmin.local -q "getprinc {principal}"', user="root")
    missing = (
        exit_code != 0
        or b"does not exist" in output.lower()
        or b"no such principal" in output.lower()
    )
    if missing:
        ec, out = kdc.exec_run(f'kadmin.local -q "addprinc -randkey {principal}"', user="root")
        if ec != 0:
            raise RuntimeError(f"addprinc failed for {principal}: {out.decode()}")

    exit_code, output = kdc.exec_run(
        f'kadmin.local -q "xst -k {kdc_tmp_path} {principal}"', user="root"
    )
    if exit_code != 0:
        raise RuntimeError(f"keytab export failed for {principal}: {output.decode()}")
    local = os.path.join(tempfile.gettempdir(), os.path.basename(dest_keytab_path))
    subprocess.check_call(f"docker cp {KDC_CONTAINER}:{kdc_tmp_path} {local}", shell=True)
    subprocess.check_call(f"docker cp {local} {dest_container_name}:{dest_keytab_path}", shell=True)
    dest.exec_run(f"chmod 400 {dest_keytab_path}", user="root")
    os.remove(local)


def ensure_keyadmin_keytab():
    # creation of keyadmin@example.com  & install keytab in ranger-kms
    _ensure_kdc_principal_keytab(KEYADMIN_PRINCIPAL, KMS_CONTAINER_NAME, KEYADMIN_KEYTAB,"/tmp/keyadmin.keytab",)

def _kinit_with_retry(principal: str, keytab: str, ensure_keytab_fn, max_attempts: int = 5, wait: int = 10) -> None:
    """Ensure the keytab exists then kinit, retrying to absorb KDC warm-up delays."""
    kms = get_kms_container()
    exit_code, _ = kms.exec_run(f"test -f {keytab}", user="root")
    if exit_code != 0:
        ensure_keytab_fn()

    kms.exec_run("kdestroy -A 2>/dev/null || true", user="root")
    for _ in range(max_attempts):
        exit_code, output = kms.exec_run(
            f"kinit -kt {keytab} {principal}", user="root"
        )
        if exit_code == 0:
            return
        time.sleep(wait)
    raise RuntimeError(f"kinit failed for {principal} after {max_attempts} attempts: {output.decode()}")

def ensure_ticket():
    """Obtain a keyadmin Kerberos ticket (backward-compat alias for ensure_keyadmin_ticket)."""
    _kinit_with_retry(KEYADMIN_PRINCIPAL, KEYADMIN_KEYTAB, ensure_keyadmin_keytab)

def ensure_keyadmin_ticket():
    """Obtain a keyadmin Kerberos ticket."""
    _kinit_with_retry(KEYADMIN_PRINCIPAL, KEYADMIN_KEYTAB, ensure_keyadmin_keytab)

def ensure_testuser_keytab():
    _ensure_kdc_principal_keytab(
        TESTUSER_PRINCIPAL,
        KMS_CONTAINER_NAME,
        TESTUSER_KEYTAB,
        f"/tmp/{TESTUSER}.keytab",
    )


def ensure_hrt_21_keytab():
    _ensure_kdc_principal_keytab(
        HRT_21_PRINCIPAL,
        KMS_CONTAINER_NAME,
        HRT_21_KEYTAB,
        f"/tmp/{HRT_21}.keytab",
    )

def ensure_testuser_ticket():
    """Obtain a testuser Kerberos ticket."""
    _kinit_with_retry(TESTUSER_PRINCIPAL, TESTUSER_KEYTAB, ensure_testuser_keytab)


def _kinit_in_kms_container(keytab, principal):
    kms = get_kms_container()
    kms.exec_run("kdestroy -A 2>/dev/null || true", user="root")
    exit_code, output = kms.exec_run(
        f"kinit -kt {keytab} {principal}", user="root"
    )
    if exit_code != 0:
        raise RuntimeError(
            f"kinit failed for {principal}: {output.decode() if output else ''}"
        )


@contextmanager
def kerberos_as_testuser():
    """Run KMS REST calls in the container as ``testuser@EXAMPLE.COM``."""
    _kinit_in_kms_container(TESTUSER_KEYTAB, TESTUSER_PRINCIPAL)
    try:
        yield
    finally:
        _kinit_in_kms_container(KEYADMIN_KEYTAB, KEYADMIN_PRINCIPAL)


@contextmanager
def kerberos_as_keyadmin():
    """Ensure the active container ticket is keyadmin for KMS REST calls."""
    ensure_keyadmin_ticket()
    try:
        yield
    finally:
        pass


@contextmanager
def kerberos_as_hrt_21():
    """Run KMS REST calls in the container as ``hrt_21@EXAMPLE.COM``."""
    _kinit_in_kms_container(HRT_21_KEYTAB, HRT_21_PRINCIPAL)
    try:
        yield
    finally:
        _kinit_in_kms_container(KEYADMIN_KEYTAB, KEYADMIN_PRINCIPAL)


def kerberos_context_for_user(username):
    if username == TESTUSER:
        return kerberos_as_testuser()
    if username == HRT_21:
        return kerberos_as_hrt_21()
    if username == "keyadmin":
        return kerberos_as_keyadmin()
    raise ValueError(f"No Kerberos context configured for KMS user: {username}")


def modify_blacklist_property(operation, users, action="add"):
    dbks_site_path = (
        "/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp"
        "/WEB-INF/classes/conf/dbks-site.xml"
    )

    ensure_ticket()

    result = get_kms_container().exec_run(f"cat {dbks_site_path}", user="root")
    if result.exit_code != 0:
        raise RuntimeError(f"Cannot read dbks-site.xml: {result.output.decode()}")

    root = ET.fromstring(result.output.decode("utf-8"))
    prop_name = f"hadoop.kms.blacklist.{operation}"

    prop = None
    for elem in root.findall("property"):
        name = elem.find("name")
        if name is not None and name.text == prop_name:
            prop = elem
            break

    if prop is None:
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = prop_name
        ET.SubElement(prop, "value").text = ""

    val_elem = prop.find("value")
    current = val_elem.text.split(",") if val_elem.text else []
    updated = set(current)

    if action == "add":
        updated.update(users)
    elif action == "remove":
        updated -= set(users)

    val_elem.text = ",".join(sorted(updated))

    modified_xml = ET.tostring(root, encoding="utf-8", method="xml").decode()

    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar:
        data = modified_xml.encode()
        info = tarfile.TarInfo(name="dbks-site.xml")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    tarstream.seek(0)

    get_kms_container().put_archive(
        path="/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp/WEB-INF/classes/conf/",
        data=tarstream,
    )
    print(f"Successfully {'added' if action == 'add' else 'removed'} {users} in {prop_name}")


def blacklist_op_users(operation, users=[]):
    modify_blacklist_property(operation, users, action="add")


def unblacklist_op_users(operation, users=[]):
    modify_blacklist_property(operation, users, action="remove")

def ensure_test_user_exists(username: str) -> None:
    payload = {
        "name": username,
        "firstName": "Test",
        "lastName": "User",
        "password": "Password123!",
        "description": "pytest dummy user created via API",
        "status": 1,
        "isVisible": 1,
        "userSource": 0,
        "userRoleList": ["ROLE_USER"],
    }

    r = requests.post(BASE_URL_RANGER_USERS, auth=RANGER_ADMIN_AUTH, json=payload)
    if r.status_code in (200, 201):
        return
    raise RuntimeError(f"Failed to create Ranger user {username}: {r.status_code} {r.text}")

def delete_test_user(username: str) -> None:
    r = requests.delete(
        f"{BASE_URL_RANGER_USERS_BY_NAME}/{username}",
        params={"forceDelete": "true"},
        auth=RANGER_ADMIN_AUTH,
    )
    if r.status_code in (200, 204, 404):
        return
    raise RuntimeError(f"Failed to delete Ranger user {username}: {r.status_code} {r.text}")

# Public alias for jvm_locale_test
ensure_kdc_principal_keytab = _ensure_kdc_principal_keytab

# --- Ranger Admin policy (``test_kms_04``) -------------------------------------------------------

RANGER_SERVICE_POLICY_URL = (
    f"http://localhost:6080/service/public/v2/api/service/{KMS_SERVICE_NAME}/policy"
)
DEFAULT_KMS_WILDCARD_POLICY_NAME = "all - keyname"

_KMS_04_POLICY_RESTORE_SNAPSHOT = None

KMS_ALL_PERMISSIONS = [
    "create",
    "delete",
    "rollover",
    "get",
    "getkeys",
    "getmetadata",
    "generateeek",
    "decrypteek",
    "setkeymaterial",
]

KMS_KEYADMIN_INFRA_USER = "keyadmin"


def create_policy_item(access_types, user, group=None, delegate_admin=False, exclude_access=False):
    """Build a KMS policy item dict for Ranger Admin."""
    accesses = []
    groups = []
    users = []

    if exclude_access:
        for item in KMS_ALL_PERMISSIONS:
            if not access_types or item not in access_types:
                accesses.append({"type": item, "isAllowed": True})
    else:
        for access_type in access_types or []:
            accesses.append({"type": access_type.lower(), "isAllowed": True})

    if user is not None:
        users.append(user)
    if group is not None:
        groups.append(group)

    return {
        "accesses": accesses,
        "users": users,
        "groups": groups,
        "delegateAdmin": delegate_admin,
    }


def _kms_policies_from_response(data):
    if isinstance(data, list):
        return data
    return data.get("policies", [])


def fetch_dev_kms_policies():
    response = requests.get(RANGER_SERVICE_POLICY_URL, auth=RANGER_KMS_AUTH, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"Cannot list KMS policies: {response.text}")
    return _kms_policies_from_response(response.json())


def find_wildcard_keyname_policy(policies):
    for policy in policies:
        keyname_values = (policy.get("resources") or {}).get("keyname", {}).get("values", [])
        if keyname_values == ["*"]:
            return policy
    return None


def _is_duplicate_resource_policy_error(response_text):
    return "3010" in response_text or "Another policy already exists for matching resource" in response_text


def kms_04_initial_policy_items(kms_user, hdfs_user="hdfs"):
    """
    hdfs exclude-all row + ``create`` for ``kms_user`` (``test_kms_04`` module setup).

    keyadmin row is infrastructure only; scenarios still use ``policyItems[1]`` for hrt_21.
    """
    return [
        create_policy_item(None, hdfs_user, exclude_access=True),
        create_policy_item(["create"], kms_user),
        create_policy_item(None, KMS_KEYADMIN_INFRA_USER, exclude_access=True),
    ]


def _keyadmin_item_has_list_access(policy_items):
    for item in policy_items or []:
        if KMS_KEYADMIN_INFRA_USER not in (item.get("users") or []):
            continue
        access_types = {a.get("type", "").lower() for a in item.get("accesses") or []}
        if item.get("delegateAdmin"):
            return True
        if len(access_types) >= len(KMS_ALL_PERMISSIONS):
            return True
        if "getkeys" in access_types:
            return True
    return False


def ensure_keyadmin_can_list_kms_keys():
    """Ensure keyadmin can ``GET /keys/names`` (repairs hijacked or stale wildcard policy)."""
    response = execute_kms_rest_test(
        "GET",
        "/keys/names",
        expected_status=None,
        step="KMS.ensure_keyadmin_can_list",
    )
    if response.status_code == 200:
        return

    wildcard = find_wildcard_keyname_policy(fetch_dev_kms_policies())
    if wildcard is None:
        raise RuntimeError(
            "keyadmin cannot list KMS keys and no wildcard keyname=* policy was found"
        )

    items = list(wildcard.get("policyItems") or [])
    if not _keyadmin_item_has_list_access(items):
        items.append(create_policy_item(None, KMS_KEYADMIN_INFRA_USER, exclude_access=True))
        wildcard = copy.deepcopy(wildcard)
        wildcard["policyItems"] = items
        update_kms_policy(wildcard)

    execute_kms_rest_test(
        "GET",
        "/keys/names",
        expected_status=200,
        step="KMS.ensure_keyadmin_can_list.retry",
    )


def create_kms_04_module_policy(policy_name, kms_user, hdfs_user="hdfs"):
    """
    Initial policy for ``test_kms_04`` — hdfs row + ``create`` for ``kms_user`` on ``*``.

    Docker ships a seed policy (``all - keyname``) on ``keyname=*``; Ranger rejects a second
    wildcard policy. On that conflict we replace ``policyItems`` on the existing policy and
    restore the prior JSON in :func:`teardown_kms_04_module_policy`.
    """
    global _KMS_04_POLICY_RESTORE_SNAPSHOT

    policy_items = kms_04_initial_policy_items(kms_user, hdfs_user)
    policy_data = {
        "policyName": policy_name,
        "service": KMS_SERVICE_NAME,
        "resources": {
            "keyname": {
                "values": ["*"],
                "isExcludes": False,
                "isRecursive": False,
            }
        },
        "policyItems": policy_items,
    }
    response = requests.post(BASE_URL_RANGER, auth=RANGER_KMS_AUTH, json=policy_data)
    time.sleep(5)
    if response.status_code in (200, 201):
        _KMS_04_POLICY_RESTORE_SNAPSHOT = None
        return response.json()

    if not _is_duplicate_resource_policy_error(response.text):
        raise RuntimeError(f"Cannot create policy {policy_name}: {response.text}")

    existing = find_wildcard_keyname_policy(fetch_dev_kms_policies())
    if existing is None:
        raise RuntimeError(
            f"Cannot create policy {policy_name}: {response.text} "
            "(and no existing wildcard keyname policy to update)"
        )

    _KMS_04_POLICY_RESTORE_SNAPSHOT = copy.deepcopy(existing)
    updated = copy.deepcopy(existing)
    updated["policyItems"] = policy_items
    return update_kms_policy(updated)


def update_kms_policy(policy):
    """Push policy JSON to Ranger Admin."""
    policy_id = policy["id"]
    update_url = f"{BASE_URL_RANGER}/{policy_id}"
    response = requests.put(update_url, auth=RANGER_KMS_AUTH, json=policy)
    time.sleep(5)
    if response.status_code != 200:
        raise RuntimeError(f"Cannot update policy {policy_id}: {response.text}")
    return response.json()


def delete_kms_policy_by_id(policy_id):
    requests.delete(f"{BASE_URL_RANGER}/{policy_id}", auth=RANGER_KMS_AUTH)


def teardown_kms_04_module_policy(policy):
    """Restore hijacked seed policy or delete a policy created only for this module."""
    global _KMS_04_POLICY_RESTORE_SNAPSHOT
    if _KMS_04_POLICY_RESTORE_SNAPSHOT is not None:
        update_kms_policy(_KMS_04_POLICY_RESTORE_SNAPSHOT)
        _KMS_04_POLICY_RESTORE_SNAPSHOT = None
        return
    if policy and policy.get("id"):
        delete_kms_policy_by_id(policy["id"])


def execute_kms_rest_as_user(
    method,
    url_path,
    username,
    *,
    json_body=None,
    expected_status=None,
    extra_params=None,
    step=None,
):
    params = {"user.name": username}
    if extra_params:
        params.update(extra_params)

    with kerberos_context_for_user(username):
        return execute_kms_rest_test(
            method,
            url_path,
            json_body=json_body,
            params=params,
            expected_status=expected_status,
            step=step or f"KMS.{username}.{method}.{url_path}",
        )


# --- KMS REST client -----------------------------------------------------------------------------


def _kms_url(url_path):
    if url_path.startswith("http://") or url_path.startswith("https://"):
        return url_path
    if not url_path.startswith("/"):
        url_path = "/" + url_path
    return BASE_URL + url_path


def execute_kms_rest_test(
    method,
    url_path,
    *,
    json_body=None,
    params=None,
    headers=None,
    expected_status=None,
    step=None,
):
    """
    Run one KMS REST call via Kerberos curl inside the ranger-kms container.

    Asserts ``expected_status`` when provided; returns the response object.
    """
    params = PARAMS if params is None else params
    url = _kms_url(url_path)
    method_upper = method.upper()
    if method_upper == "GET":
        response = krb_requests.get(url, headers=headers, params=params)
    elif method_upper == "POST":
        response = krb_requests.post(
            url, headers=headers, json=json_body, params=params
        )
    elif method_upper == "PUT":
        response = krb_requests.put(
            url, headers=headers, json=json_body, params=params
        )
    elif method_upper == "DELETE":
        response = krb_requests.delete(url, params=params)
    else:
        raise ValueError("Unsupported HTTP method: " + method)

    label = step or (method_upper + " " + url_path)
    log_http_response(logger, label, response)

    if expected_status is not None and response.status_code != expected_status:
        error_logs = fetch_logs()
        raise AssertionError(
            f"{label} failed: expected HTTP {expected_status}, "
            f"got {response.status_code}: {response.text}\nLogs:\n{error_logs}"
        )
    return response


class KmsRestTestContext:
    """Per-module context (assign ``headers`` in ``setup_module`` if needed)."""

    USER_KEYADMIN = "keyadmin"
    DEFAULT_HEADERS = {"Content-Type": "application/json"}

    def __init__(self):
        self.headers = dict(self.DEFAULT_HEADERS)


def fetch_kms_key_names():
    """Return key names from ``GET /keys/names``."""
    response = execute_kms_rest_test(
        "GET",
        "/keys/names",
        expected_status=200,
        step="KMS.fetch_kms_key_names",
    )
    return response.json()


def delete_kms_key_by_name(key_name):
    execute_kms_rest_test(
        "DELETE",
        f"/key/{key_name}",
        expected_status=200,
        step="KMS.delete_kms_key_by_name",
    )


def teardown_kms_keys_created_since(list_of_keys_before_test):
    """
    Delete keys not present in the pre-module snapshot.
    """
    if list_of_keys_before_test is None:
        list_of_keys_before_test = []

    ensure_keyadmin_can_list_kms_keys()
    list_of_keys = fetch_kms_key_names()
    for key_name in list_of_keys:
        if key_name not in list_of_keys_before_test:
            delete_kms_key_by_name(key_name)
            assert key_name not in fetch_kms_key_names(), (
                "KMS Encryption Zone Key deletion not successful"
            )


class KmsRestTest:
    """Declarative KMS REST step with expected status and optional JSON checks."""

    def __init__(
        self,
        test_id,
        test_case,
        method,
        url_path,
        *,
        json=None,
        params=None,
        expected_status=None,
    ):
        self.test_id = test_id
        self.test_case = test_case
        self.method = method
        self.url_path = url_path
        self.json_body = json
        self.params = params
        self.expected_status = expected_status

    def execute(self, headers=None):
        log_testcase_begin(logger, self.test_id)
        headers = headers or self.test_case.headers
        return execute_kms_rest_test(
            self.method,
            self.url_path,
            json_body=self.json_body,
            params=self.params,
            headers=headers,
            expected_status=self.expected_status,
            step=self.test_id,
        )


# --- KmsXaTestCase ---------------------------------------------------------------------------


class KMSEncryptionZoneKey:
    """In-memory KMS encryption zone key metadata from REST responses."""

    def __init__(
        self,
        name,
        description="",
        cipher="AES/CTR/NoPadding",
        length=128,
        versions=1,
        attributes=None,
    ):
        self.name = name
        self.description = description
        self.cipher = cipher
        self.length = length
        self.versions = versions
        self.attributes = attributes if attributes else {}


class KmsXaTestCase:
    """Shared helpers for KMS v1 REST and Ranger policy enforcement tests."""

    hadoopUser_1 = PLUGIN_TEST_USER
    hadoopAdminUser = "keyadmin"
    hdfs_service_user = "hdfs"

    keyMaterial_2 = "Vb502Ia__rW3c9G8dC_RmQVb502Ia__rW3c9G8dC_Rm"

    policy = None
    policy_name = None
    createdKeys = None

    def __init__(self):
        self.createdKeys = []

    def translate_username(self, user):
        return user

    def _run_kms(
        self,
        test_name,
        username,
        method,
        url_path,
        *,
        json_body=None,
        expected_status=200,
        test_desc=None,
        extra_params=None,
        check_audit_log=False,
        new_audit_log_must_exist=False,
        audit_log_filter=None,
    ):
        step = test_desc or test_name
        log_testcase_begin(logger, step)
        if check_audit_log and new_audit_log_must_exist:
            logger.info(
                "Audit check requested for %s (skipped unless AUDIT_INDEX_STORE is configured)",
                step,
            )

        with kerberos_context_for_user(username):
            response = execute_kms_rest_test(
                method,
                url_path,
                json_body=json_body,
                params={"user.name": username, **(extra_params or {})},
                expected_status=expected_status,
                step=step,
            )
        if response.status_code == expected_status and response.text:
            try:
                return response.json()
            except json.JSONDecodeError:
                return None
        return None

    def createEncryptionZoneKey(
        self,
        test_name,
        user,
        keyname,
        expected_status=201,
        extra_expected_string=None,
        test_desc=None,
        checkAuditLog=False,
        newAuditLogMustExist=False,
        auditLogFilter=None,
        isInRollingUpgrade=False,
        base_url=None,
    ):
        del extra_expected_string, isInRollingUpgrade, base_url
        hadoop_user = self.translate_username(user)
        body = self._run_kms(
            test_name,
            hadoop_user,
            "POST",
            "/keys",
            json_body={"name": keyname},
            expected_status=expected_status,
            test_desc=test_desc,
            check_audit_log=checkAuditLog,
            new_audit_log_must_exist=newAuditLogMustExist,
            audit_log_filter=auditLogFilter,
        )
        if expected_status == 201:
            logger.info("adding key:%s to list", keyname)
            self.createdKeys.append(keyname)
        return body

    def createKmsKey(
        self,
        test_name,
        user,
        key_data,
        expected_status=201,
        test_desc=None,
    ):
        """POST ``/keys`` with full payload (RANGER-5175 / ``test_keys.py``)."""
        hadoop_user = self.translate_username(user)
        body = self._run_kms(
            test_name,
            hadoop_user,
            "POST",
            "/keys",
            json_body=key_data,
            expected_status=expected_status,
            test_desc=test_desc or test_name,
        )
        if expected_status == 201:
            key_name = key_data.get("name")
            if key_name:
                logger.info("adding key:%s to list", key_name)
                self.createdKeys.append(key_name)
        return body

    def deleteEncryptionZoneKey(
        self,
        test_name,
        user,
        keyname,
        expected_status=200,
        extra_expected_string="",
        test_desc=None,
        checkAuditLog=False,
        newAuditLogMustExist=False,
        auditLogFilter=None,
        isInRollingUpgrade=False,
        base_url=None,
    ):
        del extra_expected_string, isInRollingUpgrade, base_url
        hadoop_user = self.translate_username(user)
        self._run_kms(
            test_name,
            hadoop_user,
            "DELETE",
            f"/key/{keyname}",
            expected_status=expected_status,
            test_desc=test_desc,
            check_audit_log=checkAuditLog,
            new_audit_log_must_exist=newAuditLogMustExist,
            audit_log_filter=auditLogFilter,
        )
        if keyname in self.createdKeys:
            self.createdKeys.remove(keyname)

    def getEncryptionZoneKeyCurrentVersion(
        self,
        test_name,
        user,
        keyname,
        expected_status=200,
        extra_expected_string="",
        test_desc=None,
        checkAuditLog=False,
        newAuditLogMustExist=False,
        auditLogFilter=None,
        isInRollingUpgrade=False,
        base_url=None,
    ):
        del extra_expected_string, isInRollingUpgrade, base_url
        hadoop_user = self.translate_username(user)
        body = self._run_kms(
            test_name,
            hadoop_user,
            "GET",
            f"/key/{keyname}/_currentversion",
            expected_status=expected_status,
            test_desc=test_desc,
            check_audit_log=checkAuditLog,
            new_audit_log_must_exist=newAuditLogMustExist,
            audit_log_filter=auditLogFilter,
        )
        if body and body.get("versionName") is not None:
            return body["versionName"]
        return None

    def setEncryptionZoneKeyMaterial(
        self,
        test_name,
        user,
        keyname,
        keymaterial,
        expected_status=200,
        extra_expected_string="",
        test_desc=None,
        checkAuditLog=False,
        newAuditLogMustExist=False,
        auditLogFilter=None,
        isInRollingUpgrade=False,
        base_url=None,
    ):
        del extra_expected_string, isInRollingUpgrade, base_url
        hadoop_user = self.translate_username(user)
        return self._run_kms(
            test_name,
            hadoop_user,
            "POST",
            f"/key/{keyname}",
            json_body={"material": keymaterial},
            expected_status=expected_status,
            test_desc=test_desc,
            check_audit_log=checkAuditLog,
            new_audit_log_must_exist=newAuditLogMustExist,
            audit_log_filter=auditLogFilter,
        )

    def rolloverEncryptionZoneKey(
        self,
        test_name,
        user,
        keyname,
        expected_status=200,
        extra_expected_string="",
        test_desc=None,
        checkAuditLog=False,
        newAuditLogMustExist=False,
        auditLogFilter=None,
        isInRollingUpgrade=False,
        base_url=None,
    ):
        del extra_expected_string, isInRollingUpgrade, base_url
        hadoop_user = self.translate_username(user)
        return self._run_kms(
            test_name,
            hadoop_user,
            "POST",
            f"/key/{keyname}",
            json_body={"name": keyname},
            expected_status=expected_status,
            test_desc=test_desc,
            check_audit_log=checkAuditLog,
            new_audit_log_must_exist=newAuditLogMustExist,
            audit_log_filter=auditLogFilter,
        )

    def getEncryptionZoneKeyMetadata(
        self,
        test_name,
        user,
        keyname,
        expected_status=200,
        extra_expected_string="",
        test_desc=None,
        checkAuditLog=False,
        newAuditLogMustExist=False,
        auditLogFilter=None,
        isInRollingUpgrade=False,
        base_url=None,
    ):
        del extra_expected_string, isInRollingUpgrade, base_url
        hadoop_user = self.translate_username(user)
        return self._run_kms(
            test_name,
            hadoop_user,
            "GET",
            f"/key/{keyname}",
            expected_status=expected_status,
            test_desc=test_desc,
            check_audit_log=checkAuditLog,
            new_audit_log_must_exist=newAuditLogMustExist,
            audit_log_filter=auditLogFilter,
        )

    def getKmsEncryptionZoneKey(
        self,
        test_name,
        user,
        keyname,
        expected_status=200,
        test_desc=None,
    ):
        """GET ``/key/{name}`` and return a :class:`KMSEncryptionZoneKey`."""
        body = self.getEncryptionZoneKeyMetadata(
            test_name,
            user,
            keyname,
            expected_status=expected_status,
            test_desc=test_desc,
        )
        if body is None:
            return None
        return KMSEncryptionZoneKey(
            name=keyname,
            description=body.get("description", ""),
            cipher=body.get("cipher", "AES/CTR/NoPadding"),
            length=body.get("length", 128),
            versions=body.get("versions", 1),
            attributes=body.get("attributes") or {},
        )

    def generateDekWithEdek(
        self,
        test_name,
        user,
        keyname,
        expected_status=200,
        test_desc=None,
    ):
        """GET ``/key/{name}/_dek``."""
        hadoop_user = self.translate_username(user)
        return self._run_kms(
            test_name,
            hadoop_user,
            "GET",
            f"/key/{keyname}/_dek",
            expected_status=expected_status,
            test_desc=test_desc or test_name,
        )

    def decryptEncryptedKeyExport(
        self,
        test_name,
        user,
        keyname,
        payload,
        *,
        key_version_suffix="@0",
        expected_status=200,
        test_desc=None,
    ):
        """POST ``/keyversion/{name}@N/_eek?eek_op=decrypt``."""
        hadoop_user = self.translate_username(user)
        return self._run_kms(
            test_name,
            hadoop_user,
            "POST",
            f"/keyversion/{keyname}{key_version_suffix}/_eek",
            json_body=payload,
            expected_status=expected_status,
            test_desc=test_desc or test_name,
            extra_params={"eek_op": "decrypt"},
        )

    def generateEncryptedDEKs(
        self,
        test_name,
        user,
        keyname,
        num_keys=1,
        expected_status=200,
        extra_expected_string="",
        test_desc=None,
        checkAuditLog=False,
        newAuditLogMustExist=False,
        auditLogFilter=None,
        isInRollingUpgrade=False,
        base_url=None,
    ):
        del extra_expected_string, isInRollingUpgrade, base_url
        hadoop_user = self.translate_username(user)
        return self._run_kms(
            test_name,
            hadoop_user,
            "GET",
            f"/key/{keyname}/_eek",
            expected_status=expected_status,
            test_desc=test_desc,
            extra_params={"eek_op": "generate", "num_keys": str(num_keys)},
            check_audit_log=checkAuditLog,
            new_audit_log_must_exist=newAuditLogMustExist,
            audit_log_filter=auditLogFilter,
        )

    def deleteKeysForThisTest(self):
        with kerberos_as_keyadmin():
            for keyname in list(self.createdKeys):
                delete_kms_key_by_name(keyname)
        self.createdKeys.clear()

    def setUpModule(self):
        logger.info("KmsXaTestCase.setUpModule (Docker: no-op)")

    def tearDownModule(self):
        logger.info("KmsXaTestCase.tearDownModule (Docker: no-op)")
