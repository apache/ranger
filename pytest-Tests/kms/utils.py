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


# This workflow will build a Java project with Maven, and cache/restore any dependencies to improve the workflow execution time
# For more information see: https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-java-with-maven

# This workflow uses actions that are not certified by GitHub.
# They are provided by a third-party and are governed by
# separate terms of service, privacy policy, and support
# documentation.

import subprocess
import json
import io
import tarfile
import docker
import tempfile, os
import xml.etree.ElementTree as ET
import requests


TESTUSER_PARAMS = {}
BASE_URL_RANGER = "http://localhost:6080/service/public/v2/api/policy"
BASE_URL_RANGER_USERS = "http://localhost:6080/service/xusers/secure/users"
BASE_URL_RANGER_USERS_BY_NAME = "http://localhost:6080/service/xusers/users/userName"

RANGER_ADMIN_AUTH = ("admin", "rangerR0cks!")
RANGER_KMS_AUTH = ('keyadmin', 'rangerR0cks!')  # Ranger key admin user
KMS_SERVICE_NAME = "dev_kms"
TEST_USER = "testuser"

KMS_CONTAINER_NAME = "ranger-kms"
KMS_LOG_FILE = "/var/log/ranger/kms/ranger-kms-ranger-kms.rangernw-root.log"
BASE_URL = "http://ranger-kms.rangernw:9292/kms/v1"
PARAMS = {"user.name": "keyadmin"}
client = docker.from_env()
container = client.containers.get(KMS_CONTAINER_NAME)
KEYADMIN_PRINCIPAL = "keyadmin@EXAMPLE.COM"
KEYADMIN_KEYTAB = "/etc/keytabs/keyadmin.keytab"
KDC_CONTAINER = "ranger-kdc"

TESTUSER = "testuser"
TESTUSER_PRINCIPAL = f"{TESTUSER}@EXAMPLE.COM"
TESTUSER_KEYTAB = f"/etc/keytabs/{TESTUSER}.keytab"

def fetch_logs():
    try:
        cmd = f"docker exec {KMS_CONTAINER_NAME} tail -n 100 {KMS_LOG_FILE}"
        logs = subprocess.check_output(cmd, shell=True, text=True)
        error_logs = [line for line in logs.split("\n") if "ERROR" in line or "Exception" in line]
        return "\n".join(error_logs) if error_logs else "No recent errors in logs."
    except subprocess.CalledProcessError as e:
        return f"Failed to fetch logs from container. Command failed with exit code {e.returncode}: {e.output}"

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
        cmd = [
            "curl", "-s", "-o", "/tmp/curl_body.txt", "-w", "%{http_code}",
            "--negotiate", "-u", ":",
            "-X", method.upper(),
            "-H", "Content-Type: application/json",
        ]
        if json_body is not None:
            cmd += ["-d", json.dumps(json_body)]
        cmd.append(full_url)

        exit_code, output = container.exec_run(cmd, user="root")
        status_code = int(output.decode().strip()) if output else 0

        _, body_out = container.exec_run("cat /tmp/curl_body.txt", user="root")
        body = body_out.decode() if body_out else ""

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
        return __import__("json").loads(self._body)

    def __repr__(self):
        return f"<Response [{self.status_code}]>"


krb_requests = KerberosRequests()

def _ensure_kdc_principal_keytab(principal: str, dest_container_name: str, dest_keytab_path: str, kdc_tmp_path: str, ) -> None:
    # create principal in KDC, export keytab, copy to target container
    kdc = client.containers.get(KDC_CONTAINER)
    dest = client.containers.get(dest_container_name)

    exit_code, output = kdc.exec_run(f'kadmin.local -q "getprinc {principal}"', user="root")
    if exit_code != 0 or b"does not exist" in output:
        kdc.exec_run(f'kadmin.local -q "addprinc -randkey {principal}"', user="root")

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

def ensure_ticket():
    # Always kinit as keyadmin — do not reuse a stale rangerkms ticket from cache.
    exit_code, _ = container.exec_run(f"test -f {KEYADMIN_KEYTAB}", user="root")
    if exit_code != 0:
        ensure_keyadmin_keytab()

    container.exec_run("kdestroy -A 2>/dev/null || true", user="root")
    exit_code, output = container.exec_run(
        f"kinit -kt {KEYADMIN_KEYTAB} {KEYADMIN_PRINCIPAL}", user="root"
    )
    if exit_code != 0:
        raise RuntimeError(f"kinit failed: {output.decode()}")

# Blacklist helpers

def modify_blacklist_property(operation, users, action="add"):
    dbks_site_path = (
        "/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp"
        "/WEB-INF/classes/conf/dbks-site.xml"
    )

    ensure_ticket()

    result = container.exec_run(f"cat {dbks_site_path}", user="root")
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

    container.put_archive(path="/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp/WEB-INF/classes/conf/", data=tarstream)
    print(f"Successfully {'added' if action == 'add' else 'removed'} {users} in {prop_name}")


def blacklist_op_users(operation, users=[]):
    modify_blacklist_property(operation, users, action="add")


def unblacklist_op_users(operation, users=[]):
    modify_blacklist_property(operation, users, action="remove")

def ensure_testuser_keytab():
    _ensure_kdc_principal_keytab(
        TESTUSER_PRINCIPAL,
        KMS_CONTAINER_NAME,
        TESTUSER_KEYTAB,
        f"/tmp/{TESTUSER}.keytab",
    )

def ensure_testuser_ticket():
    ensure_testuser_keytab()
    container.exec_run("kdestroy -A 2>/dev/null || true", user="root")
    exit_code, output = container.exec_run(
        f"kinit -kt {TESTUSER_KEYTAB} {TESTUSER_PRINCIPAL}", user="root"
    )
    if exit_code != 0:
        raise RuntimeError(f"testuser kinit failed: {output.decode()}")

def ensure_keyadmin_ticket():
    ensure_keyadmin_keytab()
    container.exec_run("kdestroy -A 2>/dev/null || true", user="root")
    exit_code, output = container.exec_run(
        f"kinit -kt {KEYADMIN_KEYTAB} {KEYADMIN_PRINCIPAL}", user="root"
    )
    if exit_code != 0:
        raise RuntimeError(f"keyadmin kinit failed: {output.decode()}")

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