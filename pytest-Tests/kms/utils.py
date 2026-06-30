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

KMS_CONTAINER_NAME = "ranger-kms"
KMS_LOG_FILE = "/var/log/ranger/kms/ranger-kms-ranger-kms.rangernw-root.log"

def fetch_logs():
    try:
        cmd = f"docker exec {KMS_CONTAINER_NAME} tail -n 100 {KMS_LOG_FILE}"
        logs = subprocess.check_output(cmd, shell=True, text=True)
        error_logs = [line for line in logs.split("\n") if "ERROR" in line or "Exception" in line]
        return "\n".join(error_logs) if error_logs else "No recent errors in logs."
    except subprocess.CalledProcessError as e:
        return f"Failed to fetch logs from container. Command failed with exit code {e.returncode}: {e.output}"


BASE_URL = "http://ranger-kms.rangernw:9292/kms/v1"
PARAMS = {"user.name": "keyadmin"}

client = docker.from_env()
container = client.containers.get(KMS_CONTAINER_NAME)


class KerberosRequests:
    """
    Runs curl --negotiate inside the KMS container.
    Mirrors: curl --negotiate -u : "http://ranger-kms.rangernw:9292/kms/v1/keys/names"
    Works on any host regardless of local GSSAPI/Kerberos setup.
    """

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



# def ensure_keyadmin_keytab():
#     """Creates keytab inside ranger-kdc and transfers to KMS — skips if already present."""
#     exit_code, _ = container.exec_run("test -f /etc/keytabs/keyadmin.keytab", user="root")
#     if exit_code == 0:
#         print("Keytab already present in KMS container — skipping generation.")
#         return

#     print("Generating keytab inside ranger-kdc …")
#     kdc = client.containers.get("ranger-kdc")

#     kdc.exec_run('kadmin.local -q "addprinc -randkey keyadmin@EXAMPLE.COM"', user="root")

#     exit_code, output = kdc.exec_run(
#         'kadmin.local -q "xst -k /tmp/keyadmin.keytab keyadmin@EXAMPLE.COM"', user="root"
#     )
#     if exit_code != 0:
#         raise RuntimeError(f"xst failed: {output.decode()}")

#     subprocess.check_call("docker cp ranger-kdc:/tmp/keyadmin.keytab ./keyadmin.keytab", shell=True)
#     subprocess.check_call(f"docker exec {KMS_CONTAINER_NAME} mkdir -p /etc/keytabs", shell=True)
#     subprocess.check_call(f"docker cp ./keyadmin.keytab {KMS_CONTAINER_NAME}:/etc/keytabs/keyadmin.keytab", shell=True)
#     container.exec_run("chown root:root /etc/keytabs/keyadmin.keytab", user="root")
#     container.exec_run("chmod 400 /etc/keytabs/keyadmin.keytab", user="root")
#     print("Keytab generated and transferred.")

def ensure_keyadmin_keytab():
    """Creates keytab inside ranger-kdc and transfers to KMS — skips only if
    both the keytab file is present AND the principal still exists in the KDC."""
    kdc = client.containers.get("ranger-kdc")

    # Check principal actually exists in the KDC database
    exit_code, output = kdc.exec_run(
        'kadmin.local -q "getprinc keyadmin@EXAMPLE.COM"', user="root"
    )
    principal_exists = exit_code == 0 and b"does not exist" not in output

    exit_code, _ = container.exec_run("test -f /etc/keytabs/keyadmin.keytab", user="root")
    keytab_present = exit_code == 0

    if keytab_present and principal_exists:
        print("Keytab present and principal valid in KDC — skipping generation.")
        return

    if keytab_present and not principal_exists:
        print("Stale keytab found, but principal missing from KDC — regenerating.")

    print("Generating keytab inside ranger-kdc …")
    kdc.exec_run('kadmin.local -q "addprinc -randkey keyadmin@EXAMPLE.COM"', user="root")

    exit_code, output = kdc.exec_run(
        'kadmin.local -q "xst -k /tmp/keyadmin.keytab keyadmin@EXAMPLE.COM"', user="root"
    )
    if exit_code != 0:
        raise RuntimeError(f"xst failed: {output.decode()}")

    subprocess.check_call("docker cp ranger-kdc:/tmp/keyadmin.keytab ./keyadmin.keytab", shell=True)
    subprocess.check_call(f"docker exec {KMS_CONTAINER_NAME} mkdir -p /etc/keytabs", shell=True)
    subprocess.check_call(f"docker cp ./keyadmin.keytab {KMS_CONTAINER_NAME}:/etc/keytabs/keyadmin.keytab", shell=True)
    container.exec_run("chown root:root /etc/keytabs/keyadmin.keytab", user="root")
    container.exec_run("chmod 400 /etc/keytabs/keyadmin.keytab", user="root")
    print("Keytab generated and transferred.")

def ensure_ticket():
    """Ensures a valid Kerberos ticket exists inside the KMS container."""
    exit_code, _ = container.exec_run("klist -s", user="root")
    if exit_code == 0:
        return

    print("Ticket missing — running kinit inside KMS container …")
    exit_code, output = container.exec_run(
        "kinit -kt /etc/keytabs/keyadmin.keytab keyadmin@EXAMPLE.COM", user="root"
    )
    if exit_code != 0:
        raise RuntimeError(f"kinit failed: {output.decode()}")
    print("kinit succeeded.")


# **************** Blacklist helpers -----------------------------------------------

def modify_blacklist_property(operation, users, action="add"):
    dbks_site_path = (
        "/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp"
        "/WEB-INF/classes/conf/dbks-site.xml"
    )

    ensure_ticket()

    import xml.etree.ElementTree as ET

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

    container.put_archive(
        path="/opt/ranger/ranger-3.0.0-SNAPSHOT-kms/ews/webapp/WEB-INF/classes/conf/",
        data=tarstream
    )
    print(f"Successfully {'added' if action == 'add' else 'removed'} {users} in {prop_name}")


def blacklist_op_users(operation, users=[]):
    modify_blacklist_property(operation, users, action="add")


def unblacklist_op_users(operation, users=[]):
    modify_blacklist_property(operation, users, action="remove")