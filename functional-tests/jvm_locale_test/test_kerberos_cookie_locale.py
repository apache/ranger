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

import re
from email.utils import parsedate_to_datetime

from jvm_locale_test.jvm_locale_utils import JvmLocaleTestUtils
from ranger_test_utils.hive_utils import get_docker_client


pytest_plugins = ("xuserrest.conftest",)


def call_ranger_admin_with_spnego(principal, keytab):
    ranger = get_docker_client().containers.get(
        JvmLocaleTestUtils.RANGER_CONTAINER_NAME
    )
    print(f"Calling Ranger Admin lookup users API with SPNEGO principal: {principal}")
    command = (
        "kdestroy -A 2>/dev/null || true; "
        f"kinit -kt {keytab} {principal}; "
        "curl -s -i --negotiate -u : "
        "-H 'Accept: application/json' "
        "-w '\\n---STATUS---%{http_code}\\n' "
        f"{JvmLocaleTestUtils.RANGER_SPNEGO_URL}"
    )

    exit_code, output = ranger.exec_run(["bash", "-c", command], user="root")
    text = JvmLocaleTestUtils.decode(output)
    assert exit_code == 0, text

    return text


def assert_hadoop_auth_cookie_has_us_http_date(response_text):
    status = int(response_text.rsplit("---STATUS---", 1)[1].strip())
    print(f"Ranger Admin SPNEGO API HTTP status: {status}")
    assert status == 200, response_text

    headers = response_text.split("\r\n\r\n", 1)[0]
    headers = headers.split("\n\n", 1)[0]
    cookie = None

    for line in headers.splitlines():
        if line.lower().startswith("set-cookie:") and "hadoop.auth" in line:
            cookie = line
            break

    assert cookie is not None, response_text
    assert "u=" in cookie

    match = re.search(r"Expires=([^;]+)", cookie)
    assert match is not None, cookie

    expires = match.group(1)
    print(f"hadoop.auth Expires={expires}")
    parsedate_to_datetime(expires)
    assert re.match(
        r"^[A-Z][a-z]{2}, \d{2}-[A-Z][a-z]{2}-\d{4} \d{2}:\d{2}:\d{2} GMT$",
        expires,
    ), cookie
    print("Verified hadoop.auth Expires uses HTTP-date format")


def test_spnego_auth_cookie_expires_uses_http_date_with_configured_jvm_locale(
    temp_secure_user,
):
    print("Verifying Ranger Admin JVM locale before SPNEGO cookie test")
    JvmLocaleTestUtils.assert_ranger_admin_has_requested_locale()

    principal = None
    keytab = None

    try:
        test_user, _ = temp_secure_user(["user"])
        print(f"Created Ranger test user for SPNEGO: {test_user['name']}")
        principal, keytab = JvmLocaleTestUtils.ensure_spnego_principal_keytab(
            test_user["name"]
        )
        response_text = call_ranger_admin_with_spnego(principal, keytab)

        assert_hadoop_auth_cookie_has_us_http_date(response_text)
    finally:
        JvmLocaleTestUtils.cleanup_spnego_principal_keytab(principal, keytab)
