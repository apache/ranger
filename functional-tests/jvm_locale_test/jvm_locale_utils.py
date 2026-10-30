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

import os
import re

from kms.utils import _ensure_kdc_principal_keytab
from ranger_test_utils.hive_utils import KDC_CONTAINER, get_docker_client


class JvmLocaleTestUtils:
    RANGER_CONTAINER_NAME = "ranger"
    RANGER_REALM = "EXAMPLE.COM"
    RANGER_SPNEGO_URL = "http://ranger.rangernw:6080/service/xusers/lookup/users"

    @staticmethod
    def decode(output):
        return output.decode("utf-8", errors="replace") if output else ""

    @classmethod
    def assert_ranger_admin_has_requested_locale(cls):
        ranger = get_docker_client().containers.get(cls.RANGER_CONTAINER_NAME)
        command = (
            "for cmdline in /proc/[0-9]*/cmdline; do "
            "tr '\\0' ' ' < \"$cmdline\"; echo; "
            "done | grep '[o]rg.apache.ranger.server.tomcat.EmbeddedServer'"
        )
        exit_code, output = ranger.exec_run(
            ["bash", "-c", command],
            user="root",
        )
        text = cls.decode(output)

        assert exit_code == 0, text
        language = re.search(r"-Duser\.language=([^\s]+)", text)
        country = re.search(r"-Duser\.country=([^\s]+)", text)

        assert language is not None, text
        assert country is not None, text
        print(
            "Ranger Admin JVM locale: "
            f"user.language={language.group(1)}, user.country={country.group(1)}"
        )

        expected_language = os.getenv("JVM_LOCALE_EXPECTED_LANGUAGE")
        expected_country = os.getenv("JVM_LOCALE_EXPECTED_COUNTRY")
        if expected_language and expected_country:
            assert (language.group(1), country.group(1)) == (
                expected_language,
                expected_country,
            ), text
        else:
            assert (language.group(1), country.group(1)) != ("en", "US"), text

    @classmethod
    def ensure_spnego_principal_keytab(cls, username):
        principal = f"{username}@{cls.RANGER_REALM}"
        keytab_name = f"{username}.keytab"
        kdc_keytab = f"/tmp/{keytab_name}"
        ranger_keytab = f"/etc/keytabs/{keytab_name}"
        print(f"Creating SPNEGO principal/keytab: {principal} -> {ranger_keytab}")

        _ensure_kdc_principal_keytab(
            principal,
            cls.RANGER_CONTAINER_NAME,
            ranger_keytab,
            kdc_keytab,
        )

        return principal, ranger_keytab

    @classmethod
    def cleanup_spnego_principal_keytab(cls, principal, keytab):
        client = get_docker_client()

        if keytab is not None:
            print(f"Cleaning SPNEGO keytab from Ranger container: {keytab}")
            client.containers.get(cls.RANGER_CONTAINER_NAME).exec_run(
                f"rm -f {keytab}",
                user="root",
            )

        if principal is not None:
            print(f"Deleting SPNEGO principal from KDC: {principal}")
            client.containers.get(KDC_CONTAINER).exec_run(
                f'kadmin.local -q "delprinc -force {principal}"',
                user="root",
            )

