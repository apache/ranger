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
Shared OS-level helpers for the ranger docker container.

Ranger admin runs with ``ranger.authentication.method=PAM``, so every
Basic-auth request is validated by pam_unix against ``/etc/shadow``. When an
account does not exist at the OS level PAM fails first and pays a multi-second
penalty before Ranger falls back to DB auth. To keep the REST suites fast we
mirror every test user into the container's OS.

This module is intentionally suite-neutral: it lives in ``ranger_test_utils``
so that ``servicerest``, ``tagrest`` and any future suite share ONE
implementation and ONE session-scoped setup/teardown (see the root
``conftest.py``).
"""

import logging
import subprocess
import time

import requests

from ranger_test_utils.utils import RANGER_ADMIN_BASE_URL

RANGER_CONTAINER = "ranger"
BUILT_IN_OS_USERS = [
    ("admin", "rangerR0cks!"),
    ("keyadmin", "rangerR0cks!"),
]

DOCKER_TIMEOUT = 30

osl = logging.getLogger("OsEnvSetup")


def _run(args, timeout=DOCKER_TIMEOUT):
    """Run a docker command, never raising and never blocking indefinitely."""
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        osl.error(f"[OS ENV] timed out after {timeout}s: {' '.join(args)}")
        return subprocess.CompletedProcess(args, returncode=124, stdout="", stderr="timeout")


def _docker_root(cmd):
    """Run a shell command as root inside the ranger container."""
    return _run(["docker", "exec", "-u", "0", RANGER_CONTAINER, "sh", "-c", cmd])


def create_os_user(user_name, password):
    """
    Automates adding the user to the ranger container's OS:
        docker exec -u root -it ranger bash
        root@ranger:/# useradd <user_name>
        root@ranger:/# passwd <user_name>

    'passwd' is interactive, so 'chpasswd' is used to set the same password
    non-interactively. If the account already exists only the password is set,
    which keeps the call idempotent across runs.
    """
    cmd = (f"id -u {user_name} >/dev/null 2>&1 || useradd -M {user_name}; "
           f"echo '{user_name}:{password}' | chpasswd")
    result = _run(["docker", "exec", "-u", "root", RANGER_CONTAINER, "bash", "-c", cmd])

    # check in the container whether the user actually got added
    check = _run(["docker", "exec", "-u", "root", RANGER_CONTAINER, "getent", "passwd", user_name])

    if check.returncode == 0:
        osl.info(f"[OS USER] ADDED -> {check.stdout.strip()}")
    else:
        osl.error(f"[OS USER] NOT ADDED -> {user_name} : {result.stderr.strip()}")

    return result


def delete_os_user(user_name):
    """
    Removes the user from the ranger container's OS:
        docker exec -u root -it ranger bash
        root@ranger:/# userdel -r <user_name>

    After the delete it verifies with 'getent passwd' that the account is
    really gone, so the log shows whether the cleanup actually worked.
    """
    result = _run(["docker", "exec", "-u", "root", RANGER_CONTAINER,
                   "userdel", "-r", "-f", user_name])

    # verify in the container that the user no longer exists
    check = _run(["docker", "exec", "-u", "root", RANGER_CONTAINER, "getent", "passwd", user_name])

    if check.returncode != 0:
        osl.info(f"[OS USER] REMOVED -> {user_name}")
    else:
        osl.error(f"[OS USER] STILL PRESENT -> {check.stdout.strip()} : {result.stderr.strip()}")

    return check.returncode != 0


def wait_for_ranger(timeout=180):
    """
     After adding ranger machine user as supplementary user to the shadow group we need to restart the ranger container
     but if ranger is already present in shadow group from previous runs then no need to restart the container
    """
    deadline = time.time() + timeout
    started = time.time()

    while time.time() < deadline:
        try:
            resp = requests.get(RANGER_ADMIN_BASE_URL + "/plugins/services",
                                verify=False, timeout=5)
            if resp.status_code in (200, 401):
                osl.info(f"[OS ENV] ranger is back up after {int(time.time() - started)}s")
                return True
        except requests.exceptions.RequestException:
            pass

        time.sleep(3)

    osl.error(f"[OS ENV] ranger did not come back within {timeout}s")
    return False


def setup_os_environment():
    """
    Runs ONCE, before every other fixture in the session.

    Ranger admin uses ranger.authentication.method=PAM, so every Basic-auth
    request is validated by pam_unix against /etc/shadow. The ranger JVM runs
    as the non-root 'ranger' user and can only read /etc/shadow through the
    setgid helper unix_chkpwd, which needs 'ranger' to be in the shadow group.
    """
    # 1. shadow group must exist and own /etc/shadow
    _docker_root("getent group shadow || groupadd -g 42 shadow")
    _docker_root("chgrp shadow /etc/shadow && chmod 0640 /etc/shadow")
    osl.info("[OS ENV] shadow group ready, /etc/shadow set to root:shadow 0640")

    # 2. ranger process user must be a supplementary member of the shadow group
    check = _docker_root('id -nG ranger | tr " " "\\n" | grep -qx shadow && echo YES || echo NO')

    if check.stdout.strip() == "YES":
        osl.info("[OS ENV] ranger is already in the shadow group, no restart needed")
    else:
        _docker_root("usermod -aG shadow ranger")
        osl.info("[OS ENV] added ranger to the shadow group, restarting container...")

        # supplementary groups are only picked up when the process starts,
        # so the container has to be restarted for this to take effect
        _run(["docker", "restart", RANGER_CONTAINER], timeout=120)
        wait_for_ranger()

    # 3. create the built-in users as OS users if they are not present already
    for user_name, password in BUILT_IN_OS_USERS:
        create_os_user(user_name, password)


def teardown_os_environment():
    """
    Teardown: runs ONCE, after all the testing is done.

    Drops the OS accounts we created and revokes the ranger process user's
    membership of the shadow group, so the container is left exactly as it
    was before the suite started. The shadow group itself is kept, since it
    is part of the base image layout and nothing else depends on removing it.
    """
    osl.info("[OS ENV] removing built-in OS users created for this run...")

    for user_name, _password in BUILT_IN_OS_USERS:
        delete_os_user(user_name)

    # revoke the supplementary shadow group membership granted during setup
    check = _docker_root('id -nG ranger | tr " " "\\n" | grep -qx shadow && echo YES || echo NO')

    if check.stdout.strip() == "YES":
        result = _docker_root("gpasswd -d ranger shadow")

        verify = _docker_root('id -nG ranger | tr " " "\\n" | grep -qx shadow && echo YES || echo NO')

        if verify.stdout.strip() == "NO":
            osl.info("[OS ENV] removed ranger from the shadow group")
        else:
            osl.error(f"[OS ENV] ranger is STILL in the shadow group : {result.stderr.strip()}")
    else:
        osl.info("[OS ENV] ranger is not in the shadow group, nothing to revoke")






