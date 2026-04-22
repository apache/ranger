#!/usr/bin/env python3

#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#
import os
import sys

from apache_ranger.client.ranger_client import RangerClient
from apache_ranger.client.ranger_user_mgmt_client import RangerUserMgmtClient
from apache_ranger.exceptions import RangerServiceException

from log_config import configure_logging, get_logger

logger = get_logger(__name__)

DEFAULT_BASE_URL = os.environ.get("RANGER_ADMIN_BASE_URL", "http://127.0.0.1:6080").rstrip("/")


class UserPasswordBootstrap:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _get_user_mgmt_client(self, username: str, password: str) -> RangerUserMgmtClient:
        return RangerUserMgmtClient(RangerClient(self.base_url, (username, password)))

    def auth_probe_status(self, username: str, password: str) -> int:
        try:
            user_mgmt = self._get_user_mgmt_client(username, password)
            result = user_mgmt.client_http.call_api(RangerUserMgmtClient.FIND_USERS)
            return 200 if result is not None else 0
        except RangerServiceException as exc:
            if exc.statusCode == 403:
                return 403
            if exc.statusCode == 401:
                return 401
            logger.debug("apache-ranger auth probe failed for %s: %s", username, exc, exc_info=True)
            return exc.statusCode
        except Exception as exc:
            logger.debug("apache-ranger auth probe failed for %s: %s", username, exc, exc_info=True)
            return 0

    def can_authenticate(self, username: str, password: str) -> bool:
        return self.auth_probe_status(username, password) in (200, 403)

    def _update_user_password_with_client(self, admin_password: str, username: str, desired: str) -> None:
        user_mgmt = self._get_user_mgmt_client("admin", admin_password)
        user = user_mgmt.get_user(username)
        if user is None or user.id is None:
            raise ValueError(f"Unable to find Ranger user {username}")

        user.password = desired
        user_mgmt.update_user_by_id(user.id, user)

    def update_user_password(self, admin_password: str, username: str, desired: str) -> int:
        try:
            self._update_user_password_with_client(admin_password, username, desired)
            return 0
        except RangerServiceException as exc:
            logger.error("apache-ranger client update failed for %s: status=%s, message=%s", username, exc.statusCode, exc.msgDesc or exc)
            return 1
        except Exception as exc:
            logger.error("apache-ranger client update failed for %s: %s", username, exc)
            return 1

    def set_admin_password_if_needed(self, desired: str) -> int:
        if not desired:
            logger.warning("Ranger admin password not configured; skipping admin password update")
            return 0

        if self.can_authenticate("admin", desired):
            return 0

        logger.warning("Unable to authenticate to Ranger as admin with RANGER_ADMIN_PASSWORD.")
        logger.warning(" admin:<env:RANGER_ADMIN_PASSWORD> -> %s", self.auth_probe_status("admin", desired))
        logger.warning("For fresh installs, dba.py seeds the initial admin password from RANGER_ADMIN_PASSWORD during schema import. "
            "If the database already exists, changing only RANGER_ADMIN_PASSWORD will not rotate the stored admin password."
        )
        return 1

    def update_user_password_if_needed(self, admin_password: str, username: str, desired: str) -> int:
        if not desired:
            logger.warning("No password configured for %s; skipping password update", username)
            return 0

        if self.can_authenticate(username, desired):
            return 0

        if not self.can_authenticate("admin", admin_password):
            logger.warning("Unable to authenticate as admin; skipping password update for %s.", username)
            logger.warning(" admin:<provided> -> %s", self.auth_probe_status("admin", admin_password))
            return 0

        logger.info("Updating Ranger user password for %s to configured value", username)
        if self.update_user_password(admin_password, username, desired) != 0:
            return 1

        if not self.can_authenticate(username, desired):
            logger.error("Password update succeeded but auth check failed for %s", username)
            return 1

        return 0

    def run(self, admin_password: str, usersync_password: str, tagsync_password: str) -> int:
        if self.set_admin_password_if_needed(admin_password) != 0:
            return 1
        if self.update_user_password_if_needed(admin_password, "rangerusersync", usersync_password) != 0:
            return 1
        if self.update_user_password_if_needed(admin_password, "rangertagsync", tagsync_password) != 0:
            return 1
        return 0


def main() -> int:
    configure_logging()

    bootstrap = UserPasswordBootstrap(DEFAULT_BASE_URL)
    return bootstrap.run(
        admin_password=os.environ.get("RANGER_ADMIN_PASSWORD", ""),
        usersync_password=os.environ.get("RANGER_USERSYNC_PASSWORD", ""),
        tagsync_password=os.environ.get("RANGER_TAGSYNC_PASSWORD", ""),
    )


if __name__ == "__main__":
    sys.exit(main())
