#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

MODE="${1:-apply}"
LDAP_URL="ldap://local-ldap:389"
LDAP_BIND_DN="cn=admin,dc=example,dc=com"
LDAP_ADMIN_PASSWORD="${LDAP_ADMIN_PASSWORD:-p@ssw0rd}"
LDAP_BASE="dc=example,dc=com"
LDAP_USER_BASE="ou=People,${LDAP_BASE}"
LDAP_GROUP_BASE="ou=People,${LDAP_BASE}"

DOCKER_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
LDIF="${DOCKER_DIR}/scripts/ldap/ldapusergroup.ldif"

USERSYNC_XML="/opt/ranger/usersync/conf/ranger-ugsync-site.xml"
ADMIN_XML="/opt/ranger/admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-site.xml"
BACKUP="/var/tmp/ldap-runtime-backup"

for c in local-ldap ranger ranger-usersync; do
  docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true || {
    echo "$c is not running" >&2
    exit 1
  }
done

# Copy once, inside that container. A later apply must not replace this file.
backup_once() {
  local container="$1" file="$2" dest
  dest="${BACKUP}/$(basename "$file")"
  if docker exec "$container" test -f "$dest"; then
    echo "backup already present in ${container}:${dest}"
    return 0
  fi
  docker exec "$container" mkdir -p "$BACKUP"
  docker exec "$container" cp "$file" "$dest"
  echo "saved ${container}:${dest}"
}

restore() {
  local container="$1" file="$2" dest
  dest="${BACKUP}/$(basename "$file")"
  docker exec "$container" test -f "$dest"
  docker exec "$container" cp "$dest" "$file"
  echo "restored ${container}:${file}"
}

set_xml() {
  local container="$1" file="$2" name="$3" value="$4"
  docker exec "$container" bash -c "
    f='$file'; n='$name'; v='$value'
    if xmlstarlet sel -t -v \"//property[name='\$n']/value\" \"\$f\" >/dev/null 2>&1; then
      xmlstarlet ed -L -u \"//property[name='\$n']/value\" -v \"\$v\" \"\$f\"
    else
      xmlstarlet ed -L -s //configuration -t elem -n property \
        -s '//configuration/property[last()]' -t elem -n name -v \"\$n\" \
        -s '//configuration/property[last()]' -t elem -n value -v \"\$v\" \"\$f\"
    fi
  "
}

apply_ldap() {
  echo "Saving the current XML inside each container"
  backup_once ranger-usersync "$USERSYNC_XML"
  backup_once ranger "$ADMIN_XML"

  echo "Seeding ldapuser1"
  docker exec -i local-ldap ldapadd -c -x \
    -D "$LDAP_BIND_DN" -w "$LDAP_ADMIN_PASSWORD" < "$LDIF" || true

  echo "Pointing UserSync at local-ldap"
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.source.impl.class org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.sync.source ldap
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.ldap.url "$LDAP_URL"
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.ldap.binddn "$LDAP_BIND_DN"
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.ldap.ldapbindpassword "$LDAP_ADMIN_PASSWORD"
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.ldap.user.searchbase "$LDAP_USER_BASE"
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.ldap.user.objectclass inetOrgPerson
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.group.searchenabled true
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.group.searchbase "$LDAP_GROUP_BASE"
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.group.objectclass groupOfNames
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.group.memberattributename member
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.group.nameattribute cn
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.ldap.deltasync true
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.ldap.deltasync.server.type ldap
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.ldap.referral ignore
  set_xml ranger-usersync "$USERSYNC_XML" ranger.usersync.sleeptimeinmillisbetweensynccycle 60000

  if docker exec ranger-usersync test -f /opt/ranger/.setupDone; then
    echo "Restarting the UserSync container so setup is skipped"
    docker restart ranger-usersync >/dev/null
  else
    echo "No /opt/ranger/.setupDone. A container restart would run setup and replace this XML." >&2
    echo "Create that flag after a good setup, or the running process will keep the old config." >&2
    exit 1
  fi

  echo "Pointing Ranger Admin at local-ldap"
  set_xml ranger "$ADMIN_XML" ranger.authentication.method LDAP
  set_xml ranger "$ADMIN_XML" ranger.ldap.url "$LDAP_URL"
  set_xml ranger "$ADMIN_XML" ranger.ldap.base.dn "$LDAP_BASE"
  set_xml ranger "$ADMIN_XML" ranger.ldap.bind.dn "$LDAP_BIND_DN"
  set_xml ranger "$ADMIN_XML" ranger.ldap.bind.password "$LDAP_ADMIN_PASSWORD"
  set_xml ranger "$ADMIN_XML" ranger.ldap.user.dnpattern "cn={0},ou=People,${LDAP_BASE}"
  set_xml ranger "$ADMIN_XML" ranger.ldap.user.searchfilter "(cn={0})"
  set_xml ranger "$ADMIN_XML" ranger.ldap.group.searchbase "$LDAP_GROUP_BASE"
  set_xml ranger "$ADMIN_XML" ranger.ldap.group.searchfilter "(member=cn={0},ou=People,${LDAP_BASE})"
  set_xml ranger "$ADMIN_XML" ranger.ldap.group.roleattribute cn
  set_xml ranger "$ADMIN_XML" ranger.ldap.referral ignore
  echo "Admin XML updated. Do not docker restart ranger; that rebuilds this file from the YAML."
  echo "Restart Admin from inside the container only if you accept that a container exit rebuilds it."
}

revert_ldap() {
  echo "Restoring XML from ${BACKUP} inside each container"
  restore ranger-usersync "$USERSYNC_XML"
  restore ranger "$ADMIN_XML"
  if docker exec ranger-usersync test -f /opt/ranger/.setupDone; then
    docker restart ranger-usersync >/dev/null
  fi
  echo "Reverted. Admin is using the restored XML until the ranger container is recreated."
}

case "$MODE" in
  apply)  apply_ldap ;;
  revert) revert_ldap ;;
  *) echo "Usage: $0 [apply|revert]" >&2; exit 1 ;;
esac