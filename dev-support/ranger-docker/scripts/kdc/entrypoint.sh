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

set -e

REALM="${REALM:-EXAMPLE.COM}"
KDC_HOST="${KDC_HOST:-ranger-kdc.rangernw}"
MASTER_PASSWORD="${MASTER_PASSWORD:-masterpassword}"
ADMIN_PRINC="${ADMIN_PRINCIPAL:-admin/admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-adminpassword}"

DB_DIR=/var/kerberos/krb5kdc
KEYTABS_DIR=/etc/keytabs

function create_principal_and_keytab() {
  principal_name=$1
  container_name=$2

  principal=${principal_name}/${container_name}.rangernw
  keytab=${KEYTABS_DIR}/${container_name}/${principal_name}.keytab

  mkdir -p ${KEYTABS_DIR}/${container_name}

  rm -f ${keytab}

  echo "Creating kerberos principal ${principal} .."
  for i in {1..5}; do
    kadmin.local -q "addprinc -randkey ${principal}"

    if [ $? -ne 0 ]; then
      echo "[ERROR] Failed to create kerberos principal..will retry after 5 seconds"
      sleep 5
    else
      echo "[INFO] created kerberos principal ${principal}"
      break
    fi
  done


  echo "Creating keytab for principal ${principal} .."
  for i in {1..5}; do
    kadmin.local -q "ktadd -k ${keytab} ${principal}"

    if [ $? -ne 0 ]; then
      echo "[ERROR] Failed to create keytab for principal..will retry after 5 seconds"
      sleep 5
    else
      echo "[INFO] created keytab kerberos principal ${principal} in ${keytab}"
      ls -lFa ${keytab}
      break
    fi
  done

  chmod 444 ${keytab}
}

function create_keytabs() {
  create_principal_and_keytab HTTP         ranger
  create_principal_and_keytab rangeradmin  ranger
  create_principal_and_keytab rangerlookup ranger

  create_principal_and_keytab rangertagsync ranger-tagsync

  create_principal_and_keytab rangerusersync ranger-usersync

  create_principal_and_keytab rangerkms ranger-kms

  create_principal_and_keytab dn          ranger-hadoop
  create_principal_and_keytab hdfs        ranger-hadoop
  create_principal_and_keytab healthcheck ranger-hadoop
  create_principal_and_keytab HTTP        ranger-hadoop
  create_principal_and_keytab nm          ranger-hadoop
  create_principal_and_keytab nn          ranger-hadoop
  create_principal_and_keytab rm          ranger-hadoop
  create_principal_and_keytab yarn        ranger-hadoop

  create_principal_and_keytab hbase ranger-hbase

  create_principal_and_keytab hive ranger-hive
  create_principal_and_keytab hdfs ranger-hive
  create_principal_and_keytab HTTP ranger-hive

  create_principal_and_keytab kafka ranger-kafka

  create_principal_and_keytab knox ranger-knox

  create_principal_and_keytab HTTP ranger-solr
}

function create_testusers() {
  for container in "$@"; do
    create_principal_and_keytab testuser1 "$container"
    create_principal_and_keytab testuser2 "$container"
    create_principal_and_keytab testuser3 "$container"
  done
}

# ensure directories
mkdir -p $DB_DIR
chown -R root.root /etc/krb5kdc || true
chown -R root.root $DB_DIR || true

if [ ! -f $DB_DIR/principal ]; then
  echo "=== Creating KDC database for realm $REALM ==="
  # create DB noninteractive
  echo "$MASTER_PASSWORD" | kdb5_util create -s -r $REALM -P "$MASTER_PASSWORD"
  # create admin principal
  kadmin.local -q "addprinc -pw $ADMIN_PASSWORD $ADMIN_PRINC@${REALM}"
  # add kadmind keytab
  kadmin.local -q "ktadd -k /etc/krb5kdc/kadm5.keytab kadmin/admin@$REALM"
  echo "Database initialized"

  create_keytabs
  create_testusers ranger ranger-usersync ranger-tagsync ranger-audit ranger-hadoop ranger-hive ranger-hbase ranger-kafka ranger-solr ranger-knox ranger-kms ranger-ozone ranger-trino
else
  echo "KDC DB already exists; skipping create"
fi

# Ensure ownership and perms
chown -R root:root /var/kerberos
chmod 700 /var/kerberos/krb5kdc

# start krb5kdc in foreground and then kadmind
echo "Starting krb5kdc..."
/usr/sbin/krb5kdc -n &
KDC_PID=$!

echo "Starting kadmind..."
/usr/sbin/kadmind -nofork

# if kadmind exits, bring down krb5kdc
kill $KDC_PID || true
wait $KDC_PID || true

