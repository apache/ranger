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
KDC_HOST="${KDC_HOST:-ranger-kdc.example.com}"
MASTER_PASSWORD="${MASTER_PASSWORD:-masterpassword}"
ADMIN_PRINC="${ADMIN_PRINCIPAL:-admin/admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-adminpassword}"

DB_DIR=/var/kerberos/krb5kdc

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

