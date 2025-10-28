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

ADMIN_PRINCIPAL=admin/admin
ADMIN_PASSWORD=rangerR0cks!

PRINCIPAL_NAME=$1
KEYTAB_DIR=$2
KEYTAB_OWNER=$3

PRINCIPAL=${PRINCIPAL_NAME}/`hostname -f`
KEYTAB=${KEYTAB_DIR}/${PRINCIPAL_NAME}.keytab

# if keytab file already exists, do nothing
if [ -f ${KEYTAB} ]
then
  exit 0
fi

mkdir -p ${KEYTAB_DIR}

echo "Creating kerberos principal ${PRINCIPAL} .."
for i in {1..5}; do
  echo ${ADMIN_PASSWORD} | kadmin -p ${ADMIN_PRINCIPAL} -q "addprinc -randkey ${PRINCIPAL}"

  if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to create kerberos principal..will retry after 5 seconds"
    sleep 5
  else
    echo "Successfully created kerberos principal ${PRINCIPAL}"
    break
  fi
done


echo "Creating keytab for principal ${PRINCIPAL} .."
for i in {1..5}; do
  echo ${ADMIN_PASSWORD} | kadmin -p ${ADMIN_PRINCIPAL} -q "ktadd -k ${KEYTAB} ${PRINCIPAL}"

  if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to create keytab for principal..will retry after 5 seconds"
    sleep 5
  else
    echo "Successfully created keytab kerberos principal ${PRINCIPAL} in ${KEYTAB}"
    break
  fi
done

if [ "${KEYTAB_OWNER}" != "" ]
then
    chmod 440 ${KEYTAB}
    chown ${KEYTAB_OWNER} ${KEYTAB}
fi
