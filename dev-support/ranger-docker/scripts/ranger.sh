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

export RANGER_VERSION=`cat ${RANGER_DIST}/version`


if [ -e ${RANGER_HOME}/admin ]
then
  SETUP_RANGER=false
else
  SETUP_RANGER=true
fi

if [ "${SETUP_RANGER}" == "true" ]
then
  # Download PostgreSQL JDBC library
  wget "https://search.maven.org/remotecontent?filepath=org/postgresql/postgresql/42.2.16.jre7/postgresql-42.2.16.jre7.jar" -O /usr/share/java/postgresql.jar

  cd ${RANGER_HOME}
  tar xvfz ${RANGER_DIST}/ranger-${RANGER_VERSION}-admin.tar.gz --directory=${RANGER_HOME}
  ln -s ranger-${RANGER_VERSION}-admin admin
  cp -f ${RANGER_SCRIPTS}/ranger-admin-install.properties admin/install.properties

  cd ${RANGER_HOME}/admin
  ./setup.sh
fi

cd ${RANGER_HOME}/admin
./ews/ranger-admin-services.sh start

if [ "${SETUP_RANGER}" == "true" ]
then
  # Wait for Ranger Admin to become ready
  sleep 30

  python3 ${RANGER_SCRIPTS}/ranger-hdfs-service-dev_hdfs.py
  python3 ${RANGER_SCRIPTS}/ranger-hive-service-dev_hive.py
fi

# prevent the container from exiting
/bin/bash
