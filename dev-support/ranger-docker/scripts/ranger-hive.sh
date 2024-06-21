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

if [ "${OS_NAME}" = "UBUNTU" ]; then
  service ssh start
fi

if [ ! -e ${HIVE_HOME}/.setupDone ]
then
  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" hdfs
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" hdfs
  su -c "chmod 0600 ~/.ssh/authorized_keys" hdfs

  if [ "${OS_NAME}" = "RHEL" ]; then
    ssh-keygen -A
    /usr/sbin/sshd
  fi

  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" yarn
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" yarn
  su -c "chmod 0600 ~/.ssh/authorized_keys" yarn

  # pdsh is unavailable with microdnf in rhel based image.
  echo "ssh" > /etc/pdsh/rcmd_default


  if "${RANGER_SCRIPTS}"/ranger-hive-setup.sh;
  then
    touch "${HIVE_HOME}"/.setupDone
  else
    echo "Ranger Hive Setup Script didn't complete proper execution."
  fi
fi

cd "${HIVE_HOME}" || exit

# Start Hive MetaStore
su -c "nohup ${HIVE_HOME}/bin/hive --service metastore > metastore.log 2>&1 &" hive

# Start HiveServer2
su -c "nohup ${HIVE_HOME}/bin/hiveserver2 > hive-server2.log 2>&1 &" hive

sleep 10

HIVE_SERVER2_PID=`ps -ef  | grep -v grep | grep -i "org.apache.hive.service.server.HiveServer2" | awk '{print $2}'`

# prevent the container from exiting
if [ -z "$HIVE_SERVER2_PID" ]
then
  echo "The HiveServer2 process probably exited, no process id found!"
else
  tail --pid="$HIVE_SERVER2_PID" -f /dev/null
fi
