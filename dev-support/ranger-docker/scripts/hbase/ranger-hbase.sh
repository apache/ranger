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

if [ ! -e ${HBASE_HOME}/.setupDone ]
then
  su -c "[ ! -f ~/.ssh/id_rsa ] && ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" hbase
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" hbase
  su -c "chmod 0600 ~/.ssh/authorized_keys" hbase

  ssh-keygen -A
  /usr/sbin/sshd -D &

  # pdsh is unavailable with microdnf in rhel based image.
  echo "ssh" > /etc/pdsh/rcmd_default

  if "${RANGER_SCRIPTS}"/ranger-hbase-setup.sh;
  then
    touch "${HBASE_HOME}"/.setupDone
  else
    echo "Ranger Hbase Setup Script didn't complete proper execution."
  fi
fi

# Single-node docker: keep master/regionserver on this host (not ZK).
echo "ranger-hbase.rangernw" > "${HBASE_HOME}/conf/regionservers"
echo "ranger-hbase.rangernw" > "${HBASE_HOME}/conf/masters"

# Start master + regionserver locally (avoid ssh-based start-hbase.sh in docker).
su -c "${HBASE_HOME}/bin/hbase-daemon.sh start master" hbase
su -c "${HBASE_HOME}/bin/hbase-daemon.sh start regionserver" hbase

HBASE_MASTER_PID=""
for _ in $(seq 1 24); do
  HBASE_MASTER_PID=`ps -ef | grep -v grep | grep -i "org.apache.hadoop.hbase.master.HMaster" | awk '{print $2}'`
  if [ -n "$HBASE_MASTER_PID" ]; then
    break
  fi
  sleep 5
done

# prevent the container from exiting
if [ -z "$HBASE_MASTER_PID" ]
then
  echo "The HBase process probably exited, no process id found!"
else
  tail --pid=$HBASE_MASTER_PID -f /dev/null
fi
