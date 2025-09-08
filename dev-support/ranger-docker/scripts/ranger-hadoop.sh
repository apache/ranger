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

CREATE_HDFS_DIR=false

# Always ensure SSH daemon is running (required for Hadoop services)
echo "Starting SSH daemon..."
# Create SSH privilege separation directory if it doesn't exist
mkdir -p /run/sshd
/usr/sbin/sshd

if [ -f /home/hdfs/.ssh/id_rsa ]; then
  echo "Waiting for SSH daemon to be ready..."
  SSH_READY=false
  for i in {1..30}; do
    if su -c "ssh -o ConnectTimeout=2 -o StrictHostKeyChecking=no localhost exit" hdfs 2>/dev/null; then
      echo "SSH daemon is ready for hdfs service..."
      SSH_READY=true
      break
    fi
    echo "Waiting for SSH daemon... ($i/30)"
    sleep 2
  done

  if [ "$SSH_READY" = false ]; then
    echo "WARNING: SSH daemon did not become ready within 60 seconds, Hadoop Services may fail to start properly...."
    echo "Attempting to restart SSH daemon..."
    pkill sshd 2>/dev/null || true
    # Ensure SSH privilege separation directory exists
    mkdir -p /run/sshd
    /usr/sbin/sshd
    sleep 3
  fi
else
  echo "SSH keys not yet generated, skipping SSH connectivity test"
  sleep 2
fi

if [ ! -e ${HADOOP_HOME}/.setupDone ]
then
  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" hdfs
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" hdfs
  su -c "chmod 0600 ~/.ssh/authorized_keys" hdfs

  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" yarn
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" yarn
  su -c "chmod 0600 ~/.ssh/authorized_keys" yarn

  ssh-keygen -A

  # pdsh is unavailable with microdnf in rhel based image.
  echo "ssh" > /etc/pdsh/rcmd_default


  if "${RANGER_SCRIPTS}"/ranger-hadoop-setup.sh;
  then
    su -c "${HADOOP_HOME}/bin/hdfs namenode -format" hdfs

    CREATE_HDFS_DIR=true

    touch "${HADOOP_HOME}"/.setupDone
  else
    echo "Ranger Hadoop Setup Script didn't complete proper execution."
  fi
fi

su -c "${HADOOP_HOME}/sbin/start-dfs.sh" hdfs
su -c "${HADOOP_HOME}/sbin/start-yarn.sh" yarn

if [ "${CREATE_HDFS_DIR}" == "true" ]
then
  su -c "${RANGER_SCRIPTS}/ranger-hadoop-mkdir.sh" hdfs
fi

NAMENODE_PID=`ps -ef  | grep -v grep | grep -i "org.apache.hadoop.hdfs.server.namenode.NameNode" | awk '{print $2}'`

# prevent the container from exiting
if [ -z "$NAMENODE_PID" ]
then
  echo "The NameNode process probably exited, no process id found!"
else
  tail --pid=$NAMENODE_PID -f /dev/null
fi