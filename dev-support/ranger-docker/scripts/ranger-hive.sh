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
  echo "Starting SSH service (Ubuntu)..."
  service ssh start
else
  echo "Starting SSH daemon (RHEL/CentOS)..."
  # Create SSH privilege separation directory if it doesn't exist
  mkdir -p /run/sshd
  /usr/sbin/sshd
fi

# Wait for SSH daemon to be fully ready before proceeding
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
    echo "WARNING: SSH daemon did not become ready within 60 seconds, Hive Services may fail to start properly...."
    echo "Attempting to restart SSH daemon..."
    pkill sshd 2>/dev/null || true
    if [ "${OS_NAME}" = "UBUNTU" ]; then
      service ssh start
    else
      # Ensure SSH privilege separation directory exists
      mkdir -p /run/sshd
      /usr/sbin/sshd
    fi
    sleep 3
  fi
else
  echo "SSH keys not yet generated, skipping SSH connectivity test"
  sleep 2
fi

if [ ! -e ${HIVE_HOME}/.setupDone ]
then
  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" hdfs
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" hdfs
  su -c "chmod 0600 ~/.ssh/authorized_keys" hdfs

  if [ "${OS_NAME}" = "RHEL" ]; then
    ssh-keygen -A
  fi

  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" yarn
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" yarn
  su -c "chmod 0600 ~/.ssh/authorized_keys" yarn

  su -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa" hive
  su -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys" hive
  su -c "chmod 0600 ~/.ssh/authorized_keys" hive

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
echo "Starting Hive MetaStore..."
su -c "nohup ${HIVE_HOME}/bin/hive --service metastore > metastore.log 2>&1 &" hive

# Start HiveServer2
echo "Starting HiveServer2..."
su -c "nohup ${HIVE_HOME}/bin/hiveserver2 > hive-server2.log 2>&1 &" hive

# Wait for services to initialize
echo "Waiting for Hive services to initialize..."
sleep 10

# Verify Hive services are running and ready
echo "Verifying Hive services are ready for beeline connections..."
METASTORE_PID=`ps -ef | grep -v grep | grep -i "org.apache.hadoop.hive.metastore.HiveMetaStore" | awk '{print $2}'`
HIVE_SERVER2_PID=`ps -ef | grep -v grep | grep -i "org.apache.hive.service.server.HiveServer2" | awk '{print $2}'`

if [ -n "$METASTORE_PID" ]; then
  echo "Hive MetaStore is running (PID: $METASTORE_PID)"
else
  echo "WARNING: Hive MetaStore process not found!"
fi

if [ -n "$HIVE_SERVER2_PID" ]; then
  echo "HiveServer2 is running (PID: $HIVE_SERVER2_PID)"
else
  echo "WARNING: HiveServer2 process not found!"
fi

# Additional verification: Check if HiveServer2 is listening on port 10000
echo "Checking if HiveServer2 is listening on port 10000..."
for i in {1..30}; do
  if timeout 2 bash -c "echo > /dev/tcp/localhost/10000" 2>/dev/null; then
    echo "HiveServer2 is ready and listening on port 10000...."
    break
  fi
  if [ $i -eq 30 ]; then
    echo "WARNING: HiveServer2 is not listening on port 10000 after 60 seconds"
    echo "Beeline connections may fail. Check metastore.log and hive-server2.log for errors."
  else
    echo "Waiting for HiveServer2 to listen on port 10000... ($i/30)"
    sleep 2
  fi
done

# prevent the container from exiting
if [ -z "$HIVE_SERVER2_PID" ]
then
  echo "The HiveServer2 process probably exited, no process id found!"
else
  tail --pid="$HIVE_SERVER2_PID" -f /dev/null
fi
