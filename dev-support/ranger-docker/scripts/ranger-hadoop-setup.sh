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

echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

cat <<EOF > /etc/ssh/ssh_config
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
EOF

if [ "${KERBEROS_ENABLED}" == "true" ]
then
  KEYTABS_DIR=/opt/hadoop/keytabs

  ${RANGER_SCRIPTS}/create_principal_and_keytab.sh hdfs ${KEYTABS_DIR} hdfs:hadoop
  ${RANGER_SCRIPTS}/create_principal_and_keytab.sh nn ${KEYTABS_DIR} hdfs:hadoop
  ${RANGER_SCRIPTS}/create_principal_and_keytab.sh dn ${KEYTABS_DIR} hdfs:hadoop
  ${RANGER_SCRIPTS}/create_principal_and_keytab.sh HTTP ${KEYTABS_DIR} hdfs:hadoop
  ${RANGER_SCRIPTS}/create_principal_and_keytab.sh nm ${KEYTABS_DIR} yarn:hadoop
  ${RANGER_SCRIPTS}/create_principal_and_keytab.sh rm ${KEYTABS_DIR} yarn:hadoop
  ${RANGER_SCRIPTS}/create_principal_and_keytab.sh yarn ${KEYTABS_DIR} yarn:hadoop
  ${RANGER_SCRIPTS}/create_principal_and_keytab.sh healthcheck ${KEYTABS_DIR} hdfs:hadoop
fi

cp ${RANGER_SCRIPTS}/core-site.xml ${HADOOP_HOME}/etc/hadoop/core-site.xml
cp ${RANGER_SCRIPTS}/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
cp ${RANGER_SCRIPTS}/yarn-site.xml ${HADOOP_HOME}/etc/hadoop/yarn-site.xml

mkdir -p /opt/hadoop/logs
chown -R hdfs:hadoop /opt/hadoop/
chmod g+w /opt/hadoop/logs
# user logs directory permissions for NodeManager health
mkdir -p ${HADOOP_HOME}/logs/userlogs
chown -R yarn:hadoop ${HADOOP_HOME}/logs/userlogs
chmod -R 777 ${HADOOP_HOME}/logs/userlogs

# Install Tez JARs for YARN NodeManager
echo "Installing Tez JARs for YARN NodeManager..."
if [ -d "/opt/tez" ]; then
    echo "Copying Tez JARs to YARN lib directory..."
    cp /opt/tez/lib/*.jar /opt/hadoop/share/hadoop/yarn/lib/ 2>/dev/null
    cp /opt/tez/*.jar /opt/hadoop/share/hadoop/yarn/lib/ 2>/dev/null

    # Set up Tez environment
    export TEZ_HOME=/opt/tez
    export TEZ_CONF_DIR=${TEZ_HOME}/conf
    mkdir -p ${TEZ_CONF_DIR}

    echo "Tez JARs installed successfully for YARN NodeManager"
else
    echo "WARNING: Tez directory not found at /opt/tez"
fi

cd ${RANGER_HOME}/ranger-hdfs-plugin
./enable-hdfs-plugin.sh

cd ${RANGER_HOME}/ranger-yarn-plugin
./enable-yarn-plugin.sh
