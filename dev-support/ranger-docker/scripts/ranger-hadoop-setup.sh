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

cat <<EOF > ${HADOOP_HOME}/etc/hadoop/core-site.xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://ranger-hadoop:9000</value>
  </property>
</configuration>
EOF

cat <<EOF > ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>
</configuration>
EOF

cat <<EOF > ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>ranger-hadoop</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>4096</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>4096</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>256</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.timeline-service.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.timeline-service.hostname</name>
    <value>ranger-hadoop</value>
  </property>
  <property>
    <name>yarn.timeline-service.http-cross-origin.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.resourcemanager.system-metrics-publisher.enabled</name>
    <value>true</value>
  </property>
</configuration>
EOF

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
