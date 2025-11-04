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
  ${RANGER_SCRIPTS}/wait_for_keytab.sh hive.keytab
fi

cp ${RANGER_SCRIPTS}/hive-site.xml ${HIVE_HOME}/conf/hive-site.xml
cp ${RANGER_SCRIPTS}/hive-site.xml ${HIVE_HOME}/conf/hiveserver2-site.xml

mkdir -p ${HADOOP_HOME}/etc/hadoop

cp ${RANGER_SCRIPTS}/core-site.xml ${HADOOP_HOME}/etc/hadoop/core-site.xml

# Create mapred-site.xml for YARN integration
cat <<EOF > ${HADOOP_HOME}/etc/hadoop/mapred-site.xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=/opt/hadoop</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=/opt/hadoop</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=/opt/hadoop</value>
  </property>
</configuration>
EOF

# Create yarn-site.xml for YARN ResourceManager connection
cat <<EOF > ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>ranger-hadoop</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>ranger-hadoop:8032</value>
  </property>
</configuration>
EOF


# Copy all Hadoop configurations to Hive conf directory so Hive can find them
cp ${HADOOP_HOME}/etc/hadoop/core-site.xml ${HIVE_HOME}/conf/
cp ${HADOOP_HOME}/etc/hadoop/mapred-site.xml ${HIVE_HOME}/conf/
cp ${HADOOP_HOME}/etc/hadoop/yarn-site.xml ${HIVE_HOME}/conf/

# Create HDFS user directory for hive
su -c "${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /user/hive" hdfs
su -c "${HADOOP_HOME}/bin/hdfs dfs -chmod -R 777 /user/hive" hdfs

# Fix /tmp directory permissions for Ranger (critical for INSERT operations)
su -c "${HADOOP_HOME}/bin/hdfs dfs -chmod 777 /tmp" hdfs

# Create /user/root directory for YARN job execution
su -c "${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /user/root" hdfs
su -c "${HADOOP_HOME}/bin/hdfs dfs -chmod 777 /user/root" hdfs

# Initialize Hive schema
su -c "${HIVE_HOME}/bin/schematool -dbType ${RANGER_DB_TYPE} -initSchema" hive

mkdir -p /opt/hive/logs
chown -R hive:hadoop /opt/hive/
chmod g+w /opt/hive/logs

cd ${RANGER_HOME}/ranger-hive-plugin
./enable-hive-plugin.sh
