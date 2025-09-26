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

cp ${RANGER_SCRIPTS}/hive-site.xml ${HIVE_HOME}/conf/hive-site.xml
cp ${RANGER_SCRIPTS}/hive-site.xml ${HIVE_HOME}/conf/hiveserver2-site.xml

# Configure Tez
mkdir -p ${TEZ_HOME}/conf

# Create Tez configuration directory for Hadoop
mkdir -p ${HADOOP_HOME}/etc/hadoop

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

# Fix tez-site.xml to use absolute HDFS path (critical for Tez to find libraries)
cat <<EOF > ${TEZ_HOME}/conf/tez-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>tez.lib.uris</name>
        <value>hdfs://ranger-hadoop:9000/apps/tez/apache-tez-${TEZ_VERSION}-bin.tar.gz</value>
        <description>Comma-delimited list of the location of the Tez libraries which will be localized for DAGs.</description>
    </property>
    <property>
        <name>tez.use.cluster.hadoop-libs</name>
        <value>true</value>
        <description>Use Hadoop libraries provided by cluster instead of those packaged with Tez</description>
    </property>
    <property>
        <name>tez.am.resource.memory.mb</name>
        <value>1024</value>
        <description>The amount of memory to be used by the AppMaster</description>
    </property>
    <property>
        <name>tez.am.java.opts</name>
        <value>-Xmx768m</value>
        <description>Java opts for the Tez AppMaster process</description>
    </property>
    <property>
        <name>tez.task.resource.memory.mb</name>
        <value>1024</value>
        <description>The amount of memory to be used by tasks</description>
    </property>
    <property>
        <name>tez.task.launch.cmd-opts</name>
        <value>-Xmx768m</value>
        <description>Java opts for tasks</description>
    </property>
    <property>
        <name>tez.staging-dir</name>
        <value>/tmp/hive</value>
        <description>The staging directory for Tez applications in HDFS.</description>
    </property>
</configuration>
EOF

# Copy Tez JARs to Hive lib directory
cp ${TEZ_HOME}/lib/tez-*.jar ${HIVE_HOME}/lib/
cp ${TEZ_HOME}/tez-*.jar ${HIVE_HOME}/lib/

# Copy all Hadoop configurations to Hive conf directory so Hive can find them
cp ${HADOOP_HOME}/etc/hadoop/core-site.xml ${HIVE_HOME}/conf/
cp ${HADOOP_HOME}/etc/hadoop/mapred-site.xml ${HIVE_HOME}/conf/
cp ${HADOOP_HOME}/etc/hadoop/yarn-site.xml ${HIVE_HOME}/conf/
cp ${TEZ_HOME}/conf/tez-site.xml ${HIVE_HOME}/conf/

# Upload Tez libraries to HDFS
su -c "${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /apps/tez" hdfs

# Recreate Tez tarball if it doesn't exist (it gets removed during Docker build)
if [ ! -f "/opt/apache-tez-${TEZ_VERSION}-bin.tar.gz" ]; then
    echo "Recreating Tez tarball for HDFS upload..."
    cd /opt
    tar czf apache-tez-${TEZ_VERSION}-bin.tar.gz apache-tez-${TEZ_VERSION}-bin/
fi

su -c "${HADOOP_HOME}/bin/hdfs dfs -put /opt/apache-tez-${TEZ_VERSION}-bin.tar.gz /apps/tez/" hdfs
su -c "${HADOOP_HOME}/bin/hdfs dfs -chmod -R 755 /apps/tez" hdfs

# Create HDFS user directory for hive
su -c "${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /user/hive" hdfs
su -c "${HADOOP_HOME}/bin/hdfs dfs -chmod -R 777 /user/hive" hdfs

# Create HDFS /tmp/hive directory for Tez staging
su -c "${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /tmp/hive" hdfs
su -c "${HADOOP_HOME}/bin/hdfs dfs -chmod -R 777 /tmp/hive" hdfs

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
