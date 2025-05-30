# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This workflow will build a Java project with Maven, and cache/restore any dependencies to improve the workflow execution time
# For more information see: https://docs.github.com/en/actions/automating-builds-and-tests/building-and-testing-java-with-maven

# This workflow uses actions that are not certified by GitHub.
# They are provided by a third-party and are governed by
# separate terms of service, privacy policy, and support
# documentation.



##Contains all constant values regarding USER, PATH, HDFS Commands----------------------


HDFS_USER = "hdfs"
HIVE_USER = "hive"
HBASE_USER= "hbase"
KEY_ADMIN="keyadmin"
HEADERS={"Content-Type": "application/json","Accept":"application/json"}
PARAMS={"user.name":"keyadmin"}
BASE_URL="http://localhost:9292/kms/v1"
HADOOP_CONTAINER = "ranger-hadoop"
HDFS_USER = "hdfs"
KMS_CONTAINER = "ranger-kms"

#KMS configs that needs to be added in XML file------------add more if needed
KMS_PROPERTY = """<property><name>hadoop.security.key.provider.path</name><value>kms://http@host.docker.internal:9292/kms</value></property>"""

CORE_SITE_XML_PATH = "/opt/hadoop/etc/hadoop/core-site.xml"

 # Ensure PATH is set for /opt/hadoop/bin
SET_PATH_CMD="echo 'export PATH=/opt/hadoop/bin:$PATH' >> /etc/profile && export PATH=/opt/hadoop/bin:$PATH"

HADOOP_NAMENODE_LOG_PATH="/opt/hadoop/logs/hadoop-hdfs-namenode-ranger-hadoop.example.com.log"

KMS_LOG_PATH="/var/log/ranger/kms/ranger-kms-ranger-kms.example.com-root.log"


# HDFS Commands----------------------------------------------------
CREATE_KEY_COMMAND = "hadoop key create {key_name} -size 128 -provider kms://http@host.docker.internal:9292/kms"

VALIDATE_KEY_COMMAND = "hadoop key list -provider kms://http@host.docker.internal:9292/kms"

CREATE_EZ_COMMANDS = [
    "hdfs dfs -mkdir /{ez_name}",
    "hdfs crypto -createZone -keyName {key_name} -path /{ez_name}",
    "hdfs crypto -listZones"
]

GRANT_PERMISSIONS_COMMANDS = [
    "hdfs dfs -chmod -R 700 /{ez_name}",
    "hdfs dfs -chown -R {user}:{user} /{ez_name}"
]

CREATE_FILE_COMMAND = [ 'echo "{filecontent}" > /home/{user}/{filename}.txt && ls -l /home/{user}/{filename}.txt' ]

ACTIONS_COMMANDS = [
    "hdfs dfs -put /home/{user}/{filename}.txt /{ez_name}/",
    "hdfs dfs -ls /{ez_name}/",
    "hdfs dfs -cat /{ez_name}/{filename}.txt"
]

CROSS_EZ_ACTION_COMMANDS = [
    "hdfs dfs -put /home/{user}/{filename}.txt /{ez_name}/{dirname}/",
    "hdfs dfs -ls /{ez_name}/",
    "hdfs dfs -cat /{ez_name}/{dirname}/{filename}.txt"
]

READ_EZ_FILE=[
    "hdfs dfs -cat /{ez_name}/{filename}.txt"
]

UNAUTHORIZED_WRITE_COMMAND = 'hdfs dfs -put /home/{user}/{filename}.txt /{ez_name}/'

UNAUTHORIZED_READ_COMMAND = "hdfs dfs -cat /{ez_name}/{filename}.txt"

CLEANUP_COMMANDS = [
    "hdfs dfs -rm /{ez_name}/{filename}.txt",
    "hdfs dfs -rm -R /{ez_name}"
]
CLEANUP_EZ = [
    "hdfs dfs -rm -R /{ez_name}"
]
CLEANUP_EZ_FILE = [
    "hdfs dfs -rm /{ez_name}/{filename}.txt"
]
KEY_DELETION_CMD = "bash -c \"echo 'Y' | hadoop key delete {key_name} -provider kms://http@host.docker.internal:9292/kms\""


