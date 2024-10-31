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

#
# Downloads HDFS/Hive/HBase/Kafka/Knox/Ozone archives to a local cache directory.
# The downloaded archives will be used while building docker images that run these services.
#

#
# source .env file to get versions to download
#
source .env


downloadIfNotPresent() {
  local fileName=$1
  local urlBase=$2

  if [ ! -f "downloads/${fileName}" ]
  then
    echo "downloading ${urlBase}/${fileName}.."

    curl -L ${urlBase}/${fileName} --output downloads/${fileName}
  else
    echo "file already in cache: ${fileName}"
  fi
}

createShaded() {
  local jar_name=$1
  local suffix="jakarta"

  echo "migrating ${jar}"
  java -jar ./downloads/jakartaee-migration-1.0.8-shaded.jar ./downloads/${jar_name}-${HADOOP_VERSION}.jar ./dist/${jar_name}-${HADOOP_VERSION}-${suffix}.jar
}

downloadIfNotPresent jakartaee-migration-1.0.8-shaded.jar https://archive.apache.org/dist/tomcat/jakartaee-migration/v1.0.8/binaries/
downloadIfNotPresent hadoop-common-${HADOOP_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}
downloadIfNotPresent hadoop-auth-${HADOOP_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/${HADOOP_VERSION}

createShaded hadoop-common
createShaded hadoop-auth
