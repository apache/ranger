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

downloadIfNotPresent postgresql-42.2.16.jre7.jar            "https://search.maven.org/remotecontent?filepath=org/postgresql/postgresql/42.2.16.jre7"
downloadIfNotPresent mysql-connector-java-8.0.28.jar        "https://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/8.0.28"
downloadIfNotPresent ojdbc8.jar                             https://download.oracle.com/otn-pub/otn_software/jdbc/236
downloadIfNotPresent mssql-jdbc-12.8.1.jre8.jar             https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.8.1.jre8
downloadIfNotPresent log4jdbc-1.2.jar                       https://repo1.maven.org/maven2/com/googlecode/log4jdbc/log4jdbc/1.2

if [[ $# -eq 0 ]]
then
    downloadIfNotPresent hadoop-${HADOOP_VERSION}.tar.gz        https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}
    downloadIfNotPresent hbase-${HBASE_VERSION}-bin.tar.gz      https://archive.apache.org/dist/hbase/${HBASE_VERSION}
    downloadIfNotPresent apache-hive-${HIVE_VERSION}-bin.tar.gz https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}
    downloadIfNotPresent hadoop-${HIVE_HADOOP_VERSION}.tar.gz   https://archive.apache.org/dist/hadoop/common/hadoop-${HIVE_HADOOP_VERSION}
    downloadIfNotPresent kafka_2.12-${KAFKA_VERSION}.tgz        https://archive.apache.org/dist/kafka/${KAFKA_VERSION}
    downloadIfNotPresent knox-${KNOX_VERSION}.tar.gz            https://archive.apache.org/dist/knox/${KNOX_VERSION}
    downloadIfNotPresent ozone-${OZONE_VERSION}.tar.gz          https://archive.apache.org/dist/ozone/${OZONE_VERSION}
    if [ ! -d downloads/ozone-${OZONE_VERSION} ]
    then
      tar xvfz downloads/ozone-${OZONE_VERSION}.tar.gz --directory=downloads/
    fi
else
  for arg in "$@"; do
    if [[ $arg == 'hadoop' ]]
    then
      downloadIfNotPresent hadoop-${HADOOP_VERSION}.tar.gz        https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}
    elif [[ $arg == 'hbase' ]]
    then
      downloadIfNotPresent hbase-${HBASE_VERSION}-bin.tar.gz      https://archive.apache.org/dist/hbase/${HBASE_VERSION}
    elif [[ $arg == 'hive' ]]
    then
      downloadIfNotPresent apache-hive-${HIVE_VERSION}-bin.tar.gz https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}
      downloadIfNotPresent hadoop-${HIVE_HADOOP_VERSION}.tar.gz   https://archive.apache.org/dist/hadoop/common/hadoop-${HIVE_HADOOP_VERSION}
    elif [[ $arg == 'kafka' ]]
    then
      downloadIfNotPresent kafka_2.12-${KAFKA_VERSION}.tgz        https://archive.apache.org/dist/kafka/${KAFKA_VERSION}
    elif [[ $arg == 'knox' ]]
    then
      downloadIfNotPresent knox-${KNOX_VERSION}.tar.gz            https://archive.apache.org/dist/knox/${KNOX_VERSION}
    elif [[ $arg == 'ozone' ]]
    then
      downloadIfNotPresent ozone-${OZONE_VERSION}.tar.gz          https://archive.apache.org/dist/ozone/${OZONE_VERSION}
      if [ ! -d downloads/ozone-${OZONE_VERSION} ]
      then
        tar xvfz downloads/ozone-${OZONE_VERSION}.tar.gz --directory=downloads/
      fi
    else
      echo "Passed argument $arg is invalid!"
    fi
  done
fi
