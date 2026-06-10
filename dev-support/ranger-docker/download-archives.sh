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

    # -f: fail on HTTP errors; -L: follow redirects; --retry*: tolerate transient
    # archive.apache.org / Maven mirror failures during CI cold-cache downloads.
    curl -fL --retry 3 --retry-all-errors --retry-delay 10 --connect-timeout 30 \
      ${urlBase}/${fileName} --output downloads/${fileName}
  else
    echo "file already in cache: ${fileName}"
  fi
}

# Ozone compose mounts downloads/ozone-${OZONE_VERSION}/ (extracted tree), not the tarball.
# Re-extract only when the dir is missing, incomplete, or older than the tarball.
extractOzoneIfNeeded() {
  local tarball="downloads/ozone-${OZONE_VERSION}.tar.gz"
  local extractDir="downloads/ozone-${OZONE_VERSION}"

  downloadIfNotPresent ozone-${OZONE_VERSION}.tar.gz https://archive.apache.org/dist/ozone/${OZONE_VERSION}

  if [ ! -f "${tarball}" ]; then
    echo "ERROR: missing ${tarball}" >&2
    exit 1
  fi

  local needExtract=false
  if [ ! -d "${extractDir}" ]; then
    needExtract=true
    echo "ozone extract dir missing: ${extractDir}"
  elif [ ! -f "${extractDir}/bin/ozone" ]; then
    needExtract=true
    echo "ozone extract dir incomplete, re-extracting: ${extractDir}"
    rm -rf "${extractDir}"
  elif [ "${tarball}" -nt "${extractDir}" ]; then
    needExtract=true
    echo "ozone tarball newer than extract dir, re-extracting"
    rm -rf "${extractDir}"
  else
    echo "ozone extract dir up to date: ${extractDir}"
  fi

  if [ "${needExtract}" = true ]; then
    tar xzf "${tarball}" --directory=downloads/
  fi

  # Remove other ozone versions left from partial cache restores after .env bumps.
  local stale
  for stale in downloads/ozone-*.tar.gz; do
    [ -e "${stale}" ] || continue
    [ "${stale}" = "${tarball}" ] && continue
    echo "removing stale ozone tarball: ${stale}"
    rm -f "${stale}"
  done
  for stale in downloads/ozone-*/; do
    [ -d "${stale}" ] || continue
    [ "${stale}" = "${extractDir}/" ] && continue
    echo "removing stale ozone extract dir: ${stale}"
    rm -rf "${stale}"
  done
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
    downloadIfNotPresent apache-tez-${TEZ_VERSION}-bin.tar.gz   https://archive.apache.org/dist/tez/${TEZ_VERSION}
    downloadIfNotPresent kafka_2.12-${KAFKA_VERSION}.tgz        https://archive.apache.org/dist/kafka/${KAFKA_VERSION}
    downloadIfNotPresent knox-${KNOX_VERSION}.tar.gz            https://archive.apache.org/dist/knox/${KNOX_VERSION}
    extractOzoneIfNeeded
    downloadIfNotPresent opensearch-${OPENSEARCH_VERSION}-linux-x64.tar.gz https://artifacts.opensearch.org/releases/bundle/opensearch/${OPENSEARCH_VERSION}
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
      downloadIfNotPresent apache-tez-${TEZ_VERSION}-bin.tar.gz   https://archive.apache.org/dist/tez/${TEZ_VERSION}
    elif [[ $arg == 'kafka' ]]
    then
      downloadIfNotPresent kafka_2.12-${KAFKA_VERSION}.tgz        https://archive.apache.org/dist/kafka/${KAFKA_VERSION}
    elif [[ $arg == 'knox' ]]
    then
      downloadIfNotPresent knox-${KNOX_VERSION}.tar.gz            https://archive.apache.org/dist/knox/${KNOX_VERSION}
    elif [[ $arg == 'ozone' ]]
    then
      extractOzoneIfNeeded
    elif [[ $arg == 'opensearch' ]]
    then
      downloadIfNotPresent opensearch-${OPENSEARCH_VERSION}-linux-x64.tar.gz https://artifacts.opensearch.org/releases/bundle/opensearch/${OPENSEARCH_VERSION}
    else
      echo "Passed argument $arg is invalid!"
    fi
  done
fi
