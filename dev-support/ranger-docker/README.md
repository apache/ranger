<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Overview

Docker files in this folder create docker images and run them to build Apache Ranger, deploy Apache Ranger and dependent services in containers.

## Usage

1. Ensure that you have recent version of Docker installed from [docker.io](http://www.docker.io) (as of this writing: Engine 20.10.5, Compose 1.28.5).
   Make sure to configure docker with at least 6gb of memory.

2. Set this folder as your working directory.

3. Update environment variables in .env file, if necessary

4. Execute following command to download necessary archives to setup Ranger/HDFS/Hive/HBase/Kafka services:
     ./download-archives.sh

5. Build and deploy Apache Ranger in containers using docker-compose

   5.1. Execute following command to build Apache Ranger:

        docker-compose -f docker-compose.ranger-base.yml -f docker-compose.ranger-build.yml up

   Time taken to complete the build might vary (upto an hour), depending on status of ${HOME}/.m2 directory cache.

   5.2. Execute following command to start Ranger, Ranger enabled HDFS/YARN/HBase/Kafka and dependent services (Solr, DB) in containers:

        docker-compose -f docker-compose.ranger-base.yml -f docker-compose.ranger.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-hive.yml up -d

6. Ranger Admin can be accessed at http://localhost:6080 (admin/rangerR0cks!)
