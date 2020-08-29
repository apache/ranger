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

1. Ensure that you have recent version of Docker installed from [docker.io](http://www.docker.io) (as of this writing: Engine 19.03, Compose 1.26.2).

2. Set this folder as your working directory.

3. Using docker-compose is the simpler way to build and deploy Apache Ranger in containers.

   3.1. Execute following command to build Apache Ranger:

        docker-compose -f docker-compose.ranger-base.yml -f docker-compose.ranger-build.yml up --remove-orphans

   Time taken to complete the build might vary (upto an hour), depending on status of ${HOME}/.m2 directory cache.

   3.2. Execute following command to start Ranger, Ranger enabled HDFS/YARN/HBase/Kafka and dependent services (Solr, DB) in containers:

        docker-compose -f docker-compose.ranger-base.yml -f docker-compose.ranger.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml -f docker-compose.ranger-kafka.yml up -d

4. Alternatively docker command can be used to build and deploy Apache Ranger.

   4.1. Execute following command to build Docker image **ranger-base**:

        docker build -f Dockerfile.ranger-base -t ranger-base .

   This might take about 10 minutes to complete.

   4.2. Execute following command to build Docker image **ranger-build**:

        docker build -f Dockerfile.ranger-build -t ranger-build .

   4.3. Build Apache Ranger in a container with the following command:

        docker run -it --rm -v ${HOME}/.m2:/home/ranger/.m2 -v $(pwd)/dist:/home/ranger/dist -e BRANCH=ranger-2.1 -e PROFILE=all -e SKIPTESTS=true ranger-build

   Time taken to complete the build might vary (upto an hour), depending on status of ${HOME}/.m2 directory cache.

   4.4. Execute following command to build Docker image **ranger**:

        docker build -f Dockerfile.ranger -t ranger .

   This might take about 10 minutes to complete.

   4.5. Execute following command to build a Docker image **ranger-solr**:

        docker build -f Dockerfile.ranger-solr -t ranger-solr .

   4.6. Execute following command to start a container that runs database for use by Ranger Admin:

        docker run --name ranger-db --hostname ranger-db.example.com -e POSTGRES_PASSWORD='rangerR0cks!' -d postgres:12

   4.7. Execute following command to start a container that runs Solr for use by Ranger Admin:

        docker run --name ranger-solr --hostname ranger-solr.example.com -p 8983:8983 -d ranger-solr solr-precreate ranger_audits /opt/solr/server/solr/configsets/ranger_audits/

   4.8. Execute following command to install and run Ranger services in a container:

        docker run -it -d --name ranger --hostname ranger.example.com -p 6080:6080 --link ranger-db:ranger-db --link ranger-solr:ranger-solr ranger

   This might take few minutes to complete.

   4.9. Execute following command to build Docker image **ranger-hadoop**:

        docker build -f Dockerfile.ranger-hadoop -t ranger-hadoop .

   This step includes downloading of Hadoop tar balls, and can take a while to complete.

   4.10. Execute following command to install and run Ranger enabled HDFS in a container:

         docker run -it -d --name ranger-hadoop --hostname ranger-hadoop.example.com -p 9000:9000 -p 8088:8088 --link ranger:ranger --link ranger-solr:ranger-solr ranger-hadoop

   This might take few minutes to complete.

   4.11. Execute following command to build Docker image **ranger-hbase**:

         docker build -f Dockerfile.ranger-hbase -t ranger-hbase .

   This step includes downloading of HBase tar ball, and can take a while to complete.

   4.12. Execute following command to install and run Ranger enabled HBase in a container:

         docker run -it -d --name ranger-hbase --hostname ranger-hbase.example.com --link ranger-hadoop:ranger-hadoop --link ranger:ranger --link ranger-solr:ranger-solr ranger-hbase

   This might take few minutes to complete.

   4.13. Execute following command to build Docker image **ranger-kafka**:

         docker build -f Dockerfile.ranger-kafka -t ranger-kafka .

   This step includes downloading of Kafka tar ball, and can take a while to complete.

   4.12. Execute following command to install and run Ranger enabled Kafka in a container:

         docker run -it -d --name ranger-kafka --hostname ranger-kafka.example.com --link ranger-hadoop:ranger-hadoop --link ranger:ranger --link ranger-solr:ranger-solr ranger-kafka

   This might take few minutes to complete.

5. Ranger Admin can be accessed at http://localhost:6080 (admin/rangerR0cks!)
