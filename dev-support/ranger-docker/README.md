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

Use Dockerfiles in this directory to create docker images and run them to build Apache Ranger, deploy Apache Ranger and dependent services in containers.

### Environment Setup

- Ensure that you have recent version of Docker installed from [docker.io](http://www.docker.io) (as of this writing: Engine v24.0.5, Compose v2.20.2).
   Make sure to configure docker with at least 6gb of memory.

- Update environment variables in ```.env``` file, if necessary

- Set ```dev-support/ranger-docker``` as your working directory.

- Execute following command to download necessary archives to setup Ranger/HDFS/Hive/HBase/Kafka/Knox/Ozone services:
   ~~~
   chmod +x download-archives.sh
   # use a subset of the below to download specific services
   ./download-archives.sh hadoop hive hbase kafka knox ozone
   ~~~

- Execute following commands to set environment variables to build Apache Ranger docker containers:
   ~~~
   export RANGER_DB_TYPE=postgres
  
  # valid values for RANGER_DB_TYPE: mysql/postgres/oracle
   ~~~

### Apache Ranger Build

#### In containers using docker compose

Execute following command to build Apache Ranger:
~~~
# optional step: a fresh build ensures that the correct jdk version is used
docker compose -f docker-compose.ranger-build.yml build

docker compose -f docker-compose.ranger-build.yml up -d
~~~
Time taken to complete the build might vary (upto an hour), depending on status of ```${HOME}/.m2``` directory cache.  


#### OR
#### Regular build

~~~
cd ./../../
mvn clean package -DskipTests
cp target/ranger-* dev-support/ranger-docker/dist/
cp target/version dev-support/ranger-docker/dist/
cd dev-support/ranger-docker
~~~

### Run Ranger Services in Containers

#### Bring up ranger-core services: ranger, usersync, tagsync and ranger-kms in containers
~~~
# To enable file based sync source for usersync do:
# export ENABLE_FILE_SYNC_SOURCE=true

# valid values for RANGER_DB_TYPE: mysql/postgres/oracle

docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-kms.yml up -d

# Ranger Admin can be accessed at http://localhost:6080 (admin/rangerR0cks!)
~~~
#### Bring up hive container
~~~
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hive.yml up -d
~~~
#### Bring up hbase container
~~~
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml up -d
~~~
#### Bring up ozone containers
~~~
./scripts/ozone-plugin-docker-setup.sh
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-ozone.yml up -d
~~~
#### Bring up trino container (requires docker build with jdk 11):
~~~
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-trino.yml up -d
~~~
Similarly, check the `depends` section of the `docker-compose.ranger-service.yaml` file and add docker-compose files for these services when trying to bring up the `service` container.

#### Bring up all containers
~~~
./scripts/ozone-plugin-docker-setup.sh
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-kms.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-hive.yml -f docker-compose.ranger-knox.yml -f docker-compose.ranger-ozone.yml up -d
~~~
          
#### To rebuild specific images and start containers with the new image:
~~~
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-kms.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-hive.yml -f docker-compose.ranger-trino.yml -f docker-compose.ranger-knox.yml up -d --no-deps --force-recreate --build <service-1> <service-2>
~~~
