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
   Make sure to configure docker with at least 8gb of memory.

- Update environment variables in ```.env``` file, if necessary

- Set ```dev-support/ranger-docker``` as your working directory.

- Execute following command to download necessary archives to setup Ranger/HDFS/Hive/HBase/Kafka/Knox/Ozone/OpenSearch services:
   ~~~
   chmod +x download-archives.sh
   # use a subset of the below to download specific services
   ./download-archives.sh hadoop hive hbase kafka knox ozone opensearch
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

chmod +x scripts/**/*.sh

# optional step: a fresh build ensures that the correct jdk version is used
docker compose -f docker-compose.ranger-build.yml build

docker compose -f docker-compose.ranger-build.yml up
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

#### Bring up ranger-core services: ranger, usersync, tagsync, pdp and kms in containers
~~~
# To enable file based sync source for usersync do:
# export ENABLE_FILE_SYNC_SOURCE=true

# valid values for RANGER_DB_TYPE: mysql/postgres/oracle

docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-solr.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-pdp.yml -f docker-compose.ranger-kms.yml up -d

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

##### Ozone action-matcher feature flag

The flag is controlled at runtime by
`ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition` in
`ranger-admin-site.xml`. Docker maps it from
`FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION` in
`scripts/admin/ranger-admin-install-*.properties` during Admin setup.

**Prerequisite:** the admin distribution must include an uncommented property
entry in `conf.dist/ranger-admin-site.xml`. Rebuild dist before building the
Admin image:

~~~
docker compose -f docker-compose.ranger-build.yml build
docker compose -f docker-compose.ranger-build.yml up
~~~

**First-time setup:** set `FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION=true` in
`scripts/admin/ranger-admin-install-<db>.properties`, then bring up services.

**Changing the flag later** (choose one):

1. Edit `FF_ENABLE_OZONE_ACTION_MATCHES_CONDITION` in
   `scripts/admin/ranger-admin-install-<db>.properties` and recreate Admin so
   setup runs again:
   ~~~
   docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-solr.yml -f docker-compose.ranger-ozone.yml \
     up -d --build --force-recreate ranger
   ~~~
   Works even if the DB already exists; setup re-runs on a fresh container.

2. Edit the live value in the container's
   `ranger-admin-site.xml` and restart Admin.

`install.properties` is not re-read on restart alone — only when setup runs.
`RangerServiceDefService` applies the site.xml value when serving the ozone
service-def.

~~~
./scripts/ozone/ozone-plugin-docker-setup.sh
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-solr.yml -f docker-compose.ranger-ozone.yml up -d
~~~

Verify (after login):

~~~
curl -s -u admin:rangerR0cks! http://localhost:6080/service/plugins/definitions/name/ozone \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print('options', d.get('options')); print('conditions', [c['name'] for c in d.get('policyConditions',[])])"
~~~
#### Bring up trino container (requires docker build with jdk 11):
~~~
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-trino.yml up -d
~~~
#### Bring up solr container:
~~~
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-solr.yml up -d
~~~
#### Bring up opensearch container:
~~~
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-opensearch.yml up -d
~~~

#### OpenSearch audit flow (replace Solr for access audits)

OpenSearch can replace Solr for **audit storage and UI queries**. Ranger Admin reads audits via
`audit_store=opensearch` using a native low-level REST client (compatible with any OpenSearch version).

**Write path:** access audits flow through audit-server ingestor, Kafka, and the Java
`ranger-audit-dispatcher-opensearch` service into the OpenSearch `ranger_audits` index.
Ranger Admin policy/admin transaction audits remain DB-backed; this is the same boundary
used by the Solr audit path.

##### Setup

With the default `RANGER_DB_TYPE=postgres`, OpenSearch auditing is preconfigured and runs
out of the box — the commands below need no `install.properties` changes.

~~~
# Prerequisites: build Ranger artifacts (admin, audit ingestor/dispatcher, ...) and download archives
mvn clean package -DskipTests -pl distro -am
cp target/ranger-* dev-support/ranger-docker/dist/
cd dev-support/ranger-docker
./download-archives.sh kafka opensearch hadoop

export RANGER_DB_TYPE=postgres

# 1. Start OpenSearch first (Ranger Admin's bootstrapper needs it on startup)
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-opensearch.yml \
  -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-hadoop.yml \
  -f docker-compose.ranger-audit-ingestor.yml \
  -f docker-compose.ranger-audit-dispatcher-opensearch.yml up -d ranger-opensearch

# 2. Start core stack (Ranger Admin, Kafka, Hadoop)
#    Kafka auto-creates the ranger_audits topic on startup.
#    Ranger Admin auto-creates the OpenSearch index via OpenSearchIndexBootStrapper.
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-opensearch.yml \
  -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-hadoop.yml \
  -f docker-compose.ranger-audit-ingestor.yml \
  -f docker-compose.ranger-audit-dispatcher-opensearch.yml up -d ranger ranger-kafka ranger-hadoop

# 3. Start audit ingestor and OpenSearch dispatcher
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-opensearch.yml \
  -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-hadoop.yml \
  -f docker-compose.ranger-audit-ingestor.yml \
  -f docker-compose.ranger-audit-dispatcher-opensearch.yml up -d ranger-audit-ingestor ranger-audit-dispatcher-opensearch
~~~

To use OpenSearch with **mysql or oracle** instead, enable the OpenSearch block in the matching
`scripts/admin/ranger-admin-install-${RANGER_DB_TYPE}.properties` (uncomment the `audit_store=opensearch`
and `audit_opensearch_*` lines and comment out the Solr block) before running the setup commands.

For **existing Solr-based installs**, switch stores by setting `audit_store=opensearch` (and the
`audit_opensearch_*` properties) in install.properties and restarting Ranger Admin.
Similarly, check the `depends` section of the `docker-compose.ranger-service.yaml` file and add docker-compose files for these services when trying to bring up the `service` container.

#### Bring up all containers
~~~
./scripts/ozone/ozone-plugin-docker-setup.sh
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-solr.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-pdp.yml -f docker-compose.ranger-kms.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-hive.yml -f docker-compose.ranger-knox.yml -f docker-compose.ranger-ozone.yml up -d
~~~
          
#### To rebuild specific images and start containers with the new image:
~~~
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-solr.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-kms.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-hive.yml -f docker-compose.ranger-trino.yml -f docker-compose.ranger-knox.yml up -d --no-deps --force-recreate --build <service-1> <service-2>
~~~

#### To bring up audit server ingestor + dispatchers. Make sure kafka, solr, and hdfs containers are running before bringing up audit server services.
~~~
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-solr.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-audit-server.yml up -d
~~~

#### To bring up audit server services individually:
~~~
# Audit ingestor
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-audit-ingestor.yml up -d

# Solr dispatcher (requires the ranger-solr container; see "Bring up solr container" above)
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-solr.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-audit-dispatcher-solr.yml up -d

# OpenSearch dispatcher (requires the ranger-opensearch container; see "OpenSearch audit flow" above)
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-opensearch.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-audit-dispatcher-opensearch.yml up -d

# HDFS dispatcher
docker compose -f docker-compose.ranger.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-kafka.yml -f docker-compose.ranger-audit-dispatcher-hdfs.yml up -d
~~~
