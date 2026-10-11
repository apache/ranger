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
   Make sure to configure docker with at least 4gb of memory for the full stack (Ranger core services default to 256 MB JVM heap each; see `.env`).

- Update environment variables in ```.env``` file, if necessary. JVM heap per service is controlled by `RANGER_*_MAX_HEAP` variables; docker compose passes them into each container and upstream start scripts read them when building `JAVA_OPTS`. Non-docker installs keep upstream defaults (typically `1g`) when these variables are unset.

- Set ```dev-support/ranger-docker``` as your working directory.

- Execute following command to download necessary archives to setup Ranger/HDFS/Hive/HBase/Kafka/Knox/Ozone/OpenSearch services:
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

- Ranger Admin container is configured at runtime from files in `scripts/admin/configs`, mounted at `/opt/ranger/admin/configs`:
  - `ranger-admin-site.xml`, when mounted at `/opt/ranger/admin/configs`, is used as it is.
  - if `ranger-admin-site.xml` is not present, then `ranger-admin-site-${RANGER_DB_TYPE}.yaml` is the complete Ranger Admin configuration, as a flat map of property name to value. On start, `scripts/admin/dba.py` renders `conf/ranger-admin-site.xml` from it, without using the configuration shipped in the admin distribution; update this file to change Ranger Admin configuration.
  - any other file in this directory (for example, `logback.xml`) is copied to `conf/` as-is.
  - passwords of the database user and built-in users (admin, rangerusersync, rangertagsync, keyadmin) are read from `RANGER_*_PASSWORD` variables in `.env`.
  - container logs (`docker logs ranger`) consist of logs from `dba.py` (with progress of database/java patches), `create-ranger-services.py`, `catalina.out` and Ranger Admin log (`ranger-admin-<hostname>-<user>.log`), which continues to be written to `/var/log/ranger` as well. A log line marks the moment Ranger Admin is ready. Logs are colored when the container has a TTY (as with docker compose); set `NO_COLOR` to disable colors.
  - trusted header authentication (`X-Forwarded-User`) is enabled, so that the readiness endpoint `/service/actuator/health/readiness` can be queried as `healthcheck` user; any client reaching Ranger Admin can use this header, so enable it only behind a trusted proxy outside of this development setup.

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

Every container declares a healthcheck, so `docker ps` reports each service as `healthy` only
once it is actually serving. Add `--wait` to any of the `up -d` commands below to block until
that is true (and fail if a container never gets there) instead of polling `docker ps`:

~~~
docker compose ... up -d --wait --wait-timeout 900
~~~

#### Bring up ranger-core services: ranger, usersync, tagsync, pdp, kms and audit in containers
~~~
# To enable file based sync source for usersync do:
export ENABLE_FILE_SYNC_SOURCE=true

# valid values for RANGER_DB_TYPE: mysql/postgres/oracle
export RANGER_DB_TYPE=postgres

# valid values for AUDIT_INDEX_STORE: opensearch (default) | solr
# overrides ranger.audit.source.type of Ranger Admin (via ranger.sh); set matching profile for compose:
export AUDIT_INDEX_STORE=opensearch
export AUDIT_DESTINATIONS=audit-store-${AUDIT_INDEX_STORE}
docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-pdp.yml -f docker-compose.ranger-kms.yml up -d

# Ranger Admin can be accessed at http://localhost:6080 (admin/rangerR0cks!)
~~~
#### Bring up hive container
~~~
docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hive.yml up -d
~~~
#### Bring up hbase container
~~~
docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml up -d
~~~
#### Bring up ozone containers
~~~
./scripts/ozone/ozone-plugin-docker-setup.sh
docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-ozone.yml up -d
~~~

#### Bring up trino container (requires docker build with jdk 11):
~~~
docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-trino.yml up -d
~~~


#### Bring up all containers
~~~
./scripts/ozone/ozone-plugin-docker-setup.sh
docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-pdp.yml -f docker-compose.ranger-kms.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml -f docker-compose.ranger-hive.yml -f docker-compose.ranger-knox.yml -f docker-compose.ranger-ozone.yml up -d
~~~
          
#### To rebuild specific images and start containers with the new image:
~~~
docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-usersync.yml -f docker-compose.ranger-tagsync.yml -f docker-compose.ranger-kms.yml -f docker-compose.ranger-hadoop.yml -f docker-compose.ranger-hbase.yml -f docker-compose.ranger-hive.yml -f docker-compose.ranger-trino.yml -f docker-compose.ranger-knox.yml up -d --no-deps --force-recreate --build <service-1> <service-2>
~~~

##### Also send audits to HDFS
Audits fan out to `AUDIT_INDEX_STORE` **and** HDFS when the `audit-store-hdfs` profile is enabled:
~~~
export AUDIT_DESTINATIONS=audit-store-${AUDIT_INDEX_STORE}
docker compose --profile ${AUDIT_DESTINATIONS} --profile audit-store-hdfs \
  -f docker-compose.ranger.yml \
  -f docker-compose.ranger-audit-service.yml \
  -f docker-compose.ranger-audit-destination-hdfs.yml up -d
~~~
