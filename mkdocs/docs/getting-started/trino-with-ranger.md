---
title: "Trino with Ranger"
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

[DockerHub]: https://hub.docker.com/r/apache/ranger

# Trino with Ranger

This guide walks you through the steps to run Trino with Apache Ranger as the access control
enforcer. Trino ships its own Ranger access control plugin, so the Trino container only needs to be
pointed at a Ranger service and given the Ranger plugin configuration files. Ranger Admin is assumed
to be running on the `rangernw` Docker network, as described in [Run Ranger with Docker](docker.md).

## Run the Trino container

=== "Docker Hub images"

    With Ranger Admin started from the [DockerHub] images, run the image published by Trino on the
    same network, then follow the remaining sections of this page to configure it:

    ```shell title="Run Trino in Ranger's network"
    docker pull trinodb/trino
    docker run -p 8080:8080 --name trino --network rangernw trinodb/trino

    # for more details: https://hub.docker.com/r/trinodb/trino
    ```

=== "Build from source (dev-support/ranger-docker)"

    `docker-compose.ranger-trino.yml` builds a `ranger-trino` container from `trinodb/trino` with the
    files from `scripts/trino` already in `/etc/trino/`, and Ranger Admin creates the `dev_trino`
    service on start. Nothing else on this page needs to be done by hand:

    ```shell
    docker compose --profile ${AUDIT_DESTINATIONS} -f docker-compose.ranger.yml -f docker-compose.ranger-audit-service.yml -f docker-compose.ranger-trino.yml up -d
    ```

    The shipped `ranger-trino-audit.xml` sends audits directly to Solr at `ranger-solr.rangernw:8983`;
    start the setup with `AUDIT_INDEX_STORE=solr` to see Trino audits in Ranger Admin.

## Create the Trino service in Ranger Admin

In **Service Manager**, unless it is already listed, add a new service of type Trino named `dev_trino` (the name referenced by
`ranger.service.name` below). See the [Trino plugin](../plugins/trino.md) page for the service
configuration properties.

## Configure the Ranger plugin in the Trino container

```properties title="Update access-control.properties in /etc/trino/ in the Trino container"
access-control.name=ranger
ranger.service.name=dev_trino
ranger.plugin.config.resource=/etc/trino/ranger-trino-security.xml,/etc/trino/ranger-trino-audit.xml,/etc/trino/ranger-policymgr-ssl.xml
ranger.hadoop.config.resource=

# For details to configure Apache Ranger: https://trino.io/docs/current/security/ranger-access-control.html
```

Copy `ranger-trino-security.xml`, `ranger-trino-audit.xml` and `ranger-policymgr-ssl.xml` into
`/etc/trino/`. Working examples, pointing at `http://ranger:6080` and the `dev_trino` service, are in
[`dev-support/ranger-docker/scripts/trino`](https://github.com/apache/ranger/blob/master/dev-support/ranger-docker/scripts/trino).
The Docker Hub quick start names the Ranger Admin container `ranger-admin`, so with those images set
`ranger.plugin.trino.policy.rest.url` to `http://ranger-admin:6080`.

## Restart the Trino container

```shell
docker restart trino
```

After the restart, Trino downloads the `dev_trino` policies and every query is authorized by Ranger.
Confirm the plugin is connected under **Audit → Plugin Status** in Ranger Admin, then create
policies for catalogs, schemas, tables and columns as described in
Resource-based policies.
