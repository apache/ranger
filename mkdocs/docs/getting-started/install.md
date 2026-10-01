---
title: Getting started with Ranger
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

[Docker Docs]: https://docs.docker.com/get-started/overview/
[Docker Hub]: https://hub.docker.com/r/apache/ranger

# Installation

There are two ways to get Apache Ranger running, and both use Docker: start a released version from
the images on [Docker Hub], or build the current source and run it with the Docker Compose setup in
`dev-support/ranger-docker`. Both end with Ranger Admin listening on port 6080, and both are
described step by step in [Run Ranger with Docker](docker.md).

!!! tip

    If you're new to Docker, we recommend reading [Docker Docs],
    which provides extensive documentation around how to get started with Docker.

## Which setup should I use?

Docker Hub images
:   Choose this to try a **released** version of Ranger Admin. Three containers are started with
    `docker run`: `apache/ranger`, `apache/ranger-db` (PostgreSQL) and `apache/ranger-solr` (the audit
    store); `apache/ranger-zk` is only needed when Solr runs in SolrCloud mode. You need Docker and
    nothing else. No other Ranger service or plugin image is published, so this setup ends at Ranger
    Admin: you can explore the UI and the REST API, define services and policies, and point your own
    plugin-enabled service at it. Go to [Docker Hub images](docker.md#start).

Build from source (`dev-support/ranger-docker`)
:   Choose this to try the latest code, to develop Ranger, or to run UserSync, TagSync, KMS, PDP and
    Hadoop, Hive, HBase, Kafka, Knox, Ozone or Trino containers with Ranger authorization already
    configured. On master the minimal setup is Ranger Admin, its database, Kafka, OpenSearch, the
    audit ingestor and an audit dispatcher; the audit server is not yet part of a release, which is
    why it is available only here. You need Docker with Compose v2 and git; a local JDK and Maven are
    optional because Ranger can be built in a container. Go to
    [Prepare the development setup](docker.md#build-from-source). [Your first policy](first-policy.md)
    uses this setup.

!!! warning

    Both setups are intended for development and evaluation and are not suitable for production
    deployments.

To build Ranger yourself and to learn which archives the build produces, Released source and binary archives are listed on the
[download page](../release-notes/download.md).

This page does not describe how to configure each Ranger service or plugin for your own environment.
That lives with the component: see [Configure Ranger for your environment](#configure-ranger-for-your-environment).

## Default credentials

Ranger Admin in Docker: user `admin`, password `rangerR0cks!`. In the development setup the passwords
of the built-in accounts (`admin`, `rangerusersync`, `rangertagsync`, `keyadmin`) are taken from the
`RANGER_*_PASSWORD` variables in `dev-support/ranger-docker/.env` when the database is first prepared;
all default to `rangerR0cks!`. These are development credentials; change them before exposing Ranger
Admin to anyone else.

## Configure Ranger for your environment

Each Ranger service reads its settings from site configuration files, and each plugin reads a set of
`ranger-<service>-*.xml` files from the classpath of the service it protects. The reference for each
lives on its own page:

- [Ranger Admin](../services/admin/service.md), with Database and
  Authentication
- [UserSync](../services/usersync/service.md) and [TagSync](../services/tagsync/service.md)
- [KMS](../services/kms/service.md), PDP and the
  Audit server

## Next steps

- [Your first policy](first-policy.md) — create a policy and watch it being enforced.
- [Run Ranger with Docker](docker.md) — both setups, every compose file, port and option.
