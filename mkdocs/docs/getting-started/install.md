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
# Installation
!!! tip

    If you're new to Docker 🐳, we recommend reading [Docker Docs],
    which provides extensive documentation around how to get started with Docker.
### docker <small>recommended</small> { #docker data-toc-label="docker" }

The official [Docker image] is a great way to get up and running in a few
minutes, as it comes with all dependencies pre-installed. It contains instructions to pull the images and run docker containers for Apache Ranger and dependent services (`Postgres DB`, `Apache Solr` & `Apache Zookeeper`).

???+ warning

    The Docker container is intended for development purposes only and
    is not suitable for production based deployment.
    [Docker Image]: https://hub.docker.com/r/apache/ranger


