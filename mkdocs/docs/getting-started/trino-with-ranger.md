---
title: "Trino with Ranger"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

[DockerHub]: https://hub.docker.com/r/apache/ranger
## Introduction

This guide will walk you through the steps to run Trino with Apache Ranger as the Access Control Enforcer.

### Run Trino Container

```shell title="Run Trino in Ranger's Network"
docker pull trinodb/trino
docker run -p 8080:8080 --name trino --network rangernw trinodb/trino

# for more details: https://hub.docker.com/r/trinodb/trino
```

### Configure Ranger Plugin in Trino Container

```.properties title="Update access-control.properties in /etc/trino/ in Trino container"
access-control.name=ranger
ranger.service.name=dev_trino
ranger.plugin.config.resource=/etc/trino/ranger-trino-security.xml,/etc/trino/ranger-trino-audit.xml,/etc/trino/ranger-policymgr-ssl.xml
ranger.hadoop.config.resource=

# For details to configure Apache Ranger: https://trino.io/docs/current/security/ranger-access-control.html
```

### Restart Trino Container
