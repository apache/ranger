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

# ranger-plugins-common (`agents-common`)

Core policy engine and shared library for all Apache Ranger service plugins. Every Ranger authorization plugin (HDFS, Hive, Kafka, HBase, etc.) depends on this module to evaluate access requests against centrally managed policies.

## How It Works

Service plugins extend `RangerBasePlugin`, which handles:

1. **Policy synchronization** - background download of policies from Ranger Admin via `PolicyRefresher`
2. **Access evaluation** - request preprocessing (enrichment with tags, user-store, GDS metadata), zone-aware policy lookup via `RangerResourceTrie`, and per-policy evaluation
3. **Audit generation** - producing audit events routed to configured destinations

## Usage

```java
// Create and initialize plugin
RangerHdfsPlugin plugin = new RangerHdfsPlugin(configFile);
plugin.init();

// Evaluate access
Map<String, Object> resource = new HashMap<>();
resource.put("path", "/data/finance");

RangerAccessRequestImpl request = new RangerAccessRequestImpl();
request.setResource(new RangerAccessResourceImpl(resource));
request.setUser("alice");
request.setUserGroups(new HashSet<>(Arrays.asList("finance-team")));
request.setAccessType("read");

RangerAccessResult result = plugin.isAccessAllowed(request);
// result.getIsAllowed(), result.getPolicyId(), result.getMaskType()

// Shutdown
plugin.cleanup();
```

For a real integration example, see `hdfs-agent/src/main/java/org/apache/ranger/authorization/hadoop/RangerHdfsPlugin.java`.

## Service Definitions

JSON service definitions in `src/main/resources/service-defs/` declare the resource hierarchy, access types, policy conditions, and context enrichers for each supported service:

HDFS, Hive, HBase, Kafka, Knox, Solr, Storm, YARN, Sqoop, Kylin, Elasticsearch, Atlas, KMS, Ozone, NiFi, NiFi-Registry, Presto, Trino, Schema-Registry, Kudu, WASB, ABFS, Tag, GDS, NestedStructure, Polaris.

## Building

```bash
# Build this module and its dependencies (from the repository root)
mvn clean install -pl agents-common -am -DskipTests

# Rebuild only this module (after dependencies are already built)
mvn clean install -pl agents-common -DskipTests

# Run tests
mvn test -pl agents-common -am
```

## Testing

Tests are data-driven - JSON fixtures in `src/test/resources/policyengine/` define policies, access requests, and expected results. The engine evaluates each fixture and asserts the outcome matches.

## Adding as a Dependency

For plugin developers building a new Ranger authorization plugin:

```xml
<dependency>
  <groupId>org.apache.ranger</groupId>
  <artifactId>ranger-plugins-common</artifactId>
  <version>${ranger.version}</version>
</dependency>
```
