<!--
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
# Apache Ranger

[![License](https://img.shields.io/:license-Apache%202-green.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![CI](https://github.com/apache/ranger/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/ranger/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-apache.org-blue.svg)](https://ranger.apache.org)
[![Docker Pulls](https://img.shields.io/docker/pulls/apache/ranger)](https://hub.docker.com/r/apache/ranger)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/apache-ranger?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads)](https://pypi.org/project/apache-ranger/)

Apache Ranger is a framework for centralized, fine-grained authorization and auditing across data and AI
platforms. Policies are managed in one place, Ranger Admin, through its web UI or REST API, and are enforced by
Ranger plugins inside the protected services, which also record an audit trail of access decisions. Policies can
be based on resources, tags or attributes, and can filter rows and mask columns.

Documentation, release notes and downloads: <https://ranger.apache.org/>

## Quick start

Released versions of Ranger Admin are published on Docker Hub as
[`apache/ranger`](https://hub.docker.com/r/apache/ranger). It runs with a database,
[`apache/ranger-db`](https://hub.docker.com/r/apache/ranger-db) (PostgreSQL), and an audit store,
[`apache/ranger-solr`](https://hub.docker.com/r/apache/ranger-solr); the `apache/ranger` page has the commands to
start all three. Then open <http://localhost:6080> and log in as `admin` / `rangerR0cks!`.

These images are for evaluation and development; they use well-known default passwords.

## Plugins

Ranger Admin stores policies; each service enforces them through a Ranger plugin that runs inside it. To protect a
service, set up its plugin. The plugin downloads the service's policies from Ranger Admin, authorizes each request
locally and sends audit events to the audit store.

| Service | Plugin runs in | Plugin ships with |
|---|---|---|
| Apache Polaris | Polaris server | Apache Polaris |
| Trino | Trino coordinator | Trino |
| Apache Impala | `impalad` and `catalogd` | Apache Impala |
| Apache Ozone | Ozone Manager | Apache Ranger |
| Apache Kudu | Kudu master | Apache Kudu |
| Schema Registry | Schema Registry server | Schema Registry |
| Presto | Presto coordinator | Apache Ranger |
| Elasticsearch | Elasticsearch nodes | Apache Ranger |
| Apache Kylin | Kylin server | Apache Ranger |
| Apache Sqoop | Sqoop 2 server | Apache Ranger |
| Apache NiFi Registry | NiFi Registry server | Apache NiFi |
| Apache NiFi | NiFi nodes | Apache NiFi |
| Apache Atlas | Atlas server | Apache Ranger |
| Apache Kafka | Kafka brokers | Apache Ranger |
| Apache Solr | Solr nodes | Apache Ranger |
| Apache Ranger KMS | Ranger KMS server | Apache Ranger |
| Apache Hadoop YARN | ResourceManager | Apache Ranger |
| Apache Knox | Knox gateway | Apache Ranger |
| Apache Storm | Nimbus | Apache Ranger |
| Apache HBase | HBase Master and RegionServers | Apache Ranger |
| Apache Hive | HiveServer2 | Apache Ranger |
| Apache Hadoop HDFS | NameNode | Apache Ranger |

Plugins that ship with Apache Ranger are released as `ranger-<version>-<service>-plugin.tar.gz` archives, also
written to `target/` by a source build; the KMS plugin is part of this repo. The other services ship the
plugin themselves and enable it in their own configuration. Applications not listed above can authorize requests
with the Ranger authorization API (`authz-api`), either evaluating policies in process (`authz-embedded`) or
sending each request to the PDP server (`authz-remote`, available from the 2.9 release onwards).

## Build from source

### Requirements

- Linux or macOS
- JDK 17
- Apache Maven 3.6.3 or newer
- Git
- Python 3, for the Python client tests (not needed with `-DskipTests`)
- Docker with Compose v2, only to build or run Ranger in containers

### Build

```bash
git clone https://github.com/apache/ranger.git
cd ranger
mvn clean install                # full build: unit tests and code checks
mvn clean package -DskipTests    # faster: skips unit tests and code checks
```

The build writes a `ranger-<version>-<component>.tar.gz` archive for each service, plugin and tool to `target/`,
for example `ranger-<version>-admin.tar.gz`.

To build without a local JDK or Maven, use the build container in
[`dev-support/ranger-docker`](dev-support/ranger-docker/README.md#in-containers-using-docker-compose); it writes
the archives to `dev-support/ranger-docker/dist/`.

To work in an IDE, open the root `pom.xml` as a Maven project. An IntelliJ IDEA code style scheme is in
[`dev-support/RangerCodeScheme-IntelliJ.xml`](dev-support/RangerCodeScheme-IntelliJ.xml).

## Run your build in Docker

[`dev-support/ranger-docker`](dev-support/ranger-docker/README.md) builds images from these archives and runs
Ranger Admin with its database and audit store, the other Ranger services, and services with Ranger plugins
enabled, such as Trino, Ozone, Kafka, Hive and HBase. CI uses the same setup. Its README has the steps; like the
Docker Hub images, it is meant for development only.

## Contributing

Contributions are accepted as GitHub pull requests.

1. Find or file an issue in the [RANGER JIRA project](https://issues.apache.org/jira/browse/RANGER). Discuss
   larger changes on dev@ranger.apache.org first.
2. Open a pull request against `master` titled `RANGER-XXXX: <subject>` and fill in the template.
3. Keep CI green. It runs `mvn clean verify` on JDK 17 (unit tests, checkstyle, PMD, SpotBugs, RAT) and checks
   that the Docker containers start.

A committer merges the pull request once it is approved. See the
[contributing guide](mkdocs/docs/project/contributing.md) and the
[code style guide](mkdocs/docs/project/java-code-style.md).

## Reporting security issues

Report suspected vulnerabilities privately to security@apache.org, not in JIRA, GitHub or the mailing lists.
See [SECURITY.md](SECURITY.md).

## Community

- Questions about using Ranger: user@ranger.apache.org ([subscribe](mailto:user-subscribe@ranger.apache.org))
- Development: dev@ranger.apache.org ([subscribe](mailto:dev-subscribe@ranger.apache.org))
- Chat: [Ranger channel on the ASF Slack](https://the-asf.slack.com/archives/C4SC5NXAA)
- Issues: [RANGER JIRA project](https://issues.apache.org/jira/browse/RANGER)

## License

Apache Ranger is licensed under the [Apache License, Version 2.0](LICENSE.txt).
