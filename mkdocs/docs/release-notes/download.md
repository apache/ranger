---
title: "Download and verify"
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

# Download and verify Apache Ranger

Apache Ranger is distributed as a source tarball, as binary tarballs for each service and plugin, as Docker
images and as Maven artifacts. All releases are made under the
[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0); the `LICENSE` and `NOTICE` files in
each artifact list the terms of the bundled third-party components.

Whatever you download, verify it. Every file published by the project comes with a detached OpenPGP signature
(`.asc`) and a checksum file (`.sha512`, and `.sha256` for the source tarball). The signing keys are in the
project's `KEYS` file. Verification takes a minute and protects you from a corrupted mirror or a tampered file.

## Current release

The current stable release is **Apache Ranger 2.9.0** (August 8, 2026); see the
[2.9.0 release notes](2.9.0.md). Download it from the ASF download server, which serves the closest mirror:

| Artifact | Link |
| --- | --- |
| Source | [apache-ranger-2.9.0.tar.gz](https://downloads.apache.org/ranger/2.9.0/apache-ranger-2.9.0.tar.gz) ([asc](https://downloads.apache.org/ranger/2.9.0/apache-ranger-2.9.0.tar.gz.asc), [sha512](https://downloads.apache.org/ranger/2.9.0/apache-ranger-2.9.0.tar.gz.sha512), [sha256](https://downloads.apache.org/ranger/2.9.0/apache-ranger-2.9.0.tar.gz.sha256)) |
| Services | [services/](https://downloads.apache.org/ranger/2.9.0/services/) — `admin`, `usersync`, `tagsync`, `kms`, `pdp` |
| Plugins | [plugins/](https://downloads.apache.org/ranger/2.9.0/plugins/) — `atlas`, `elasticsearch`, `hbase`, `hdfs`, `hive`, `kafka`, `knox`, `kylin`, `ozone`, `presto`, `schema-registry`, `solr`, `sqoop`, `storm`, `trino`, `yarn` |
| Tools | [tools/](https://downloads.apache.org/ranger/2.9.0/tools/) — `migration-util`, `ranger-tools`, `sample-client`, `solr_audit_conf` |
| Everything | [downloads.apache.org/ranger/2.9.0/](https://downloads.apache.org/ranger/2.9.0/) |

Binary tarballs follow the naming pattern `ranger-<version>-<component>.tar.gz`, for example
`services/admin/ranger-2.9.0-admin.tar.gz` and `plugins/hive/ranger-2.9.0-hive-plugin.tar.gz`. Each sits next to
its `.asc` and `.sha512` files. Binary tarballs have been published for 2.6.0 and later (the `pdp` service and the
`trino` plugin from 2.9.0); for older releases build them from the source tarball.

Previous releases are listed on the [Releases](index.md) page. Every release ever made, including the
incubating ones, stays available at
[archive.apache.org/dist/ranger/](https://archive.apache.org/dist/ranger/) and
[archive.apache.org/dist/incubator/ranger/](https://archive.apache.org/dist/incubator/ranger/).

## Verify signatures and checksums

Signatures and checksums must always be fetched from the ASF servers (`downloads.apache.org` or
`archive.apache.org`), not from a mirror, so that a compromised mirror cannot serve matching fake files.

### 1. Import the signing keys

The `KEYS` file contains the public keys of all Ranger release managers:

```bash
curl -O https://downloads.apache.org/ranger/KEYS
gpg --import KEYS
```

The same file is available from `https://dist.apache.org/repos/dist/release/ranger/KEYS`. Release announcement
emails name the key used for that release; you can show a key's fingerprint with `gpg --fingerprint <key-id>`.
Because you import keys from a file rather than through a web of trust, `gpg` will report the signature as good
but the key as not certified; that warning is expected.

### 2. Verify the OpenPGP signature

```bash
export RANGER_VERSION=2.9.0
curl -O https://downloads.apache.org/ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz
curl -O https://downloads.apache.org/ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz.asc

gpg --verify apache-ranger-${RANGER_VERSION}.tar.gz.asc apache-ranger-${RANGER_VERSION}.tar.gz
```

A successful check prints `Good signature from "<release manager>"`. Any other result means the file must not be
used.

### 3. Verify the checksum

For releases from 2.5.0 onward the `.sha512` file is produced with `sha512sum`, so it can be checked directly:

=== "Linux"

    ```bash
    curl -O https://downloads.apache.org/ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz.sha512
    sha512sum -c apache-ranger-${RANGER_VERSION}.tar.gz.sha512
    ```

=== "macOS"

    ```bash
    curl -O https://downloads.apache.org/ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz.sha512
    shasum -a 512 -c apache-ranger-${RANGER_VERSION}.tar.gz.sha512
    ```

The source tarball also ships a `.sha256` file; `sha256sum -c` (or `shasum -a 256 -c`) checks it the same way.
Repeat steps 2 and 3 for every binary tarball you download.

!!! note "Older releases"

    The `.sha512` files of releases 2.0.0 through 2.4.0 are in `gpg --print-md SHA512` format (upper-case hex in
    groups), which `sha512sum -c` cannot read. For those, run `gpg --print-md SHA512 apache-ranger-<version>.tar.gz`
    and compare the output with the file. The 2.2.0 checksum files are named with an upper-case extension,
    `apache-ranger-2.2.0.tar.gz.SHA512` and `.SHA256`. Releases 0.4.0 through 0.7.0 (except 0.6.3) have no
    `.sha512` file; their digests are in a single `.mds` file (`gpg --print-mds` output), so compare the matching
    line with `gpg --print-md SHA512 <file>` or `gpg --print-md SHA1 <file>`.

## Docker images

Official images are published to Docker Hub under the `apache` organization for each release from 2.4.0 onward.
The release manager builds them from the `dev-support/ranger-docker` Dockerfiles at the release tag; see
[Release process](../project/release-process.md).

| Image | Purpose | Tags |
| --- | --- | --- |
| [apache/ranger](https://hub.docker.com/r/apache/ranger) | Ranger Admin | `2.4.0` … `2.9.0` |
| [apache/ranger-db](https://hub.docker.com/r/apache/ranger-db) | PostgreSQL database initialized for Ranger | same as `apache/ranger` |
| [apache/ranger-solr](https://hub.docker.com/r/apache/ranger-solr) | Solr with the `ranger_audits` collection | same as `apache/ranger` |
| [apache/ranger-zk](https://hub.docker.com/r/apache/ranger-zk) | ZooKeeper for Solr | `2.4.0` … `2.8.0` (no `2.9.0` tag has been published) |
| [apache/ranger-base](https://hub.docker.com/r/apache/ranger-base) | Base image (OS + JDK) used to build and run the other images | `<date>-<n>-<jdk>`, for example `20260806-2-17` |

The images are not signed; pull them by version tag rather than `latest` and check the image digest shown by
Docker Hub. The quick start below follows the description published with the
[apache/ranger](https://hub.docker.com/r/apache/ranger) image. It starts Ranger Admin with its PostgreSQL database
and a standalone Solr for audits; `apache/ranger-zk` is only needed when Solr runs in SolrCloud mode.

```bash
export RANGER_VERSION=2.9.0
docker pull apache/ranger-solr:${RANGER_VERSION}
docker pull apache/ranger-db:${RANGER_VERSION}
docker pull apache/ranger:${RANGER_VERSION}

docker network create rangernw

docker run -d --name ranger-solr --hostname ranger-solr.rangernw --network rangernw -p 8983:8983 \
  apache/ranger-solr:${RANGER_VERSION} solr-precreate ranger_audits /opt/solr/server/solr/configsets/ranger_audits/

docker run -d \
  -e POSTGRES_PASSWORD=rangerR0cks! \
  -e RANGER_DB_USER=rangeradmin \
  -e RANGER_DB_PASSWORD=rangerR0cks! \
  --name ranger-db --hostname ranger-db.rangernw --network rangernw \
  --health-cmd='su -c "pg_isready -q" postgres' --health-interval=10s --health-timeout=2s --health-retries=30 \
  apache/ranger-db:${RANGER_VERSION}

docker run -d \
  -e POSTGRES_PASSWORD=rangerR0cks! \
  -e RANGER_DB_USER=rangeradmin \
  -e RANGER_DB_PASSWORD=rangerR0cks! \
  --name ranger-admin --hostname ranger-admin.rangernw --network rangernw -p 6080:6080 \
  apache/ranger:${RANGER_VERSION}
```

Ranger Admin is then available at `http://localhost:6080/login.jsp`. To run the full stack with plugins, or to
build images from a source checkout, use the compose files described in
[Running Ranger with Docker](../getting-started/docker.md).

## Maven Central

Ranger libraries are published to Maven Central under the group `org.apache.ranger`, with the release version
as the artifact version. Releases 0.6.0 through 2.9.0 are available; artifacts for a new release are deployed
during the release process, so check
[repo1.maven.org/maven2/org/apache/ranger/](https://repo1.maven.org/maven2/org/apache/ranger/) if the version
you need is not there yet.

Artifacts you are most likely to depend on:

| Artifact | Contents |
| --- | --- |
| `ranger-plugins-common` | Policy engine, `RangerBasePlugin`, policy refresher, `RangerAccessRequest`/`RangerAccessResult` |
| `ranger-plugins-audit` (up to 2.6.0) / `ranger-audit-dest-solr`, `ranger-audit-dest-hdfs`, `ranger-audit-dest-es`, `ranger-audit-dest-kafka`, `ranger-audit-dest-log4j`, `ranger-audit-dest-cloudwatch` (2.7.0 and later) | Audit framework and destinations; see the [2.7.0 breaking changes](2.7.0.md#breaking-changes) |
| `ranger-intg` | Java client for the Ranger Admin REST API (`RangerClient`) |
| `ranger-authz-api`, `authz-embedded` | Component-neutral authorization API and embedded authorizer (2.8.0 and later) |
| `ranger-<component>-plugin`, `ranger-<component>-plugin-shim` | Per-component authorizers and their shim classes (`hdfs`, `hive`, `hbase`, `kafka`, `knox`, `solr`, `ozone`, `trino`, …) |
| `ranger-plugin-classloader` | Classloader used by the shims to isolate plugin dependencies |
| `ranger-kms`, `ranger-tagsync`, `unixusersync`, `security-admin-web` | Service modules |

```xml title="pom.xml"
<dependency>
    <groupId>org.apache.ranger</groupId>
    <artifactId>ranger-plugins-common</artifactId>
    <version>2.9.0</version>
</dependency>
```

The Python client is published to PyPI as [`apache-ranger`](https://pypi.org/project/apache-ranger/)
(`pip install apache-ranger`); its source is in `intg/src/main/python` of the repository.

## Source code and release tags

Every release since 0.7.1 is tagged in the Git repository as `release-ranger-<version>`
(<https://github.com/apache/ranger/tags>); the tag matches the contents of the source tarball. Release branches
are named `ranger-<major>.<minor>`, for example `ranger-2.9`. To build a release from source:

```bash
git clone https://github.com/apache/ranger.git
cd ranger
git checkout release-ranger-2.9.0
mvn clean package -DskipTests
```

The build writes the same `ranger-<version>-<component>.tar.gz` files that are published in the `services/`,
`plugins/` and `tools/` directories to `target/`.

## Further reading

- [Releases](index.md) — every release with dates and links.
- [Release process](../project/release-process.md) — how artifacts are built, signed and published.
- [Installation overview](../getting-started/install.md) — choosing between tarballs, Docker and building from source.
- [ASF release signing guide](https://infra.apache.org/release-signing.html) and
  [verifying downloads](https://www.apache.org/dyn/closer.cgi#verify).
