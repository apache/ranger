---
title: "Ranger Releases"
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
# Apache Ranger Releases

This page lists every Apache Ranger release, from the first incubating release in 2014 to the current stable
release, with the release date, the release notes and where to download the artifacts. Release notes for the
2.x line are part of this site; notes for 1.x and older releases live on the Apache Ranger cwiki and are
summarized in [1.x and older](1.x-and-older.md).

Release dates are the dates recorded for the corresponding `fixVersion` in the
[Apache Ranger JIRA project](https://issues.apache.org/jira/projects/RANGER). The source code for every release is
tagged in Git as `release-ranger-<version>` from 0.7.1 onward; older releases carry tags such as `ranger-0.7.0`,
`release-0.6.3` or only release-candidate tags (`ranger-<version>-rc<n>`). The `master` branch builds
version `3.0.0-SNAPSHOT`.

!!! tip "Latest release: Apache Ranger 2.9.0 (August 8, 2026)"

    Ranger 2.9.0 adds the standalone Ranger PDP server and `authz-remote` thin-client library, a plugin for Apache
    Polaris, action-based policies for Apache Ozone, header-based authentication and many security fixes.
    Read the [2.9.0 release notes](2.9.0.md) or go straight to [Download and verify](download.md).

## 2.x releases

| Version | Release date | Release notes | Download |
| --- | --- | --- | --- |
| 2.9.0 | 2026-08-08 | [2.9.0](2.9.0.md) | [downloads.apache.org/ranger/2.9.0](https://downloads.apache.org/ranger/2.9.0/) |
| 2.8.0 | 2026-03-01 | [2.8.0](2.8.0.md) | [downloads.apache.org/ranger/2.8.0](https://downloads.apache.org/ranger/2.8.0/) |
| 2.7.0 | 2025-07-30 | [2.7.0](2.7.0.md) | [downloads.apache.org/ranger/2.7.0](https://downloads.apache.org/ranger/2.7.0/) |
| 2.6.0 | 2025-02-15 | [2.6.0](2.6.0.md) | [downloads.apache.org/ranger/2.6.0](https://downloads.apache.org/ranger/2.6.0/) |
| 2.5.0 | 2024-08-08 | [2.5.0](2.5.0.md) | [archive.apache.org/dist/ranger/2.5.0](https://archive.apache.org/dist/ranger/2.5.0/) |
| 2.4.0 | 2023-03-30 | [2.4.0](2.4.0.md) | [archive.apache.org/dist/ranger/2.4.0](https://archive.apache.org/dist/ranger/2.4.0/) |
| 2.3.0 | 2022-07-09 | [2.3.0](2.3.0.md) | [archive.apache.org/dist/ranger/2.3.0](https://archive.apache.org/dist/ranger/2.3.0/) |
| 2.2.0 | 2021-11-01 | [2.2.0](2.2.0.md) | [archive.apache.org/dist/ranger/2.2.0](https://archive.apache.org/dist/ranger/2.2.0/) |
| 2.1.0 | 2020-09-03 | [2.1.0](2.1.0.md) | [archive.apache.org/dist/ranger/2.1.0](https://archive.apache.org/dist/ranger/2.1.0/) |
| 2.0.0 | 2019-08-07 | [2.0.0](2.0.0.md) | [archive.apache.org/dist/ranger/2.0.0](https://archive.apache.org/dist/ranger/2.0.0/) |

Each 2.x line has had a single release so far; fixes ship in the next minor version rather than in patch releases.
Every 2.x release is also published to Maven Central under the `org.apache.ranger` group, and releases from 2.4.0
onward have Docker images on Docker Hub. See [Download and verify](download.md) for details.

## 1.x and 0.x releases

The release notes for these versions are on the cwiki. [1.x and older](1.x-and-older.md) summarizes the
highlights of each release.

| Version | Release date | Release notes | Download |
| --- | --- | --- | --- |
| 1.2.0 | 2018-10-04 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=93325735) | [archive.apache.org/dist/ranger/1.2.0](https://archive.apache.org/dist/ranger/1.2.0/) |
| 1.1.0 | 2018-07-09 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=87297827) | [archive.apache.org/dist/ranger/1.1.0](https://archive.apache.org/dist/ranger/1.1.0/) |
| 1.0.0 | 2018-03-20 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=75975356) | [archive.apache.org/dist/ranger/1.0.0](https://archive.apache.org/dist/ranger/1.0.0/) |
| 0.7.1 | 2017-06-07 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=70257650) | [archive.apache.org/dist/ranger/0.7.1](https://archive.apache.org/dist/ranger/0.7.1/) |
| 0.7.0 | 2017-02-27 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=68715504) | [archive.apache.org/dist/ranger/0.7.0](https://archive.apache.org/dist/ranger/0.7.0/) |
| 0.6.3 | 2017-01-30 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=67637684) | [archive.apache.org/dist/ranger/0.6.3](https://archive.apache.org/dist/ranger/0.6.3/) |
| 0.6.2 | 2016-11-08 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65877405) | [archive.apache.org/dist/incubator/ranger/0.6.2-incubating](https://archive.apache.org/dist/incubator/ranger/0.6.2-incubating/) |
| 0.6.1 | 2016-08-20 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65865300) | [archive.apache.org/dist/incubator/ranger/0.6.1-incubating](https://archive.apache.org/dist/incubator/ranger/0.6.1-incubating/) |
| 0.6.0 | 2016-07-18 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65146308) | [archive.apache.org/dist/incubator/ranger/0.6.0-incubating](https://archive.apache.org/dist/incubator/ranger/0.6.0-incubating/) |
| 0.5.3 | 2016-05-31 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=62694969) | [archive.apache.org/dist/incubator/ranger/0.5.3-incubating](https://archive.apache.org/dist/incubator/ranger/0.5.3-incubating/) |
| 0.5.2 | 2016-02-29 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=61339642) | [archive.apache.org/dist/incubator/ranger/0.5.2-incubating](https://archive.apache.org/dist/incubator/ranger/0.5.2-incubating/) |
| 0.5.1 | 2016-01-26 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=61337303) | [archive.apache.org/dist/incubator/ranger/0.5.1-incubating](https://archive.apache.org/dist/incubator/ranger/0.5.1-incubating/) |
| 0.5.0 | 2015-06-10 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=57906100) | [archive.apache.org/dist/incubator/ranger/0.5.0-incubating](https://archive.apache.org/dist/incubator/ranger/0.5.0-incubating/) |
| 0.4.0 | 2014-11-17 | [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=59688883) | [archive.apache.org/dist/incubator/ranger/0.4.0-incubating](https://archive.apache.org/dist/incubator/ranger/0.4.0-incubating/) |

Releases up to 0.6.2 were made while Ranger was in the Apache Incubator; their artifacts are named
`apache-ranger-incubating-<version>.tar.gz` (`ranger-<version>-incubating.tar.gz` for 0.4.0 and 0.5.0) and live under the `incubator/ranger` path of the archive.

## Where release information comes from

- **Release notes** are compiled by the release manager from the JIRA issues with the matching `fixVersion`; the
  process is described in [Release process](../project/release-process.md).
- **Announcements** go to the `dev@`, `user@` and `announce@apache.org` mailing lists; see
  [Community](../project/community.md).
- **Security fixes** in each release are tracked separately in the [CVE list](../project/cve-list.md).
- The [cwiki release folders](https://cwiki.apache.org/confluence/display/RANGER/Release+Folders) keep a page per
  release with links to the notes, artifacts and Git tag.
