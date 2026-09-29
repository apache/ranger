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

# Community

Apache Ranger is developed by a volunteer community under the Apache Software Foundation (ASF). Anyone can
use the software, ask questions, report bugs, propose changes and, over time, earn a vote in how the project
is run. This page lists where the community meets, how decisions are made, and the policies the project
commits to.

If you have a question about running Ranger, start on the user mailing list or Slack. If you want to change
Ranger, read [Contributing](contributing.md).

## Mailing lists

Mailing lists are the official channel: decisions, votes and release announcements happen here, and the
archives are the project's record. Subscribe by sending an empty email to the subscribe address, then reply to
the confirmation mail.

| List | Purpose | Subscribe | Unsubscribe | Archive |
|---|---|---|---|---|
| `user@ranger.apache.org` | Questions about installing, configuring and using Ranger | `user-subscribe@ranger.apache.org` | `user-unsubscribe@ranger.apache.org` | [lists.apache.org](https://lists.apache.org/list.html?user@ranger.apache.org) |
| `dev@ranger.apache.org` | Design discussions, JIRA and PR notifications, release votes | `dev-subscribe@ranger.apache.org` | `dev-unsubscribe@ranger.apache.org` | [lists.apache.org](https://lists.apache.org/list.html?dev@ranger.apache.org) |
| `commits@ranger.apache.org` | Notifications for every commit to the repository | `commits-subscribe@ranger.apache.org` | `commits-unsubscribe@ranger.apache.org` | [lists.apache.org](https://lists.apache.org/list.html?commits@ranger.apache.org) |

Tips for a useful post:

- Include the Ranger version, the component (Admin, UserSync, a specific plugin), the relevant configuration
  and the exact log messages.
- Search the archive first; many questions have been answered before.
- Do not post security vulnerabilities to any list. See [Security](security.md).

## Chat

The community also uses a Ranger channel in the ASF Slack workspace:
<https://the-asf.slack.com/archives/C4SC5NXAA>. Committers can join with their `@apache.org` address;
if you do not have one, ask for an invitation on the dev list. Slack is good for quick questions, but anything that affects
the project (designs, decisions) should be brought to the dev list so it is archived.

## Issue tracking

Bugs, improvements and new features are tracked in the `RANGER` project in Apache JIRA:
<https://issues.apache.org/jira/browse/RANGER>. You need an Apache JIRA account to file issues; new users can
request one from the [self-service page](https://selfserve.apache.org/jira-account.html).

When filing a bug, include: the Ranger version, the affected component, steps to reproduce, expected and actual
behavior, and relevant log excerpts. Every code change is tied to a JIRA id (`RANGER-XXXX`), which appears in
the commit message and the pull request title.

## Source code and CI

- Repository: <https://github.com/apache/ranger> (canonical ASF mirror at
  `https://gitbox.apache.org/repos/asf/ranger.git`)
- Pull requests: <https://github.com/apache/ranger/pulls>
- CI: GitHub Actions workflows in
  [`.github/workflows`](https://github.com/apache/ranger/tree/master/.github/workflows) run the build, unit
  tests and Docker smoke tests on every push and pull request; the project also has a Jenkins job at
  <https://ci-builds.apache.org/job/Ranger>.

## Project team

The Project Management Committee (PMC) and committers are listed on the ASF projects site:
<https://projects.apache.org/committee.html?ranger>.

| Role | What it means |
|---|---|
| Contributor | Anyone who files issues, answers questions, writes docs or submits patches. No account beyond JIRA/GitHub needed. |
| Committer | Has write access to the repository and merges reviewed contributions. Committers are invited by the PMC based on sustained, quality contributions and sign an Individual Contributor License Agreement (ICLA). |
| PMC member | Votes on releases and on adding committers and PMC members, and is responsible for project oversight. PMC members are elected from the committers. |
| PMC chair | The project's Vice President, who reports to the ASF board. |

Committers and PMC members are added by a vote on the PMC's private list, following the same process for every
candidate. Contributions of any kind, not only code, count toward being invited.

## How decisions are made

Ranger follows [The Apache Way](https://www.apache.org/theapacheway/):

- All important discussions happen in writing on `dev@ranger.apache.org`, so anyone can take part and the
  reasoning is archived. Decisions taken elsewhere (Slack, meetings) are brought back to the list.
- Most decisions are made by consensus. Where discussion is not enough, the project uses the standard
  [ASF voting rules](https://www.apache.org/foundation/voting.html): `+1`, `0`, `-1`. Vetoes apply only to code
  changes and must be backed by a technical justification.
- Releases need at least three binding `+1` votes from PMC members and more `+1` than `-1` votes. Anyone can
  test a release candidate and cast a non-binding vote; see [Voting on a release](contributing.md#voting-on-a-release).
- Contributors act as individuals, not as representatives of their employer. The project is independent of any
  vendor.

## Project maturity

Ranger tracks itself against the
[Apache Project Maturity Model](https://community.apache.org/apache-way/apache-project-maturity-model.html).
The self-assessment on the [cwiki](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=66850011)
was written during incubation; the substance still holds and is summarized here.

| Area | How Ranger meets it |
|---|---|
| Code | Released under the Apache License 2.0; source in a public git repository with full history; every release is tagged; reproducible Maven build; provenance of every change is established through authenticated commits and JIRA-linked commit messages. |
| Licenses and copyright | Dependencies are limited to ASF Category A/B licenses and reviewed in release votes; `LICENSE` and `NOTICE` files ship in source and binary distributions; all committers have an ICLA on file; code donations have a Software Grant Agreement. |
| Releases | Releases are source releases in standard archive formats, signed with the release manager's PGP key and accompanied by SHA-256/SHA-512 digests, approved by PMC vote. Convenience binaries and Docker images are provided but are not the official release. The [release process](release-process.md) is documented and has been followed by different release managers. |
| Quality | Bugs are tracked openly in JIRA; security is a stated priority with a private reporting channel ([Security](security.md)); backward compatibility is a documented policy (below); the project aims to respond to bug reports promptly. |
| Community | Public homepage linking to source, JIRA, lists and docs; newcomers are welcomed on the lists; contributions of all kinds are recognized; committers and PMC members are added through a documented, uniform process; user questions are usually answered within hours. |
| Consensus building | The list of PMC members is public; decisions are made by consensus on the dev list and documented there; standard ASF voting rules apply; vetoes are rare and must be technically justified. |
| Independence | The project is independent of any corporate influence; contributors act as themselves. |

## Backward compatibility

Ranger commits to backward compatibility across releases in two areas:

- **Public REST APIs.** The public APIs under `service/public/v2/api/...` for service definitions, services,
  policies and related objects remain backward compatible: clients written against an earlier release keep
  working against a newer Ranger Admin. New fields may be added; existing fields and semantics are not removed
  or changed.
- **Policy authoring.** Policies created in an earlier release continue to be valid and to evaluate the same way
  after an upgrade. New policy features (for example row filters, data masking, deny and exception items,
  validity schedules, conditions) are additive; existing policies do not need to be rewritten.

Changes that cannot be made compatibly are discussed on the dev list and called out in the
[release notes](../release-notes/index.md). Database schema changes are handled by the versioned patches
applied at upgrade time, so an existing policy store is
migrated in place.

## Roadmap

Ranger does not maintain a separate roadmap document. Planned work is visible as:

- open JIRA issues of type *New Feature* and *Improvement* in the
  [RANGER project](https://issues.apache.org/jira/browse/RANGER), including their target fix versions;
- design discussions and `[DISCUSS]` threads on the dev list;
- the highlights of each release in the [release notes](../release-notes/index.md).

If you want to influence the roadmap, file a JIRA describing the use case and start a thread on the dev list.

## Project history

Ranger joined the Apache Incubator in 2014 and is now an Apache Top-Level Project. Release announcements are
posted on the user and dev lists; the highlights below are taken from the project news archive.

| Date | Release | Highlights |
|---|---|---|
| 2014-11-17 | 0.4.0 | First Apache release: centralized authorization and audit for HDFS, HiveServer2, HBase, Knox and Storm. |
| 2015-06-10 | 0.5.0 | Adds YARN, Kafka and Solr support. |
| 2016-01-26 | 0.5.1 | Maintenance release. |
| 2016-02-29 | 0.5.2 | Maintenance release. |
| 2023-03-29 | 2.4.0 | Fine-grained access control over nested structures; upgraded HBase support; security fixes. |
| 2024-08-08 | 2.5.0 | New React-based UI; performance improvements; HA for UserSync and TagSync; roles as security-zone admins and auditors. |
| 2025-02-15 | 2.6.0 | Policy evaluation performance improvements; TagSync support for Ozone resources; validity schedules as a policy-item condition. |
| 2025-07-30 | 2.7.0 | Improved Docker setup scripts; audit module refactoring for fewer dependencies; service configuration UI for service admins and super users. |
| 2026-03-01 | 2.8.0 | `authz-api` and `authz-embedded` modules; service-managed ACLs (inline policies); Kerberos in the Docker setup; plugins no longer depend on `hadoop-common`; JWT authentication between plugins and Ranger Admin. |
| 2026-08-08 | 2.9.0 | Latest release; see the [release notes](../release-notes/2.9.0.md). |

Releases between 0.5.2 and 2.4.0 are summarized in [1.x and older](../release-notes/1.x-and-older.md) and the
per-version pages under [Releases](../release-notes/index.md).

## Further reading

- [Contributing](contributing.md)
- [Security](security.md)
- [ASF index](asf-index.md): links to the foundation, sponsorship and licensing pages.
- [Apache Community Development](https://community.apache.org/)
