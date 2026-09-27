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

# FAQ

Short answers to the questions people ask most often about Apache Ranger: what it does, how it
enforces policies, how it relates to the native security of the systems it protects, and what to do
when something does not work. Each answer links to the page with the details.

## General

### What does Apache Ranger offer?

An authorization and audit framework for data and AI platforms. Using the Ranger Admin console or
REST API you manage policies that describe who may access which resource (a catalog, table, column,
bucket, key, topic, path, …) with which action, and Ranger enforces those policies inside the
services. Audit records of every access decision and every administrative change are collected
centrally. Ranger can also delegate administration of parts of a service to other owners (delegated
admin and [security zones](../features/sec-zone/intro.md)).

### Which services does Ranger support?

Polaris, Trino, Ozone, Kudu, Schema Registry, Presto, Elasticsearch, Kylin, Sqoop, NiFi Registry,
NiFi, Atlas, Kafka, Solr, Ranger KMS, YARN, Knox, Storm, HBase, Hive and HDFS, plus a plugin for
nested data structures that any application can embed. See the table in the
[Introduction](introduction.md#what-ranger-protects) and the plugins overview.

### Can I use Ranger for my own application?

Yes. Define a service type for your application, then either embed the policy engine as a Java
library or call the Ranger PDP server over REST from any language. No change to Ranger itself is
needed.

### How does it work?

Ranger Admin is a web application with policy administration, audit and reporting modules. Policies
are enforced by plugins, Java libraries that run inside the process of the protected service: the
Trino coordinator, the Ozone Manager, Kafka brokers, HiveServer2, and so on. No additional process
runs next to the protected service. See [How enforcement works](introduction.md#how-enforcement-works).

### Is Ranger a single point of failure?

No. Plugins pull policies from Ranger Admin at a configurable interval (30 seconds by default) and
cache them on local disk. If Ranger Admin or its database is down, plugins continue to enforce the last
policies they downloaded, and audit events are spooled locally until the audit store is reachable.
Ranger Admin itself can run as several instances behind a load balancer;

### Does Ranger authenticate users?

No. Ranger is an authorization and audit framework. Users are authenticated by the protected service
with whatever mechanism it supports (Kerberos, for example), and Ranger authorizes the identity that
service reports. Ranger Admin authenticates *its own* UI and REST users through LDAP, Active
Directory, Kerberos SPNEGO, Knox SSO or JWT;

### Where do the users and groups in policies come from?

Ranger UserSync imports them from LDAP, Active Directory, UNIX (`/etc/passwd` and `/etc/group`) or a
file. You can also create users, groups and roles directly in Ranger Admin under
**Settings → Users/Groups/Roles**. See [UserSync](../services/usersync/service.md) and
Users, groups and roles.

### What is the difference between a service definition, a service and a policy?

A *service definition* describes a type of service (its resources and access types). A *service* is a
named instance of that type, such as `dev_trino`, that a plugin subscribes to. A *policy* belongs to a
service and grants or denies access types on resources to users, groups and roles. See
[Key concepts](introduction.md#key-concepts) and [Policy model](../arch/policy-model.md).

### How long does a policy change take to be enforced?

Up to one poll interval of the plugin, `ranger.plugin.<service>.policy.pollIntervalMs`, 30 seconds by
default. **Audit → Plugin Status** in Ranger Admin shows when each plugin last downloaded policies.

## Running and operating

### How do I try Ranger?

With Docker: either the released images on Docker Hub (Ranger Admin, its database and Solr) or the
compose setup in `dev-support/ranger-docker`, which builds the current source and can also start
UserSync, TagSync, KMS, PDP, the audit server and Trino, Ozone, Kafka, Hive and other services with
Ranger authorization configured. See
[Installation](install.md) and [Run Ranger with Docker](docker.md).

### What are the default credentials?

In both Docker setups, `admin` / `rangerR0cks!`; the compose setup takes it from
`RANGER_ADMIN_PASSWORD` in `dev-support/ranger-docker/.env`. See [Installation](install.md#default-credentials).

### Which databases can Ranger Admin use?

MySQL/MariaDB, PostgreSQL, Oracle, SQL Server and SQL Anywhere.

### Where are audits stored?

Ranger Admin reads access audits from Solr, OpenSearch, Elasticsearch or Amazon CloudWatch
(`ranger.audit.source.type`). Plugins can write to those stores directly, to HDFS, to Log4j, to Amazon
CloudWatch, or to the Ranger audit server (on master; not yet part of a release), which forwards them. Audits are no longer stored in the
relational database.

### In which order do I upgrade?

Upgrade Ranger Admin first, then UserSync, TagSync and KMS, then the plugins. Ranger Admin ships the
database and Java patches that migrate its schema. You can rehearse an upgrade in
the development Docker setup with [Upgrading Ranger in Docker](docker.md#upgrading-ranger-in-docker).

### My plugin is configured but nothing is enforced. What should I check?

- **Audit → Plugin Status** in Ranger Admin: if the plugin is missing, it never reached Ranger Admin.
  Check `ranger.plugin.<service>.policy.rest.url` and `ranger.plugin.<service>.service.name` in
  `ranger-<service>-security.xml`, and that a service with that name exists in Ranger Admin.
- With Kerberos, the user the protected service runs as must be listed in the service's
  `policy.download.auth.users`; otherwise downloads fail with HTTP 401/403.
- Is the Ranger authorizer active in the protected service's own configuration (for Trino
  `access-control.name=ranger`, for Kafka `authorizer.class.name`, for Ozone
  `ozone.acl.authorizer.class`), and was the service restarted afterwards? These settings are read
  only at start-up.
- Look at the service log for `RangerBasePlugin` / `PolicyRefresher` messages and at the policy
  cache directory (`ranger.plugin.<service>.policy.cache.dir`).

## Integrations

Each plugin page describes how enforcement hooks into the service; start from the
plugins overview. The questions below come up repeatedly.

### How does Ranger authorize Trino?

Trino ships its own Ranger access control. You activate it in Trino's `access-control.properties`
(`access-control.name=ranger`), name the Ranger service and point it at the Ranger plugin
configuration files. See [Trino plugin](../plugins/trino.md) and the
[Trino with Ranger](trino-with-ranger.md) walkthrough.

### Can Kafka clients that connect without authentication be authorized?

Only by IP address. Over a non-authenticated listener Kafka cannot assert the client's identity and
reports the user as `ANONYMOUS`, so user- or group-based policies cannot match. Create policies for
the `public` group with the IP-range policy condition restricted to the client hosts. Brokers
themselves must be allowed (grant the cluster and all topics to the broker hosts), and you cannot
control access from broker hosts, since they are allowed everything.

### Can Ranger control Kafka topic creation?

Yes for topics auto-created by producers and consumers: grant `create` (and `publish` or `consume`)
on the topic resource. Creating topics with the `kafka-topics.sh` admin client is also authorized by
the broker's authorizer as a `create` on the topic or on the cluster. Requests that bypass the brokers
and talk to ZooKeeper directly are not seen by Ranger; there is no Ranger plugin for ZooKeeper.

### Do SQL `GRANT` and `REVOKE` statements still work with Hive?

Yes. With `xasecure.hive.update.xapolicies.on.grant.revoke=true` in `ranger-hive-security.xml`,
`GRANT` and `REVOKE` statements issued in Hive create and update Ranger policies. Compared with
Hive's SQL standard authorization, Ranger adds column-level granularity, wildcards in resource names,
row filters, masking, tag-based rules and central auditing.

### Does Ranger replace or emulate HDFS POSIX permissions?

Neither. Ranger enforces its own policies and does not change the file permissions stored in HDFS.
By default, if no Ranger policy covers a request, the plugin falls back to the native HDFS permission
check, so existing permissions keep working. This fallback can be turned off in the plugin
configuration. Only the NameNodes host the plugin; DataNodes do not.

## Getting help

- User mailing list: `user@ranger.apache.org` (subscribe by mailing `user-subscribe@ranger.apache.org`);
  developer list `dev@ranger.apache.org`.
- Bugs and feature requests: [RANGER JIRA](https://issues.apache.org/jira/browse/RANGER).
- More on [Community](../project/community.md) and on reporting vulnerabilities in
  [Security](../project/security.md).
