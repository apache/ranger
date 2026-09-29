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

# Apache Ranger blogs

Technical articles from the Apache Ranger community on the policy model, access control patterns and
integration guides. Posts are listed newest first. They are reproduced as written; where a post describes a
feature in depth, the reference documentation is linked at the end of the post.

## [Integrating applications with Apache Ranger](integrating-applications.md)

*Madhan Neethiraj · Aug 24, 2026*

How an application integrates with Ranger in three steps: register a service definition that describes its
resources, access types, masking and row-filter capabilities; request authorization decisions either through an
embedded plugin (`authz-api` / `authz-embedded`) or remotely from the Ranger PDP server (REST, Java
`authz-remote`, Python); and enforce the returned decision. Includes complete service-definition JSON fragments,
plugin configuration properties, Java and Python samples, and the administrative steps to register the service,
delegate policy administration and use roles and security zones.

## [Dynamic expressions](dynamic-expressions.md)

*Madhan Neethiraj · Dec 12, 2023*

Ranger policies can use attributes from the access context - resource owner, time of access, tags on the
resource, attributes of the user, groups and tags, roles and groups of the user - through dynamic expressions
evaluated by the policy engine. This post shows where expressions can be used (policy conditions, row filters,
resource names) and provides the full table of supported variables and functions such as `USER`, `TAG`, `REQ`,
`IS_IN_GROUP()` and `GET_UG_ATTR_Q()`, with default values, separators and quoting options.

## [Adventures in attribute-based access control (ABAC) - part 2](abac-part-2.md)

*Barbara Eckman · Oct 15, 2023*

Picks up the GlobalSalesPartners example from part 1 and shows how eight role-based row-filter conditions collapse
into a single ABAC condition that compares column values with `$USER` attributes loaded into the Ranger
UserStore by usersync. The same idea is then applied to tag-based policies, matching a user attribute against a
tag attribute so that new sales regions need no policy changes.

## [Adventures in attribute-based access control (ABAC) - part 1](abac-part-1.md)

*Barbara Eckman · Apr 29, 2023*

Walks through progressively harder access-control requirements for a sales dataset - resource and
identity-based policies, then tag-based and role-based policies with row filters - and shows how the number of
roles and filter conditions grows combinatorially as regions and partners are added. Sets up the problem that
attribute-based access control solves in part 2.

## [Apache Ranger policy model](policy-model.md)

*Madhan Neethiraj · Mar 08, 2022*

An overview of the Ranger policy model: resources and permissions declared per service, users, groups and roles,
delegated administration, security zones, allow/deny with exceptions, wildcards and macros in resource names,
validity schedules, attribute-based, resource-based and tag-based access control, data masking, row filters and
access audit logs.
