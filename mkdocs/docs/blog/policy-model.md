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

# Apache Ranger Policy Model

*Madhan Neethiraj, Apache Ranger committer · Mar 08, 2022*

[← All blogs](blog.md)

## Introduction

Apache Ranger is an extensible framework that enables enterprises to adopt a consistent approach to authorize
access to their resources across multiple services/applications/cloud. Apache Ranger framework also enables
enterprises to collect audit logs of access to their resources, to help meet various compliance requirements.

Apache Ranger is a central part of security in many large deployments in enterprises across various domains
like finance, retail, insurance, healthcare, services. Apache Ranger has out-of-the box support for a large
number of popular services and many more services are supported by commercial vendors. Apache Ranger is highly
optimized for performance, adds negligible overhead in authorizing access to resources. It has been very well
proven in very high throughput services like Apache Kafka, Apache HBase which perform thousands of
authorizations per second.

Apache Ranger provides an intuitive web user interface to manage authorization policies and audit logs for
access to resources across a large number of services. Apache Ranger also provides REST, Python, Java APIs for
programmatic integration with tools used by enterprises. Open framework provided by Apache Ranger enables
enterprises to extend Apache Ranger authorization to their own applications and services as well.

Here are few key points that make Apache Ranger a compelling option for enterprises looking to standardize
authorization of access to their resources:

1. out-of-the-box support for more than a dozen popular services like Apache Hive, Apache HBase, Apache Kafka, Apache Solr, Elasticsearch, Apache NiFi and Presto.
2. support for services like Amazon EMR, AWS S3, ADLS-Gen2, GCS, Snowflake, Google BigQuery, Trino, Dremio, Starburst, Apache Impala, Postgres, MS-SQL and Amazon Redshift by commercial vendors.
3. policies for access authorization, row-filters, data masking.
4. resource-based, classification-based policies, role-based, attribute-based policies.
5. delegated administration, deny and exceptions in policies, custom conditions.
6. centralized audit logs of accesses to enterprise resources across multiple services, interactive user interface to view audit logs of accesses.
7. intuitive policy management UI.
8. Java, Python, REST APIs for programmatic integration for policy management.
9. open framework which enables enterprises to extend Apache Ranger authorization to their own applications and services.

## Policy Model

At the core of Apache Ranger authorization is its policy model. We will go through key aspects of the Apache
Ranger policy model in this section.

### Resources

A resource is a fundamental element in the Apache Ranger policy model. Apache Ranger enables policies to
authorize access to resources. In this context, a resource is anything whose access needs to be authorized,
like a file/path, database, table, column, topic; but can also be a service – like Apache Knox topology.
Apache Ranger policy model captures details of resources of a service in a declarative way – details like
hierarchy, case-sensitivity, supports row-filter/data-masking, etc.

Type of resources vary across services/applications, as seen in the table below:

| Service | Resources |
|---|---|
| Apache Hive | databases, tables, columns, udfs |
| Apache Kafka | topics |
| Apache Solr | collections |
| AWS S3 | buckets, objects |
| ADLS-Gen2 | storage-accounts, containers, objects |
| Azure PowerBI | workspaces |
| Google BigQuery | projects, datasets, tables, columns |
| Snowflake | databases, schemas, tables, columns, warehouses |
| Trino | catalogs, schemas, tables, columns, procedures |
| ... | |

### Permissions

A permission is another fundamental element in the Apache Ranger policy model. A permission is an action
performed on a resource, like reading a file, creating a directory, querying a table, or publishing a message
to a topic. Apache Ranger policy model captures details of permissions of a service in a declarative way –
details like which permissions are applicable to specific resource types, implied permissions, etc.

Like resources, list of permissions varies across services/applications, as seen in the table below:

| Service | Permissions |
|---|---|
| Apache Hive | create, alter, drop, select, insert, .. |
| Apache Kafka | publish, consume, create, delete, describe, configure, .. |
| Apache Solr | query, update, others, Solr admin |
| AWS S3 | read, write, delete, .. |
| ADLS-Gen2 | read, write, delete, .. |
| Azure PowerBI | contributor, member, admin, none |
| Google BigQuery | project-list, dataset-create, table-create, table-list, query, .. |
| Snowflake | CreateSchema, CreateTable, Select, Insert, Update, .. |
| Trino | create, alter, drop, select, insert, .. |
| ... | |

### Users, Groups, Roles

Apache Ranger enables authorization policies to be set up to allow/deny permissions to users, groups, and
roles. Users and groups are typically obtained from an enterprise directory like AD/LDAP. Apache Ranger
user-sync module handles details of bringing users and groups from sources like LDAP/AD/OS, and keeping up
with the changes in the sources - like addition of users and groups, addition/removal of a user from a group.

Apache Ranger user-sync supports retrieving attributes of users and groups as well. Such attributes, like
dept/location/site-id, can be used in authorization policies to allow/deny access to resources, and set up
row-filters that restrict users to access relevant subset of data. More on this later in this document.

In addition to users and groups, Apache Ranger supports roles to be used in authorization policies. A role in
Apache Ranger is a grouping of users, groups, and other roles. Roles can be managed using Apache Ranger UI and
REST APIs by authorized users. Role based authorization is widely used in enterprises and having support for
roles in Apache Ranger makes it possible to use well established enterprise security practices in Apache Ranger
authorization policies.

### Delegated Admin

Apache Ranger enables decentralization of authorization policies management with support for delegated-admin
feature. A set of users, groups and roles can be granted permission, via an Apache Ranger policy (what else!),
to manage authorization policies for a subset of resources and permissions. For example, users in
`finance-admin` group can be granted permissions to manage authorization policies for contents of
Snowflake database named `finance`, and AWS S3 objects under `s3://mybucket/dept/finance`.
This offers a scalable approach to manage authorization in large deployments.

### Security Zone

Apache Ranger supports security zones to enable multi-tenancy within an organization where admins from
different lines of businesses can manage security policies for their own resources. For example, data that
belongs to the sales team can be managed by administrators of the sales team, similarly data of marketing,
sales, operations teams can be managed by respective administrators.

Also, security zones can be used to isolate resources based on purpose. For example, it is common for a data
lake to have distinct areas and authorization policies for test data, unprocessed/raw data, semi-processed
data, and production data. Apache Ranger makes it easier to manage security policies in such deployments with
use of security zones like:

- Test zone
- Landing zone
- Staging zone
- Production zone

A security zone can contain resources from multiple services/applications, like AWS S3, ADLS-Gen2, GCS,
Snowflake, Amazon Redshift, Postgres, Apache Hadoop, Apache Hive, Apache HBase, Apache Kafka. This makes it
easier to set up consistent authorization policies across multiple services by a set of administrators
designated for each security zone.

### Allow, Deny, Exceptions

In addition to authorization policies that can grant access to resources, Apache Ranger also enables policies
to be setup to:

- deny access to users/groups/roles on resources
- exclude a subset of users from accesses allowed/denied above
- deny all access to specific resources other than the ones allowed in the policy

This makes it easier to set up policies to protect sensitive resources.

### Wildcards, macros, variables in resource names

Apache Ranger policies support use of wildcards, macros, and variables in resource names. This makes it
possible to use small number of policies for a large number of resources, as shown below:

| Policy Resource | Description |
|---|---|
| `test_*` | matches all resources having name that start with test_ |
| `/home/{USER}` | a path under /home having name of current user |
| `/dept/${{USER.dept}}` | a path under /dept having name of current user's department |

### Policy validity schedule

Apache Ranger enables policies to be effective only for specific time schedules. This feature can be used to
create policies that need to be effective at a future time, for example to allow access to revenue reports for
a wider audience only after a specific time. This feature can also be used to allow temporary access to
specific users/groups/roles, with a specific start and end times.

## Attribute based access control

Apache Ranger enables use of user, group, resource, classification, and the environment attributes in
authorization policies. ABAC makes it possible to express authorization policies without prior knowledge of
specific resources, specific users – which helps avoid the need for new policies as new resources or users are
introduced.

For example:

- allow each user to access all tables owned by them, using ***{OWNER}*** macro:

| | |
|---|---|
| `resource` | database=\*, table=\* |
| users | **{OWNER}** |
| permissions | all |

- allow users to access their department data in AWS S3, by using user attribute ***${{USER.dept}}***:

| | |
|---|---|
| `resource` | bucket=mycompany, object=/data/***${{USER.dept}}***/\* |
| users | **{USER}** |
| permissions | read,write |

- allow users in mktg group to access `PII` data of email type, by using tag attribute ***TAG.piiType***:

| | |
|---|---|
| `resource` | tag=PII |
| groups | mktg |
| condition | ***TAG.piiType == 'email'*** |
| permissions | select |

- tables with `SENSITIVE` classification should be accessible only by users having privileges for that sensitive level

| | |
|---|---|
| `resource` | tag=SENSITIVE |
| groups | public |
| condition | ***TAG.sensitiveLevel < USER.allowedSensitiveLevel*** |
| permissions | select |

## Resource based access control

Apache Ranger enables setting up policies to grant or deny permissions to users/group/roles based on specific
resource names, like:

| Service | Resource | Permission |
|---|---|---|
| Apache Hive | database: sales<br>table: order_data<br>column: order_amount | select |
| Apache Kafka | topic: finance | publish, consume |
| AWS S3 | bucket: mycompany<br>path: /home/{USER} | read, write, delete |
| ADLS-Gen2 | storage-account: mycompany<br>container: home<br>path: /{USER} | read, write, delete |
| ... | | |

## Tag based access control

In addition to authorization policies on resources, Apache Ranger enables policies to be set up on
classifications (tags) associated with resources. This feature enables enterprises to separate responsibility
of classification of resources (PII, PCI, PHI, credit card number, etc.) from setting up access-control
policies. Classifications created, by a team of data stewards and tools that scan data for sensitive
information, can be leveraged to drive authorization to access the resources.

Authorization policies on the classifications themselves, instead of directly on the resources, will ensure
that appropriate policies will automatically be applied as classifications are added, removed, and updated on
resources. Also, a single tag-based policy (for example on PII) can be used to authorize access to resources
across multiple services like AWS S3, ADLS-Gen2, Snowflake, Databricks SQL, Apache Hive, Apache HBase, Apache
Kafka. This can significantly reduce the complexity in managing authorization policies.

## Data masking

Apache Ranger data-masking policies enable enterprises to allow access to sensitive data suitably masked
depending on the context in which a user accesses the data. Some users will need the data without masking,
while some other users can only be allowed to see partial or masked or transformed value. While authorization
policies can be used to either allow or deny access to certain data, data-masking policies enable dynamically
mask sensitive data as users access the data, for example to ensure that:

- analysts have access to only specific part of birthday (year or month or day)
- only last 4 digits of a national id are available to customer service representatives
- only salary ranges of employees (i.e., not the salary) are available to analysts

In addition to supporting data-masking policies on resources, like columns in Apache Hive/Snowflake/Databricks
SQL/Presto, Apache Ranger enables setting up data-masking policies based on classifications (tags) associated
with resources. This can significantly reduce the complexity in managing masking policies. In addition,
tag-based masking policies leverage classifications added to resources by data stewards and tools that scan
data for sensitive information.

## Row-filter

Apache Ranger row-filter policies enable enterprises to allow users to access only a subset of data depending
upon the context in which a user accesses the data. When a table having a row-filter is accessed by the user,
only a subset of rows will be visible to the user – depending upon the filter setup in row-filter policy.
Row-filters can be used for example to ensure that:

- data of customers residing in a country is available only to analysts authorized to access the country's data
- a store manager has access to only data relevant to the store she/he works in
- analysts don't have access to sensitive records

## Access audit logs

Apache Ranger generates audit logs of accesses to resources protected by Apache Ranger authorization. Apache
Ranger can be configured to store audit logs in multiple destinations, including Solr, HDFS, AWS S3, AWS
CloudWatch, ADLS-Gen2, Elasticsearch. Audit logs generated by Apache Ranger include following details, which
can help enterprises to satisfy various compliance requirements:

- resource accessed; action performed; was access allowed
- time of access, tags associated with the resource (PII, PCI, PHI, ..)
- who performed the access, IP address from which the access was performed
- ID of Apache Ranger policy that allowed or denied the access

Apache Ranger provides an interactive user interface to view audit logs stored in Solr, Elasticsearch or AWS
CloudWatch, with search capabilities to look for access audits for specific resources, specific users, client
IP addresses, within a given time frame, specific classifications. Apache Ranger audit logs can be stored in
ORC or JSON formats, which can then be loaded into various tools for analysis.

## References

- [Apache Ranger: tag-based policies](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=61322361)
- [Apache Ranger: row-filter and data-masking policies](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868896)
- [Apache Ranger: security zones](https://cwiki.apache.org/confluence/display/RANGER/Introduction+of+Security+Zones+in+Ranger)
- [Apache Ranger: Python](https://pypi.org/project/apache-ranger/)
- [Apache Ranger: Java](https://cwiki.apache.org/confluence/display/RANGER/Ranger+Client+Libraries)
- Apache Ranger: REST API
