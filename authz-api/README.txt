# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


1. Introduction
   Authorization APIs introduced in this module make it simpler for applications to use Apache Ranger to authorize
   access to their resources. This document includes few examples of authorization requests and corresponding
   responses. Libraries in Java and Python will be made available for easier integration in applications using
   these languages. Support for other languages will be added later as needed.

2. Terminology
   2.1. User
        An actor who performs actions on resources. Each user is identified by an unique name. A user can belong
        to multiple groups and can have multiple roles. A user can also have multiple attributes, like department
        name, work location. Apache Ranger policies can be setup to grant access to resources based on any of the
        following: user name, groups the user belongs to, roles the user is assigned to and user attributes.

   2.2. Resource
        Any object on which actions can be performed. Few examples of resources and actions performed on them:
         - file: create, delete, write, read
         - table: create, alter, drop, insert, select, delete
         - topic: create, alter, delete, produce, consume

        Resources are identified by their name, in format: "resource-type:resource-value". Few examples of resource
        names:
         - path:/warehouse/hive/mktg/visitors
         - table:db1.tbl1
         - object:s3a://mybucket/p1/p2/data.parquet

        Resources can have attributes, like owner, createTime. Access to resources can be granted based on resource
        attributes, like: owner of a resource should be allowed all actions.

        Resources can have sub-resources, like columns of a table. This is useful in optimizing authorization for
        access to a resource and several of its sub-resources in a single request.

   2.3. Action
        An action performed on a resource. Examples of actions include: query, list, read, write, delete. In the
        context of authorization, the action given in the request is used only to record in audit log and does not
        affect the authorization decision. The authorization decision is based on the permissions requested for the
        resource.

   2.4. Permission
        A privilege necessary to perform an action on a resources. Apache Ranger policies are used to grant or deny
        permissions to users. An action might require one or more permissions. Examples of permissions include:
        select, insert, read, write, delete.

   2.5. Context
        Additional information about the request that can be used to make authorization decisions. Examples of
        context information include: access time, client IP address, cluster name, cluster type.

   2.6. Decision
        The result of the authorization request. The decision can be either "ALLOWED" or "DENIED". The decision is
        based on the policies defined in Apache Ranger and the user, resource, permissions and context information
        provided in the request.

   2.7. Row Filter
        For resources that support rows, like tables, Apache Ranger policies can be setup to filter rows that a user
        can access. Response from authorization request for such resources can include a row filter that should be
        applied by the caller, to ensure that the user only accesses rows they are allowed to. For example, a row
        filter can be defined to restrict access to rows in a table based on the department the user belongs to.

   2.8. Data Mask
        For resources that support data masking, like columns of a table, Apache Ranger policies can be setup to
        mask (or transformation) values of columns having sensitive data. Response from authorization request for
        such resources can include a data mask that should be applied by the caller, to ensure that the user only
        has accesses to masked value of sensitive data. For example, a data mask can be defined on a column having
        phone number, credit card number or social security number.

3. Examples
   This section includes few examples of authorization requests and corresponding responses. The examples include
   authorizing access to a single resource, authorizing access to a resource and sub-resources, authorizing access
   to multiple resources in a single request, row-filter and data-mask information in the response.

   3.1 Authorize access to a single resource - a path
    request:
    {
      "requestId": "9198b532-a386-4464-9770-d61a8e8bc206",
      "user":      { "name": "gary.adams", "groups": [ "fte", "mktg" ], "roles": [ "analyst" ] },
      "access":    { "resource": { "name": ""path:/warehouse/hive/mktg/visitors", "attributes": { "OWNER": "nancy.boxer" } }, "action": "LIST", "permissions": [ "list" ] },
      "context":   { "serviceName": "s3", "accessTime": 1755543894, "clientIpAddress": "172.16.45.59", "additionalInfo": { "clusterName": "cl1", "clusterType": "onprem" } }
    }

    result:
    {
      "requestId": "9198b532-a386-4464-9770-d61a8e8bc206",
      "decision":  "ALLOWED",
      "permissions": {
        "list": { "access": { "result": "ALLOWED", "policy": { "id": 1, "version": 1 } }
        }
      }
    }

   3.2 Authorize access to a single resource and its sub-resources - a table and 3 columns
    request:
    {
      "requestId": "0a4134c1-44af-42e1-8a27-f15f18e60850",
      "user":      { "name": "gary.adams", "groups": [ "fte", "mktg" ], "roles": [ "analyst" ] },
      "access":    { "resource": { "name": ""table:db1.tbl1", "subResources: [ "column:col1", "column:col2", "column:col3" ], "attributes": { "OWNER": "nancy.boxer" } }, "action": "QUERY", "permissions": [ "select" ] },
      "context":   { "serviceName": "hive", "accessTime": 1755543894, "clientIpAddress": "172.16.120.64", "additionalInfo": { "clientType": "beeline", "clusterName": "cl1", "clusterType": "onprem" } }
    }

    result:
    {
      "requestId": "0a4134c1-44af-42e1-8a27-f15f18e60850",
      "decision":  "ALLOWED",
      "permissions": {
        "select": {
          "rowFilter": { "filterExpr": "dept = 'mktg'", "policy": { "id": 11, "version": 3 } }
          "subResources": {
            "column:col1": { "access":   { "decision": "ALLOWED", "policy": { "id": 5, "version": 1 } },
                             "dataMask": { "maskType": "MASK_SHOW_LAST_4", "maskedValue": "mask_show_last_n({col}, 4, 'x', 'x', 'x', -1, '1')", "policy": { "id": 26, "version": 2 } } },
            "column:col2": { "access":   { "decision": "ALLOWED", "policy": { "id": 2, "version": 1 } },
                              "dataMask": { "maskType": "MASK_HASH", "maskedValue": "mask_hash({col})", "policy": { "id": 27, "version": 4 } } },
            "column:col3": { "access":   { "decision": "ALLOWED", "policy": { "id": 3, "version": 1 } },
                             "dataMask": { "maskType": "MASK_HASH", "maskedValue": "mask_hash({col})", "policy": { "id": 27, "version": 4 } } }
          }
        }
      }
    }

   3.3: Authorize access to multiple resources - select on 2 tables and create on a table
    request:
    {
      "requestId": "4aa68265-34f1-4115-b026-d88dff292669",
      "user":      { "name": "gary.adams", "groups": [ "fte", "mktg" ], "roles": [ "analyst" ] }
      "accesses": [
        { "resource": { "name": "table:db1.tbl1", "attributes": { "OWNER": "nancy.boxer" } }, "action": "QUERY", "permissions": [ "select" ] },
        { "resource": { "name": "table:db1.tbl2", "attributes": { "OWNER": "nancy.boxer" } }, "action": "QUERY", "permissions": [ "select" ] },
        { "resource": { "name": "table:db1.vw1" },  "action": "CREATE", "permissions": [ "create" ] }
      ],
      "context": { "serviceName": "hive", "accessTime": 1755543894, "clientIpAddress": "172.16.27.152", "additionalInfo": { "clientType": "jdbc", "clusterName": "cl1", "clusterType": "onprem" } }
    }

    result:
    {
      "requestId": "4aa68265-34f1-4115-b026-d88dff292669",
      "decision":  "DENIED",
      "accesses": [
        {
          "decision": "ALLOWED",
          "permissions": {
            "select": {
              "access":    { "decision": "ALLOWED", "policy": { "id": 1, "version": 1 } },
              "rowFilter": { "filterExpr": "dept = 'mktg'", "policy": { "id": 11, "version": 3 } }
            }
          }
        },
        {
          "decision": "DENIED",
          "permissions": {
            "select": {
              "access": { "decision": "DENIED", "policy": { "id": 21, "version": 1 } }
            }
          }
        },
        {
          "decision": "ALLOWED",
          "permissions": {
            "create": {
              "access": { "decision": "ALLOWED", "policy": { "id": 23, "version": 3 } }
            }
          }
        }
      ]
    }
