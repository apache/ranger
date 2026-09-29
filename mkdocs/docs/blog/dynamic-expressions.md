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

# Apache Ranger - Dynamic Expressions

*Madhan Neethiraj, Apache Ranger committer · Dec 12, 2023*

[← All blogs](blog.md)

## Introduction

Apache Ranger policy model offers a rich set of features that help security administrators handle various
access and governance requirements with ease. These features include:

1. Consistent model to authorize access data in large number of services
2. Ability to dynamically apply data masking and row-filtering
3. Delegated access control administration
4. Ability to explicitly deny access
5. Use of wildcards in resource names in access policies
6. Role-based access control (RBAC)
7. Tag-based access control (TBAC), based on tags associated with resources
8. Attribute-based access control (ABAC), based on attributes of users, groups and tags

In addition to above, Apache Ranger policies can use various attributes available in the access context to
authorize the access - attributes including resource owner, time of access, tags associated with the
accessed resource, attributes of user/groups/tags, groups/roles the user belongs to. This document explores
use cases that can leverage such attributes in policies using dynamic expressions.

## Dynamic expressions

Apache Ranger policy engine evaluates dynamic expressions specified in policies using the script engine
included in the JVM, in a sandboxed environment. Dynamic expressions can be used in Apache Ranger policies in
following contexts:

### Policy conditions

Expressions can used in policy conditions to decide whether to evaluate the policy or a policy-item. These
expressions should evaluate to a boolean value i.e., `true` or `false`. Examples:

Condition for highly sensitive data (level >= 10)

```text
TAG.sensitiveLevel >= 10
```

Condition to check if the user has appropriate level of clearance to access sensitive data

```text
USER.allowedSensitiveLevel >= TAG.sensitiveLevel
```

Condition to check if the user belongs to group `finance` and is in role `analyst`

```text
IS_IN_GROUP('finance') AND IS_IN_ROLE('analyst')
```

### Row filters

Expressions can be used to set up row-filters with dynamic values. To distinguish expressions from the rest
of the row-filter text, they should be enclosed within delimiters `${{` and `}}`. Examples:

Row-filter expression to restrict users to access only rows belonging to their department:

```text
dept_code == ${{USER.department}}
```

Row-filter expression to restrict users to access only rows from data sources specified in user attribute named `allowedSources`:

```text
data_source in (${{USER.allowedSources}})
```

### Resource names

Use of expressions in resource names can help reduce the number of policies, which in turn makes it easier to
manage policies. Examples:

Policy resource for home directory of the user:

```text
/home/${{REQ.user}}
```

Policy resource for directory of user's department:

```text
/data/dept/${{USER.dept}}
```

Policy resource for database of user's department:

```text
db_${{USER.dept}}
```

## Supported expressions

Expressions fall into three groups: functions that return a value, tests that return `true` or `false`, and
context objects that you can navigate with dot notation.

### Value functions

Each function returns a string. Functions that can produce several values return them as a CSV (comma separated
values) string.

| Function | Returns | Example |
|---|---|---|
| `GET_TAG_NAMES()` | Names of tags associated with the resource | `PII,FINANCE` |
| `GET_TAG_ATTR_NAMES()` | Names of attributes in all tags associated with the resource | `piiType,sensitiveLevel` |
| `GET_TAG_ATTR(attrName)` | Value of the given attribute in tags associated with the resource | `email` |
| `GET_UG_NAMES()` | Names of groups the user belongs to | `managers,finance-admins` |
| `GET_UG_ATTR_NAMES()` | Names of all attributes in groups the user belongs to | `attr1,attr2` |
| `GET_UG_ATTR(attrName)` | Value of the given attribute in groups the user belongs to | `val1` |
| `GET_UR_NAMES()` | Names of roles assigned to the user | `analyst,dba` |
| `GET_USER_ATTR_NAMES()` | Names of all attributes of the user | `allowedSensitiveLevel,allowedSources` |
| `GET_USER_ATTR(attrName)` | Value of the given attribute associated with the user | `10` |

### Boolean tests

Each test evaluates to `true` or `false`.

| Test | Question it answers |
|---|---|
| `HAS_TAG(tagName)` | Is the given tag associated with the resource? |
| `HAS_ANY_TAG` | Is any tag associated with the resource? |
| `HAS_NO_TAG` | Are no tags associated with the resource? |
| `HAS_TAG_ATTR(attrName)` | Does any tag associated with the resource have the specified attribute? |
| `HAS_USER_ATTR(attrName)` | Does the user have the given attribute? |
| `HAS_UG_ATTR(attrName)` | Does any group associated with the user have the specified attribute? |
| `IS_IN_GROUP(groupName)` | Does the user belong to the specified group? |
| `IS_IN_ROLE(roleName)` | Is the user assigned to the specified role? |
| `IS_IN_ANY_GROUP` | Does the user belong to any group? |
| `IS_IN_ANY_ROLE` | Is any role assigned to the user? |
| `IS_NOT_IN_ANY_GROUP` | Does the user belong to no group? |
| `IS_NOT_IN_ANY_ROLE` | Is the user associated with no roles? |

### Context objects

Context objects expose the request, the resource, the user and the tags as maps and lists. Access their members with
dot notation, for example `USER.allowedSensitiveLevel` or `TAG.sensitiveLevel`.

`REQ`
:   Request details, as a map.

    ```json
    {
      "accessType":      "select",
      "clientIPAddress": "10.120.27.49",
      "clusterType":     "etl",
      "clusterName":     "etl-e1",
      "user":            "scott",
      "userGroups":      [ "g1" ],
      "userRoles":       [ "r1" ]
    }
    ```

`RES`
:   Resource details, as a map.

    ```json
    {
      "database":   "db1",
      "table":      "tbl1",
      "column":     "col1",
      "_ownerUser": "jane"
    }
    ```

`USER`
:   Current user, as a map of the user name and the user's attributes.

    ```json
    {
      "_name": "scott",
      "allowedSensitiveLevel": 10
    }
    ```

`UGNAMES`
:   Names of groups the user belongs to, as a list.

    ```json
    [ "g1" ]
    ```

`URNAMES`
:   Names of roles the user is assigned to, as a list.

    ```json
    [ "r1" ]
    ```

`TAG`
:   Current tag, as a map. Available only in tag-based policies.

    ```json
    {
      "_type": "SENSITIVE",
      "sensitiveLevel": 10
    }
    ```

`TAGNAMES`
:   Names of tags associated with the resource, as a list.

    ```json
    [ "PII", "SENSITIVE" ]
    ```

`TAGS`
:   All tags associated with the resource, as a map keyed by tag name.

    ```json
    {
      "SENSITIVE": {
        "_type": "SENSITIVE",
        "level": 10
      },
      "PII": {
        "_type":   "PII",
        "piiType": "email"
      }
    }
    ```

Most functions listed above take optional parameters, to make it easier to handle use cases that require special handling.

### Default value

A function call can include a default value as an optional parameter, which will be returned when there is no
value available. For example, consider the following expression:

```text
USER.allowedSensitiveLevel >= TAG.sensitiveLevel
```

When the user doesn't have an attribute named `allowedSensitiveLevel`, the expression will always evaluate to
false since `USER.allowedSensitiveLevel` would evaluate to `null`. To handle such cases, consider the following
alternate expression which would use `0` as the value instead of `null`:

```text
GET_USER_ATTR('allowedSensitiveLevel', 0) >= TAG.sensitiveLevel
```

Here is another example of using default value in function calls:

```text
dept_code in  (${{GET_UG_ATTR('deptCode', -1)}})
```

### Separator

Functions that return a CSV string, like GET_TAG_NAMES(), can include following optional parameters:

- optional #1. default value: value to return when no value is available
- optional #2. separator: string to use as the separator between values

Here is an example of using optional parameters:

```text
GET_TAG_NAMES('', '|') == 'tag1|tag2|tag3'
```

### Quotes

Each function that returns a CSV string has another version with _Q appended to the function name; this version
surrounds each value within quotes. For example, consider the following row-filter expression:

```text
location_state IN (${{GET_UG_ATTR_Q('state')}})
```

The expression can evaluate to the following, if the user belongs to groups having an attribute named state:

```text
location_state IN ('CA','OR','WA')
```
