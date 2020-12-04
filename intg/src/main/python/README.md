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

# Apache Ranger - Python client

This is a python library for Apache Ranger. Users can integrate with Apache Ranger using the python client.
Currently, compatible with Python 3.5+

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install python client for Apache Ranger.

```bash
> pip install apache-ranger
```

Verify if apache-ranger client is installed:
```bash
> pip list

Package      Version
------------ ---------
apache-ranger 0.0.1
```

## Usage

```python init_dev_hive.py```
```python
# init_dev_hive.py

from apache_ranger.model.ranger_service import RangerService
from apache_ranger.client.ranger_client import RangerClient
from apache_ranger.model.ranger_policy  import RangerPolicy, RangerPolicyResource, RangerPolicyItem, RangerPolicyItemAccess

service_name = 'dev_hive'

service = RangerService(name=service_name, type='hive')
service.configs = {'username':'hive', 'password':'hive', 'jdbc.driverClassName': 'org.apache.hive.jdbc.HiveDriver', 'jdbc.url': 'jdfb:hive2://ranger-hadoop:10000', 'hadoop.security.authorization': 'true'}

policy = RangerPolicy(service=service_name, name='test policy')
policy.resources = {'database': RangerPolicyResource(['test_db']), 'table': RangerPolicyResource(['test_tbl']), 'column': RangerPolicyResource(['*'])}
policy.policyItems.append(RangerPolicyItem(users=['admin'], accesses=[RangerPolicyItemAccess('create'), RangerPolicyItemAccess('alter'), RangerPolicyItemAccess('drop')], delegateAdmin=True))
policy.denyPolicyItems.append(RangerPolicyItem(users=['admin'], accesses=[RangerPolicyItemAccess('select')]))


ranger_client   = RangerClient('http://localhost:6080', 'admin', 'rangerR0cks!')
created_service = ranger_client.create_service(service)
created_policy  = ranger_client.create_policy(policy)

```
For more examples, checkout `sample-client` python  project in [ranger-examples](https://github.com/apache/ranger/blob/master/ranger-examples/sample-client/src/main/python/sample_client.py) module.
