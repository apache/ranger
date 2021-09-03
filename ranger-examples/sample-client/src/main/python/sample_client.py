#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time

from apache_ranger.model.ranger_service import *
from apache_ranger.client.ranger_client import *
from apache_ranger.model.ranger_policy  import *


## create a client to connect to Apache Ranger admin server
ranger_url  = 'http://localhost:6080'
ranger_auth = ('admin', 'rangerR0cks!')

# For Kerberos authentication
#
# from requests_kerberos import HTTPKerberosAuth
#
# ranger_auth = HTTPKerberosAuth()


print('Using Ranger at ' + ranger_url);

ranger = RangerClient(ranger_url, ranger_auth)

# to disable SSL certificate validation (not recommended for production use!)
#
# ranger.session.verify = False


print('Listing service-defs..')
service_defs = ranger.find_service_defs()

print('    ' + str(len(service_defs)) + ' service-defs found')
for service_def in service_defs:
    print('        ' + 'id: ' + str(service_def.id) + ', name: ' + service_def.name)


service_name = 'dev_hive-' + str(int(time.time() * 1000))

print('Creating service: name=' + service_name)

service         = RangerService({'name': service_name, 'type': 'hive'})
service.configs = {'username':'hive', 'password':'hive', 'jdbc.driverClassName': 'org.apache.hive.jdbc.HiveDriver', 'jdbc.url': 'jdbc:hive2://ranger-hadoop:10000', 'hadoop.security.authorization': 'true'}

created_service = ranger.create_service(service)

print('    created service: id: ' + str(created_service.id) + ', name: ' + created_service.name)

service_id = created_service.id


print('Retrieving service: id=' + str(service_id))

retrieved_service = ranger.get_service_by_id(service_id)

print('    retrieved service: id: ' + str(retrieved_service.id) + ', name: ' + retrieved_service.name)


print('Retrieving service: name=' + service_name)

retrieved_service = ranger.get_service(service_name)

print('    retrieved service: id: ' + str(retrieved_service.id) + ', name: ' + retrieved_service.name)


print('Updating service: id=' + str(service_id))

saved_value                 = created_service.displayName
created_service.displayName = service_name + '-UPDATE1'

updated_service1 = ranger.update_service_by_id(service_id, created_service)

print('    updated service: id: ' + str(updated_service1.id) + ', displayName: ' + saved_value + ', updatedDisplayName: ' + updated_service1.displayName)


print('Updating service: name=' + service_name)

saved_value                  = updated_service1.displayName
updated_service1.displayName = service_name + '-UPDATE2'

updated_service2 = ranger.update_service(service_name, updated_service1)

print('    updated service: id: ' + str(updated_service2.id) + ', displayName: ' + saved_value + ', updatedDisplayName: ' + updated_service2.displayName)


print('Listing services..')
services = ranger.find_services()

print('    ' + str(len(services)) + ' services found')
for svc in services:
    print('        ' + 'id: ' + str(svc.id) + ', type: ' + svc.type + ', name: ' + svc.name)


print('Deleting service id=' + str(service_id))

ranger.delete_service_by_id(service_id)

print('    deleted service: id: ' + str(service_id) + ', name: ' + updated_service2.name)


print('Deleting service: name=' + service.name)

service_to_delete = ranger.create_service(service)

print('    created service: id: ' + str(service_to_delete.id) + ', name: ' + service_to_delete.name)

ranger.delete_service(service_to_delete.name)

print('    deleted service: id: ' + str(service_to_delete.id) + ', name: ' + service_to_delete.name)


print('Listing services..')
services = ranger.find_services()

print('    ' + str(len(services)) + ' services found')
for svc in services:
    print('        ' + 'id: ' + str(svc.id) + ', type: ' + svc.type + ', name: ' + svc.name)


policy_name = 'test policy'

print('Creating policy: name=' + policy_name)

created_service = ranger.create_service(service)

print('    created service: id: ' + str(created_service.id) + ', name: ' + created_service.name)

service_id   = created_service.id
service_name = created_service.name

policy             = RangerPolicy()
policy.service     = service_name
policy.name        = policy_name
policy.description = 'test description'
policy.resources   = { 'database': RangerPolicyResource({ 'values': ['test_db'] }),
                       'table':    RangerPolicyResource({ 'values': ['test_tbl'] }),
					   'column':   RangerPolicyResource({ 'values': ['*'] }) }

allowItem1          = RangerPolicyItem()
allowItem1.users    = [ 'admin' ]
allowItem1.accesses = [ RangerPolicyItemAccess({ 'type': 'create' }),
                        RangerPolicyItemAccess({ 'type': 'alter' }),
                        RangerPolicyItemAccess({ 'type': 'select' }) ]

denyItem1          = RangerPolicyItem()
denyItem1.users    = [ 'admin' ]
denyItem1.accesses = [ RangerPolicyItemAccess({ 'type': 'drop' }) ]

policy.policyItems     = [ allowItem1 ]
policy.denyPolicyItems = [ denyItem1 ]

created_policy = ranger.create_policy(policy)

print('    created policy: id: ' + str(created_policy.id) + ', name: ' + created_policy.name)

policy_id = created_policy.id


data_mask_policy_name = 'test masking policy'

print('Creating data-masking policy: name=' + data_mask_policy_name)

data_mask_policy             = RangerPolicy()
data_mask_policy.service     = service_name
data_mask_policy.policyType  = RangerPolicy.POLICY_TYPE_DATAMASK
data_mask_policy.name        = data_mask_policy_name
data_mask_policy.description = 'test description'
data_mask_policy.resources   = { 'database': RangerPolicyResource({ 'values': ['test_db'] }),
                                 'table':    RangerPolicyResource({ 'values': ['test_tbl'] }),
					             'column':   RangerPolicyResource({ 'values': ['test_col'] }) }

policyItem1              = RangerDataMaskPolicyItem()
policyItem1.users        = [ 'admin' ]
policyItem1.accesses     = [ RangerPolicyItemAccess({ 'type': 'select' }) ]
policyItem1.dataMaskInfo = RangerPolicyItemDataMaskInfo({ 'dataMaskType': 'MASK_SHOW_LAST_4' })

data_mask_policy.dataMaskPolicyItems = [ policyItem1 ]

created_data_mask_policy = ranger.create_policy(data_mask_policy)

print('    created data-masking policy: id: ' + str(created_data_mask_policy.id) + ', name: ' + created_data_mask_policy.name)


row_filter_policy_name = 'test row filter policy'

print('Creating row-filtering policy: name=' + row_filter_policy_name)

row_filter_policy             = RangerPolicy()
row_filter_policy.service     = service_name
row_filter_policy.policyType  = RangerPolicy.POLICY_TYPE_ROWFILTER
row_filter_policy.name        = row_filter_policy_name
row_filter_policy.description = 'test description'
row_filter_policy.resources   = { 'database': RangerPolicyResource({ 'values': ['test_db'] }),
                                  'table':    RangerPolicyResource({ 'values': ['test_tbl'] }) }

policyItem1               = RangerRowFilterPolicyItem()
policyItem1.users         = [ 'admin' ]
policyItem1.accesses      = [ RangerPolicyItemAccess({ 'type': 'select' }) ]
policyItem1.rowFilterInfo = RangerPolicyItemRowFilterInfo({ 'filterExpr': 'country_code = "US"' })

row_filter_policy.rowFilterPolicyItems = [ policyItem1 ]

create_row_filter_policy = ranger.create_policy(row_filter_policy)

print('    created row-filtering policy: id: ' + str(create_row_filter_policy.id) + ', name: ' + create_row_filter_policy.name)


print('Retrieving policy: id=' + str(policy_id))

retrieved_policy = ranger.get_policy_by_id(policy_id)

print('    retrieved policy: id: ' + str(retrieved_policy.id) + ', name: ' + retrieved_policy.name)


print('Retrieving policy: service_name=' + service_name + ', policy_name=' + data_mask_policy.name)

retrieved_policy = ranger.get_policy(service_name, data_mask_policy.name)

print('    retrieved policy: id: ' + str(retrieved_policy.id) + ', name: ' + retrieved_policy.name)


print('Retrieving policy: service_name=' + service_name + ', policy_name=' + row_filter_policy.name)

retrieved_policy = ranger.get_policy(service_name, row_filter_policy.name)

print('    retrieved policy: id: ' + str(retrieved_policy.id) + ', name: ' + retrieved_policy.name)


print('Retrieving policies in service ' + created_policy.service + '..')

policies = ranger.get_policies_in_service(created_policy.service)

print('    ' + str(len(policies)) + ' policies found')
for plcy in policies:
    print('        id: ' + str(plcy.id) + ', service: ' + plcy.service + ', name: ' + plcy.name)


print('Updating policy: id=' + str(policy_id))

saved_value                = created_policy.description
created_policy.description = 'updated description - #1'

updated_policy1 = ranger.update_policy_by_id(policy_id, created_policy)

print('    updated policy: id: ' + str(updated_policy1.id) + ', description: ' + saved_value + ', updatedDescription: ' + updated_policy1.description)


print('Updating policy: service_name=' + service_name + ', policy_name=' + policy_name)

saved_value                 = updated_policy1.description
updated_policy1.description = 'updated description - #2'

updated_policy2 = ranger.update_policy(service_name, policy_name, updated_policy1)

print('    updated policy: id: ' + str(updated_policy2.id) + ', description: ' + saved_value + ', updatedDescription: ' + updated_policy2.description)


print('Deleting policy: id=' + str(policy_id))

ranger.delete_policy_by_id(policy_id)

print('    deleted policy: id: ' + str(policy_id) + ', name: ' + updated_policy2.name)


print('Deleting policy: service_name=' + data_mask_policy.service + ', policy_name=' + data_mask_policy.name)

ranger.delete_policy(data_mask_policy.service, data_mask_policy.name)

print('    deleted policy: id: ' + str(data_mask_policy.id) + ', name: ' + data_mask_policy.name)


print('Deleting policy: service_name=' + row_filter_policy.service + ', policy_name=' + row_filter_policy.name)

ranger.delete_policy(row_filter_policy.service, row_filter_policy.name)

print('    deleted policy: id: ' + str(row_filter_policy.id) + ', name: ' + row_filter_policy.name)

ranger.delete_service_by_id(service_id)


print('Listing policies..')
policies = ranger.find_policies()

print('    ' + str(len(policies)) + ' policies found')
for policy in policies:
    print('        id: ' + str(policy.id) + ', service: ' + policy.service + ', name: ' + policy.name)


print('Listing security zones..')
security_zones = ranger.find_security_zones()

print('    ' + str(len(security_zones)) + ' security zones found')
for security_zone in security_zones:
    print('        id: ' + str(security_zone.id) + ', name: ' + security_zone.name)


print('Listing roles..')
roles = ranger.find_roles()

print('    ' + str(len(roles)) + ' roles zones found')
for role in roles:
    print('        id: ' + str(role.id) + ', name: ' + role.name)

