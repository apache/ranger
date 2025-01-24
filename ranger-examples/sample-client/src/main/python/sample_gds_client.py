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


from apache_ranger.client.ranger_client           import *
from apache_ranger.client.ranger_gds_client       import *
from apache_ranger.client.ranger_user_mgmt_client import *
from apache_ranger.model.ranger_gds               import *
from apache_ranger.model.ranger_policy            import *
from apache_ranger.model.ranger_principal         import *
from apache_ranger.model.ranger_security_zone     import *
from apache_ranger.model.ranger_user_mgmt         import *

ranger_url  = 'http://localhost:6080'
ranger_auth = ('admin', 'rangerR0cks!')

ranger    = RangerClient(ranger_url, ranger_auth)
gds       = RangerGdsClient(ranger)
user_mgmt = RangerUserMgmtClient(ranger)

def get_or_create_principal(principal):
  if isinstance(principal, RangerUser):
    existing = user_mgmt.get_user(principal.name)

    ret = existing if existing is not None else user_mgmt.create_user(principal)
  elif isinstance(principal, RangerGroup):
    existing = user_mgmt.get_group(principal.name)

    ret = existing if existing is not None else user_mgmt.create_group(principal)
  else:
    ret = None

  return ret

def create_or_update_zone(zone):
  try:
    existing = ranger.get_security_zone(zone.name)
  except Exception:
    existing = None

  if existing is None:
    ret = ranger.create_security_zone(zone)
  else:
    ret = ranger.update_security_zone_by_id(existing.id, zone)

  return ret

usr_john_d   = get_or_create_principal(RangerUser({ 'name': 'John.Doe',       'firstName': 'John',  'emailAddress': 'john.doe@example.com' ,      'password': 'rangerR0cks!' }))
usr_diane_s  = get_or_create_principal(RangerUser({ 'name': 'Diane.Scott',    'firstName': 'Diane', 'emailAddress': 'diane.scott@example.com' ,   'password': 'rangerR0cks!' }))
usr_sandy_w  = get_or_create_principal(RangerUser({ 'name': 'Sandy.Williams', 'firstName': 'Sandy', 'emailAddress': 'sandy.williams@example.com', 'password': 'rangerR0cks!' }))
grp_sales    = get_or_create_principal(RangerGroup({ 'name': 'sales'     }))
grp_finance  = get_or_create_principal(RangerGroup({ 'name': 'finance'   }))
grp_shipping = get_or_create_principal(RangerGroup({ 'name': 'shipping' }))

hive_service = 'dev_hive'
hdfs_service = 'dev_hdfs'

zone_sales    = RangerSecurityZone({ 'name': 'sales',    'description': 'Sales data',    'services': { }, 'adminUsers': [ 'admin' ], 'auditUsers': [ 'admin' ] })
zone_finance  = RangerSecurityZone({ 'name': 'finance',  'description': 'Finance data',  'services': { }, 'adminUsers': [ 'admin' ], 'auditUsers': [ 'admin' ] })
zone_shipping = RangerSecurityZone({ 'name': 'shipping', 'description': 'Shipping data', 'services': { }, 'adminUsers': [ 'admin' ], 'auditUsers': [ 'admin' ] })

zone_sales.services[hive_service]    = RangerSecurityZoneService({ 'resources': [ { 'database': [ 'sales'     ] } ] })
zone_sales.services[hdfs_service]    = RangerSecurityZoneService({ 'resources': [ { 'path':     [ '/sales'    ] } ] })
zone_finance.services[hive_service]  = RangerSecurityZoneService({ 'resources': [ { 'database': [ 'finance'   ] } ] })
zone_finance.services[hdfs_service]  = RangerSecurityZoneService({ 'resources': [ { 'path':     [ '/finance'  ] } ] })
zone_shipping.services[hive_service] = RangerSecurityZoneService({ 'resources': [ { 'database': [ 'shipping'  ] } ] })
zone_shipping.services[hdfs_service] = RangerSecurityZoneService({ 'resources': [ { 'path':     [ '/shipping' ] } ] })

dataset_1 = RangerDataset({ 'name': 'dataset-1', 'description': 'the first dataset!', 'acl': { 'users': { usr_john_d.name: GdsPermission.ADMIN } } })
dataset_2 = RangerDataset({ 'name': 'dataset-2', 'description': 'the second dataset!', 'acl': { 'groups': { grp_sales.name: GdsPermission.ADMIN } } })
dataset_3 = RangerDataset({ 'name': 'dataset-3', 'description': 'the third dataset!', 'acl': { 'users': { usr_john_d.name: GdsPermission.ADMIN } } })
dataset_4 = RangerDataset({ 'name': 'dataset-4', 'description': 'the fourth dataset!', 'acl': { 'groups': { grp_sales.name: GdsPermission.ADMIN } } })

project_1 = RangerProject({ 'name': 'project-1', 'description': 'the first project!', 'acl': { 'users': { usr_diane_s.name: GdsPermission.ADMIN } } })
project_2 = RangerProject({ 'name': 'project-2', 'description': 'the second project!', 'acl': { 'groups': { grp_shipping.name: GdsPermission.ADMIN } } })

hive_share_1 = RangerDataShare({ 'name': 'hive-sales-2023',         'service': hive_service, 'zone': zone_sales.name,    'acl': { 'groups': { grp_sales.name:    GdsPermission.ADMIN }}})
hive_share_2 = RangerDataShare({ 'name': 'hive-finance-2023',       'service': hive_service, 'zone': zone_finance.name,  'acl': { 'groups': { grp_finance.name:  GdsPermission.ADMIN }}})
hive_share_3 = RangerDataShare({ 'name': 'hive-shipping-2023',      'service': hive_service, 'zone': zone_shipping.name, 'acl': { 'groups': { grp_shipping.name: GdsPermission.ADMIN }}})
hive_share_4 = RangerDataShare({ 'name': 'hive-new-customers-2023', 'service': hive_service, 'zone': None,               'acl': { 'users':  { usr_sandy_w.name:  GdsPermission.ADMIN }}})
hive_share_5 = RangerDataShare({ 'name': 'hive-facilities',         'service': hive_service, 'zone': None,               'acl': { 'users':  { usr_diane_s.name:  GdsPermission.ADMIN }}})
hdfs_share_1 = RangerDataShare({ 'name': 'hdfs-sales-2023',         'service': hdfs_service, 'zone': zone_sales.name,    'acl': { 'groups': { grp_sales.name:    GdsPermission.ADMIN }}})
hdfs_share_2 = RangerDataShare({ 'name': 'hdfs-finance-2023',       'service': hdfs_service, 'zone': zone_finance.name,  'acl': { 'groups': { grp_finance.name:  GdsPermission.ADMIN }}})

hive_share_1.defaultAccessTypes = [ 'select' ]
hive_share_1.defaultTagMasks    = [ { 'values': [ 'PII' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hive_share_2.defaultAccessTypes = [ 'select' ]
hive_share_2.defaultTagMasks    = [ { 'values': [ 'PII' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hive_share_3.defaultAccessTypes = [ 'select' ]
hive_share_3.defaultTagMasks    = [ { 'values': [ 'PII' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hive_share_4.defaultAccessTypes = [ 'select' ]
hive_share_4.defaultTagMasks    = [ { 'values': [ 'PII' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hive_share_5.defaultAccessTypes = [ 'select' ]
hive_share_5.defaultTagMasks    = [ { 'values': [ 'PII' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hdfs_share_1.defaultAccessTypes = [ 'read' ]

hdfs_share_2.defaultAccessTypes = [ 'read' ]


print(f'Creating zone: name={zone_sales}')
zone_sales = create_or_update_zone(zone_sales)
print(f'  created zone: {zone_sales}')

print(f'Creating zone: name={zone_finance.name}')
zone_finance = create_or_update_zone(zone_finance)
print(f'  created zone: {zone_finance}')

print(f'Creating zone: name={zone_shipping.name}')
zone_finance = create_or_update_zone(zone_shipping)
print(f'  created zone: {zone_shipping}')

print(f'Creating dataset: name={dataset_1.name}')
dataset_1 = gds.create_dataset(dataset_1)
print(f'  created dataset: {dataset_1}')

print(f'Creating dataset: name={dataset_2.name}')
dataset_2 = gds.create_dataset(dataset_2)
print(f'  created dataset: {dataset_2}')

print(f'Creating dataset: name={dataset_3.name}')
dataset_3 = gds.create_dataset(dataset_3)
print(f'  created dataset: {dataset_3}')

print(f'Creating dataset: name={dataset_4.name}')
dataset_4 = gds.create_dataset(dataset_4)
print(f'  created dataset: {dataset_4}')

print(f'Creating project: name={project_1.name}')
project_1 = gds.create_project(project_1)
print(f'  created project: {project_1}')

print(f'Creating project: name={project_2.name}')
project_2 = gds.create_project(project_2)
print(f'  created project: {project_2}')

print(f'Creating data_share: name={hive_share_1.name}')
hive_share_1 = gds.create_data_share(hive_share_1)
print(f'  created data_share: {hive_share_1}')

print(f'Creating data_share: name={hive_share_2.name}')
hive_share_2 = gds.create_data_share(hive_share_2)
print(f'  created data_share: {hive_share_2}')

print(f'Creating data_share: name={hive_share_3.name}')
hive_share_3 = gds.create_data_share(hive_share_3)
print(f'  created data_share: {hive_share_3}')

print(f'Creating data_share: name={hive_share_4.name}')
hive_share_4 = gds.create_data_share(hive_share_4)
print(f'  created data_share: {hive_share_4}')

print(f'Creating data_share: name={hive_share_5.name}')
hive_share_5 = gds.create_data_share(hive_share_5)
print(f'  created data_share: {hive_share_5}')

print(f'Creating data_share: name={hdfs_share_1.name}')
hdfs_share_1 = gds.create_data_share(hdfs_share_1)
print(f'  created data_share: {hdfs_share_1}')

print(f'Creating data_share: name={hdfs_share_2.name}')
hdfs_share_2 = gds.create_data_share(hdfs_share_2)
print(f'  created data_share: {hdfs_share_2}')


hive_resource_1 = RangerSharedResource({ 'dataShareId': hive_share_1.id, 'name': 'sales.prospects', 'resource': { 'database': { 'values': ['sales'] },    'table': { 'values': ['prospects'] } }, 'subResourceType': 'column', 'subResource': { 'values': [ '*' ]}, 'accessTypes': ['select']})
hive_resource_2 = RangerSharedResource({ 'dataShareId': hive_share_1.id, 'name': 'sales.orders', 'resource': { 'database': { 'values': ['sales'] },    'table': { 'values': ['orders'] } },    'subResourceType': 'column', 'subResource': { 'values': [ '*' ]}, 'accessTypes': ['select']})
hive_resource_3 = RangerSharedResource({ 'dataShareId': hive_share_2.id, 'name': 'finance.invoices', 'resource': { 'database': { 'values': ['finance'] },  'table': { 'values': ['invoices'] } },  'subResourceType': 'column', 'subResource': { 'values': [ '*' ]}, 'accessTypes': ['select']})
hive_resource_4 = RangerSharedResource({ 'dataShareId': hive_share_2.id, 'name': 'finance.payments', 'resource': { 'database': { 'values': ['finance'] },  'table': { 'values': ['payments'] } },  'subResourceType': 'column', 'subResource': { 'values': [ '*' ]}, 'accessTypes': ['select']})
hive_resource_5 = RangerSharedResource({ 'dataShareId': hive_share_3.id, 'name': 'shipping.shipments', 'resource': { 'database': { 'values': ['shipping'] }, 'table': { 'values': ['shipments'] } },  'subResourceType': 'column', 'subResource': { 'values': [ '*' ]}, 'accessTypes': ['select']})
hive_resource_6 = RangerSharedResource({ 'dataShareId': hive_share_4.id, 'name': 'customers.contact_info', 'resource': { 'database': { 'values': ['customers'] }, 'table': { 'values': ['contact_info'] } },  'subResourceType': 'column', 'subResource': { 'values': [ '*' ]}, 'accessTypes': ['select']})
hive_resource_7 = RangerSharedResource({ 'dataShareId': hive_share_5.id, 'name': 'operations.facilities', 'resource': { 'database': { 'values': ['operations'] }, 'table': { 'values': ['facilities'] } },  'subResourceType': 'column', 'subResource': { 'values': [ '*' ]}, 'accessTypes': ['select']})
hdfs_resource_1 = RangerSharedResource({ 'dataShareId': hdfs_share_1.id, 'name': '/sales/2023/', 'resource': { 'path': { 'values': [ '/sales/2023/' ],   'isRecursive': True } }, 'accessTypes': ['read']})
hdfs_resource_2 = RangerSharedResource({ 'dataShareId': hdfs_share_2.id, 'name': '/finance/2023/', 'resource': { 'path': { 'values': [ '/finance/2023/' ], 'isRecursive': True } }, 'accessTypes': ['read']})

hive_resource_1.rowFilter        = { 'filterExpr': "created_time >= '2023-01-01' and created_time < '2024-01-01'" }
hive_resource_1.subResourceMasks = [ { 'values': [ 'col1' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hive_resource_2.rowFilter        = { 'filterExpr': "created_time >= '2023-01-01' and created_time < '2024-01-01'" }
hive_resource_2.subResourceMasks = [ { 'values': [ 'amount' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hive_resource_3.rowFilter        = { 'filterExpr': "created_time >= '2023-01-01' and created_time < '2024-01-01'" }
hive_resource_3.subResourceMasks = [ { 'values': [ 'amount' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hive_resource_4.rowFilter        = { 'filterExpr': "created_time >= '2023-01-01' and created_time < '2024-01-01'" }
hive_resource_4.subResourceMasks = [ { 'values': [ 'amount' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hive_resource_5.rowFilter        = { 'filterExpr': "created_time >= '2023-01-01' and created_time < '2024-01-01'" }
hive_resource_5.subResourceMasks = [ { 'values': [ 'amount' ], 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hive_resource_6.rowFilter        = { 'filterExpr': "created_time >= '2023-01-01' and created_time < '2024-01-01'" }


print(f'Adding shared resource: ')
hive_resource_1 = gds.add_shared_resource(hive_resource_1)
print(f'  created shared resource: {hive_resource_1}')

print(f'Adding shared resource: ')
hive_resource_2 = gds.add_shared_resource(hive_resource_2)
print(f'  created shared resource: {hive_resource_2}')

print(f'Adding shared resource: ')
hive_resource_3 = gds.add_shared_resource(hive_resource_3)
print(f'  created shared resource: {hive_resource_3}')

print(f'Adding shared resource: ')
hive_resource_4 = gds.add_shared_resource(hive_resource_4)
print(f'  created shared resource: {hive_resource_4}')

print(f'Adding shared resource: ')
hive_resource_5 = gds.add_shared_resource(hive_resource_5)
print(f'  created shared resource: {hive_resource_5}')

print(f'Adding shared resource: ')
hive_resource_6 = gds.add_shared_resource(hive_resource_6)
print(f'  created shared resource: {hive_resource_6}')

print(f'Adding shared resource: ')
hive_resource_7 = gds.add_shared_resource(hive_resource_7)
print(f'  created shared resource: {hive_resource_7}')

print(f'Adding shared resource: ')
hdfs_resource_1 = gds.add_shared_resource(hdfs_resource_1)
print(f'  created shared resource: {hdfs_resource_1}')

print(f'Adding shared resource: ')
hdfs_resource_2 = gds.add_shared_resource(hdfs_resource_2)
print(f'  created shared resource: {hdfs_resource_2}')


dshid_1 = RangerDataShareInDataset({ 'dataShareId': hive_share_1.id, 'datasetId': dataset_1.id, 'status': GdsShareStatus.REQUESTED, 'validitySchedule': { 'startTime': '2023/01/01', 'endTime': '2024/01/01' } })
dshid_2 = RangerDataShareInDataset({ 'dataShareId': hive_share_2.id, 'datasetId': dataset_1.id, 'status': GdsShareStatus.REQUESTED })
dshid_3 = RangerDataShareInDataset({ 'dataShareId': hive_share_2.id, 'datasetId': dataset_2.id, 'status': GdsShareStatus.ACTIVE })
dshid_4 = RangerDataShareInDataset({ 'dataShareId': hive_share_3.id, 'datasetId': dataset_2.id, 'status': GdsShareStatus.ACTIVE })
dshid_5 = RangerDataShareInDataset({ 'dataShareId': hive_share_4.id, 'datasetId': dataset_3.id, 'status': GdsShareStatus.ACTIVE })
dshid_6 = RangerDataShareInDataset({ 'dataShareId': hive_share_5.id, 'datasetId': dataset_4.id, 'status': GdsShareStatus.ACTIVE })
dshid_7 = RangerDataShareInDataset({ 'dataShareId': hdfs_share_1.id, 'datasetId': dataset_1.id, 'status': GdsShareStatus.ACTIVE })
dshid_8 = RangerDataShareInDataset({ 'dataShareId': hdfs_share_2.id, 'datasetId': dataset_2.id, 'status': GdsShareStatus.ACTIVE })

print(f'Adding data_share_in_dataset: ')
dshid_1 = gds.add_data_share_in_dataset(dshid_1)
print(f'  created data_share_in_dataset: {dshid_1}')

print(f'Adding data_share_in_dataset: ')
dshid_2 = gds.add_data_share_in_dataset(dshid_2)
print(f'  created data_share_in_dataset: {dshid_2}')

print(f'Adding data_share_in_dataset: ')
dshid_3 = gds.add_data_share_in_dataset(dshid_3)
print(f'  created data_share_in_dataset: {dshid_3}')

print(f'Adding data_share_in_dataset: ')
dshid_4 = gds.add_data_share_in_dataset(dshid_4)
print(f'  created data_share_in_dataset: {dshid_4}')

print(f'Adding data_share_in_dataset: ')
dshid_5 = gds.add_data_share_in_dataset(dshid_5)
print(f'  created data_share_in_dataset: {dshid_5}')

print(f'Adding data_share_in_dataset: ')
dshid_6 = gds.add_data_share_in_dataset(dshid_6)
print(f'  created data_share_in_dataset: {dshid_6}')

print(f'Adding data_share_in_dataset: ')
dshid_7 = gds.add_data_share_in_dataset(dshid_7)
print(f'  created data_share_in_dataset: {dshid_7}')

print(f'Updating data_share_in_dataset: id={dshid_1.id}')
dshid = RangerDataShareInDataset()
dshid.id          = dshid_1.id
dshid.dataShareId = dshid_1.dataShareId
dshid.datasetId   = dshid_1.datasetId
dshid.status = GdsShareStatus.GRANTED
# dshid_1.status = GdsShareStatus.GRANTED
dshid_1 = gds.update_data_share_in_dataset(dshid.id, dshid)
print(f'  updated data_share_in_dataset: {dshid_1}')

print(f'Updating data_share_in_dataset: id={dshid_1.id}')
dshid_1.status = GdsShareStatus.ACTIVE
dshid_1 = gds.update_data_share_in_dataset(dshid_1.id, dshid_1)
print(f'  updated data_share_in_dataset: {dshid_1}')

print(f'Updating data_share_in_dataset: id={dshid_2.id}')
dshid_2.status           = GdsShareStatus.GRANTED
dshid_2.validitySchedule = { 'startTime': '2023/01/01', 'endTime': '2024/01/01' }
dshid_2 = gds.update_data_share_in_dataset(dshid_2.id, dshid_2)
print(f'  updated data_share_in_dataset: {dshid_2}')

print(f'Adding policy for dataset {dataset_1.name}: ')
policy = gds.add_dataset_policy(dataset_1.id, RangerPolicy({ 'name': dataset_1.name }))
print(f'  added policy for dataset {dataset_1.name}: {policy}')

print(f'Adding policy for dataset {dataset_2.name}: ')
policy = gds.add_dataset_policy(dataset_2.id, RangerPolicy({ 'name': dataset_2.name }))
print(f'  added policy for dataset {dataset_2.name}: {policy}')

print(f'Adding policy for dataset {dataset_3.name}: ')
policy = gds.add_dataset_policy(dataset_3.id, RangerPolicy({ 'name': dataset_3.name }))
print(f'  added policy for dataset {dataset_3.name}: {policy}')

print(f'Adding policy for dataset {dataset_4.name}: ')
policy = gds.add_dataset_policy(dataset_4.id, RangerPolicy({ 'name': dataset_4.name }))
print(f'  added policy for dataset {dataset_4.name}: {policy}')


d1_in_p1 = RangerDatasetInProject({ 'datasetId': dataset_1.id, 'projectId': project_1.id, 'status': GdsShareStatus.GRANTED, 'validitySchedule': { 'startTime': '2023/01/01', 'endTime': '2023/04/01' }})
d2_in_p1 = RangerDatasetInProject({ 'datasetId': dataset_2.id, 'projectId': project_1.id, 'status': GdsShareStatus.GRANTED })
d3_in_p2 = RangerDatasetInProject({ 'datasetId': dataset_3.id, 'projectId': project_2.id, 'status': GdsShareStatus.REQUESTED })

print(f'Creating dataset_in_project: {d1_in_p1.name}')
d1_in_p1 = gds.add_dataset_in_project(d1_in_p1)
print(f'  created dataset_in_project: {d1_in_p1}')

print(f'Creating dataset_in_project: {d2_in_p1.name}')
d2_in_p1 = gds.add_dataset_in_project(d2_in_p1)
print(f'  created dataset_in_project: {d2_in_p1}')

print(f'Creating dataset_in_project: {d3_in_p2.name}')
d3_in_p2 = gds.add_dataset_in_project(d3_in_p2)
print(f'  created dataset_in_project: {d3_in_p2}')

print(f'Updating dataset_in_project: id={d2_in_p1.id}')
d2_in_p1.status = GdsShareStatus.ACTIVE
d2_in_p1        = gds.update_dataset_in_project(d2_in_p1.id, d2_in_p1)
print(f'  updated dataset_in_project: {d2_in_p1}')

print(f'Updating dataset_in_project: id={d3_in_p2.id}')
d3_in_p2.status = GdsShareStatus.ACTIVE
d3_in_p2        = gds.update_dataset_in_project(d3_in_p2.id, d3_in_p2)
print(f'  updated dataset_in_project: {d3_in_p2}')

print(f'Adding policy for project {project_1.name}: ')
policy = gds.add_project_policy(project_1.id, RangerPolicy({ 'name': project_1.name }))
print(f'  added policy for project {project_1.name}: {policy}')

policies = gds.get_project_policies(project_1.id)
print(f'  policies for project {project_1.name}: {policies}')

print(f'Adding policy for project {project_2.name}: ')
policy = gds.add_project_policy(project_2.id, RangerPolicy({ 'name': project_2.name }))
print(f'  added policy for project {project_2.name}: {policy}')


print(f'Deleting project: id={project_1.id}')
gds.delete_project(project_1.id, True)
print(f'  deleted project: id={project_1.id}, name={project_1.name}')

print(f'Deleting project: id={project_2.id}')
gds.delete_project(project_2.id, True)
print(f'  deleted project: id={project_2.id}, name={project_2.name}')

print(f'Deleting dataset: id={dataset_1.id}')
gds.delete_dataset(dataset_1.id, True)
print(f'  deleted dataset: id={dataset_1.id}, name={dataset_1.name}')

print(f'Deleting dataset: id={dataset_2.id}')
gds.delete_dataset(dataset_2.id, True)
print(f'  deleted dataset: id={dataset_2.id}, name={dataset_2.name}')

print(f'Deleting dataset: id={dataset_3.id}')
gds.delete_dataset(dataset_3.id, True)
print(f'  deleted dataset: id={dataset_3.id}, name={dataset_3.name}')

print(f'Deleting dataset: id={dataset_4.id}')
gds.delete_dataset(dataset_4.id, True)
print(f'  deleted dataset: id={dataset_4.id}, name={dataset_4.name}')

print(f'Deleting data_share: id={hive_share_1.id}')
gds.delete_data_share(hive_share_1.id, True)
print(f'  deleted data_share: id={hive_share_1.id}, name={hive_share_1.name}')

print(f'Deleting data_share: id={hive_share_2.id}')
gds.delete_data_share(hive_share_2.id, True)
print(f'  deleted data_share: id={hive_share_2.id}, name={hive_share_2.name}')

print(f'Deleting data_share: id={hive_share_3.id}')
gds.delete_data_share(hive_share_3.id, True)
print(f'  deleted data_share: id={hive_share_3.id}, name={hive_share_3.name}')

print(f'Deleting data_share: id={hive_share_4.id}')
gds.delete_data_share(hive_share_4.id, True)
print(f'  deleted data_share: id={hive_share_4.id}, name={hive_share_4.name}')

print(f'Deleting data_share: id={hive_share_5.id}')
gds.delete_data_share(hive_share_5.id, True)
print(f'  deleted data_share: id={hive_share_5.id}, name={hive_share_5.name}')

print(f'Deleting data_share: id={hdfs_share_1.id}')
gds.delete_data_share(hdfs_share_1.id, True)
print(f'  deleted data_share: id={hdfs_share_1.id}, name={hdfs_share_1.name}')

print(f'Deleting data_share: id={hdfs_share_2.id}')
gds.delete_data_share(hdfs_share_2.id, True)
print(f'  deleted data_share: id={hdfs_share_2.id}, name={hdfs_share_2.name}')
