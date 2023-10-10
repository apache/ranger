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


from apache_ranger.client.ranger_client     import *
from apache_ranger.client.ranger_gds_client import *
from apache_ranger.model.ranger_gds         import *
from apache_ranger.model.ranger_policy      import *
from apache_ranger.model.ranger_principal   import *

ranger_url  = 'http://localhost:6080'
ranger_auth = ('admin', 'rangerR0cks!')

ranger = RangerClient(ranger_url, ranger_auth)
gds    = RangerGdsClient(ranger)

userJohnDoe = RangerPrincipal({ 'type': PrincipalType.USER, 'name': 'John.Doe' })


dataset_1 = RangerDataset({ 'name': 'dataset-1', 'description': 'the first dataset!', 'acl': { 'users': { 'John.Doe': GdsPermission.ADMIN } }, 'termsOfUse': None })
dataset_2 = RangerDataset({ 'name': 'dataset-2', 'description': 'the second dataset!', 'acl': { 'groups': { 'sales': GdsPermission.ADMIN } }, 'termsOfUse': None })

project_1 = RangerProject({ 'name': 'project-1', 'description': 'the first project!', 'acl': { 'users': { 'Diane.Scott': GdsPermission.ADMIN } }, 'termsOfUse': None })
project_2 = RangerProject({ 'name': 'project-2', 'description': 'the second project!', 'acl': { 'groups': { 'marketing': GdsPermission.ADMIN } }, 'termsOfUse': None })

hive_share_1 = RangerDataShare({ 'name': 'datashare-1', 'description': 'the first datashare!', 'acl': { 'users': { 'Sandy.Williams': GdsPermission.ADMIN } }, 'termsOfUse': None })
hive_share_1.service            = 'dev_hive'
hive_share_1.zone               = None
hive_share_1.conditionExpr      = "HAS_TAG('SCAN_COMPLETE')"
hive_share_1.defaultAccessTypes = [ '_READ' ]
hive_share_1.defaultTagMasks    = [ { 'tagName': 'PII', 'maskInfo': { 'dataMaskType': 'MASK' } } ]

hdfs_share_1 = RangerDataShare({ 'name': 'datashare-2', 'description': 'the second datashare!', 'acl': { 'groups': { 'finance': GdsPermission.ADMIN } }, 'termsOfUse': None })
hdfs_share_1.service            = 'dev_hdfs'
hdfs_share_1.zone               = None
hdfs_share_1.conditionExpr      = "HAS_TAG('SCAN_COMPLETE')"
hdfs_share_1.defaultAccessTypes = [ '_READ' ]
hdfs_share_1.defaultTagMasks    = None

print(f'Creating dataset: name={dataset_1.name}')
dataset_1 = gds.create_dataset(dataset_1)
print(f'  created dataset: {dataset_1}')

print(f'Creating dataset: name={dataset_2.name}')
dataset_2 = gds.create_dataset(dataset_2)
print(f'  created dataset: {dataset_2}')

print(f'Creating project: name={project_1.name}')
project_1 = gds.create_project(project_1)
print(f'  created project: {project_1}')

print(f'Creating project: name={project_2.name}')
project_2 = gds.create_project(project_2)
print(f'  created project: {project_2}')

print(f'Creating data_share: name={hive_share_1.name}')
hive_share_1 = gds.create_data_share(hive_share_1)
print(f'  created data_share: {hive_share_1}')

print(f'Creating data_share: name={hdfs_share_1.name}')
hdfs_share_1 = gds.create_data_share(hdfs_share_1)
print(f'  created data_share: {hdfs_share_1}')


hive_resource_1 = RangerSharedResource({ 'dataShareId': hive_share_1.id, 'name': 'db1.tbl1' })
hive_resource_1.resource         = { 'database': { 'values': ['db1'] }, 'table': { 'values': ['tbl1'] } }
hive_resource_1.subResource      = { 'values': [ 'col1', 'col2' ] }
hive_resource_1.subResourceType  = 'columnn'
hive_resource_1.conditionExpr    = "HAS_TAG('SCAN_COMPLETE') && !HAS_TAG('PII') && TAGS['DATA_QUALITY'].score > 0.8"
hive_resource_1.accessTypes      = [ '_READ' ]
hive_resource_1.rowFilter        = { 'filterExpr': "country = 'US'" }
hive_resource_1.subResourceMasks = { 'col1': { 'dataMaskType': 'MASK' } }
hive_resource_1.profiles         = [ 'GDPR', 'HIPPA' ]

hive_resource_2 = RangerSharedResource({ 'dataShareId': hive_share_1.id, 'name': 'db2.tbl2' })
hive_resource_2.resource         = { 'database': { 'values': ['db2'] }, 'table': { 'values': ['tbl2'] } }
hive_resource_2.subResource      = { 'values': [ '*' ] }
hive_resource_2.subResourceType  = 'column'
hive_resource_2.accessTypes      = [ '_READ', '_WRITE' ]
hive_resource_2.profiles         = [ 'GDPR' ]

hdfs_resource_1 = RangerSharedResource({ 'dataShareId': hdfs_share_1.id, 'name': '/home/dept/sales'})
hdfs_resource_1.resource = { 'path': { 'values': [ '/home/dept/sales' ], 'isRecursive': True } }
hdfs_resource_1.profiles = [ 'GDPR' ]

print(f'Adding shared resource: ')
hive_resource_1 = gds.add_shared_resource(hive_resource_1)
print(f'  created shared resource: {hive_resource_1}')

print(f'Adding shared resource: ')
hive_resource_2 = gds.add_shared_resource(hive_resource_2)
print(f'  created shared resource: {hive_resource_2}')

print(f'Adding shared resource: ')
hdfs_resource_1 = gds.add_shared_resource(hdfs_resource_1)
print(f'  created shared resource: {hdfs_resource_1}')


dshid_1 = RangerDataShareInDataset({ 'dataShareId': hive_share_1.id, 'datasetId': dataset_1.id, 'status': GdsShareStatus.REQUESTED, 'validitySchedule': { 'startTime': '2023/01/01', 'endTime': '2023/04/01' } })
dshid_2 = RangerDataShareInDataset({ 'dataShareId': hdfs_share_1.id, 'datasetId': dataset_2.id, 'status': GdsShareStatus.REQUESTED })

print(f'Adding data_share_in_dataset: ')
dshid_1 = gds.add_data_share_in_dataset(dshid_1)
print(f'  created data_share_in_dataset: {dshid_1}')

print(f'Adding data_share_in_dataset: ')
dshid_2 = gds.add_data_share_in_dataset(dshid_2)
print(f'  created data_share_in_dataset: {dshid_2}')

print(f'Updating data_share_in_dataset: id={dshid_1.id}')
dshid_1.status = GdsShareStatus.GRANTED
dshid_1 = gds.update_data_share_in_dataset(dshid_1.id, dshid_1)
print(f'  updated data_share_in_dataset: {dshid_1}')

print(f'Updating data_share_in_dataset: id={dshid_1.id}')
dshid_1.status = GdsShareStatus.ACTIVE
dshid_1 = gds.update_data_share_in_dataset(dshid_1.id, dshid_1)
print(f'  updated data_share_in_dataset: {dshid_1}')

print(f'Updating data_share_in_dataset: id={dshid_2.id}')
dshid_2.status           = GdsShareStatus.GRANTED
dshid_2.validitySchedule = { 'startTime': '2023/02/01', 'endTime': '2023/03/01' }
dshid_2 = gds.update_data_share_in_dataset(dshid_2.id, dshid_2)
print(f'  updated data_share_in_dataset: {dshid_2}')

print(f'Adding policy for dataset {dataset_1.name}: ')
policy = gds.add_dataset_policy(dataset_1.id, RangerPolicy({ 'name': dataset_1.name }))
print(f'  added policy for dataset {dataset_1.name}: {policy}')

policies = gds.get_dataset_policies(dataset_1.id)
print(f'  policies for dataset {dataset_1.name}: {policies}')


d1_in_p1 = RangerDatasetInProject({ 'datasetId': dataset_1.id, 'projectId': project_1.id, 'status': GdsShareStatus.GRANTED, 'validitySchedule': { 'startTime': '2023/01/01', 'endTime': '2023/04/01' }})
d1_in_p2 = RangerDatasetInProject({ 'datasetId': dataset_1.id, 'projectId': project_2.id, 'status': GdsShareStatus.GRANTED, 'validitySchedule': { 'startTime': '2023/01/01', 'endTime': '2023/04/01' }})
d2_in_p2 = RangerDatasetInProject({ 'datasetId': dataset_2.id, 'projectId': project_2.id, 'status': GdsShareStatus.REQUESTED })

print(f'Creating dataset_in_project: {d1_in_p1.name}')
d1_in_p1 = gds.add_dataset_in_project(d1_in_p1)
print(f'  created dataset_in_project: {d1_in_p1}')

print(f'Creating dataset_in_project: {d1_in_p2.name}')
d1_in_p2 = gds.add_dataset_in_project(d1_in_p2)
print(f'  created dataset_in_project: {d1_in_p2}')

print(f'Creating dataset_in_project: {d2_in_p2.name}')
d2_in_p2 = gds.add_dataset_in_project(d2_in_p2)
print(f'  created dataset_in_project: {d2_in_p2}')

print(f'Updating dataset_in_project: id={d2_in_p2.id}')
d2_in_p2.status = GdsShareStatus.GRANTED
d2_in_p2        = gds.update_dataset_in_project(d2_in_p2.id, d2_in_p2)
print(f'  updated dataset_in_project: {d2_in_p2}')

print(f'Adding policy for project {project_1.name}: ')
policy = gds.add_project_policy(project_1.id, RangerPolicy({ 'name': project_1.name }))
print(f'  added policy for project {project_1.name}: {policy}')

policies = gds.get_project_policies(project_1.id)
print(f'  policies for project {project_1.name}: {policies}')


print(f'Removing dataset_in_project: id={d1_in_p1.id}')
gds.remove_dataset_in_project(d1_in_p1.id)
print(f'  deleted dataset_in_project: id={d1_in_p1.id}')

print(f'Removing dataset_in_project: id={d1_in_p2.id}')
gds.remove_dataset_in_project(d1_in_p2.id)
print(f'  deleted dataset_in_project: id={d1_in_p2.id}')

print(f'Removing dataset_in_project: id={d2_in_p2.id}')
gds.remove_dataset_in_project(d2_in_p2.id)
print(f'  deleted dataset_in_project: id={d2_in_p2.id}')

print(f'Removing data_share_in_dataset: id={dshid_1.id}')
gds.remove_data_share_in_dataset(dshid_1.id)
print(f'  deleted data_share_in_dataset: id={dshid_1.id}')

print(f'Removing data_share_in_dataset: id={dshid_2.id}')
gds.remove_data_share_in_dataset(dshid_2.id)
print(f'  deleted data_share_in_dataset: id={dshid_2.id}')

print(f'Removing shared_resource: id={hive_resource_1.id}')
gds.remove_shared_resource(hive_resource_1.id)
print(f'  removed shared_resource: id={hive_resource_1.id}')

print(f'Removing shared_resource: id={hive_resource_2.id}')
gds.remove_shared_resource(hive_resource_2.id)
print(f'  removed shared_resource: id={hive_resource_2.id}')

print(f'Removing shared_resource: id={hdfs_resource_1.id}')
gds.remove_shared_resource(hdfs_resource_1.id)
print(f'  removed shared_resource: id={hdfs_resource_1.id}')

print(f'Deleting data_share: id={hive_share_1.id}')
gds.delete_data_share(hive_share_1.id)
print(f'  deleted data_share: id={hive_share_1.id}')

print(f'Deleting data_share: id={hdfs_share_1.id}')
gds.delete_data_share(hdfs_share_1.id)
print(f'  deleted data_share: id={hdfs_share_1.id}')

print(f'Deleting project: id={project_1.id}')
gds.delete_project(project_1.id)
print(f'  deleted project: id={project_1.id}')

print(f'Deleting project: id={project_2.id}')
gds.delete_project(project_2.id)
print(f'  deleted project: id={project_2.id}')

print(f'Deleting dataset: id={dataset_1.id}')
gds.delete_dataset(dataset_1.id)
print(f'  deleted dataset: id={dataset_1.id}')

print(f'Deleting dataset: id={dataset_2.id}')
gds.delete_dataset(dataset_2.id)
print(f'  deleted dataset: id={dataset_2.id}')
