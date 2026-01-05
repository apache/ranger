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


from apache_ranger.client.ranger_client       import *
from apache_ranger.model.ranger_security_zone import *
from apache_ranger.model.ranger_principal     import *
from datetime                                 import datetime



## create a client to connect to Apache Ranger admin server
ranger_url  = 'http://localhost:6080'
ranger_auth = ('admin', 'rangerR0cks!')

# For Kerberos authentication
#
# from requests_kerberos import HTTPKerberosAuth
#
# ranger_auth = HTTPKerberosAuth()


print(f'\nUsing Ranger at {ranger_url}');

ranger = RangerClient(ranger_url, ranger_auth)

# to disable SSL certificate validation (not recommended for production use!)
#
# ranger.session.verify = False


print('\nListing security-zones..')
zones = ranger.find_security_zones_v2()

print(f'    {len(zones.list)} security-zones found')
for zone in zones.list:
    print(f'        id: {zone.id}, name: {zone.name}')

now = datetime.now()

zone_name = 'zone1-' + now.strftime('%Y%m%d-%H%M%S-%f')

zone             = RangerSecurityZoneV2()
zone.name        = zone_name
zone.description = 'zone created by example script'
zone.admins      = [ RangerPrincipal({ 'type': PrincipalType.USER, 'name': 'admin' }) ]
zone.auditors    = [ RangerPrincipal({ 'type': PrincipalType.USER, 'name': 'admin' }) ]

print(f'\nCreating security-zone: name={zone_name}')

created_zone = ranger.create_security_zone_v2(zone)

print(f'    created zone: {created_zone}')

zone_id = created_zone.id


print(f'\nRetrieving zone by ID: id={zone_id}')

retrieved_zone = ranger.get_security_zone_v2_by_id(zone_id)

print(f'    retrieved zone: id: {retrieved_zone.id}, name: {retrieved_zone.name}')


print(f'\nRetrieving zone by name: name={zone_name}')

retrieved_zone = ranger.get_security_zone_v2(zone_name)

print(f'    retrieved zone: id: {retrieved_zone.id}, name: {retrieved_zone.name}')


print('\nListing security-zones..')
zones = ranger.find_security_zones_v2()

print(f'    {len(zones.list)} security-zones found')
for zone in zones.list:
    print(f'        id: {zone.id}, name: {zone.name}')


change_req                   = RangerSecurityZoneChangeRequest()
change_req.resourcesToUpdate = { 'dev_hive': RangerSecurityZoneServiceV2({ 'resources': [ { 'resource': { 'database': [ 'db1' ] } } ] }), 'dev_hdfs': RangerSecurityZoneServiceV2({ 'resources': [ { 'resource': { 'path': [ '/path1' ] } } ] }) }
change_req.tagServicesToAdd  = [ 'dev_tag' ]
change_req.adminsToAdd       = [ RangerPrincipal({ 'type': 'GROUP', 'name': 'public' }) ]
change_req.auditorsToAdd     = [ RangerPrincipal({ 'type': 'GROUP', 'name': 'public' }) ]

print(f'\nUpdating zone: add resources, add tag-services, add admins, add auditors..')
print(f'    change-request: {change_req}')

ranger.partial_update_security_zone_v2(created_zone.id, change_req)
retrieved_zone = ranger.get_security_zone_v2(zone_name)

print(f'    updated_zone: {retrieved_zone}')


change_req                     = RangerSecurityZoneChangeRequest()
change_req.resourcesToRemove   = { 'dev_hive': RangerSecurityZoneServiceV2({ 'resources': [ { 'id': 0 } ] }) } # remove resource by ID
change_req.tagServicesToRemove = [ 'dev_tag' ]
change_req.adminsToRemove      = [ RangerPrincipal({ 'type': 'USER', 'name': 'admin' }) ]
change_req.auditorsToRemove    = [ RangerPrincipal({ 'type': 'USER', 'name': 'admin' }) ]

print(f'\nUpdating zone: remove resource-by-id, remove tag-services, remove admins, remove auditors..')
print(f'    change-request: {change_req}')

ranger.partial_update_security_zone_v2(created_zone.id, change_req)
retrieved_zone = ranger.get_security_zone_v2(zone_name)

print(f'    updated_zone: {retrieved_zone}')


change_req                   = RangerSecurityZoneChangeRequest()
change_req.resourcesToUpdate = { 'dev_hdfs': RangerSecurityZoneServiceV2({ 'resources': [ { 'resource': { 'path': [ zone.name ] } } ] }) }
change_req.resourcesToRemove = { 'dev_hdfs': RangerSecurityZoneServiceV2({ 'resources': [ { 'resource': { 'path': [ '/path1' ] } } ] }) } # remove resource by value

print(f'\nUpdating zone: remove resource-by-value, add resource..')
print(f'    change-request: {change_req}')

ranger.partial_update_security_zone_v2(created_zone.id, change_req)
retrieved_zone = ranger.get_security_zone_v2(zone_name)

print(f'    updated_zone: {retrieved_zone}')

print(f'\nDeleting zone id={zone_id}')

ranger.delete_security_zone_by_id(zone_id)

print(f'    deleted zone: id: {zone_id}, name: {zone.name}')


print('\nListing security-zones..')
zones = ranger.find_security_zones_v2()

print(f'    {len(zones.list)} security-zones found')
for zone in zones.list:
    print(f'        id: {zone.id}, name: {zone.name}')
