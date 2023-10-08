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
from apache_ranger.utils                          import *
from apache_ranger.model.ranger_user_mgmt         import *
from apache_ranger.client.ranger_user_mgmt_client import *
from datetime                                     import datetime

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

user_mgmt = RangerUserMgmtClient(ranger)


print('\nListing users')

users = user_mgmt.find_users()

print(f'    {len(users.list)} users found')

for user in users.list:
    print(f'        id: {user.id}, name: {user.name}')


print('\nListing groups')

groups = user_mgmt.find_groups()

print(f'    {len(groups.list)} groups found')

for group in groups.list:
    print(f'        id: {group.id}, name: {group.name}')

print('\nListing group-users')

group_users = user_mgmt.find_group_users()

print(f'    {len(group_users.list)} group-users found')

for group_user in group_users.list:
    print(f'        id: {group_user.id}, groupId: {group_user.parentGroupId}, userId: {group_user.userId}')


now = datetime.now()

name_suffix = '-' + now.strftime('%Y%m%d-%H%M%S-%f')
user_name   = 'test-user' + name_suffix
group_name  = 'test-group' + name_suffix


user = RangerUser({ 'name': user_name, 'firstName': user_name, 'lastName': 'user', 'emailAddress': user_name + '@test.org', 'password': 'Welcome1', 'userRoleList': [ 'ROLE_USER' ], 'otherAttributes': '{ "dept": "test" }' })

print(f'\nCreating user: name={user.name}')

created_user = user_mgmt.create_user(user)

print(f'    created user: {created_user}')


group = RangerGroup({ 'name': group_name, 'otherAttributes': '{ "dept": "test" }' })

print(f'\nCreating group: name={group.name}')

created_group = user_mgmt.create_group(group)

print(f'    created group: {created_group}')


group_user = RangerGroupUser({ 'name': created_group.name, 'parentGroupId': created_group.id, 'userId': created_user.id })

print(f'\nAdding user {created_user.name} to group {created_group.name}')

created_group_user = user_mgmt.create_group_user(group_user)

print(f'    created group-user: {created_group_user}')


print('\nListing group-users')

group_users = user_mgmt.find_group_users()

print(f'    {len(group_users.list)} group-users found')

for group_user in group_users.list:
    print(f'        id: {group_user.id}, groupId: {group_user.parentGroupId}, userId: {group_user.userId}')


print(f'\nListing users for group {group.name}')

group_users = user_mgmt.get_group_users_for_group(group.name)

print(f'    group: {group_users.group}')
print(f'    users: {group_users.users}')


print(f'\nDeleting group-user {created_group_user.id}')

user_mgmt.delete_group_user_by_id(created_group_user.id)


print(f'\nDeleting group {group.name}')

user_mgmt.delete_group_by_id(created_group.id, True)


print(f'\nDeleting user {user.name}')

user_mgmt.delete_user_by_id(created_user.id, True)
