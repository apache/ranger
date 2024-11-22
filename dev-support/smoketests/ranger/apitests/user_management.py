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

from apache_ranger.client.ranger_client import *
from apache_ranger.utils import *
from apache_ranger.model.ranger_user_mgmt import *
from apache_ranger.client.ranger_user_mgmt_client import *


class TestUserManagement:
    def __init__(self, ranger_url, username, password):
        self.ranger = RangerClient(ranger_url, (username, password))
        self.ranger.session.verify = False
        self.ugclient = RangerUserMgmtClient(self.ranger)
        return

    ROBOT_LIBRARY_SCOPE = 'SUITE'

    def find_users(self):
        print('Listing all users!')
        users = self.ugclient.find_users()
        print(f'{len(users.list)} users found')
        return users

    def find_groups(self):
        print('Listing all groups!')
        groups = self.ugclient.find_groups()
        print(f'{len(groups.list)} groups found')
        return groups

    def create_user(self, user_name, role):
        user = RangerUser({'name': user_name,
                           'firstName': user_name,
                           'lastName': 'lnu',
                           'emailAddress': user_name + '@test.org',
                           'password': 'Welcome1',
                           'userRoleList': [role],
                           'otherAttributes': '{ "dept": "test" }'})

        created_user = self.ugclient.create_user(user)
        print(f'User {created_user.name} created!')
        return created_user

    def create_group(self, group_name):
        group = RangerGroup({'name': group_name, 'otherAttributes': '{ "dept": "test" }'})
        created_group = self.ugclient.create_group(group)
        print(f'Group {created_group.name} created!')
        return created_group

    def add_to_group(self, group_name, group_id, user_id):
        group_user = RangerGroupUser({'name': group_name, 'parentGroupId': group_id, 'userId': user_id})
        created_group_user = self.ugclient.create_group_user(group_user)
        print(f'Created group-user: {created_group_user}')
        return created_group_user

    def list_users_in_group(self, group_name):
        users = self.ugclient.get_users_in_group(group_name)
        return users

    def list_groups_for_user(self, user_name):
        groups = self.ugclient.get_groups_for_user(user_name)
        return groups

    def list_group_users(self):
        group_users = self.ugclient.find_group_users()
        print(f'{len(group_users.list)} group-users found')

        for group_user in group_users.list:
            print(f'id: {group_user.id}, groupId: {group_user.parentGroupId}, userId: {group_user.userId}')
        return group_users

    def delete_user_by_id(self, id):
        self.ugclient.delete_user_by_id(id, True)
        return

    def delete_group_by_id(self, id):
        self.ugclient.delete_group_by_id(id, True)
        return

    def delete_group_user_by_id(self, id):
        self.ugclient.delete_group_user_by_id(id)
        return

