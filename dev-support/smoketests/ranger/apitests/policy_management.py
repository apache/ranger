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

from apache_ranger.model.ranger_service import *
from apache_ranger.client.ranger_client import *
from apache_ranger.model.ranger_policy import *


class TestPolicyManagement:
    ROBOT_LIBRARY_SCOPE = 'SUITE'

    def __init__(self, ranger_url, username, password):
        self.ranger = RangerClient(ranger_url, (username, password))
        self.login_user = username
        self.ranger.session.verify = False
        self.test_hive_policy_prefix = 'test_hive_policy'
        self.test_hive_db_prefix = 'test_hive_db'
        self.test_hive_table_prefix = 'test_hive_table'
        return

    def get_hive_policy(self, service_name, policy_name):
        return self.ranger.get_policy(service_name, policy_name)

    def delete_hive_policy(self, service_name, policy_name):
        return self.ranger.delete_policy(service_name, policy_name)

    @staticmethod
    def _create_policy_item_accesses(access_types):
        ret = []
        for access_type in access_types:
            ret.append(RangerPolicyItemAccess({'type': access_type}))
        return ret

    @staticmethod
    def _create_policy_item(users, access_types):
        allow_item = RangerPolicyItem()
        allow_item.users = users
        allow_item.accesses = TestPolicyManagement._create_policy_item_accesses(access_types)
        return allow_item

    @staticmethod
    def _create_policy_item_with_delegate_admin(users, access_types):
        allow_item = TestPolicyManagement._create_policy_item(users, access_types)
        allow_item.delegateAdmin = True
        return allow_item

    @staticmethod
    def _create_hive_policy_resource(db_name, table_name, column_name):
        resources = {
            'database': RangerPolicyResource({'values': [db_name]}),
            'table': RangerPolicyResource({'values': [table_name]}),
            'column': RangerPolicyResource({'values': [column_name]})
        }
        return resources

    def create_hive_policy(self, service_name, policy_name, db_name, table_name):
        policy = RangerPolicy()
        policy.service = service_name
        policy.name = policy_name
        policy.resources = TestPolicyManagement._create_hive_policy_resource(db_name, table_name, "*")
        allow_item = TestPolicyManagement._create_policy_item_with_delegate_admin(['test_user_1'], ['create', 'alter'])
        deny_item = TestPolicyManagement._create_policy_item([self.login_user], ['drop'])
        policy.policyItems = [allow_item]
        policy.denyPolicyItems = [deny_item]

        created_policy = self.ranger.create_policy(policy)
        print(f'Created policy: name={created_policy.name}, id={created_policy.id}')
        return created_policy

    def get_all_policies(self):
        all_policies = self.ranger.find_policies()
        return all_policies

    def create_policies_in_bulk(self, service_name, count):
        count = int(count)
        for i in range(count):
            policy_name = f'{self.test_hive_policy_prefix}_{i}'
            db_name = f'{self.test_hive_db_prefix}_{i}'
            table_name = f'{self.test_hive_table_prefix}_{i}'
            self.create_hive_policy(service_name, policy_name, db_name, table_name)
        return

    def delete_policies_in_bulk(self, service_name, count):
        count = int(count)
        for i in range(count):
            policy_name = f'{self.test_hive_policy_prefix}_{i}'
            self.delete_hive_policy(service_name, policy_name)
        return


class TestServiceManagement:
    ROBOT_LIBRARY_SCOPE = 'SUITE'

    def __init__(self, ranger_url, username, password):
        self.ranger = RangerClient(ranger_url, (username, password))
        self.ranger.session.verify = False
        return

    def create_service(self, service_name, service_type, configs):
        service = RangerService()
        service.name = service_name
        service.type = service_type
        service.configs = configs
        return self.ranger.create_service(service)

    def delete_service(self, service_name):
        return self.ranger.delete_service(service_name)

