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

import argparse
import logging.config

from os import path

from apache_ranger.model.ranger_service     import RangerService
from apache_ranger.model.ranger_role        import RangerRole, RoleMember
from apache_ranger.client.ranger_client     import RangerClient
from apache_ranger.model.ranger_policy      import RangerPolicy, RangerPolicyResource
from apache_ranger.model.ranger_service_def import RangerServiceConfigDef, RangerAccessTypeDef, RangerResourceDef, RangerServiceDef

LOG_CONFIG = path.dirname(__file__) + '/../resources/logging.conf'

logging.config.fileConfig(LOG_CONFIG, disable_existing_loggers=False)

log = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parameters required for connection to Ranger Admin!')

    parser.add_argument('--url', default='http://localhost:6080')
    parser.add_argument('--username')
    parser.add_argument('--password')

    args = parser.parse_args()

    service_def_name = "sampleServiceDef"
    service_name     = "sampleService"
    policy_name      = "samplePolicy"
    role_name        = "sampleRole"

    ranger_client = RangerClient(args.url, args.username, args.password)


    # create a new service definition
    config       = RangerServiceConfigDef(itemId=1, name='sampleConfig', type='string')
    access_type  = RangerAccessTypeDef(itemId=1, name='sampleAccess')
    resource_def = RangerResourceDef(itemId=1, name='root', type='string')
    service_def  = RangerServiceDef(name=service_def_name, configs=[config], accessTypes=[access_type],
                                    resources=[resource_def])

    created_service_def = ranger_client.create_service_def(service_def)

    log.info('New Service Definition created successfully {}'.format(created_service_def))


    # create a new service
    service = RangerService(name=service_name, type=service_def_name)

    created_service = ranger_client.create_service(service)

    log.info('New Service created successfully {}'.format(created_service))


    # create a new policy
    resource = RangerPolicyResource(['/path/to/sample/resource'], False, False)
    policy   = RangerPolicy(service=service_name, name=policy_name, resources={'root': resource})

    created_policy = ranger_client.create_policy(policy)

    log.info('New Ranger Policy created successfully {}'.format(created_policy))


    # update an existing policy
    policy = RangerPolicy(service=service_name, name=policy_name, description='Policy Updated!')

    updated_policy = ranger_client.update_policy(service_name, policy_name, policy)

    log.info('Ranger Policy updated successfully {}'.format(updated_policy))


    # get a policy by name
    fetched_policy = ranger_client.get_policy(service_name, policy_name)

    log.info('Policy {} fetched {}'.format(policy_name, fetched_policy))


    # get all policies
    all_policies = ranger_client.find_policies({})

    for id, policy in enumerate(all_policies):
        log.info('Policy #{}: {}'.format(id, RangerPolicy(**policy)))


    # delete a policy by name
    ranger_client.delete_policy(service_name, policy_name)

    log.info('Policy {} successfully deleted'.format(policy_name))


    # create a role in ranger
    sample_role = RangerRole(name=role_name, description='Sample Role', users=[RoleMember(isAdmin=True)])

    created_role = ranger_client.create_role(service_name, sample_role)

    log.info('New Role successfully created {}'.format(created_role))


    # update a role in ranger
    sample_role = RangerRole(name=role_name, description='Updated Sample Role!')

    updated_role = ranger_client.update_role(created_role.id, sample_role)

    log.info('Role {} successfully updated {}'.format(role_name, updated_role))


    # get all roles in ranger
    all_roles = ranger_client.find_roles({})

    log.info('List of Roles {}'.format(all_roles)) if all_roles is not None else log.info('No roles found!')


    # delete a role in ranger
    ranger_client.delete_role(role_name, ranger_client.username, service_name)

    log.info('Role {} successfully deleted'.format(role_name))


    # delete a service
    ranger_client.delete_service(service_name)

    log.info('Service {} successfully deleted'.format(service_name))


    # delete a service definition
    ranger_client.delete_service_def(service_def_name)

    log.info('Service Definition {} successfully deleted'.format(service_def_name))
