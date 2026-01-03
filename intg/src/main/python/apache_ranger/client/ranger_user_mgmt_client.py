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


import logging
from apache_ranger.model.ranger_user_mgmt import *
from apache_ranger.utils                  import *

LOG = logging.getLogger(__name__)

class RangerUserMgmtClient:
    def __init__(self, ranger_client):
        self.client_http = ranger_client.client_http

    def create_user(self, user):
        resp = self.client_http.call_api(RangerUserMgmtClient.CREATE_USER, request_data=user)

        return type_coerce(resp, RangerUser)

    def update_user_by_id(self, user_id, user):
        resp = self.client_http.call_api(RangerUserMgmtClient.UPDATE_USER.format_path({'id': user_id}), request_data=user)

        return type_coerce(resp, RangerUser)

    def delete_user_by_id(self, user_id, is_force_delete=False):
        self.client_http.call_api(RangerUserMgmtClient.DELETE_USER.format_path({'id': user_id}), query_params={'forceDelete': is_force_delete})

    def get_user_by_id(self, user_id):
        resp = self.client_http.call_api(RangerUserMgmtClient.GET_USER_BY_ID.format_path({'id': user_id}))

        return type_coerce(resp, RangerUser)

    def get_user(self, user_name):
        resp = self.find_users({ 'name': user_name })

        if resp is not None and resp.list is not None:
            for user in resp.list:
                if user.name == user_name:
                    return user

        return None

    def get_groups_for_user(self, name):
        user = self.get_user(name)

        if user is not None and user.groupNameList is not None:
            ret = user.groupNameList
        else:
            ret = None

        return ret

    def find_users(self, filter=None):
        resp = self.client_http.call_api(RangerUserMgmtClient.FIND_USERS, filter)

        vList = PList(resp)

        vList.list = resp.get('vXUsers')

        vList.type_coerce_list(RangerUser)

        return vList


    def create_group(self, group):
        resp = self.client_http.call_api(RangerUserMgmtClient.CREATE_GROUP, request_data=group)

        return type_coerce(resp, RangerGroup)

    def update_group_by_id(self, group_id, group):
        resp = self.client_http.call_api(RangerUserMgmtClient.UPDATE_GROUP.format_path({'id': group_id}), request_data=group)

        return type_coerce(resp, RangerGroup)

    def delete_group_by_id(self, group_id, is_force_delete=False):
        self.client_http.call_api(RangerUserMgmtClient.DELETE_GROUP.format_path({'id': group_id}), query_params={'forceDelete': is_force_delete})

    def get_group_by_id(self, group_id):
        resp = self.client_http.call_api(RangerUserMgmtClient.GET_GROUP_BY_ID.format_path({'id': group_id}))

        return type_coerce(resp, RangerGroup)

    def get_group(self, group_name):
        resp = self.find_groups({ 'name': group_name })

        if resp is not None and resp.list is not None:
            for group in resp.list:
                if group.name == group_name:
                    return group

        return None

    def get_users_in_group(self, name):
        group_users = self.get_group_users_for_group(name)

        if group_users is not None and group_users.users is not None:
            ret = []
            for user in group_users.users:
                ret.append(user.name)
        else:
            ret = None

        return ret

    def find_groups(self, filter=None):
        resp = self.client_http.call_api(RangerUserMgmtClient.FIND_GROUPS, filter)

        vList = PList(resp)

        vList.list = resp.get('vXGroups')

        vList.type_coerce_list(RangerGroup)

        return vList


    def create_group_user(self, group_user):
        resp = self.client_http.call_api(RangerUserMgmtClient.CREATE_GROUP_USER, request_data=group_user)

        return type_coerce(resp, RangerGroupUser)

    def update_group_user(self, group_user):
        resp = self.client_http.call_api(RangerUserMgmtClient.UPDATE_GROUP_USER, request_data=group_user)

        return type_coerce(resp, RangerGroupUser)

    def delete_group_user_by_id(self, group_user_id):
        self.client_http.call_api(RangerUserMgmtClient.DELETE_GROUP_USER.format_path({'id': group_user_id}))

    def find_group_users(self, filter=None):
        resp = self.client_http.call_api(RangerUserMgmtClient.FIND_GROUP_USERS, filter)

        vList = PList(resp)

        vList.list = resp.get('vXGroupUsers')

        vList.type_coerce_list(RangerGroupUser)

        return vList

    def get_group_users_for_group(self, name):
        resp = self.client_http.call_api(RangerUserMgmtClient.GET_GROUP_USERS_FOR_GROUP.format_path({'name': name}))

        return type_coerce(resp, RangerGroupUsers)

    # URIs
    URI_XUSERS_BASE                  = 'service/xusers'
    URI_XUSERS_USERS                 = URI_XUSERS_BASE + '/users'
    URI_XUSERS_SECURE_USERS          = URI_XUSERS_BASE + '/secure/users'
    URI_XUSERS_SECURE_USER_BY_ID     = URI_XUSERS_SECURE_USERS + '/{id}'
    URI_XUSERS_DELETE_USER           = URI_XUSERS_USERS + '/{id}'
    URI_XUSERS_GROUPS                = URI_XUSERS_BASE + '/groups'
    URI_XUSERS_SECURE_GROUPS         = URI_XUSERS_BASE + '/secure/groups'
    URI_XUSERS_SECURE_GROUP_BY_ID    = URI_XUSERS_SECURE_GROUPS + '/{id}'
    URI_XUSERS_DELETE_GROUP          = URI_XUSERS_GROUPS + '/{id}'
    URI_XUSERS_GROUP_USERS           = URI_XUSERS_BASE + '/groupusers'
    URI_XUSERS_GROUP_USER_BY_ID      = URI_XUSERS_GROUP_USERS + '/{id}'
    URI_XUSERS_GROUP_USERS_FOR_GROUP = URI_XUSERS_GROUP_USERS + '/groupName/{name}'


    # APIs
    CREATE_USER    = API(URI_XUSERS_SECURE_USERS, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_USER    = API(URI_XUSERS_SECURE_USER_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_USER    = API(URI_XUSERS_DELETE_USER, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_USER_BY_ID = API(URI_XUSERS_SECURE_USER_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    FIND_USERS     = API(URI_XUSERS_USERS, HttpMethod.GET, HTTPStatus.OK)

    CREATE_GROUP    = API(URI_XUSERS_SECURE_GROUPS, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_GROUP    = API(URI_XUSERS_SECURE_GROUP_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_GROUP    = API(URI_XUSERS_DELETE_GROUP, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_GROUP_BY_ID = API(URI_XUSERS_SECURE_GROUP_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    FIND_GROUPS     = API(URI_XUSERS_GROUPS, HttpMethod.GET, HTTPStatus.OK)

    CREATE_GROUP_USER = API(URI_XUSERS_GROUP_USERS, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_GROUP_USER = API(URI_XUSERS_GROUP_USERS, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_GROUP_USER = API(URI_XUSERS_GROUP_USER_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    FIND_GROUP_USERS  = API(URI_XUSERS_GROUP_USERS, HttpMethod.GET, HTTPStatus.OK)

    GET_GROUP_USERS_FOR_GROUP = API(URI_XUSERS_GROUP_USERS_FOR_GROUP, HttpMethod.GET, HTTPStatus.OK)
