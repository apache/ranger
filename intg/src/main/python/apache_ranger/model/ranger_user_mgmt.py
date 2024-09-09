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


from apache_ranger.model.ranger_base import *
from apache_ranger.utils             import *

class RangerUser(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.id              = attrs.get('id')
        self.createDate      = attrs.get('createDate')
        self.updateDate      = attrs.get('updateDate')
        self.owner           = attrs.get('owner')
        self.updatedBy       = attrs.get('updatedBy')

        self.name            = attrs.get('name')
        self.description     = attrs.get('description')
        self.firstName       = attrs.get('firstName')
        self.lastName        = attrs.get("lastName")
        self.emailAddress    = attrs.get('emailAddress')
        self.password        = attrs.get('password')
        self.credStoreId     = attrs.get("credStoreId")
        self.status          = attrs.get('status')
        self.isVisible       = attrs.get('isVisible')
        self.userSource      = attrs.get('userSource')
        self.userRoleList    = attrs.get('userRoleList')
        self.otherAttributes = attrs.get('otherAttributes')
        self.syncSource      = attrs.get('syncSource')
        self.groupIdList     = attrs.get('groupIdList')
        self.groupNameList   = attrs.get('groupNameList')

        if self.status is None:
            self.status = 1

        if self.userRoleList is None:
            self.userRoleList = [ 'ROLE_USER' ]


class RangerGroup(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.id              = attrs.get('id')
        self.createDate      = attrs.get('createDate')
        self.updateDate      = attrs.get('updateDate')
        self.owner           = attrs.get('owner')
        self.updatedBy       = attrs.get('updatedBy')

        self.name            = attrs.get('name')
        self.description     = attrs.get('description')
        self.groupType       = attrs.get('groupType')
        self.groupSource     = attrs.get("groupSource")
        self.credStoreId     = attrs.get("credStoreId")
        self.isVisible       = attrs.get('isVisible')
        self.otherAttributes = attrs.get('otherAttributes')
        self.syncSource      = attrs.get('syncSource')

class RangerGroupUser(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.id              = attrs.get('id')
        self.createDate      = attrs.get('createDate')
        self.updateDate      = attrs.get('updateDate')
        self.owner           = attrs.get('owner')
        self.updatedBy       = attrs.get('updatedBy')

        self.name            = attrs.get('name')
        self.parentGroupId   = attrs.get('parentGroupId')
        self.userId          = attrs.get('userId')


class RangerGroupUsers(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.id              = attrs.get('id')
        self.createDate      = attrs.get('createDate')
        self.updateDate      = attrs.get('updateDate')
        self.owner           = attrs.get('owner')
        self.updatedBy       = attrs.get('updatedBy')

        self.group = attrs.get('xgroupInfo')
        self.users = attrs.get('xuserInfo')

    def type_coerce_attrs(self):
        super(RangerGroupUsers, self).type_coerce_attrs()

        self.group = type_coerce(self.group, RangerGroup)
        self.users = type_coerce_list(self.users, RangerUser)
