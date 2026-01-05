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

from apache_ranger.model.ranger_base      import RangerBase, RangerBaseModelObject
from apache_ranger.utils                  import *
from apache_ranger.model.ranger_principal import RangerPrincipal


class RangerSecurityZoneResourceBase(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBase.__init__(self, attrs)

        self.id         = attrs.get('id')
        self.createdBy  = attrs.get('createdBy')
        self.updatedBy  = attrs.get('updatedBy')
        self.createTime = attrs.get('createTime')
        self.updateTime = attrs.get('updateTime')


class RangerSecurityZoneService(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBase.__init__(self, attrs)

        self.resources         = attrs.get('resources')
        self.resourcesBaseInfo = attrs.get('resourcesBaseInfo')

    def type_coerce_attrs(self):
        super(RangerSecurityZoneService, self).type_coerce_attrs()

        self.resources         = type_coerce_list(self.resources, dict)
        self.resourcesBaseInfo = type_coerce_list(self.resourcesBaseInfo, RangerSecurityZoneResourceBase)


class RangerSecurityZone(RangerBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.name            = attrs.get('name')
        self.services        = attrs.get('services')
        self.tagServices     = attrs.get('tagServices')
        self.adminUsers      = attrs.get('adminUsers')
        self.adminUserGroups = attrs.get('adminUserGroups')
        self.adminRoles      = attrs.get('adminRoles')
        self.auditUsers      = attrs.get('auditUsers')
        self.auditUserGroups = attrs.get('auditUserGroups')
        self.auditRoles      = attrs.get('auditRoles')
        self.description     = attrs.get('description')

    def type_coerce_attrs(self):
        super(RangerSecurityZone, self).type_coerce_attrs()

        self.services = type_coerce_dict(self.services, RangerSecurityZoneService)


class RangerSecurityZoneResource(RangerSecurityZoneResourceBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerSecurityZoneResourceBase.__init__(self, attrs)

        self.resource = attrs.get('resource')

    def type_coerce_attrs(self):
        super(RangerSecurityZoneResource, self).type_coerce_attrs()

        self.resource = type_coerce_dict(self.resource, list)


class RangerSecurityZoneServiceV2(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBase.__init__(self, attrs)

        self.resources = attrs.get('resources')

    def type_coerce_attrs(self):
        super(RangerSecurityZoneServiceV2, self).type_coerce_attrs()

        self.resources = type_coerce_list(self.resources, RangerSecurityZoneResource)


class RangerSecurityZoneV2(RangerBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.name        = attrs.get('name')
        self.description = attrs.get('description')
        self.services    = attrs.get('services')
        self.tagServices = attrs.get('tagServices')
        self.admins      = attrs.get('admins')
        self.auditors    = attrs.get('auditors')

    def type_coerce_attrs(self):
        super(RangerSecurityZoneV2, self).type_coerce_attrs()

        self.services = type_coerce_dict(self.services, RangerSecurityZoneServiceV2)
        self.admins   = type_coerce_list(self.admins, RangerPrincipal)
        self.auditors = type_coerce_list(self.auditors, RangerPrincipal)

class RangerSecurityZoneChangeRequest(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.name                = attrs.get('name')
        self.description         = attrs.get('description')
        self.resourcesToUpdate   = attrs.get('resourcesToUpdate')
        self.resourcesToRemove   = attrs.get('resourcesToRemove')
        self.tagServicesToAdd    = attrs.get('tagServicesToAdd')
        self.tagServicesToRemove = attrs.get('tagServicesToRemove')
        self.adminsToAdd         = attrs.get('adminsToAdd')
        self.adminsToRemove      = attrs.get('adminsToRemove')
        self.auditorsToAdd       = attrs.get('auditorsToAdd')
        self.auditorsToRemove    = attrs.get('auditorsToRemove')

    def type_coerce_attrs(self):
        super(RangerSecurityZoneChangeRequest, self).type_coerce_attrs()

        self.resourcesToUpdate = type_coerce_dict(self.resourcesToUpdate, RangerSecurityZoneServiceV2)
        self.resourcesToRemove = type_coerce_dict(self.resourcesToRemove, RangerSecurityZoneServiceV2)
        self.adminsToAdd       = type_coerce_list(self.adminsToAdd, RangerPrincipal)
        self.adminsToRemove    = type_coerce_list(self.adminsToRemove, RangerPrincipal)
        self.auditorsToAdd     = type_coerce_list(self.auditorsToAdd, RangerPrincipal)
        self.auditorsToRemove  = type_coerce_list(self.auditorsToRemove, RangerPrincipal)

class RangerSecurityZoneHeaderInfo(RangerBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.name = attrs.get('name')
