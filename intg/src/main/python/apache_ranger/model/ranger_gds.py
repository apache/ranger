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
from apache_ranger.model.ranger_policy    import *
from apache_ranger.model.ranger_principal import *
from apache_ranger.utils                  import *

class GdsPermission(StrEnum):
    NONE         = 'NONE'
    LIST         = 'LIST'
    VIEW         = 'VIEW'
    AUDIT        = 'AUDIT'
    POLICY_ADMIN = 'POLICY_ADMIN'
    ADMIN        = 'ADMIN'

    @classmethod
    def value_of(cls, val):
        if isinstance(val, GdsPermission):
            return val
        else:
            for key, member in cls.__members__.items():
                if val == member.name or val == member.value:
                    return member
            raise ValueError(f"'{cls.__name__}' enum not found for '{val}'")

class GdsShareStatus(StrEnum):
  NONE      = 'NONE'
  REQUESTED = 'REQUESTED'
  GRANTED   = 'GRANTED'
  DENIED    = 'DENIED'
  ACTIVE    = 'ACTIVE'

  @classmethod
  def value_of(cls, val):
    if isinstance(val, GdsShareStatus):
      return val
    else:
      for key, member in cls.__members__.items():
        if val == member.name or val == member.value:
          return member
      else:
        raise ValueError(f"'{cls.__name__}' enum not found for '{val}'")

class RangerGdsBaseModelObject(RangerBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.description    = attrs.get('description')
        self.options        = attrs.get('options')
        self.additionalInfo = attrs.get('additionalInfo')

    def type_coerce_attrs(self):
        super(RangerGdsBaseModelObject, self).type_coerce_attrs()


class RangerDataset(RangerGdsBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerGdsBaseModelObject.__init__(self, attrs)

        self.name       = attrs.get('name')
        self.acl        = attrs.get('acl')
        self.termsOfUse = attrs.get('termsOfUse')

    def type_coerce_attrs(self):
        super(RangerDataset, self).type_coerce_attrs()

        self.acl = type_coerce_dict(self.acl, RangerGdsObjectACL)


class RangerProject(RangerGdsBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerGdsBaseModelObject.__init__(self, attrs)

        self.name       = attrs.get('name')
        self.acl        = attrs.get('acl')
        self.termsOfUse = attrs.get('termsOfUse')

    def type_coerce_attrs(self):
        super(RangerProject, self).type_coerce_attrs()

        self.acl = type_coerce_dict(self.acl, RangerGdsObjectACL)


class RangerDataShare(RangerGdsBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerGdsBaseModelObject.__init__(self, attrs)

        self.name               = attrs.get('name')
        self.acl                = attrs.get('acl')
        self.service            = attrs.get('service')
        self.zone               = attrs.get('zone')
        self.conditionExpr      = attrs.get('conditionExpr')
        self.defaultAccessTypes = attrs.get('defaultAccessTypes')
        self.defaultTagMasks    = attrs.get('defaultTagMasks')
        self.termsOfUse         = attrs.get('termsOfUse')

    def type_coerce_attrs(self):
        super(RangerDataShare, self).type_coerce_attrs()

        self.acl             = type_coerce_dict(self.acl, RangerGdsObjectACL)
        self.defaultTagMasks = type_coerce_list(self.defaultTagMasks, RangerGdsMaskInfo)


class RangerSharedResource(RangerBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.name              = attrs.get('name')
        self.dataShareId       = attrs.get('dataShareId')
        self.resource          = attrs.get('resource')
        self.subResource       = attrs.get('subResource')
        self.subResourceType   = attrs.get('subResourceType')
        self.conditionExpr     = attrs.get('conditionExpr')
        self.accessTypes       = attrs.get('accessTypes')
        self.rowFilter         = attrs.get('rowFilter')
        self.subResourceMasks  = attrs.get('subResourceMasks')
        self.profiles          = attrs.get('profiles')

    def type_coerce_attrs(self):
        super(RangerSharedResource, self).type_coerce_attrs()

        self.resource         = type_coerce_dict(self.resource, RangerPolicyResource)
        self.subResource      = type_coerce(self.subResource, RangerPolicyResource)
        self.rowFilter        = type_coerce(self.rowFilter, RangerPolicyItemRowFilterInfo)
        self.subResourceMasks = type_coerce_list(self.subResourceMasks, RangerGdsMaskInfo)


class RangerDataShareInDataset(RangerGdsBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerGdsBaseModelObject.__init__(self, attrs)

        self.dataShareId      = attrs.get('dataShareId')
        self.datasetId        = attrs.get('datasetId')
        self.status           = attrs.get('status')
        self.validitySchedule = attrs.get('validitySchedule')
        self.profiles         = attrs.get('profiles')

    def type_coerce_attrs(self):
        super(RangerDataShareInDataset, self).type_coerce_attrs()

        self.status           = type_coerce(self.status, GdsShareStatus)
        self.validitySchedule = type_coerce(self.validitySchedule, RangerValiditySchedule)


class RangerDatasetInProject(RangerGdsBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerGdsBaseModelObject.__init__(self, attrs)

        self.datasetId        = attrs.get('datasetId')
        self.projectId        = attrs.get('projectId')
        self.status           = attrs.get('status')
        self.validitySchedule = attrs.get('validitySchedule')
        self.profiles         = attrs.get('profiles')

    def type_coerce_attrs(self):
        super(RangerDatasetInProject, self).type_coerce_attrs()

        self.status           = type_coerce(self.status, GdsShareStatus)
        self.validitySchedule = type_coerce(self.validitySchedule, RangerValiditySchedule)


class RangerGdsObjectACL(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBase.__init__(self, attrs)

        self.users  = attrs.get('users')
        self.groups = attrs.get('groups')
        self.roles  = attrs.get('roles')

    def type_coerce_attrs(self):
        super(RangerGdsObjectACL, self).type_coerce_attrs()

        self.users  = type_coerce_dict(self.users, GdsPermission)
        self.groups = type_coerce_dict(self.groups, GdsPermission)
        self.roles  = type_coerce_dict(self.roles, GdsPermission)


class RangerGdsMaskInfo(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBase.__init__(self, attrs)

        self.values   = attrs.get('values')
        self.maskInfo = attrs.get('maskInfo')

    def type_coerce_attrs(self):
        super(RangerGdsMaskInfo, self).type_coerce_attrs()

        self.maskInfo  = type_coerce(self.maskInfo, RangerPolicyItemDataMaskInfo)


class DataShareInDatasetSummary(RangerBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.name          = attrs.get('name')
        self.serviceId     = attrs.get('serviceId')
        self.serviceName   = attrs.get('serviceName')
        self.zoneId        = attrs.get('zoneId')
        self.zoneName      = attrs.get('zoneName')
        self.resourceCount = attrs.get('resourceCount')
        self.shareStatus   = attrs.get('shareStatus')
        self.approver      = attrs.get('approver')

    def type_coerce_attrs(self):
        super(DataShareInDatasetSummary, self).type_coerce_attrs()

        self.shareStatus = type_coerce(self.shareStatus, GdsShareStatus)


class DatasetSummary(RangerBaseModelObject):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBaseModelObject.__init__(self, attrs)

        self.name                = attrs.get('name')
        self.description         = attrs.get('description')
        self.permissionForCaller = attrs.get('permissionForCaller')
        self.principalsCount     = attrs.get('principalsCount')
        self.projectsCount       = attrs.get('projectsCount')
        self.totalResourceCount  = attrs.get('totalResourceCount')
        self.dataShares          = attrs.get('dataShares')

    def type_coerce_attrs(self):
        super(DatasetSummary, self).type_coerce_attrs()

        self.permissionForCaller = type_coerce(self.permissionForCaller, GdsPermission)
        self.principalsCount     = type_coerce_kv(self.principalsCount, PrincipalType, int)
        self.dataShares          = type_coerce_list(self.dataShares, DataShareInDatasetSummary)
