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

import json

from apache_ranger.model.ranger_base import RangerBase


class RangerPolicyResource:
    def __init__(self, values=None, isExcludes=None, isRecursive=None):
        self.values      = values if values is not None else []
        self.isExcludes  = isExcludes if isExcludes is not None else False
        self.isRecursive = isRecursive if isRecursive is not None else False

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerPolicyItemCondition:
    def __init__(self, type=None, values=None):
        self.type   = type
        self.values = values if values is not None else []

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerPolicyItem:
    def __init__(self, accesses=None, users=None, groups=None, roles=None, conditions=None, delegateAdmin=None):
        self.accesses      = accesses if accesses is not None else []
        self.users         = users if users is not None else []
        self.groups        = groups if groups is not None else []
        self.roles         = roles if roles is not None else []
        self.conditions    = conditions if conditions is not None else []
        self.delegateAdmin = delegateAdmin if delegateAdmin is not None else False

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerPolicyItemAccess:
    def __init__(self, type=None, isAllowed=None):
        self.type      = type
        self.isAllowed = isAllowed if isAllowed is not None else True

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerPolicyItemDataMaskInfo:
    def __init__(self, dataMaskType=None, conditionExpr=None, valueExpr=None):
        self.dataMaskType  = dataMaskType
        self.conditionExpr = conditionExpr
        self.valueExpr     = valueExpr

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerDataMaskPolicyItem(RangerPolicyItem):
    def __init__(self, dataMaskInfo=None, accesses=None, users=None, groups=None, roles=None, conditions=None, delegateAdmin=None):
        super().__init__(accesses, users, groups, roles, conditions, delegateAdmin)

        self.dataMaskInfo = dataMaskInfo if dataMaskInfo is not None else RangerPolicyItemDataMaskInfo()

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerRowFilterPolicyItem(RangerPolicyItem):
    def __init__(self, rowFilterInfo=None, accesses=None, users=None, groups=None, roles=None, conditions=None, delegateAdmin=None):
        super().__init__(accesses, users, groups, roles, conditions, delegateAdmin)

        self.rowFilterInfo = rowFilterInfo

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerPolicy(RangerBase):
    def __init__(self, id=None, guid=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None,
                 service=None, name=None, description=None, isEnabled=True, isAuditEnabled=None, resources=None,
                 policyItems=None, dataMaskPolicyItems=None, rowFilterPolicyItems=None, serviceType=None, options=None,
                 policyLabels=None, zoneName=None, isDenyAllElse=None, validitySchedules=None, version=None,
                 denyPolicyItems=None, denyExceptions=None, allowExceptions=None, resourceSignature=None,
                 policyType=None, policyPriority=None, conditions=None):
        super().__init__(id, guid, createdBy, updatedBy, createTime, updateTime, version, isEnabled)

        self.service              = service
        self.name                 = name
        self.policyType           = policyType
        self.policyPriority       = policyPriority if policyPriority is not None else 0
        self.description          = description
        self.resourceSignature    = resourceSignature
        self.isAuditEnabled       = isAuditEnabled if isAuditEnabled is not None else True
        self.resources            = resources if resources is not None else {}
        self.policyItems          = policyItems if policyItems is not None else []
        self.denyPolicyItems      = denyPolicyItems if denyPolicyItems is not None else []
        self.allowExceptions      = allowExceptions if allowExceptions is not None else []
        self.denyExceptions       = denyExceptions if denyExceptions is not None else []
        self.dataMaskPolicyItems  = dataMaskPolicyItems if dataMaskPolicyItems is not None else []
        self.rowFilterPolicyItems = rowFilterPolicyItems if rowFilterPolicyItems is not None else []
        self.serviceType          = serviceType
        self.options              = options if options is not None else {}
        self.validitySchedules    = validitySchedules if validitySchedules is not None else []
        self.policyLabels         = policyLabels if policyLabels is not None else []
        self.zoneName             = zoneName
        self.conditions           = conditions
        self.isDenyAllElse        = isDenyAllElse if isDenyAllElse is not None else False

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)
