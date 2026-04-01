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

from apache_ranger.model.ranger_base import RangerBase
from apache_ranger.utils             import non_null, type_coerce, type_coerce_dict, type_coerce_list


class RangerUserInfo(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)

        self.name       = attrs.get("name")
        self.attributes = attrs.get("attributes")
        self.groups     = attrs.get("groups")
        self.roles      = attrs.get("roles")


class RangerResourceInfo(RangerBase):
    SCOPE_SELF                   = "SELF"
    SCOPE_SELF_OR_ANY_CHILD      = "SELF_OR_ANY_CHILD"
    SCOPE_SELF_OR_ANY_DESCENDANT = "SELF_OR_ANY_DESCENDANT"

    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)

        self.name           = attrs.get("name")
        self.subResources   = attrs.get("subResources")
        self.nameMatchScope = attrs.get("nameMatchScope")
        self.attributes     = attrs.get("attributes")


class RangerAccessInfo(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)

        self.resource    = attrs.get("resource")
        self.action      = attrs.get("action")
        self.permissions = attrs.get("permissions")

    def type_coerce_attrs(self):
        super(RangerAccessInfo, self).type_coerce_attrs()
        self.resource = type_coerce(self.resource, RangerResourceInfo)


class RangerAccessContext(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)

        self.serviceType          = attrs.get("serviceType")
        self.serviceName          = attrs.get("serviceName")
        self.accessTime           = attrs.get("accessTime")
        self.clientIpAddress      = attrs.get("clientIpAddress")
        self.forwardedIpAddresses = attrs.get("forwardedIpAddresses")
        self.additionalInfo       = attrs.get("additionalInfo")


class RangerAuthzRequest(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)

        self.requestId = attrs.get("requestId")
        self.user      = attrs.get("user")
        self.access    = attrs.get("access")
        self.context   = attrs.get("context")

    def type_coerce_attrs(self):
        super(RangerAuthzRequest, self).type_coerce_attrs()
        self.user    = type_coerce(self.user, RangerUserInfo)
        self.access  = type_coerce(self.access, RangerAccessInfo)
        self.context = type_coerce(self.context, RangerAccessContext)


class RangerMultiAuthzRequest(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)

        self.requestId = attrs.get("requestId")
        self.user      = attrs.get("user")
        self.accesses  = attrs.get("accesses")
        self.context   = attrs.get("context")

    def type_coerce_attrs(self):
        super(RangerMultiAuthzRequest, self).type_coerce_attrs()
        self.user     = type_coerce(self.user, RangerUserInfo)
        self.accesses = type_coerce_list(self.accesses, RangerAccessInfo)
        self.context  = type_coerce(self.context, RangerAccessContext)


class RangerPolicyInfo(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.id      = attrs.get("id")
        self.version = attrs.get("version")


class RangerAccessResult(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.decision = attrs.get("decision")
        self.policy   = attrs.get("policy")

    def type_coerce_attrs(self):
        super(RangerAccessResult, self).type_coerce_attrs()
        self.policy = type_coerce(self.policy, RangerPolicyInfo)


class RangerDataMaskResult(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.maskType    = attrs.get("maskType")
        self.maskedValue = attrs.get("maskedValue")
        self.policy      = attrs.get("policy")

    def type_coerce_attrs(self):
        super(RangerDataMaskResult, self).type_coerce_attrs()
        self.policy = type_coerce(self.policy, RangerPolicyInfo)


class RangerRowFilterResult(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.filterExpr = attrs.get("filterExpr")
        self.policy     = attrs.get("policy")

    def type_coerce_attrs(self):
        super(RangerRowFilterResult, self).type_coerce_attrs()
        self.policy = type_coerce(self.policy, RangerPolicyInfo)


class RangerResultInfo(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.access         = attrs.get("access")
        self.dataMask       = attrs.get("dataMask")
        self.rowFilter      = attrs.get("rowFilter")
        self.additionalInfo = attrs.get("additionalInfo")

    def type_coerce_attrs(self):
        super(RangerResultInfo, self).type_coerce_attrs()
        self.access    = type_coerce(self.access, RangerAccessResult)
        self.dataMask  = type_coerce(self.dataMask, RangerDataMaskResult)
        self.rowFilter = type_coerce(self.rowFilter, RangerRowFilterResult)


class RangerPermissionResult(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.permission     = attrs.get("permission")
        self.access         = attrs.get("access")
        self.dataMask       = attrs.get("dataMask")
        self.rowFilter      = attrs.get("rowFilter")
        self.additionalInfo = attrs.get("additionalInfo")
        self.subResources   = attrs.get("subResources")

    def type_coerce_attrs(self):
        super(RangerPermissionResult, self).type_coerce_attrs()
        self.access       = type_coerce(self.access, RangerAccessResult)
        self.dataMask     = type_coerce(self.dataMask, RangerDataMaskResult)
        self.rowFilter    = type_coerce(self.rowFilter, RangerRowFilterResult)
        self.subResources = type_coerce_dict(self.subResources, RangerResultInfo)


class RangerAuthzResult(RangerBase):
    DECISION_ALLOW          = "ALLOW"
    DECISION_DENY           = "DENY"
    DECISION_NOT_DETERMINED = "NOT_DETERMINED"
    DECISION_PARTIAL        = "PARTIAL"

    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.requestId   = attrs.get("requestId")
        self.decision    = attrs.get("decision")
        self.permissions = attrs.get("permissions")

    def type_coerce_attrs(self):
        super(RangerAuthzResult, self).type_coerce_attrs()
        self.permissions = type_coerce_dict(self.permissions, RangerPermissionResult)


class RangerMultiAuthzResult(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.requestId = attrs.get("requestId")
        self.decision  = attrs.get("decision")
        self.accesses  = attrs.get("accesses")

    def type_coerce_attrs(self):
        super(RangerMultiAuthzResult, self).type_coerce_attrs()
        self.accesses = type_coerce_list(self.accesses, RangerAuthzResult)


class RangerResourcePermissionsRequest(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.requestId = attrs.get("requestId")
        self.resource  = attrs.get("resource")
        self.context   = attrs.get("context")

    def type_coerce_attrs(self):
        super(RangerResourcePermissionsRequest, self).type_coerce_attrs()
        self.resource = type_coerce(self.resource, RangerResourceInfo)
        self.context  = type_coerce(self.context, RangerAccessContext)


class RangerResourcePermissions(RangerBase):
    def __init__(self, attrs=None):
        attrs = non_null(attrs, {})
        RangerBase.__init__(self, attrs)
        self.resource = attrs.get("resource")
        self.users    = attrs.get("users")
        self.groups   = attrs.get("groups")
        self.roles    = attrs.get("roles")

    def type_coerce_attrs(self):
        super(RangerResourcePermissions, self).type_coerce_attrs()
        self.resource = type_coerce(self.resource, RangerResourceInfo)
        self.users    = _coerce_principal_permissions(self.users)
        self.groups   = _coerce_principal_permissions(self.groups)
        self.roles    = _coerce_principal_permissions(self.roles)


def _coerce_principal_permissions(value):
    if not isinstance(value, dict):
        return None

    ret = {}
    for principal, permissions in value.items():
        ret[principal] = type_coerce_dict(permissions, RangerPermissionResult)
    return ret

