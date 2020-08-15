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


class RangerDataMaskDef:
    def __init__(self, maskTypes=None, accessTypes=None, resources=None):
        self.maskTypes   = maskTypes if maskTypes is not None else []
        self.accessTypes = accessTypes if accessTypes is not None else []
        self.resources   = resources if resources is not None else []

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerRowFilterDef:
    def __init__(self, accessTypes=None, resources=None):
        self.accessTypes = accessTypes if accessTypes is not None else []
        self.resources   = resources if resources is not None else []

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerServiceConfigDef:
    def __init__(self, itemId=None, name=None, type=None, subType=None, mandatory=None, defaultValue=None,
                 validationRegEx=None, validationMessage=None, uiHint=None, label=None, description=None,
                 rbKeyLabel=None, rbKeyDescription=None, rbKeyValidationMessage=None):
        self.itemId                 = itemId
        self.name                   = name
        self.type                   = type
        self.subType                = subType
        self.mandatory              = mandatory if mandatory is not None else False
        self.defaultValue           = defaultValue
        self.validationRegEx        = validationRegEx
        self.validationMessage      = validationMessage
        self.uiHint                 = uiHint
        self.label                  = label
        self.description            = description
        self.rbKeyLabel             = rbKeyLabel
        self.rbKeyDescription       = rbKeyDescription
        self.rbKeyValidationMessage = rbKeyValidationMessage

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerAccessTypeDef:
    def __init__(self, itemId=None, name=None, label=None, rbKeyLabel=None, impliedGrants=None):
        self.itemId        = itemId
        self.name          = name
        self.label         = label
        self.rbKeyLabel    = rbKeyLabel
        self.impliedGrants = impliedGrants if impliedGrants is not None else []

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerResourceDef:
    def __init__(self, itemId=None, name=None, type=None, level=None, parent=None, mandatory=None, lookupSupported=None,
                 recursiveSupported=None, excludesSupported=None, matcher=None, matcherOptions=None,
                 validationRegEx=None, validationMessage=None, uiHint=None, label=None, description=None,
                 rbKeyLabel=None, rbKeyDescription=None, rbKeyValidationMessage=None, accessTypeRestrictions=None,
                 isValidLeaf=None):
        self.itemId                 = itemId
        self.name                   = name
        self.type                   = type
        self.level                  = level if level is not None else 1
        self.parent                 = parent
        self.mandatory              = mandatory if mandatory is not None else False
        self.lookupSupported        = lookupSupported if lookupSupported is not None else False
        self.recursiveSupported     = recursiveSupported if recursiveSupported is not None else False
        self.excludesSupported      = excludesSupported if excludesSupported is not None else False
        self.matcher                = matcher
        self.matcherOptions         = matcherOptions if matcherOptions is not None else {}
        self.validationRegEx        = validationRegEx
        self.validationMessage      = validationMessage
        self.uiHint                 = uiHint
        self.label                  = label
        self.description            = description
        self.rbKeyLabel             = rbKeyLabel
        self.rbKeyDescription       = rbKeyDescription
        self.rbKeyValidationMessage = rbKeyValidationMessage
        self.accessTypeRestrictions = accessTypeRestrictions if accessTypeRestrictions is not None else []
        self.isValidLeaf            = isValidLeaf

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerServiceDef(RangerBase):
    def __init__(self, id=None, guid=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None,
                 version=None, isEnabled=None, name=None, displayName=None, implClass=None, label=None,
                 description=None, rbKeyLabel=None, rbKeyDescription=None, options=None, configs=None, resources=None,
                 accessTypes=None, policyConditions=None, contextEnrichers=None, enums=None, dataMaskDef=None,
                 rowFilterDef=None):
        super().__init__(id, guid, createdBy, updatedBy, createTime, updateTime, version, isEnabled)

        self.name             = name
        self.displayName      = displayName
        self.implClass        = implClass
        self.label            = label
        self.description      = description
        self.rbKeyLabel       = rbKeyLabel
        self.rbKeyDescription = rbKeyDescription
        self.options          = options if options is not None else {}
        self.configs          = configs if configs is not None else []
        self.resources        = resources if resources is not None else []
        self.accessTypes      = accessTypes if accessTypes is not None else []
        self.policyConditions = policyConditions if policyConditions is not None else []
        self.contextEnrichers = contextEnrichers if contextEnrichers is not None else []
        self.enums            = enums if enums is not None else []
        self.dataMaskDef      = dataMaskDef if dataMaskDef is not None else RangerDataMaskDef()
        self.rowFilterDef     = rowFilterDef if rowFilterDef is not None else RangerRowFilterDef()

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)
