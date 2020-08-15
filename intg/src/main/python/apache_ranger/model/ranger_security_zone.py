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


class RangerSecurityZoneService:
    def __init__(self, resources=None):
        self.resources = resources if resources is not None else []

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerSecurityZone(RangerBase):
    def __init__(self, id=None, guid=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None,
                 version=None, isEnabled=None, name=None, services=None, tagServices=None, adminUsers=None,
                 adminUserGroups=None, auditUsers=None, auditUserGroups=None, description=None):
        super().__init__(id, guid, createdBy, updatedBy, createTime, updateTime, version, isEnabled)
        self.name = name
        self.services = services if services is not None else {}
        self.tagServices = tagServices if tagServices is not None else []
        self.adminUsers = adminUsers if adminUsers is not None else []
        self.adminUserGroups = adminUserGroups if adminUserGroups is not None else []
        self.auditUsers = auditUsers if auditUsers is not None else []
        self.auditUserGroups = auditUserGroups if auditUserGroups is not None else []
        self.description = description
        return

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)
