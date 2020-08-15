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


class RoleMember:
    def __init__(self, name=None, isAdmin=None):
        self.name    = name
        self.isAdmin = isAdmin if isAdmin is not None else False

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RangerRole(RangerBase):
    def __init__(self, id=None, guid=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None,
                 version=None, isEnabled=None, name=None, description=None, options=None, users=None, groups=None,
                 roles=None, createdByUser=None):
        super().__init__(id, guid, createdBy, updatedBy, createTime, updateTime, version, isEnabled)

        self.name          = name
        self.description   = description
        self.options       = options if options is not None else {}
        self.users         = users if users is not None else []
        self.groups        = groups if groups is not None else []
        self.roles         = roles if roles is not None else []
        self.createdByUser = createdByUser

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)
