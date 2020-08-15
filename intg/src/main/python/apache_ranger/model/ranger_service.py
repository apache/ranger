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


class RangerService(RangerBase):
    def __init__(self, id=None, guid=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None,
                 version=None, isEnabled=None, type=None, name=None, displayName=None, description=None,
                 tagService=None, configs=None, policyVersion=None, policyUpdateTime=None, tagVersion=None,
                 tagUpdateTime=None):
        super().__init__(id, guid, createdBy, updatedBy, createTime, updateTime, version, isEnabled)

        self.type             = type
        self.name             = name
        self.displayName      = displayName
        self.description      = description
        self.tagService       = tagService
        self.configs          = configs if configs is not None else {}
        self.policyVersion    = policyVersion
        self.policyUpdateTime = policyUpdateTime
        self.tagVersion       = tagVersion
        self.tagUpdateTime    = tagUpdateTime

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)
