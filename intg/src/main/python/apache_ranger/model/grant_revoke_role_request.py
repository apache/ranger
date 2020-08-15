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


class GrantRevokeRoleRequest:
    def __init__(self, grantor=None, grantorGroups=None, targetRoles=None, users=None, groups=None, roles=None, grantOption=None, clientIPAddress=None, clientType=None, requestData=None, sessionId=None, clusterName=None):
        self.grantor         = grantor
        self.grantorGroups   = grantorGroups if grantorGroups is not None else []
        self.targetRoles     = targetRoles if targetRoles is not None else []
        self.users           = users if users is not None else []
        self.groups          = groups if groups is not None else []
        self.roles           = roles if roles is not None else []
        self.grantOption     = grantOption if grantOption is not None else False
        self.clientIPAddress = clientIPAddress
        self.clientType      = clientType
        self.requestData     = requestData
        self.sessionId       = sessionId
        self.clusterName     = clusterName

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)

