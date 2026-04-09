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

from apache_ranger.client.ranger_pdp_client import RangerPDPClient
from apache_ranger.model.ranger_authz       import RangerAccessContext, RangerAccessInfo
from apache_ranger.model.ranger_authz       import RangerAuthzRequest, RangerMultiAuthzRequest
from apache_ranger.model.ranger_authz       import RangerResourceInfo, RangerResourcePermissionsRequest, RangerUserInfo


##
## Step 1: create a client to connect to Ranger PDP
##
pdp_url = "http://localhost:6500"

# For Kerberos authentication
#
# from requests_kerberos import HTTPKerberosAuth
#
# pdp = RangerPDPClient(pdp_url, HTTPKerberosAuth())

# For trusted-header authN with PDP (example only):
#
pdp = RangerPDPClient(pdp_url, auth=None, headers={"X-Forwarded-User": "hive"})

print(f"\nUsing Ranger PDP at {pdp_url}")

##
## Step 2: call PDP authorization APIs
##
req = RangerAuthzRequest({
    "requestId": "req-1",
    "user":      RangerUserInfo({"name": "alice"}),
    "access":    RangerAccessInfo({"resource": RangerResourceInfo({"name": "table:default/test_tbl1"}), "permissions": ["create"]}),
    "context":   RangerAccessContext({"serviceType": "hive", "serviceName": "dev_hive"})
})

res = pdp.authorize(req)

print("authorize():")
print(f"    {req}")
print(f"    {res}")
print()

req = RangerAuthzRequest({
    "requestId": "req-2",
    "user":      RangerUserInfo({"name": "alice"}),
    "access":    RangerAccessInfo({"resource": RangerResourceInfo({"name": "table:default/test_tbl1", "subResources": ["column:id", "column:name", "column:email"]}), "permissions": ["select"]}),
    "context":   RangerAccessContext({"serviceType": "hive", "serviceName": "dev_hive"})
})

res = pdp.authorize(req)

print("authorize():")
print(f"    {req}")
print(f"    {res}")
print()

req = RangerMultiAuthzRequest({
    "requestId": "req-3",
    "user":      RangerUserInfo({"name": "alice"}),
    "accesses": [
        RangerAccessInfo({"resource": RangerResourceInfo({"name": "table:default/test_tbl1", "subResources": ["column:id", "column:name", "column:email"], "attributes": {"OWNER": "alice"}}), "permissions": ["select"]}),
        RangerAccessInfo({"resource": RangerResourceInfo({"name": "table:default/test_vw1"}), "permissions": ["create"]})
    ],
    "context": RangerAccessContext({"serviceType": "hive", "serviceName": "dev_hive"})
})

res = pdp.authorize_multi(req)

print("authorize_multi():")
print(f"    {req}")
print(f"    {res}")
print()

req = RangerResourcePermissionsRequest({
    "requestId": "req-4",
    "resource":  RangerResourceInfo({"name": "table:default/test_tbl1"}),
    "context":   RangerAccessContext({"serviceType": "hive", "serviceName": "dev_hive"})
})

res = pdp.get_resource_permissions(req)

print("get_resource_permissions():")
print(f"    {req}")
print(f"    {res}")
print()
