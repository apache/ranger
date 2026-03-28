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

import logging
from apache_ranger.client.ranger_client import RangerClientHttp
from apache_ranger.model.ranger_authz   import RangerAuthzRequest, RangerAuthzResult
from apache_ranger.model.ranger_authz   import RangerMultiAuthzRequest, RangerMultiAuthzResult
from apache_ranger.model.ranger_authz   import RangerResourcePermissions, RangerResourcePermissionsRequest
from apache_ranger.utils                import API, HttpMethod, HTTPStatus
from apache_ranger.utils                import type_coerce

LOG = logging.getLogger(__name__)


class RangerPDPClient:
    """
    Python client for Ranger PDP authorization APIs.

    Base URL should point to PDP server endpoint, for example:
      http://localhost:6500
    """

    def __init__(self, url, auth, query_params=None, headers=None):
        self.client_http = RangerClientHttp(url, auth, query_params, headers)
        self.session     = self.client_http.session
        logging.getLogger("requests").setLevel(logging.WARNING)

    def authorize(self, authz_request):
        """
        Call POST /authz/v1/authorize
        :param authz_request: dict-like or RangerAuthzRequest
        :return: RangerAuthzResult
        """
        req  = type_coerce(authz_request, RangerAuthzRequest)
        resp = self.client_http.call_api(RangerPDPClient.AUTHORIZE, request_data=req)
        return type_coerce(resp, RangerAuthzResult)

    def authorize_multi(self, multi_authz_request):
        """
        Call POST /authz/v1/authorizeMulti
        :param multi_authz_request: dict-like or RangerMultiAuthzRequest
        :return: RangerMultiAuthzResult
        """
        req  = type_coerce(multi_authz_request, RangerMultiAuthzRequest)
        resp = self.client_http.call_api(RangerPDPClient.AUTHORIZE_MULTI, request_data=req)
        return type_coerce(resp, RangerMultiAuthzResult)

    def get_resource_permissions(self, resource_perm_request):
        """
        Call POST /authz/v1/permissions
        :param resource_perm_request: dict-like OR RangerResourcePermissionsRequest
        :return: RangerResourcePermissions
        """
        req  = type_coerce(resource_perm_request, RangerResourcePermissionsRequest)
        resp = self.client_http.call_api(RangerPDPClient.GET_RESOURCE_PERMISSIONS, request_data=req)

        return type_coerce(resp, RangerResourcePermissions)

    # URIs
    URI_BASE                     = "authz/v1"
    URI_AUTHORIZE                = URI_BASE + "/authorize"
    URI_AUTHORIZE_MULTI          = URI_BASE + "/authorizeMulti"
    URI_RESOURCE_PERMISSIONS     = URI_BASE + "/permissions"

    # APIs
    AUTHORIZE                = API(URI_AUTHORIZE, HttpMethod.POST, HTTPStatus.OK)
    AUTHORIZE_MULTI          = API(URI_AUTHORIZE_MULTI, HttpMethod.POST, HTTPStatus.OK)
    GET_RESOURCE_PERMISSIONS = API(URI_RESOURCE_PERMISSIONS, HttpMethod.POST, HTTPStatus.OK)

