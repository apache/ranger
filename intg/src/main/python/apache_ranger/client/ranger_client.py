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


import enum
import json
import logging
import requests

from http     import HTTPStatus
from requests import Session, Response

from apache_ranger.model.ranger_role          import RangerRole
from apache_ranger.model.ranger_policy        import RangerPolicy
from apache_ranger.model.ranger_service       import RangerService
from apache_ranger.model.ranger_service_def   import RangerServiceDef
from apache_ranger.model.ranger_security_zone import RangerSecurityZone


APPLICATION_JSON = 'application/json'

requests.packages.urllib3.disable_warnings()

log = logging.getLogger(__name__)

class Message:
    def __init__(self, name=None, rbKey=None, message=None, objectId=None, fieldName=None):
        self.name      = name
        self.rbKey     = rbKey
        self.message   = message
        self.objectId  = objectId
        self.fieldName = fieldName

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class RESTResponse:
    def __init__(self, httpStatusCode=None, statusCode=None, msgDesc=None, messageList=None):
        self.httpStatusCode = httpStatusCode
        self.statusCode     = statusCode
        self.msgDesc        = msgDesc
        self.messageList    = messageList if messageList is not None else []

    def __repr__(self):
        return json.dumps(self, default=lambda x: x.__dict__, sort_keys=True, indent=4)


class HttpMethod(enum.Enum):
    GET    = "GET"
    PUT    = "PUT"
    POST   = "POST"
    DELETE = "DELETE"


class API:
    def __init__(self, path, method, expected_status, consumes=APPLICATION_JSON, produces=APPLICATION_JSON):
        self.path            = path
        self.method          = method
        self.expected_status = expected_status
        self.consumes        = consumes
        self.produces        = produces

    def call(self, session, baseUrl, params, request):
        session.headers['Accept']       = self.consumes
        session.headers['Content-type'] = self.produces

        if log.isEnabledFor(logging.DEBUG):
            log.debug('==> call({},{},{})'.format(self, params, request))
            log.debug('------------------------------------------------------')
            log.debug('Call         : {} {}'.format(self.method, self.path))
            log.debug('Content-type : {}'.format(self.consumes))
            log.debug('Accept       : {}'.format(self.produces))

            if request is not None:
                log.debug("Request      : {}".format(request))

        path = baseUrl + self.path

        if self.method == HttpMethod.GET:
            client_response = session.get(path)
        elif self.method == HttpMethod.POST:
            client_response = session.post(path, data=request.__repr__())
        elif self.method == HttpMethod.PUT:
            client_response = session.put(path, data=request.__repr__())
        elif self.method == HttpMethod.DELETE:
            client_response = session.delete(path, params=params)
        else:
            raise Exception('Unsupported HTTP Method {}'.format(self.method))

        if client_response.status_code == self.expected_status:
            if client_response.content:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug('Response: {}'.format(client_response.text))
            else:
                return None
        elif client_response.status_code == HTTPStatus.SERVICE_UNAVAILABLE:
            log.error('Ranger Admin unavailable. HTTP Status: {}'.format(HTTPStatus.SERVICE_UNAVAILABLE))
            log.error("client_response=: {}" + str(client_response))
            return None
        else:
            raise Exception(client_response.text)

        __json = json.loads(str(json.dumps(client_response.json())))

        if log.isEnabledFor(logging.DEBUG):
            log.debug('<== call({},{},{}), result = {}'.format(self, params, request, client_response))

        return __json

    def apply_url_params(self, params):
        try:
            return API(self.path.format(**params), self.method, self.expected_status, self.consumes, self.produces)
        except (KeyError, TypeError) as e:
            log.error('Arguments not formatted properly' + str(e))
            raise e


class RangerClient:
    # Query Params
    PARAM_ID                            = "id"
    PARAM_NAME                          = "name"
    PARAM_DAYS                          = "days"
    PARAM_EXEC_USER                     = "execUser"
    PARAM_POLICY_NAME                   = "policyname"
    PARAM_SERVICE_NAME                  = "serviceName"
    PARAM_RELOAD_SERVICE_POLICIES_CACHE = "reloadServicePoliciesCache"

    # URIs
    URI_BASE = "/service/public/v2/api"

    URI_SERVICEDEF         = URI_BASE + "/servicedef"
    URI_SERVICEDEF_BY_ID   = URI_SERVICEDEF + "/{id}"
    URI_SERVICEDEF_BY_NAME = URI_SERVICEDEF + "/name/{name}"

    URI_SERVICE             = URI_BASE + "/service"
    URI_SERVICE_BY_ID       = URI_SERVICE + "/{id}"
    URI_SERVICE_BY_NAME     = URI_SERVICE + "/name/{name}"
    URI_POLICIES_IN_SERVICE = URI_SERVICE + "/{serviceName}/policy"

    URI_POLICY         = URI_BASE + "/policy"
    URI_APPLY_POLICY   = URI_POLICY + "/apply"
    URI_POLICY_BY_ID   = URI_POLICY + "/{id}"
    URI_POLICY_BY_NAME = URI_SERVICE + "/{serviceName}/policy/{policyname}"

    URI_ROLE         = URI_BASE + "/roles"
    URI_ROLE_NAMES   = URI_ROLE + "/names"
    URI_ROLE_BY_ID   = URI_ROLE + "/{id}"
    URI_ROLE_BY_NAME = URI_ROLE + "/name/{name}"
    URI_USER_ROLES   = URI_ROLE + "/user/{name}"
    URI_GRANT_ROLE   = URI_ROLE + "/grant/{name}"
    URI_REVOKE_ROLE  = URI_ROLE + "/revoke/{name}"

    URI_ZONE         = URI_BASE + "/zones"
    URI_ZONE_BY_ID   = URI_ZONE + "/{id}"
    URI_ZONE_BY_NAME = URI_ZONE + "/name/{name}"

    URI_PLUGIN_INFO   = URI_BASE + "/plugins/info"
    URI_POLICY_DELTAS = URI_BASE + "/server/policydeltas"

    # APIs
    CREATE_SERVICEDEF         = API(URI_SERVICEDEF, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_SERVICEDEF_BY_ID   = API(URI_SERVICEDEF_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_SERVICEDEF_BY_NAME = API(URI_SERVICEDEF_BY_NAME, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_SERVICEDEF_BY_ID   = API(URI_SERVICEDEF_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_SERVICEDEF_BY_NAME = API(URI_SERVICEDEF_BY_NAME, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_SERVICEDEF_BY_ID      = API(URI_SERVICEDEF_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_SERVICEDEF_BY_NAME    = API(URI_SERVICEDEF_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    FIND_SERVICEDEFS          = API(URI_SERVICEDEF, HttpMethod.GET, HTTPStatus.OK)

    CREATE_SERVICE         = API(URI_SERVICE, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_SERVICE_BY_ID   = API(URI_SERVICE_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_SERVICE_BY_NAME = API(URI_SERVICE_BY_NAME, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_SERVICE_BY_ID   = API(URI_SERVICE_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_SERVICE_BY_NAME = API(URI_SERVICE_BY_NAME, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_SERVICE_BY_ID      = API(URI_SERVICE_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_SERVICE_BY_NAME    = API(URI_SERVICE_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    FIND_SERVICES          = API(URI_SERVICE, HttpMethod.GET, HTTPStatus.OK)

    CREATE_POLICY           = API(URI_POLICY, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_POLICY_BY_ID     = API(URI_POLICY_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_POLICY_BY_NAME   = API(URI_POLICY_BY_NAME, HttpMethod.PUT, HTTPStatus.OK)
    APPLY_POLICY            = API(URI_APPLY_POLICY, HttpMethod.POST, HTTPStatus.OK)
    DELETE_POLICY_BY_ID     = API(URI_POLICY_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_POLICY_BY_NAME   = API(URI_POLICY, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_POLICY_BY_ID        = API(URI_POLICY_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_POLICY_BY_NAME      = API(URI_POLICY_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    GET_POLICIES_IN_SERVICE = API(URI_POLICIES_IN_SERVICE, HttpMethod.GET, HTTPStatus.OK)
    FIND_POLICIES           = API(URI_POLICY, HttpMethod.GET, HTTPStatus.OK)

    CREATE_ZONE         = API(URI_ZONE, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_ZONE_BY_ID   = API(URI_ZONE_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_ZONE_BY_NAME = API(URI_ZONE_BY_NAME, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_ZONE_BY_ID   = API(URI_ZONE_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_ZONE_BY_NAME = API(URI_ZONE_BY_NAME, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_ZONE_BY_ID      = API(URI_ZONE_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_ZONE_BY_NAME    = API(URI_ZONE_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    FIND_ZONES          = API(URI_ZONE, HttpMethod.GET, HTTPStatus.OK)

    CREATE_ROLE         = API(URI_ROLE, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_ROLE_BY_ID   = API(URI_ROLE_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_ROLE_BY_ID   = API(URI_ROLE_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_ROLE_BY_NAME = API(URI_ROLE_BY_NAME, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_ROLE_BY_ID      = API(URI_ROLE_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_ROLE_BY_NAME    = API(URI_ROLE_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    GET_ALL_ROLE_NAMES  = API(URI_ROLE_NAMES, HttpMethod.GET, HTTPStatus.OK)
    GET_USER_ROLES      = API(URI_USER_ROLES, HttpMethod.GET, HTTPStatus.OK)
    GRANT_ROLE          = API(URI_GRANT_ROLE, HttpMethod.PUT, HTTPStatus.OK)
    REVOKE_ROLE         = API(URI_REVOKE_ROLE, HttpMethod.PUT, HTTPStatus.OK)
    FIND_ROLES          = API(URI_ROLE, HttpMethod.GET, HTTPStatus.OK)

    GET_PLUGIN_INFO      = API(URI_PLUGIN_INFO, HttpMethod.GET, HTTPStatus.OK)
    DELETE_POLICY_DELTAS = API(URI_POLICY_DELTAS, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)

    def __init__(self, url, username, password):
        self.__password     = password
        self.url            = url
        self.username       = username
        self.session        = Session()
        self.session.auth   = (username, password)
        self.session.verify = False

    def __call_api(self, api, params, request):
        return api.call(self.session, self.url, params, request)

    def __call_api_with_url_params(self, api, urlParams, params, request):
        return api.apply_url_params(urlParams).call(self.session, self.url, params, request)

    # Service Definition APIs
    def create_service_def(self, serviceDef):
        ret = self.__call_api(RangerClient.CREATE_SERVICEDEF, {}, serviceDef)

        return RangerServiceDef(**ret) if ret is not None else None

    def update_service_def_by_id(self, serviceDefId, serviceDef):
        ret = self.__call_api_with_url_params(RangerClient.UPDATE_SERVICEDEF_BY_ID, { RangerClient.PARAM_ID: serviceDefId }, {}, serviceDef)

        return RangerServiceDef(**ret) if ret is not None else None

    def update_service_def(self, serviceDefName, serviceDef):
        ret = self.__call_api_with_url_params(RangerClient.UPDATE_SERVICEDEF_BY_NAME, { RangerClient.PARAM_NAME: serviceDefName }, {}, serviceDef)

        return RangerServiceDef(**ret) if ret is not None else None

    def delete_service_def_by_id(self, serviceDefId):
        self.__call_api_with_url_params(RangerClient.DELETE_SERVICEDEF_BY_ID, { RangerClient.PARAM_ID: serviceDefId }, {}, {})

        return None

    def delete_service_def(self, serviceDefName):
        self.__call_api_with_url_params(RangerClient.DELETE_SERVICEDEF_BY_NAME, { RangerClient.PARAM_NAME: serviceDefName }, {}, {})

        return None

    def get_service_def_by_id(self, serviceDefId):
        ret = self.__call_api_with_url_params(RangerClient.GET_SERVICEDEF_BY_ID, { RangerClient.PARAM_ID: serviceDefId }, {}, {})

        return RangerServiceDef(**ret) if ret is not None else None

    def get_service_def(self, serviceDefName):
        ret = self.__call_api_with_url_params(RangerClient.GET_SERVICEDEF_BY_NAME, { RangerClient.PARAM_NAME: serviceDefName }, {}, {})

        return RangerServiceDef(**ret) if ret is not None else None

    def find_service_defs(self, filter):
        ret = self.__call_api(RangerClient.FIND_SERVICEDEFS, filter, {})

        return list(ret) if ret is not None else None

    # Service APIs
    def create_service(self, service):
        ret = self.__call_api(RangerClient.CREATE_SERVICE, {}, service)

        return RangerService(**ret) if ret is not None else None

    def update_service_by_id(self, serviceId, service):
        ret = self.__call_api_with_url_params(RangerClient.UPDATE_SERVICE_BY_ID, { RangerClient.PARAM_ID: serviceId }, {}, service)

        return RangerService(**ret) if ret is not None else None

    def update_service(self, serviceName, service):
        ret = self.__call_api_with_url_params(RangerClient.UPDATE_SERVICE_BY_NAME, { RangerClient.PARAM_NAME: serviceName }, {}, service)

        return RangerService(**ret) if ret is not None else None

    def delete_service_by_id(self, serviceId):
        self.__call_api_with_url_params(RangerClient.DELETE_SERVICE_BY_ID, { RangerClient.PARAM_ID: serviceId }, {}, {})

        return None

    def delete_service(self, serviceName):
        self.__call_api_with_url_params(RangerClient.DELETE_SERVICE_BY_NAME, { RangerClient.PARAM_NAME: serviceName }, {}, {})

        return None

    def get_service_by_id(self, serviceId):
        ret = self.__call_api_with_url_params(RangerClient.GET_SERVICE_BY_ID, { RangerClient.PARAM_ID: serviceId }, {}, {})

        return RangerService(**ret) if ret is not None else None

    def get_service(self, serviceName):
        ret = self.__call_api_with_url_params(RangerClient.GET_SERVICE_BY_NAME, { RangerClient.PARAM_NAME: serviceName }, {}, {})

        return RangerService(**ret) if ret is not None else None

    def find_services(self, filter):
        ret = self.__call_api(RangerClient.FIND_SERVICES, filter, {})

        return list(ret) if ret is not None else None

    # Policy APIs
    def create_policy(self, policy):
        ret = self.__call_api(RangerClient.CREATE_POLICY, {}, policy)

        return RangerPolicy(**ret) if ret is not None else None

    def update_policy_by_id(self, policyId, policy):
        ret = self.__call_api_with_url_params(RangerClient.UPDATE_POLICY_BY_ID, { RangerClient.PARAM_ID: policyId }, {}, policy)

        return RangerPolicy(**ret) if ret is not None else None

    def update_policy(self, serviceName, policyName, policy):
        path_params = { RangerClient.PARAM_SERVICE_NAME: serviceName, RangerClient.PARAM_POLICY_NAME: policyName }
        ret         = self.__call_api_with_url_params(RangerClient.UPDATE_POLICY_BY_NAME, path_params, {}, policy)

        return RangerPolicy(**ret) if ret is not None else None

    def apply_policy(self, policy):
        ret = self.__call_api(RangerClient.APPLY_POLICY, {}, policy)

        return RangerPolicy(**ret) if ret is not None else None

    def delete_policy_by_id(self, policyId):
        self.__call_api_with_url_params(RangerClient.DELETE_POLICY_BY_ID, { RangerClient.PARAM_ID: policyId }, {}, {})

        return None

    def delete_policy(self, serviceName, policyName):
        query_params = { RangerClient.PARAM_POLICY_NAME: policyName, 'servicename': serviceName }

        self.__call_api(RangerClient.DELETE_POLICY_BY_NAME, query_params, {})

        return None

    def get_policy_by_id(self, policyId):
        ret = self.__call_api_with_url_params(RangerClient.GET_POLICY_BY_ID, { RangerClient.PARAM_ID: policyId }, {}, {})

        return RangerPolicy(**ret) if ret is not None else None

    def get_policy(self, serviceName, policyName):
        path_params = {RangerClient.PARAM_SERVICE_NAME: serviceName, RangerClient.PARAM_POLICY_NAME: policyName}
        ret         = self.__call_api_with_url_params(RangerClient.GET_POLICY_BY_NAME, path_params, {}, {})

        return RangerPolicy(**ret) if ret is not None else None

    def get_policies_in_service(self, serviceName):
        ret = self.__call_api_with_url_params(RangerClient.GET_POLICIES_IN_SERVICE, { RangerClient.PARAM_SERVICE_NAME: serviceName }, {}, {})

        return list(ret) if ret is not None else None

    def find_policies(self, filter):
        ret = self.__call_api(RangerClient.FIND_POLICIES, filter, {})

        return list(ret) if ret is not None else None

    # SecurityZone APIs
    def create_security_zone(self, securityZone):
        ret = self.__call_api(RangerClient.CREATE_ZONE, {}, securityZone)

        return RangerSecurityZone(**ret) if ret is not None else None

    def update_security_zone_by_id(self, zoneId, securityZone):
        ret = self.__call_api_with_url_params(RangerClient.UPDATE_ZONE_BY_ID, { RangerClient.PARAM_ID: zoneId }, {}, securityZone)

        return RangerSecurityZone(**ret) if ret is not None else None

    def update_security_zone(self, zoneName, securityZone):
        ret = self.__call_api_with_url_params(RangerClient.UPDATE_ZONE_BY_NAME, { RangerClient.PARAM_NAME: zoneName }, {}, securityZone)

        return RangerSecurityZone(**ret) if ret is not None else None

    def delete_security_zone_by_id(self, zoneId):
        self.__call_api_with_url_params(RangerClient.DELETE_ZONE_BY_ID, { RangerClient.PARAM_ID: zoneId }, {}, {})
        return None

    def delete_security_zone(self, zoneName):
        self.__call_api_with_url_params(RangerClient.DELETE_ZONE_BY_NAME, { RangerClient.PARAM_NAME: zoneName }, {}, {})

        return None

    def get_security_zone_by_id(self, zoneId):
        ret = self.__call_api_with_url_params(RangerClient.GET_ZONE_BY_ID, { RangerClient.PARAM_ID: zoneId }, {}, {})

        return RangerSecurityZone(**ret) if ret is not None else None

    def get_security_zone(self, zoneName):
        ret = self.__call_api_with_url_params(RangerClient.GET_ZONE_BY_NAME, { RangerClient.PARAM_NAME: zoneName }, {}, {})

        return RangerSecurityZone(**ret) if ret is not None else None

    def find_security_zones(self, filter):
        ret = self.__call_api(RangerClient.FIND_ZONES, filter, {})

        return list(ret) if ret is not None else None

    # Role APIs
    def create_role(self, serviceName, role):
        query_params = {RangerClient.PARAM_SERVICE_NAME, serviceName}
        ret          = self.__call_api(RangerClient.CREATE_ROLE, query_params, role)

        return RangerRole(**ret) if ret is not None else None

    def update_role(self, roleId, role):
        ret = self.__call_api_with_url_params(RangerClient.UPDATE_ROLE_BY_ID, { RangerClient.PARAM_ID: roleId }, {}, role)

        return RangerRole(**ret) if ret is not None else None

    def delete_role_by_id(self, roleId):
        self.__call_api_with_url_params(RangerClient.DELETE_ROLE_BY_ID, { RangerClient.PARAM_ID: roleId }, {}, {})

        return None

    def delete_role(self, roleName, execUser, serviceName):
        query_params = { RangerClient.PARAM_EXEC_USER: execUser, RangerClient.PARAM_SERVICE_NAME: serviceName }

        self.__call_api_with_url_params(RangerClient.DELETE_ROLE_BY_NAME, { RangerClient.PARAM_NAME: roleName }, query_params, {})

        return None

    def get_role_by_id(self, roleId):
        ret = self.__call_api_with_url_params(RangerClient.GET_ROLE_BY_ID, { RangerClient.PARAM_ID: roleId }, {}, {})

        return RangerRole(**ret) if ret is not None else None

    def get_role(self, roleName, execUser, serviceName):
        query_params = { RangerClient.PARAM_EXEC_USER: execUser, RangerClient.PARAM_SERVICE_NAME: serviceName }
        ret          = self.__call_api_with_url_params(RangerClient.GET_ROLE_BY_NAME, { RangerClient.PARAM_NAME: roleName }, query_params, {})

        return RangerRole(**ret) if ret is not None else None

    def get_all_role_names(self, execUser, serviceName):
        query_params = { RangerClient.PARAM_EXEC_USER: execUser, RangerClient.PARAM_SERVICE_NAME: serviceName }
        ret          = self.__call_api_with_url_params(RangerClient.GET_ALL_ROLE_NAMES, { RangerClient.PARAM_NAME: serviceName }, query_params, {})

        return list(ret) if ret is not None else None

    def get_user_roles(self, user):
        ret = self.__call_api_with_url_params(RangerClient.GET_USER_ROLES, { RangerClient.PARAM_NAME: user }, {}, {})

        return list(ret) if ret is not None else None

    def find_roles(self, filter):
        ret = self.__call_api(RangerClient.FIND_ROLES, filter, {})

        return list(ret) if ret is not None else None

    def grant_role(self, serviceName, request):
        ret = self.__call_api_with_url_params(RangerClient.GRANT_ROLE, { RangerClient.PARAM_NAME: serviceName }, {}, request)

        return RESTResponse(**ret) if ret is not None else None

    def revoke_role(self, serviceName, request):
        ret = self.__call_api_with_url_params(RangerClient.REVOKE_ROLE, { RangerClient.PARAM_NAME: serviceName }, {}, request)

        return RESTResponse(**ret) if ret is not None else None

    # Admin APIs
    def delete_policy_deltas(self, days, reloadServicePoliciesCache):
        query_params = {
            RangerClient.PARAM_DAYS: days,
            RangerClient.PARAM_RELOAD_SERVICE_POLICIES_CACHE: reloadServicePoliciesCache
        }

        self.__call_api(RangerClient.DELETE_POLICY_DELTAS, query_params, {})

        return None
