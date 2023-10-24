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
import logging
from apache_ranger.exceptions                 import RangerServiceException
from apache_ranger.model.ranger_base          import RangerBase, PList
from apache_ranger.model.ranger_policy        import RangerPolicy
from apache_ranger.model.ranger_role          import RangerRole
from apache_ranger.model.ranger_security_zone import RangerSecurityZone, RangerSecurityZoneV2, RangerSecurityZoneHeaderInfo, RangerSecurityZoneResource
from apache_ranger.model.ranger_service       import RangerService, RangerServiceHeaderInfo
from apache_ranger.model.ranger_service_def   import RangerServiceDef
from apache_ranger.model.ranger_service_tags  import RangerServiceTags
from apache_ranger.utils                      import *
from requests                                 import Session
from requests                                 import Response
from requests.auth                            import AuthBase
from urllib.parse                             import urlencode
from urllib.parse                             import urljoin

LOG = logging.getLogger(__name__)

QUERY_PARAM_USER_DOT_NAME = 'user.name'.encode("utf-8")


class RangerClient:
    def __init__(self, url, auth, query_params=None, headers=None):
        self.client_http = RangerClientHttp(url, auth, query_params, headers)
        self.session     = self.client_http.session
        logging.getLogger("requests").setLevel(logging.WARNING)


    # Service Definition APIs
    def create_service_def(self, serviceDef):
        resp = self.client_http.call_api(RangerClient.CREATE_SERVICEDEF, request_data=serviceDef)

        return type_coerce(resp, RangerServiceDef)

    def update_service_def_by_id(self, serviceDefId, serviceDef):
        resp = self.client_http.call_api(RangerClient.UPDATE_SERVICEDEF_BY_ID.format_path({ 'id': serviceDefId }), request_data=serviceDef)

        return type_coerce(resp, RangerServiceDef)

    def update_service_def(self, serviceDefName, serviceDef):
        resp = self.client_http.call_api(RangerClient.UPDATE_SERVICEDEF_BY_NAME.format_path({ 'name': serviceDefName }), request_data=serviceDef)

        return type_coerce(resp, RangerServiceDef)

    def delete_service_def_by_id(self, serviceDefId, params=None):
        self.client_http.call_api(RangerClient.DELETE_SERVICEDEF_BY_ID.format_path({ 'id': serviceDefId }), params)

    def delete_service_def(self, serviceDefName, params=None):
        self.client_http.call_api(RangerClient.DELETE_SERVICEDEF_BY_NAME.format_path({ 'name': serviceDefName }), params)

    def get_service_def_by_id(self, serviceDefId):
        resp = self.client_http.call_api(RangerClient.GET_SERVICEDEF_BY_ID.format_path({ 'id': serviceDefId }))

        return type_coerce(resp, RangerServiceDef)

    def get_service_def(self, serviceDefName):
        resp = self.client_http.call_api(RangerClient.GET_SERVICEDEF_BY_NAME.format_path({ 'name': serviceDefName }))

        return type_coerce(resp, RangerServiceDef)

    def find_service_defs(self, filter=None):
        resp = self.client_http.call_api(RangerClient.FIND_SERVICEDEFS, filter)

        return type_coerce_list(resp, RangerServiceDef)


    # Service APIs
    def create_service(self, service):
        resp = self.client_http.call_api(RangerClient.CREATE_SERVICE, request_data=service)

        return type_coerce(resp, RangerService)

    def get_service_by_id(self, serviceId):
        resp = self.client_http.call_api(RangerClient.GET_SERVICE_BY_ID.format_path({ 'id': serviceId }))

        return type_coerce(resp, RangerService)

    def get_service(self, serviceName):
        resp = self.client_http.call_api(RangerClient.GET_SERVICE_BY_NAME.format_path({ 'name': serviceName }))

        return type_coerce(resp, RangerService)

    def update_service_by_id(self, serviceId, service, params=None):
        resp = self.client_http.call_api(RangerClient.UPDATE_SERVICE_BY_ID.format_path({ 'id': serviceId }), params, service)

        return type_coerce(resp, RangerService)

    def update_service(self, serviceName, service, params=None):
        resp = self.client_http.call_api(RangerClient.UPDATE_SERVICE_BY_NAME.format_path({ 'name': serviceName }), params, service)

        return type_coerce(resp, RangerService)

    def delete_service_by_id(self, serviceId):
        self.client_http.call_api(RangerClient.DELETE_SERVICE_BY_ID.format_path({ 'id': serviceId }))

    def delete_service(self, serviceName):
        self.client_http.call_api(RangerClient.DELETE_SERVICE_BY_NAME.format_path({ 'name': serviceName }))

    def find_services(self, filter=None):
        resp = self.client_http.call_api(RangerClient.FIND_SERVICES, filter)

        return type_coerce_list(resp, RangerService)


    # Policy APIs
    def create_policy(self, policy, params=None):
        resp = self.client_http.call_api(RangerClient.CREATE_POLICY, params, policy)

        return type_coerce(resp, RangerPolicy)

    def get_policy_by_id(self, policyId):
        resp = self.client_http.call_api(RangerClient.GET_POLICY_BY_ID.format_path({ 'id': policyId }))

        return type_coerce(resp, RangerPolicy)

    def get_policy(self, serviceName, policyName):
        resp = self.client_http.call_api(RangerClient.GET_POLICY_BY_NAME.format_path({ 'serviceName': serviceName, 'policyName': policyName}))

        return type_coerce(resp, RangerPolicy)

    def get_policy_by_name_zone(self, serviceName, policyName, zoneName):
        resp = self.client_http.call_api(RangerClient.GET_POLICY_BY_NAME.format_path({ 'serviceName': serviceName, 'policyName': policyName}), { 'zoneName': zoneName })

        return type_coerce(resp, RangerPolicy)

    def get_policies_in_service(self, serviceName, params=None):
        resp = self.client_http.call_api(RangerClient.GET_POLICIES_IN_SERVICE.format_path({ 'serviceName': serviceName }), params)

        return type_coerce_list(resp, RangerPolicy)

    def update_policy_by_id(self, policyId, policy):
        resp = self.client_http.call_api(RangerClient.UPDATE_POLICY_BY_ID.format_path({ 'id': policyId }), request_data=policy)

        return type_coerce(resp, RangerPolicy)

    def update_policy(self, serviceName, policyName, policy):
        resp = self.client_http.call_api(RangerClient.UPDATE_POLICY_BY_NAME.format_path({ 'serviceName': serviceName, 'policyName': policyName}), request_data=policy)

        return type_coerce(resp, RangerPolicy)

    def update_policy_by_name_zone(self, serviceName, policyName, zoneName, policy):
        resp = self.client_http.call_api(RangerClient.UPDATE_POLICY_BY_NAME.format_path({ 'serviceName': serviceName, 'policyName': policyName}), { 'zoneName': zoneName },  request_data=policy)

        return type_coerce(resp, RangerPolicy)

    def apply_policy(self, policy, params=None):
        resp = self.client_http.call_api(RangerClient.APPLY_POLICY, params, policy)

        return type_coerce(resp, RangerPolicy)

    def delete_policy_by_id(self, policyId):
        self.client_http.call_api(RangerClient.DELETE_POLICY_BY_ID.format_path({ 'id': policyId }))

    def delete_policy(self, serviceName, policyName):
        self.client_http.call_api(RangerClient.DELETE_POLICY_BY_NAME, { 'servicename': serviceName, 'policyname': policyName })

    def delete_policy_by_name_zone(self, serviceName, policyName, zoneName):
        self.client_http.call_api(RangerClient.DELETE_POLICY_BY_NAME, { 'servicename': serviceName, 'policyname': policyName, 'zoneName': zoneName })

    def find_policies(self, filter=None):
        resp = self.client_http.call_api(RangerClient.FIND_POLICIES, filter)

        return type_coerce_list(resp, RangerPolicy)


    # SecurityZone APIs
    def create_security_zone(self, securityZone):
        resp = self.client_http.call_api(RangerClient.CREATE_ZONE, request_data=securityZone)

        return type_coerce(resp, RangerSecurityZone)

    def update_security_zone_by_id(self, zoneId, securityZone):
        resp = self.client_http.call_api(RangerClient.UPDATE_ZONE_BY_ID.format_path({ 'id': zoneId }), request_data=securityZone)

        return type_coerce(resp, RangerSecurityZone)

    def delete_security_zone_by_id(self, zoneId):
        self.client_http.call_api(RangerClient.DELETE_ZONE_BY_ID.format_path({ 'id': zoneId }))

    def delete_security_zone(self, zoneName):
        self.client_http.call_api(RangerClient.DELETE_ZONE_BY_NAME.format_path({ 'name': zoneName }))

    def get_security_zone_by_id(self, zoneId):
        resp = self.client_http.call_api(RangerClient.GET_ZONE_BY_ID.format_path({ 'id': zoneId }))

        return type_coerce(resp, RangerSecurityZone)

    def get_security_zone(self, zoneName):
        resp = self.client_http.call_api(RangerClient.GET_ZONE_BY_NAME.format_path({ 'name': zoneName }))

        return type_coerce(resp, RangerSecurityZone)

    def get_security_zone_headers(self):
      resp = self.client_http.call_api(RangerClient.GET_ZONE_HEADERS)

      return type_coerce_list(resp, RangerSecurityZoneHeaderInfo)

    def get_security_zone_service_headers(self, zoneId):
      resp = self.client_http.call_api(RangerClient.GET_ZONE_SERVICE_HEADERS.format_path({ 'id': zoneId }))

      return type_coerce_list(resp, RangerServiceHeaderInfo)

    def get_zone_names_for_resource(self, serviceName, resource):
      return self.client_http.call_api(RangerClient.GET_ZONE_NAMES_FOR_RESOURCE.format_path({ 'serviceName': serviceName }), query_params=resource_to_query_params(resource))

    def find_security_zones(self, filter=None):
        resp = self.client_http.call_api(RangerClient.FIND_ZONES, filter)

        return type_coerce_list(resp, RangerSecurityZone)


    def create_security_zone_v2(self, securityZone):
        resp = self.client_http.call_api(RangerClient.CREATE_ZONE_V2, request_data=securityZone)

        return type_coerce(resp, RangerSecurityZoneV2)

    def update_security_zone_v2(self, zoneId, securityZone):
        resp = self.client_http.call_api(RangerClient.UPDATE_ZONE_V2_BY_ID.format_path({ 'id': zoneId }), request_data=securityZone)

        return type_coerce(resp, RangerSecurityZoneV2)

    def partial_update_security_zone_v2(self, zoneId, changeData):
        resp = self.client_http.call_api(RangerClient.PARTIAL_UPDATE_ZONE_V2_BY_ID.format_path({ 'id': zoneId }), request_data=changeData)

        return type_coerce(resp, RangerSecurityZoneV2)

    def get_security_zone_v2(self, zoneName):
        resp = self.client_http.call_api(RangerClient.GET_ZONE_V2_BY_NAME.format_path({ 'name': zoneName }))

        return type_coerce(resp, RangerSecurityZoneV2)

    def get_security_zone_v2_by_id(self, zoneId):
        resp = self.client_http.call_api(RangerClient.GET_ZONE_V2_BY_ID.format_path({ 'id': zoneId }))

        return type_coerce(resp, RangerSecurityZoneV2)

    def zone_v2_get_resources(self, zoneName, serviceName, filter=None):
        resp = self.client_http.call_api(RangerClient.ZONE_V2_GET_RESOURCES.format_path({'name': zoneName, 'serviceName': serviceName}), filter)
        ret  = type_coerce(resp, PList)

        if ret is not None:
          ret.type_coerce_list(RangerSecurityZoneResource)

        return ret

    def zone_v2_by_id_get_resources(self, zoneId, serviceName, filter=None):
        resp = self.client_http.call_api(RangerClient.ZONE_V2_BY_ID_GET_RESOURCES.format_path({'id': zoneId, 'serviceName': serviceName}), filter)
        ret  = type_coerce(resp, PList)

        if ret is not None:
          ret.type_coerce_list(RangerSecurityZoneResource)

        return ret

    def find_security_zones_v2(self, filter=None):
        resp = self.client_http.call_api(RangerClient.FIND_ZONES_V2, filter)
        ret  = type_coerce(resp, PList)

        if ret is not None:
          ret.type_coerce_list(RangerSecurityZoneV2)

        return ret


    # Role APIs
    def create_role(self, serviceName, role, params=None):
        if params is None:
            params = {}

        params['serviceName'] = serviceName

        resp = self.client_http.call_api(RangerClient.CREATE_ROLE, params, role)

        return type_coerce(resp, RangerRole)

    def update_role(self, roleId, role, params=None):
        resp = self.client_http.call_api(RangerClient.UPDATE_ROLE_BY_ID.format_path({ 'id': roleId }), params, role)

        return type_coerce(resp, RangerRole)

    def delete_role_by_id(self, roleId):
        self.client_http.call_api(RangerClient.DELETE_ROLE_BY_ID.format_path({ 'id': roleId }))

    def delete_role(self, roleName, execUser, serviceName):
        self.client_http.call_api(RangerClient.DELETE_ROLE_BY_NAME.format_path({ 'name': roleName }), { 'execUser': execUser, 'serviceName': serviceName })

    def get_role_by_id(self, roleId):
        resp = self.client_http.call_api(RangerClient.GET_ROLE_BY_ID.format_path({ 'id': roleId }))

        return type_coerce(resp, RangerRole)

    def get_role(self, roleName, execUser, serviceName):
        resp = self.client_http.call_api(RangerClient.GET_ROLE_BY_NAME.format_path({ 'name': roleName }), { 'execUser': execUser, 'serviceName': serviceName })

        return type_coerce(resp, RangerRole)

    def get_all_role_names(self, execUser, serviceName):
        resp = self.client_http.call_api(RangerClient.GET_ALL_ROLE_NAMES.format_path({ 'name': serviceName }), { 'execUser': execUser, 'serviceName': serviceName })

        return resp

    def get_user_roles(self, user, filters=None):
        ret = self.client_http.call_api(RangerClient.GET_USER_ROLES.format_path({ 'name': user }), filters)

        return list(ret) if ret is not None else None

    def find_roles(self, filter=None):
        resp = self.client_http.call_api(RangerClient.FIND_ROLES, filter)

        return type_coerce_list(resp, RangerRole)

    def grant_role(self, serviceName, request, params=None):
        resp = self.client_http.call_api(RangerClient.GRANT_ROLE.format_path({ 'name': serviceName }), params, request)

        return type_coerce(resp, RESTResponse)

    def revoke_role(self, serviceName, request, params=None):
        resp = self.client_http.call_api(RangerClient.REVOKE_ROLE.format_path({ 'name': serviceName }), params, request)

        return type_coerce(resp, RESTResponse)


    # Admin APIs
    def import_service_tags(self, serviceName, svcTags):
        self.client_http.call_api(RangerClient.IMPORT_SERVICE_TAGS.format_path({ 'serviceName': serviceName }), request_data=svcTags)

    def get_service_tags(self, serviceName):
        resp = self.client_http.call_api(RangerClient.GET_SERVICE_TAGS.format_path({ 'serviceName': serviceName }))

        return type_coerce(resp, RangerServiceTags)

    def delete_policy_deltas(self, days, reloadServicePoliciesCache):
        self.client_http.call_api(RangerClient.DELETE_POLICY_DELTAS, { 'days': days, 'reloadServicePoliciesCache': reloadServicePoliciesCache})

    def purge_records(self, record_type, retention_days):
        return self.client_http.call_api(RangerClient.PURGE_RECORDS, { 'type': record_type, 'retentionDays': retention_days})





    # URIs
    URI_BASE                = "service/public/v2/api"

    URI_SERVICEDEF          = URI_BASE + "/servicedef"
    URI_SERVICEDEF_BY_ID    = URI_SERVICEDEF + "/{id}"
    URI_SERVICEDEF_BY_NAME  = URI_SERVICEDEF + "/name/{name}"

    URI_SERVICE             = URI_BASE + "/service"
    URI_SERVICE_BY_ID       = URI_SERVICE + "/{id}"
    URI_SERVICE_BY_NAME     = URI_SERVICE + "/name/{name}"
    URI_POLICIES_IN_SERVICE = URI_SERVICE + "/{serviceName}/policy"

    URI_POLICY              = URI_BASE + "/policy"
    URI_APPLY_POLICY        = URI_POLICY + "/apply"
    URI_POLICY_BY_ID        = URI_POLICY + "/{id}"
    URI_POLICY_BY_NAME      = URI_SERVICE + "/{serviceName}/policy/{policyName}"

    URI_ROLE                = URI_BASE + "/roles"
    URI_ROLE_NAMES          = URI_ROLE + "/names"
    URI_ROLE_BY_ID          = URI_ROLE + "/{id}"
    URI_ROLE_BY_NAME        = URI_ROLE + "/name/{name}"
    URI_USER_ROLES          = URI_ROLE + "/user/{name}"
    URI_GRANT_ROLE          = URI_ROLE + "/grant/{name}"
    URI_REVOKE_ROLE         = URI_ROLE + "/revoke/{name}"

    URI_ZONE                    = URI_BASE + "/zones"
    URI_ZONE_BY_ID              = URI_ZONE + "/{id}"
    URI_ZONE_BY_NAME            = URI_ZONE + "/name/{name}"
    URI_ZONE_HEADERS            = URI_BASE + "/zone-headers"
    URI_ZONE_SERVICE_HEADERS    = URI_ZONE + "/{id}/service-headers"
    URI_ZONE_NAMES_FOR_RESOURCE = URI_BASE + "/zone-names/{serviceName}/resource"
    URI_ZONE_V2                   = URI_BASE + "/zones-v2"
    URI_ZONE_V2_BY_ID             = URI_ZONE_V2 + "/{id}"
    URI_ZONE_V2_BY_NAME           = URI_ZONE_V2 + "/name/{name}"
    URL_ZONE_V2_BY_ID_RESOURCES   = URI_ZONE_V2_BY_ID + "/resources/{serviceName}"
    URL_ZONE_V2_BY_NAME_RESOURCES = URI_ZONE_V2_BY_NAME+ "/resources/{serviceName}"
    URI_ZONE_V2_PARTIAL_BY_ID     = URI_ZONE_V2_BY_ID + "/partial"

    URI_SERVICE_TAGS        = URI_SERVICE + "/{serviceName}/tags"
    URI_PLUGIN_INFO         = URI_BASE + "/plugins/info"
    URI_POLICY_DELTAS       = URI_BASE + "/server/policydeltas"
    URI_PURGE_RECORDS       = URI_BASE + "/server/purge/records"

    # APIs
    CREATE_SERVICEDEF         = API(URI_SERVICEDEF, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_SERVICEDEF_BY_ID   = API(URI_SERVICEDEF_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_SERVICEDEF_BY_NAME = API(URI_SERVICEDEF_BY_NAME, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_SERVICEDEF_BY_ID   = API(URI_SERVICEDEF_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_SERVICEDEF_BY_NAME = API(URI_SERVICEDEF_BY_NAME, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_SERVICEDEF_BY_ID      = API(URI_SERVICEDEF_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_SERVICEDEF_BY_NAME    = API(URI_SERVICEDEF_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    FIND_SERVICEDEFS          = API(URI_SERVICEDEF, HttpMethod.GET, HTTPStatus.OK)

    CREATE_SERVICE            = API(URI_SERVICE, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_SERVICE_BY_ID      = API(URI_SERVICE_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_SERVICE_BY_NAME    = API(URI_SERVICE_BY_NAME, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_SERVICE_BY_ID      = API(URI_SERVICE_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_SERVICE_BY_NAME    = API(URI_SERVICE_BY_NAME, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_SERVICE_BY_ID         = API(URI_SERVICE_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_SERVICE_BY_NAME       = API(URI_SERVICE_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    FIND_SERVICES             = API(URI_SERVICE, HttpMethod.GET, HTTPStatus.OK)

    CREATE_POLICY             = API(URI_POLICY, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_POLICY_BY_ID       = API(URI_POLICY_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_POLICY_BY_NAME     = API(URI_POLICY_BY_NAME, HttpMethod.PUT, HTTPStatus.OK)
    APPLY_POLICY              = API(URI_APPLY_POLICY, HttpMethod.POST, HTTPStatus.OK)
    DELETE_POLICY_BY_ID       = API(URI_POLICY_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_POLICY_BY_NAME     = API(URI_POLICY, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_POLICY_BY_ID          = API(URI_POLICY_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_POLICY_BY_NAME        = API(URI_POLICY_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    GET_POLICIES_IN_SERVICE   = API(URI_POLICIES_IN_SERVICE, HttpMethod.GET, HTTPStatus.OK)
    FIND_POLICIES             = API(URI_POLICY, HttpMethod.GET, HTTPStatus.OK)

    CREATE_ZONE                 = API(URI_ZONE, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_ZONE_BY_ID           = API(URI_ZONE_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_ZONE_BY_ID           = API(URI_ZONE_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_ZONE_BY_NAME         = API(URI_ZONE_BY_NAME, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_ZONE_BY_ID              = API(URI_ZONE_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_ZONE_BY_NAME            = API(URI_ZONE_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    FIND_ZONES                  = API(URI_ZONE, HttpMethod.GET, HTTPStatus.OK)
    GET_ZONE_HEADERS            = API(URI_ZONE_HEADERS, HttpMethod.GET, HTTPStatus.OK)
    GET_ZONE_SERVICE_HEADERS    = API(URI_ZONE_SERVICE_HEADERS, HttpMethod.GET, HTTPStatus.OK)
    GET_ZONE_NAMES_FOR_RESOURCE = API(URI_ZONE_NAMES_FOR_RESOURCE, HttpMethod.GET, HTTPStatus.OK)
    CREATE_ZONE_V2                        = API(URI_ZONE_V2, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_ZONE_V2_BY_ID                  = API(URI_ZONE_V2_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    PARTIAL_UPDATE_ZONE_V2_BY_ID          = API(URI_ZONE_V2_PARTIAL_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    GET_ZONE_V2_BY_NAME                   = API(URI_ZONE_V2_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    GET_ZONE_V2_BY_ID                     = API(URI_ZONE_V2_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    ZONE_V2_GET_RESOURCES                 = API(URL_ZONE_V2_BY_NAME_RESOURCES, HttpMethod.GET, HTTPStatus.OK)
    ZONE_V2_BY_ID_GET_RESOURCES           = API(URL_ZONE_V2_BY_ID_RESOURCES, HttpMethod.GET, HTTPStatus.OK)
    FIND_ZONES_V2                         = API(URI_ZONE_V2, HttpMethod.GET, HTTPStatus.OK)

    CREATE_ROLE               = API(URI_ROLE, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_ROLE_BY_ID         = API(URI_ROLE_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_ROLE_BY_ID         = API(URI_ROLE_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_ROLE_BY_NAME       = API(URI_ROLE_BY_NAME, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_ROLE_BY_ID            = API(URI_ROLE_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_ROLE_BY_NAME          = API(URI_ROLE_BY_NAME, HttpMethod.GET, HTTPStatus.OK)
    GET_ALL_ROLE_NAMES        = API(URI_ROLE_NAMES, HttpMethod.GET, HTTPStatus.OK)
    GET_USER_ROLES            = API(URI_USER_ROLES, HttpMethod.GET, HTTPStatus.OK)
    GRANT_ROLE                = API(URI_GRANT_ROLE, HttpMethod.PUT, HTTPStatus.OK)
    REVOKE_ROLE               = API(URI_REVOKE_ROLE, HttpMethod.PUT, HTTPStatus.OK)
    FIND_ROLES                = API(URI_ROLE, HttpMethod.GET, HTTPStatus.OK)

    IMPORT_SERVICE_TAGS       = API(URI_SERVICE_TAGS, HttpMethod.PUT, HTTPStatus.NO_CONTENT)
    GET_SERVICE_TAGS          = API(URI_SERVICE_TAGS, HttpMethod.GET, HTTPStatus.OK)
    GET_PLUGIN_INFO           = API(URI_PLUGIN_INFO, HttpMethod.GET, HTTPStatus.OK)
    DELETE_POLICY_DELTAS      = API(URI_POLICY_DELTAS, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    PURGE_RECORDS             = API(URI_PURGE_RECORDS, HttpMethod.DELETE, HTTPStatus.OK)



class HadoopSimpleAuth(AuthBase):
  def __init__(self, user_name):
    self.user_name = user_name.encode("utf-8")

  def __call__(self, req):
    sep_char = '?'

    if req.url.find('?') != -1:
      sep_char = '&'

    req.url = req.url + sep_char + urlencode({ QUERY_PARAM_USER_DOT_NAME: self.user_name })

    return req

class Message(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBase.__init__(self, attrs)

        self.name      = attrs.get('name')
        self.rbKey     = attrs.get('rbKey')
        self.message   = attrs.get('message')
        self.objectId  = attrs.get('objectId')
        self.fieldName = attrs.get('fieldName')


class RESTResponse(RangerBase):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}

        RangerBase.__init__(self, attrs)

        self.httpStatusCode = attrs.get('httpStatusCode')
        self.statusCode     = attrs.get('statusCode')
        self.msgDesc        = attrs.get('msgDesc')
        self.messageList    = non_null(attrs.get('messageList'), [])

    def type_coerce_attrs(self):
        super(RangerPolicy, self).type_coerce_attrs()

        self.messageList = type_coerce_dict(self.messageList, Message)


class RangerClientHttp:
    def __init__(self, url, auth, query_params=None, headers=None):
        self.url          = url.rstrip('/') + '/' # ensure that self.url ends with a /
        self.query_params = query_params
        self.headers      = headers
        self.session      = Session()
        self.session.auth = auth


    def call_api(self, api, query_params=None, request_data=None):
        ret    = None
        params = { 'headers': { 'Accept': api.consumes, 'Content-type': api.produces } }

        if self.headers:
          params['headers'].update(self.headers)

        if self.query_params:
          if query_params:
              merged_query_params = {}

              merged_query_params.update(self.query_params)
              merged_query_params.update(query_params)

              query_params = merged_query_params
          else:
              query_params = self.query_params

        if query_params:
            params['params'] = query_params

        if request_data:
            params['data'] = json.dumps(request_data)

        path = urljoin(self.url, api.path.lstrip('/'))

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("------------------------------------------------------")
            LOG.debug("Call         : %s %s", api.method, path)
            LOG.debug("Content-type : %s", api.consumes)
            LOG.debug("Accept       : %s", api.produces)

        response = None

        if api.method == HttpMethod.GET:
            response = self.session.get(path, **params)
        elif api.method == HttpMethod.POST:
            response = self.session.post(path, **params)
        elif api.method == HttpMethod.PUT:
            response = self.session.put(path, **params)
        elif api.method == HttpMethod.DELETE:
            response = self.session.delete(path, **params)

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("HTTP Status: %s", response.status_code if response else "None")

        if response is None:
            ret = None
        elif response.status_code == api.expected_status:
            try:
                if response.status_code == HTTPStatus.NO_CONTENT or response.content is None:
                    ret = None
                else:
                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.debug("<== __call_api(%s, %s, %s), result=%s", vars(api), params, request_data, response)

                        LOG.debug(response.content)

                    if response.content:
                      try:
                        ret = response.json()
                      except Exception:
                        ret = response.content
            except Exception as e:
                print(e)

                LOG.exception("Exception occurred while parsing response with msg: %s", e)

                raise RangerServiceException(api, response)
        elif response.status_code == HTTPStatus.SERVICE_UNAVAILABLE:
            LOG.error("Ranger server at %s unavailable. HTTP Status: %s", self.url, HTTPStatus.SERVICE_UNAVAILABLE)

            ret = None
        elif response.status_code == HTTPStatus.NOT_FOUND:
            LOG.error("Not found. HTTP Status: %s", HTTPStatus.NOT_FOUND)

            ret = None
        elif response.status_code == HTTPStatus.NOT_MODIFIED:
            ret = None
        else:
            raise RangerServiceException(api, response)

        return ret


class RangerClientPrivate:
    def __init__(self, url, auth):
        self.client_http = RangerClientHttp(url, auth)

        logging.getLogger("requests").setLevel(logging.WARNING)

    # URLs
    URI_DELETE_USER  = "service/xusers/secure/users/{name}"
    URI_DELETE_GROUP = "service/xusers/secure/groups/{name}"
    URI_FORCE_DELETE_EXTERNAL_USERS = "service/xusers/delete/external/users"
    URI_FORCE_DELETE_EXTERNAL_GROUPS = "service/xusers/delete/external/groups"

    # APIs
    DELETE_USER  = API(URI_DELETE_USER, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_GROUP = API(URI_DELETE_GROUP, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    FORCE_DELETE_EXTERNAL_USERS = API(URI_FORCE_DELETE_EXTERNAL_USERS, HttpMethod.DELETE, HTTPStatus.OK)
    FORCE_DELETE_EXTERNAL_GROUPS = API(URI_FORCE_DELETE_EXTERNAL_GROUPS, HttpMethod.DELETE, HTTPStatus.OK)

    def delete_user(self, userName, execUser, isForceDelete='true'):
        self.client_http.call_api(RangerClientPrivate.DELETE_USER.format_path({ 'name': userName }), { 'execUser': execUser, 'forceDelete': isForceDelete })

    def delete_group(self, groupName, execUser, isForceDelete='true'):
        self.client_http.call_api(RangerClientPrivate.DELETE_GROUP.format_path({ 'name': groupName }), { 'execUser': execUser, 'forceDelete': isForceDelete })

    def force_delete_external_users(self, filter=None):
        """
        Proceed with <tt>caution</tt>.
        Force deletes external users from ranger db.
        Optionally, Query Params may be specified using the param 'filter'
        to delete specific external users.
        :param filter:
        :return:
        """
        return self.client_http.call_api(RangerClientPrivate.FORCE_DELETE_EXTERNAL_USERS, filter).decode('utf-8')

    def force_delete_external_groups(self, filter=None):
        """
        Proceed with <tt>caution</tt>.
        Force deletes external groups from ranger db.
        Optionally, Query Params may be specified using the param 'filter'
        to delete specific external groups.
        :param filter:
        :return:
        """
        return self.client_http.call_api(RangerClientPrivate.FORCE_DELETE_EXTERNAL_GROUPS, filter).decode('utf-8')

