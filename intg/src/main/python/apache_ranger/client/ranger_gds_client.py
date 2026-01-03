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
from apache_ranger.client.ranger_client       import RangerClient
from apache_ranger.model.ranger_base          import RangerBase, PList
from apache_ranger.model.ranger_gds           import *
from apache_ranger.model.ranger_policy        import RangerPolicy
from apache_ranger.model.ranger_role          import RangerRole
from apache_ranger.model.ranger_security_zone import RangerSecurityZone
from apache_ranger.model.ranger_service       import RangerService
from apache_ranger.model.ranger_service_def   import RangerServiceDef
from apache_ranger.model.ranger_service_tags  import RangerServiceTags
from apache_ranger.utils                      import *
from requests                                 import Session
from requests                                 import Response
from requests.auth                            import AuthBase
from urllib.parse                             import urlencode
from urllib.parse                             import urljoin

LOG = logging.getLogger(__name__)


class RangerGdsClient:
    def __init__(self, ranger_client):
        self.client_http = ranger_client.client_http

    # Data Sharing APIs
    def create_dataset(self, dataset):
      resp = self.client_http.call_api(RangerGdsClient.CREATE_DATASET, request_data=dataset)

      return type_coerce(resp, RangerDataset)

    def update_dataset(self, dataset_id, dataset):
      resp = self.client_http.call_api(RangerGdsClient.UPDATE_DATASET_BY_ID.format_path({ 'id': dataset_id }), request_data=dataset)

      return type_coerce(resp, RangerDataset)

    def delete_dataset(self, dataset_id, is_force_delete=False):
      resp = self.client_http.call_api(RangerGdsClient.DELETE_DATASET_BY_ID.format_path({ 'id': dataset_id }), { 'forceDelete': is_force_delete })

    def get_dataset(self, dataset_id):
      resp = self.client_http.call_api(RangerGdsClient.GET_DATASET_BY_ID.format_path({ 'id': dataset_id }))

      return type_coerce(resp, RangerDataset)

    def find_datasets(self, filter=None):
        resp = self.client_http.call_api(RangerGdsClient.FIND_DATASETS, filter)

        return PList(resp).type_coerce_list(RangerDataset)

    def get_dataset_names(self, filter=None):
        resp = self.client_http.call_api(RangerGdsClient.GET_DATASET_NAMES, filter)

        return PList(resp).type_coerce_list(str)

    def get_dataset_summary(self, filter=None):
        resp = self.client_http.call_api(RangerGdsClient.GET_DATASET_SUMMARY, filter)

        return PList(resp).type_coerce_list(DatasetSummary)

    def add_dataset_policy(self, datasetId, policy):
        resp = self.client_http.call_api(RangerGdsClient.ADD_DATASET_POLICY.format_path({ 'id': datasetId }), request_data=policy)

        return type_coerce(resp, RangerPolicy)

    def update_dataset_policy(self, datasetId, policy):
        resp = self.client_http.call_api(RangerGdsClient.UPDATE_DATASET_POLICY.format_path({ 'id': datasetId, 'policyId': policy.id }), request_data=policy)

        return type_coerce(resp, RangerPolicy)

    def delete_dataset_policy(self, datasetId, policyId):
        self.client_http.call_api(RangerGdsClient.DELETE_DATASET_POLICY.format_path({ 'id': datasetId, 'policyId': policyId }))

    def get_dataset_policy(self, datasetId, policyId):
        resp = self.client_http.call_api(RangerGdsClient.GET_DATASET_POLICY.format_path({ 'id': datasetId, 'policyId': policyId }))

        return type_coerce(resp, RangerPolicy)

    def get_dataset_policies(self, datasetId):
        resp = self.client_http.call_api(RangerGdsClient.GET_DATASET_POLICIES.format_path({ 'id': datasetId }))

        return type_coerce_list(resp, RangerPolicy)


    def create_project(self, project):
      resp = self.client_http.call_api(RangerGdsClient.CREATE_PROJECT, request_data=project)

      return type_coerce(resp, RangerProject)

    def update_project(self, project_id, project):
      resp = self.client_http.call_api(RangerGdsClient.UPDATE_PROJECT_BY_ID.format_path({ 'id': project_id }), request_data=project)

      return type_coerce(resp, RangerProject)

    def delete_project(self, project_id, is_force_delete=False):
      resp = self.client_http.call_api(RangerGdsClient.DELETE_PROJECT_BY_ID.format_path({ 'id': project_id }), { 'forceDelete': is_force_delete })

    def get_project(self, project_id):
      resp = self.client_http.call_api(RangerGdsClient.GET_PROJECT_BY_ID.format_path({ 'id': project_id }))

      return type_coerce(resp, RangerProject)

    def find_projects(self, filter=None):
        resp = self.client_http.call_api(RangerGdsClient.FIND_PROJECTS, filter)

        return PList(resp).type_coerce_list(RangerProject)

    def get_project_names(self, filter=None):
        resp = self.client_http.call_api(RangerGdsClient.GET_PROJECT_NAMES, filter)

        return PList(resp).type_coerce_list(str)

    def add_project_policy(self, projectId, policy):
        resp = self.client_http.call_api(RangerGdsClient.ADD_PROJECT_POLICY.format_path({ 'id': projectId }), request_data=policy)

        return type_coerce(resp, RangerPolicy)

    def update_project_policy(self, projectId, policy):
        resp = self.client_http.call_api(RangerGdsClient.UPDATE_PROJECT_POLICY.format_path({ 'id': projectId, 'policyId': policy.id }), request_data=policy)

        return type_coerce(resp, RangerPolicy)

    def delete_project_policy(self, projectId, policyId):
        self.client_http.call_api(RangerGdsClient.DELETE_PROJECT_POLICY.format_path({ 'id': projectId, 'policyId': policyId }))

    def get_project_policy(self, projectId, policyId):
        resp = self.client_http.call_api(RangerGdsClient.GET_PROJECT_POLICY.format_path({ 'id': projectId, 'policyId': policyId }))

        return type_coerce(resp, RangerPolicy)

    def get_project_policies(self, projectId):
        resp = self.client_http.call_api(RangerGdsClient.GET_PROJECT_POLICIES.format_path({ 'id': projectId }))

        return type_coerce_list(resp, RangerPolicy)


    def create_data_share(self, data_share):
      resp = self.client_http.call_api(RangerGdsClient.CREATE_DATA_SHARE, request_data=data_share)

      return type_coerce(resp, RangerDataShare)

    def update_data_share(self, dsh_id, data_share):
      resp = self.client_http.call_api(RangerGdsClient.UPDATE_DATA_SHARE_BY_ID.format_path({ 'id': dsh_id }), request_data=data_share)

      return type_coerce(resp, RangerDataShare)

    def delete_data_share(self, dsh_id, is_force_delete=False):
      resp = self.client_http.call_api(RangerGdsClient.DELETE_DATA_SHARE_BY_ID.format_path({ 'id': dsh_id }), { 'forceDelete': is_force_delete })

    def get_data_share(self, dsh_id):
      resp = self.client_http.call_api(RangerGdsClient.GET_DATA_SHARE_BY_ID.format_path({ 'id': dsh_id }))

      return type_coerce(resp, RangerDataShare)

    def find_data_shares(self, filter=None):
        resp = self.client_http.call_api(RangerGdsClient.FIND_DATA_SHARES, filter)

        return PList(resp).type_coerce_list(RangerDataShare)

    def add_shared_resource(self, resource):
      resp = self.client_http.call_api(RangerGdsClient.ADD_SHARED_RESOURCE, request_data=resource)

      return type_coerce(resp, RangerSharedResource)

    def update_shared_resource(self, resource_id, resource):
      resp = self.client_http.call_api(RangerGdsClient.UPDATE_SHARED_RESOURCE_BY_ID.format_path({ 'id': resource_id }), request_data=resource)

      return type_coerce(resp, RangerSharedResource)

    def remove_shared_resource(self, resource_id):
      resp = self.client_http.call_api(RangerGdsClient.REMOVE_SHARED_RESOURCE_BY_ID.format_path({ 'id': resource_id }))

    def get_shared_resource(self, resource_id):
      resp = self.client_http.call_api(RangerGdsClient.GET_SHARED_RESOURCE_BY_ID.format_path({ 'id': resource_id }))

      return type_coerce(resp, RangerSharedResource)

    def find_shared_resources(self, filter=None):
        resp = self.client_http.call_api(RangerGdsClient.FIND_SHARED_RESOURCES, filter)

        return PList(resp).type_coerce_list(RangerSharedResource)


    def add_data_share_in_dataset(self, dshid):
      resp = self.client_http.call_api(RangerGdsClient.ADD_DATA_SHARE_IN_DATASET, request_data=dshid)

      return type_coerce(resp, RangerDataShareInDataset)

    def update_data_share_in_dataset(self, dshid_id, dshid):
      resp = self.client_http.call_api(RangerGdsClient.UPDATE_DATA_SHARE_IN_DATASET_BY_ID.format_path({ 'id': dshid_id }), request_data=dshid)

      return type_coerce(resp, RangerDataShareInDataset)

    def remove_data_share_in_dataset(self, dshid_id):
      resp = self.client_http.call_api(RangerGdsClient.REMOVE_DATA_SHARE_IN_DATASET_BY_ID.format_path({ 'id': dshid_id }))

    def get_data_share_in_dataset(self, dshid_id):
      resp = self.client_http.call_api(RangerGdsClient.GET_DATA_SHARE_IN_DATASET_BY_ID.format_path({ 'id': dshid_id }))

      return type_coerce(resp, RangerSharedResource)

    def find_data_share_in_datasets(self, filter=None):
      resp = self.client_http.call_api(RangerGdsClient.FIND_DATA_SHARE_IN_DATASETS, filter)

      return PList(resp).type_coerce_list(RangerDataShareInDataset)


    def add_dataset_in_project(self, dip):
      resp = self.client_http.call_api(RangerGdsClient.ADD_DATASET_IN_PROJECT, request_data=dip)

      return type_coerce(resp, RangerDatasetInProject)

    def update_dataset_in_project(self, dip_id, dip):
      resp = self.client_http.call_api(RangerGdsClient.UPDATE_DATASET_IN_PROJECT_BY_ID.format_path({ 'id': dip_id }), request_data=dip)

      return type_coerce(resp, RangerDatasetInProject)

    def remove_dataset_in_project(self, dip_id):
      resp = self.client_http.call_api(RangerGdsClient.REMOVE_DATASET_IN_PROJECT_BY_ID.format_path({ 'id': dip_id }))

    def get_dataset_in_project(self, dip_id):
      resp = self.client_http.call_api(RangerGdsClient.GET_DATASET_IN_PROJECT_BY_ID.format_path({ 'id': dip_id }))

      return type_coerce(resp, RangerDatasetInProject)

    def find_dataset_in_projects(self, filter=None):
      resp = self.client_http.call_api(RangerGdsClient.FIND_DATASET_IN_PROJECTS, filter)

      return PList(resp).type_coerce_list(RangerDatasetInProject)


    # URIs
    URI_GDS                       = "service/gds"
    URI_DATASET                   = URI_GDS + "/dataset"
    URI_DATASET_BY_ID             = URI_DATASET + "/{id}"
    URI_DATASET_NAMES             = URI_DATASET + "/names"
    URI_DATASET_SUMMARY           = URI_DATASET + "/summary"
    URI_DATASET_POLICY            = URI_DATASET_BY_ID + "/policy"
    URI_DATASET_POLICY_ID         = URI_DATASET_POLICY + "/{policyId}"
    URI_PROJECT                   = URI_GDS + "/project"
    URI_PROJECT_BY_ID             = URI_PROJECT + "/{id}"
    URI_PROJECT_NAMES             = URI_PROJECT + "/names"
    URI_PROJECT_POLICY            = URI_PROJECT_BY_ID + "/policy"
    URI_PROJECT_POLICY_ID         = URI_PROJECT_POLICY + "/{policyId}"
    URI_DATA_SHARE                = URI_GDS + "/datashare"
    URI_DATA_SHARE_BY_ID          = URI_DATA_SHARE + "/{id}"
    URI_SHARED_RESOURCE           = URI_GDS + "/resource"
    URI_SHARED_RESOURCE_BY_ID     = URI_SHARED_RESOURCE + "/{id}"

    URI_DATA_SHARE_DATASET        = URI_DATA_SHARE + "/dataset"
    URI_DATA_SHARE_DATASET_BY_ID  = URI_DATA_SHARE_DATASET + "/{id}"
    URI_DATA_SHARE_PROJECT        = URI_DATA_SHARE + "/project"
    URI_DATA_SHARE_PROJECT_BY_ID  = URI_DATA_SHARE_PROJECT + "/{id}"
    URI_DATASET_PROJECT           = URI_DATASET + "/project"
    URI_DATASET_PROJECT_BY_ID     = URI_DATASET_PROJECT + "/{id}"

    # APIs

    CREATE_DATASET            = API(URI_DATASET, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_DATASET_BY_ID      = API(URI_DATASET_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_DATASET_BY_ID      = API(URI_DATASET_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_DATASET_BY_ID         = API(URI_DATASET_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    FIND_DATASETS             = API(URI_DATASET, HttpMethod.GET, HTTPStatus.OK)
    GET_DATASET_NAMES         = API(URI_DATASET_NAMES, HttpMethod.GET, HTTPStatus.OK)
    GET_DATASET_SUMMARY       = API(URI_DATASET_SUMMARY, HttpMethod.GET, HTTPStatus.OK)
    ADD_DATASET_POLICY        = API(URI_DATASET_POLICY, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_DATASET_POLICY     = API(URI_DATASET_POLICY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_DATASET_POLICY     = API(URI_DATASET_POLICY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_DATASET_POLICY        = API(URI_DATASET_POLICY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_DATASET_POLICIES      = API(URI_DATASET_POLICY, HttpMethod.GET, HTTPStatus.OK)

    CREATE_PROJECT            = API(URI_PROJECT, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_PROJECT_BY_ID      = API(URI_PROJECT_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_PROJECT_BY_ID      = API(URI_PROJECT_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_PROJECT_BY_ID         = API(URI_PROJECT_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    FIND_PROJECTS             = API(URI_PROJECT, HttpMethod.GET, HTTPStatus.OK)
    GET_PROJECT_NAMES         = API(URI_PROJECT_NAMES, HttpMethod.GET, HTTPStatus.OK)
    ADD_PROJECT_POLICY        = API(URI_PROJECT_POLICY, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_PROJECT_POLICY     = API(URI_PROJECT_POLICY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_PROJECT_POLICY     = API(URI_PROJECT_POLICY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_PROJECT_POLICY        = API(URI_PROJECT_POLICY_ID, HttpMethod.GET, HTTPStatus.OK)
    GET_PROJECT_POLICIES      = API(URI_PROJECT_POLICY, HttpMethod.GET, HTTPStatus.OK)

    CREATE_DATA_SHARE         = API(URI_DATA_SHARE, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_DATA_SHARE_BY_ID   = API(URI_DATA_SHARE_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_DATA_SHARE_BY_ID   = API(URI_DATA_SHARE_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_DATA_SHARE_BY_ID      = API(URI_DATA_SHARE_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    FIND_DATA_SHARES          = API(URI_DATA_SHARE, HttpMethod.GET, HTTPStatus.OK)

    ADD_SHARED_RESOURCE          = API(URI_SHARED_RESOURCE, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_SHARED_RESOURCE_BY_ID = API(URI_SHARED_RESOURCE_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    REMOVE_SHARED_RESOURCE_BY_ID = API(URI_SHARED_RESOURCE_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_SHARED_RESOURCE_BY_ID    = API(URI_SHARED_RESOURCE_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    FIND_SHARED_RESOURCES        = API(URI_SHARED_RESOURCE, HttpMethod.GET, HTTPStatus.OK)

    ADD_DATA_SHARE_IN_DATASET          = API(URI_DATA_SHARE_DATASET, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_DATA_SHARE_IN_DATASET_BY_ID = API(URI_DATA_SHARE_DATASET_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    REMOVE_DATA_SHARE_IN_DATASET_BY_ID = API(URI_DATA_SHARE_DATASET_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_DATA_SHARE_IN_DATASET_BY_ID    = API(URI_DATA_SHARE_DATASET_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    FIND_DATA_SHARE_IN_DATASETS        = API(URI_DATA_SHARE_DATASET, HttpMethod.GET, HTTPStatus.OK)

    ADD_DATASET_IN_PROJECT             = API(URI_DATASET_PROJECT, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_DATASET_IN_PROJECT_BY_ID    = API(URI_DATASET_PROJECT_BY_ID, HttpMethod.PUT, HTTPStatus.OK)
    REMOVE_DATASET_IN_PROJECT_BY_ID    = API(URI_DATASET_PROJECT_BY_ID, HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_DATASET_IN_PROJECT_BY_ID       = API(URI_DATASET_PROJECT_BY_ID, HttpMethod.GET, HTTPStatus.OK)
    FIND_DATASET_IN_PROJECTS           = API(URI_DATASET_PROJECT, HttpMethod.GET, HTTPStatus.OK)
