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

import unittest

from unittest.mock import Mock, patch

from apache_ranger.exceptions           import RangerServiceException
from apache_ranger.model.ranger_service import RangerService

try:
    import apache_ranger.client as ranger_client_package
    import apache_ranger.model as ranger_model_package

    from apache_ranger.client.ranger_client     import API, HttpMethod, HTTPStatus, RangerClient
    from apache_ranger.client.ranger_gds_client import RangerGdsClient
    from apache_ranger.client.ranger_pdp_client import RangerPDPClient
    from apache_ranger.model.ranger_authz       import (
        RangerAccessContext, RangerAccessInfo, RangerAuthzRequest, RangerAuthzResult,
        RangerPermissionResult, RangerResourceInfo, RangerResultInfo, RangerUserInfo,
    )
    from apache_ranger.model.ranger_gds         import (
        DatasetSummary, GdsPermission, GdsShareStatus, RangerDataset,
        RangerGdsMaskInfo, RangerGdsObjectACL,
    )
    from apache_ranger.model.ranger_principal import PrincipalType
except ModuleNotFoundError:  # requests not installed
    exit()  # skipping unit tests


class MockResponse:
    def __init__(self, status_code, response=None, content=None):
        self.status_code = status_code
        self.response    = response
        self.content     = content

    def json(self):
        return self.response

    def text(self):
        return str(self.content)


class TestRangerClient(unittest.TestCase):
    URL  = "url"
    AUTH = ("user", "password")

    @patch('apache_ranger.client.ranger_client.Session')
    def test_get_service_unavailable(self, mock_session):
        mock_session.return_value.get.return_value = MockResponse(HTTPStatus.SERVICE_UNAVAILABLE)
        result                                     = RangerClient(self.URL, self.AUTH).find_services()

        self.assertIsNone(result)

    @patch('apache_ranger.client.ranger_client.Session')
    def test_get_success(self, mock_session):
        response                                   = [RangerService()]
        mock_session.return_value.get.return_value = MockResponse(
            HTTPStatus.OK, response=response, content='Success')
        result                                     = RangerClient(self.URL, self.AUTH).find_services()

        self.assertEqual(response, result)

    @patch('apache_ranger.client.ranger_client.Session')
    @patch('apache_ranger.client.ranger_client.Response')
    def test_get_unexpected_status_code(self, mock_response, mock_session):
        content = 'Internal Server Error'
        mock_response.text        = content
        mock_response.content     = content
        mock_response.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
        mock_session.return_value.get.return_value = mock_response

        with self.assertRaises(RangerServiceException) as context:
            RangerClient(self.URL, self.AUTH).find_services()

        self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, context.exception.statusCode)

    @patch('apache_ranger.client.ranger_client.RangerClient.FIND_SERVICES')
    def test_unexpected_http_method(self, mock_api):
        mock_api.method = "PATCH"
        mock_api.path   = RangerClient.URI_SERVICE

        result = RangerClient(self.URL, self.AUTH).find_services()

        self.assertIsNone(result)

    def test_url_missing_format(self):
        with self.assertRaises(KeyError):
            API("{arg1}test{arg2}path{arg3}", HttpMethod.GET, HTTPStatus.OK).format_path(
                {'arg1': 1, 'arg2': 2})

    def test_url_invalid_format(self):
        with self.assertRaises(TypeError):
            API("{}test{}path{}", HttpMethod.GET, HTTPStatus.OK).format_path({'1', '2'})


class TestGDSClient(unittest.TestCase):
    def test_package_export(self):
        self.assertIs(ranger_client_package.RangerGdsClient, RangerGdsClient)
        self.assertIn("RangerGdsClient", ranger_client_package.__all__)

    def test_dataset_type_coercion(self):
        dataset = RangerDataset({
            "name": "sales",
            "acl": {
                "public": {
                    "users": {
                        "alice": "VIEW",
                    },
                    "groups": {
                        "analytics": "POLICY_ADMIN",
                    },
                },
            },
        })

        dataset.type_coerce_attrs()

        acl = dataset.acl["public"]
        self.assertIsInstance(acl, RangerGdsObjectACL)
        self.assertEqual(GdsPermission.VIEW, acl.users["alice"])
        self.assertEqual(GdsPermission.POLICY_ADMIN, acl.groups["analytics"])

    def test_dataset_summary_type_coercion(self):
        summary = DatasetSummary({
            "name":                "sales",
            "permissionForCaller": "VIEW",
            "principalsCount": {
                "USER":  2,
                "GROUP": 1,
            },
            "dataShares": [{
                "name":        "sales-share",
                "shareStatus": "ACTIVE",
            }],
        })

        summary.type_coerce_attrs()

        self.assertEqual(GdsPermission.VIEW, summary.permissionForCaller)
        self.assertEqual(2, summary.principalsCount[PrincipalType.USER])
        self.assertEqual(GdsShareStatus.ACTIVE, summary.dataShares[0].shareStatus)

    def test_mask_info_type_coercion(self):
        mask_info = RangerGdsMaskInfo({
            "values":   ["email"],
            "maskInfo": {
                "dataMaskType": "MASK_HASH",
                "valueExpr":    "hash(email)",
            },
        })

        mask_info.type_coerce_attrs()

        self.assertEqual(["email"], mask_info.values)
        self.assertEqual("MASK_HASH", mask_info.maskInfo.dataMaskType)

    def test_find_datasets_calls_expected_api(self):
        ranger_client                               = Mock()
        ranger_client.client_http.call_api.return_value = {
            "list": [{
                "id":   1,
                "name": "sales",
            }],
        }

        result                                      = RangerGdsClient(ranger_client).find_datasets({"name": "sales"})

        ranger_client.client_http.call_api.assert_called_once_with(
            RangerGdsClient.FIND_DATASETS,
            {"name": "sales"},
        )
        self.assertEqual(1, len(result.list))
        self.assertIsInstance(result.list[0], RangerDataset)


class TestPDPClient(unittest.TestCase):
    PDP_URL       = "http://localhost:6500"
    AUTHZ_REQUEST = {
        "requestId": "req-1",
        "user":      {"name": "alice", "groups": ["analytics"]},
        "access":    {"resource": {"name": "table:default/sales", "subResources": ["column:id"]}, "permissions": ["select"]},
        "context":   {"serviceType": "hive", "serviceName": "dev_hive"},
    }
    AUTHZ_RESULT  = {
        "requestId":   "req-1",
        "decision":    RangerAuthzResult.DECISION_ALLOW,
        "permissions": {"select": {
            "access":       {"decision": RangerAuthzResult.DECISION_ALLOW, "policy": {"id": 7, "version": 3}},
            "subResources": {"column:id": {"access": {"decision": RangerAuthzResult.DECISION_DENY}}},
        }},
    }

    def test_package_export(self):
        self.assertIs(ranger_client_package.RangerPDPClient, RangerPDPClient)
        self.assertIn("RangerPDPClient", ranger_client_package.__all__)

    def test_authz_model_exports(self):
        self.assertIs(ranger_model_package.RangerAuthzRequest, RangerAuthzRequest)
        self.assertIs(ranger_model_package.RangerAuthzResult, RangerAuthzResult)
        self.assertIn("RangerAuthzRequest", ranger_model_package.__all__)
        self.assertIn("RangerResourcePermissions", ranger_model_package.__all__)

    def test_authz_request_type_coercion(self):
        request = RangerAuthzRequest(self.AUTHZ_REQUEST)
        request.type_coerce_attrs()

        self.assertIsInstance(request.user, RangerUserInfo)
        self.assertIsInstance(request.access, RangerAccessInfo)
        self.assertIsInstance(request.access.resource, RangerResourceInfo)
        self.assertIsInstance(request.context, RangerAccessContext)
        self.assertEqual("alice", request.user.name)
        self.assertEqual(["select"], request.access.permissions)

    def test_authz_result_type_coercion(self):
        result = RangerAuthzResult(self.AUTHZ_RESULT)
        result.type_coerce_attrs()

        permission = result.permissions["select"]
        self.assertIsInstance(permission, RangerPermissionResult)
        self.assertEqual(7, permission.access.policy.id)
        self.assertIsInstance(permission.subResources["column:id"], RangerResultInfo)

    def test_authorize_calls_expected_api(self):
        client                      = RangerPDPClient(self.PDP_URL, auth=None)
        client.client_http.call_api = Mock(return_value={
            "requestId": "req-1",
            "decision":  RangerAuthzResult.DECISION_ALLOW,
        })

        result                      = client.authorize(self.AUTHZ_REQUEST)

        client.client_http.call_api.assert_called_once()
        api                         = client.client_http.call_api.call_args.args[0]

        self.assertEqual(RangerPDPClient.URI_AUTHORIZE, api.path)
        self.assertEqual(HttpMethod.POST, api.method)
        self.assertIsInstance(
            client.client_http.call_api.call_args.kwargs["request_data"],
            RangerAuthzRequest,
        )
        self.assertIsInstance(result, RangerAuthzResult)


if __name__ == '__main__':
    unittest.main()
