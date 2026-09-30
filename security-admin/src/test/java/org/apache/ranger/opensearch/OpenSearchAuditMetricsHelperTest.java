/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.opensearch;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.ranger.audit.metrics.AuditMetricsHelper;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.util.SearchFilter;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.WebApplicationException;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenSearchAuditMetricsHelperTest {
    @Test
    void getLatestAuditMetrics_returnsCount() throws Exception {
        OpenSearchAuditMetricsHelper helper = createHelperWithServiceId("dev_hdfs", 1L);
        stubOpenSearchResponse(helper, "{\"hits\":{\"total\":{\"value\":42}}}", 200);

        RangerAuditMetrics metrics = helper.getLatestAuditMetrics("hdfs", "dev_hdfs", null);

        assertEquals("dev_hdfs", metrics.getServiceName());
        assertEquals("hdfs", metrics.getServiceType());
        assertEquals(42L, metrics.getNumberOfAudits());
    }

    @Test
    void getLatestAuditMetrics_whenClientNull_throws() {
        OpenSearchAuditMetricsHelper helper = new OpenSearchAuditMetricsHelper();
        helper.openSearchMgr = mock(OpenSearchMgr.class);
        helper.restErrorUtil = new RESTErrorUtil();
        helper.auditMetricsHelper = new AuditMetricsHelper();

        when(helper.openSearchMgr.getClient()).thenReturn(null);

        assertThrows(WebApplicationException.class, () -> helper.getLatestAuditMetrics("hdfs", "dev_hdfs", null));
    }

    @Test
    void getLatestAuditMetrics_whenHttpError_throws() throws Exception {
        OpenSearchAuditMetricsHelper helper = createHelper();
        stubOpenSearchErrorResponse(helper, 400);

        assertThrows(WebApplicationException.class, () -> helper.getLatestAuditMetrics("hdfs", "dev_hdfs", null));
    }

    @Test
    void getLatestAuditMetricsList_includesAgentCliIpAndCluster() throws Exception {
        OpenSearchAuditMetricsHelper helper = createHelperWithDao("dev_hdfs", 1L, "hdfs", 10L);
        stubOpenSearchResponse(helper, buildNestedMetricsResponse("dev_hdfs", "hdfs", "172.18.0.16", "ABC cluster", 7L), 200);

        List<RangerAuditMetrics> metrics = helper.getLatestAuditMetricsList(new SearchFilter(), null);

        assertEquals(1, metrics.size());
        RangerAuditMetrics metric = metrics.get(0);
        assertEquals("dev_hdfs", metric.getServiceName());
        assertEquals("hdfs", metric.getAppId());
        assertEquals("172.18.0.16", metric.getClientIP());
        assertEquals("ABC cluster", metric.getClusterName());
        assertEquals(7L, metric.getNumberOfAudits());
        assertEquals(1L, metric.getId());
    }

    @Test
    void getLatestAuditMetricsList_missingAgent_omitsAppId() throws Exception {
        OpenSearchAuditMetricsHelper helper = createHelperWithDao("dev_hdfs", 1L, "hdfs", 10L);
        stubOpenSearchResponse(helper, buildNestedMetricsResponse("dev_hdfs", "", "172.18.0.16", "ABC cluster", 5L), 200);

        List<RangerAuditMetrics> metrics = helper.getLatestAuditMetricsList(new SearchFilter(), null);

        assertEquals(1, metrics.size());
        assertNull(metrics.get(0).getAppId());
        assertEquals("172.18.0.16", metrics.get(0).getClientIP());
        assertEquals("ABC cluster", metrics.get(0).getClusterName());
        assertEquals(5L, metrics.get(0).getNumberOfAudits());
    }

    @Test
    void getAuditMetricsByDays_returnsDailyBuckets() throws Exception {
        OpenSearchAuditMetricsHelper helper = createHelper();
        stubOpenSearchResponse(helper,
                "{\"aggregations\":{\"per_day\":{\"buckets\":[{\"key_as_string\":\"2026-08-11T00:00:00.000Z\",\"doc_count\":25}]}}}",
                200);

        SearchFilter filter = new SearchFilter();
        filter.setParam(SearchFilter.SERVICE_NAME, "dev_hdfs");
        filter.setParam(SearchFilter.SERVICE_TYPE, "hdfs");

        List<RangerAuditMetricsByDays> metrics = helper.getAuditMetricsByDays(7, filter, "UTC");

        assertEquals(1, metrics.size());
        assertEquals(25L, metrics.get(0).getNumberOfAudits());
        assertEquals("dev_hdfs", metrics.get(0).getServiceName());
        assertEquals("hdfs", metrics.get(0).getServiceType());
    }

    @Test
    void getAuditMetricsByHours_returnsHourlyBuckets() throws Exception {
        OpenSearchAuditMetricsHelper helper = createHelper();
        stubOpenSearchResponse(helper,
                "{\"aggregations\":{\"per_hour\":{\"buckets\":[{\"key_as_string\":\"2026-08-11T12:00:00.000Z\",\"doc_count\":15}]}}}",
                200);

        List<RangerAuditMetricsByHours> metrics = helper.getAuditMetricsByHours(new SearchFilter(), "UTC");

        assertEquals(1, metrics.size());
        assertEquals(12, metrics.get(0).getHours());
        assertEquals(15L, metrics.get(0).getNumberOfAudits());
    }

    private OpenSearchAuditMetricsHelper createHelper() {
        OpenSearchAuditMetricsHelper helper = new OpenSearchAuditMetricsHelper();
        helper.openSearchMgr = mock(OpenSearchMgr.class);
        helper.restErrorUtil = new RESTErrorUtil();
        helper.auditMetricsHelper = new AuditMetricsHelper();

        return helper;
    }

    private OpenSearchAuditMetricsHelper createHelperWithServiceId(String serviceName, Long serviceId) throws Exception {
        RangerDaoManager daoManager = mock(RangerDaoManager.class);
        XXServiceDao svcDao = mock(XXServiceDao.class);
        XXService xxService = mock(XXService.class);

        when(daoManager.getXXService()).thenReturn(svcDao);
        when(svcDao.findByName(serviceName)).thenReturn(xxService);
        when(xxService.getId()).thenReturn(serviceId);

        return injectDaoManager(createHelper(), daoManager);
    }

    private OpenSearchAuditMetricsHelper createHelperWithDao(String serviceName, Long serviceId, String serviceType, Long serviceDefId) throws Exception {
        RangerDaoManager daoManager = mock(RangerDaoManager.class);
        XXServiceDao svcDao = mock(XXServiceDao.class);
        XXServiceDefDao sdDao = mock(XXServiceDefDao.class);
        XXService xxService = mock(XXService.class);
        XXServiceDef sd = mock(XXServiceDef.class);

        when(daoManager.getXXService()).thenReturn(svcDao);
        when(daoManager.getXXServiceDef()).thenReturn(sdDao);
        when(svcDao.findByName(serviceName)).thenReturn(xxService);
        when(xxService.getId()).thenReturn(serviceId);
        when(xxService.getType()).thenReturn(serviceDefId);
        when(sdDao.getById(serviceDefId)).thenReturn(sd);
        when(sd.getName()).thenReturn(serviceType);

        return injectDaoManager(createHelper(), daoManager);
    }

    private OpenSearchAuditMetricsHelper injectDaoManager(OpenSearchAuditMetricsHelper helper, RangerDaoManager daoManager) throws Exception {
        java.lang.reflect.Field daoField = AuditMetricsHelper.class.getDeclaredField("daoManager");

        daoField.setAccessible(true);
        daoField.set(helper.auditMetricsHelper, daoManager);

        return helper;
    }

    private void stubOpenSearchResponse(OpenSearchAuditMetricsHelper helper, String json, int statusCode) throws Exception {
        RestClient client = mock(RestClient.class);
        Response response = mock(Response.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpEntity entity = mock(HttpEntity.class);

        when(helper.openSearchMgr.getClient()).thenReturn(client);
        when(helper.openSearchMgr.getIndex()).thenReturn("ranger_audits");
        when(client.performRequest(any(Request.class))).thenReturn(response);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    }

    private void stubOpenSearchErrorResponse(OpenSearchAuditMetricsHelper helper, int statusCode) throws Exception {
        RestClient client = mock(RestClient.class);
        Response response = mock(Response.class);
        StatusLine statusLine = mock(StatusLine.class);

        when(helper.openSearchMgr.getClient()).thenReturn(client);
        when(helper.openSearchMgr.getIndex()).thenReturn("ranger_audits");
        when(client.performRequest(any(Request.class))).thenReturn(response);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
    }

    private String buildNestedMetricsResponse(String serviceName, String appId, String clientIP, String clusterName, long count) {
        return "{"
                + "\"aggregations\":{"
                + "\"per_repo\":{\"buckets\":[{\"key\":\"" + serviceName + "\",\"doc_count\":" + count + ","
                + "\"per_agent\":{\"buckets\":[{\"key\":\"" + appId + "\",\"doc_count\":" + count + ","
                + "\"per_cliip\":{\"buckets\":[{\"key\":\"" + clientIP + "\",\"doc_count\":" + count + ","
                + "\"per_cluster\":{\"buckets\":[{\"key\":\"" + clusterName + "\",\"doc_count\":" + count + "}]"
                + "}}]}}]}}]}"
                + "}}";
    }
}
