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
package org.apache.ranger;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerSecurityZoneHeaderInfo;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceHeaderInfo;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerPurgeResult;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testng.annotations.BeforeMethod;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.anyString;

@ExtendWith(MockitoExtension.class)
public class TestRangerClient {
    private static final RangerClient.API GET_TEST_API  = new RangerClient.API("/relative/path/test", HttpMethod.GET, Response.Status.OK);


    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void apiGet_Success() throws Exception {
        try {
            RangerRESTClient restClient = mock(RangerRESTClient.class);
            ClientResponse   response   = mock(ClientResponse.class);
            RangerClient     client     = new RangerClient(restClient);
            RangerService    service    = new RangerService("testType", "testService", "MockedService", "testTag", new HashMap<>());

            when(restClient.get(anyString(), any())).thenReturn(response);
            when(response.getStatus()).thenReturn(GET_TEST_API.getExpectedStatus().getStatusCode());
            when(response.getEntity(String.class)).thenReturn(JsonUtilsV2.objToJson(service));

            RangerService ret = client.getService(service.getName());

            Assertions.assertNotNull(ret);
            Assertions.assertEquals(ret.getName(), service.getName());
        } catch(RangerServiceException excp){
            Assertions.fail("Not expected to fail! Found exception: " + excp);
        }
    }

    @Test
    public void apiGet_ServiceUnavailable() throws Exception {
        try {
            RangerRESTClient restClient = mock(RangerRESTClient.class);
            ClientResponse   response   = mock(ClientResponse.class);
            RangerClient     client     = new RangerClient(restClient);

            when(restClient.get(anyString(), any())).thenReturn(response);
            when(response.getStatus()).thenReturn(ClientResponse.Status.SERVICE_UNAVAILABLE.getStatusCode());

            RangerService ret = client.getService(1L);

            Assertions.fail("Expected to fail with SERVICE_UNAVAILABLE");
        } catch(RangerServiceException excp){
            Assertions.assertEquals(ClientResponse.Status.SERVICE_UNAVAILABLE,  excp.getStatus(), "Expected to fail with status SERVICE_UNAVAILABLE");
        }
    }

    @Test
    public void apiGet_FailWithUnexpectedStatusCode() throws Exception {
        try {
            RangerRESTClient restClient = mock(RangerRESTClient.class);
            ClientResponse   response   = mock(ClientResponse.class);
            RangerClient     client     = new RangerClient(restClient);

            when(restClient.get(anyString(), any())).thenReturn(response);
            when(response.getStatus()).thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());

            client.getService(1L);

            Assertions.fail("supposed to fail with RangerServiceException");
        } catch(RangerServiceException excp) {
            Assertions.assertTrue(excp.getMessage().contains("statusCode=" + ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode()));
            Assertions.assertTrue(excp.getMessage().contains("status=" + ClientResponse.Status.INTERNAL_SERVER_ERROR.getReasonPhrase()));
        }
    }

    @Test
    public void apiGet_FailWithNullResponse() throws Exception {
        try {
            RangerRESTClient restClient = mock(RangerRESTClient.class);
            RangerClient     client     = new RangerClient(restClient);

            when(restClient.get(anyString(), any())).thenReturn(null);

            client.getService(1L);

            Assertions.fail("supposed to fail with RangerServiceException");
        } catch(RangerServiceException excp) {
            Assertions.assertTrue(excp.getMessage().contains("statusCode=null"));
            Assertions.assertTrue(excp.getMessage().contains("status=null"));
        }
    }

    @Test
    public void api_UrlMissingFormat() {
        try {
            new RangerClient.API("%dtest%dpath%d", HttpMethod.GET, Response.Status.OK).applyUrlFormat(1,1);
            Assertions.fail("supposed to fail with RangerServiceException");
        } catch(RangerServiceException exp){
            Assertions.assertTrue(exp.getMessage().contains("MissingFormatArgumentException"));
        }
    }

    @Test
    public void api_UrlIllegalFormatConversion() {
        try {
            new RangerClient.API("testpath%d", HttpMethod.GET, Response.Status.OK).applyUrlFormat("1");
            Assertions.fail("supposed to fail with RangerServiceException");
        } catch(RangerServiceException exp){
            Assertions.assertTrue(exp.getMessage().contains("IllegalFormatConversionException"));
        }

        try {
            new RangerClient.API("testpath%f", HttpMethod.GET, Response.Status.OK).applyUrlFormat(1);
            Assertions.fail("supposed to fail with RangerServiceException");
        } catch(RangerServiceException exp){
            Assertions.assertTrue(exp.getMessage().contains("IllegalFormatConversionException"));
        }
    }

    @Test
    public void testGetSecurityZoneHeaders() throws Exception {
        RangerRESTClient    restClient = mock(RangerRESTClient.class);
        ClientResponse      response   = mock(ClientResponse.class);
        RangerClient        client     = new RangerClient(restClient);

        List<RangerSecurityZoneHeaderInfo> expected = new ArrayList<>();

        expected.add(new RangerSecurityZoneHeaderInfo(1L, "zone-1"));
        expected.add(new RangerSecurityZoneHeaderInfo(2L, "zone-2"));

        when(restClient.get(anyString(), any())).thenReturn(response);
        when(response.getStatus()).thenReturn(GET_TEST_API.getExpectedStatus().getStatusCode());
        when(response.getEntity(String.class)).thenReturn(JsonUtilsV2.listToJson(expected));

        List<RangerSecurityZoneHeaderInfo> actual = client.getSecurityZoneHeaders(Collections.emptyMap());

        Assertions.assertEquals(expected.size(), actual.size(), "incorrect count");

        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i).getId(), actual.get(i).getId(), "mismatched 'id' at index " + i);
            Assertions.assertEquals(expected.get(i).getName(), actual.get(i).getName(), "mismatched 'name' at index " + i);
        }
    }

    @Test
    public void testGetSecurityZoneServiceHeaders() throws Exception {
        RangerRESTClient    restClient = mock(RangerRESTClient.class);
        ClientResponse      response   = mock(ClientResponse.class);
        RangerClient        client     = new RangerClient(restClient);

        List<RangerServiceHeaderInfo> expected = new ArrayList<>();

        expected.add(new RangerServiceHeaderInfo(1L, "dev_hdfs", "HDFS: DEV", "hdfs"));
        expected.add(new RangerServiceHeaderInfo(2L, "dev_hive", "DEV HADOOP-SQL: DEV", "hive"));

        when(restClient.get(anyString(), any())).thenReturn(response);
        when(response.getStatus()).thenReturn(GET_TEST_API.getExpectedStatus().getStatusCode());
        when(response.getEntity(String.class)).thenReturn(JsonUtilsV2.listToJson(expected));

        List<RangerServiceHeaderInfo> actual = client.getSecurityZoneServiceHeaders(Collections.emptyMap());

        Assertions.assertEquals(expected.size(), actual.size(), "incorrect count");

        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i).getId(), actual.get(i).getId(), "mismatched 'id' at index " + i);
            Assertions.assertEquals(expected.get(i).getName(), actual.get(i).getName(), "mismatched 'name' at index " + i);
            Assertions.assertEquals(expected.get(i).getDisplayName(), actual.get(i).getDisplayName(), "mismatched 'displayName' at index " + i);
            Assertions.assertEquals(expected.get(i).getType(), actual.get(i).getType(), "mismatched 'type' at index " + i);
        }
    }

    @Test
    public void testGetSecurityZoneNamesForResource() throws RangerServiceException {
        RangerClient        client      = Mockito.mock(RangerClient.class);
        String              serviceName = "dev_hive";
        Map<String, String> resource    = new HashMap<String, String>() {{
                                                put("database", "testdb");
                                                put("table", "testtbl1");
                                            }};

        when(client.getSecurityZoneNamesForResource(serviceName, resource)).thenReturn(Collections.emptySet());

        Set<String> zoneNames = client.getSecurityZoneNamesForResource(serviceName, resource);

        Assertions.assertEquals(Collections.emptySet(), zoneNames);
    }

    @Test
    public void testFindSecurityZones() throws RangerServiceException {
        RangerClient        client = Mockito.mock(RangerClient.class);
        Map<String, String> filter = Collections.emptyMap();

        when(client.findSecurityZones(filter)).thenReturn(Collections.emptyList());

        List<RangerSecurityZone> securityZones = client.findSecurityZones(filter);

        Assertions.assertEquals(Collections.emptyList(), securityZones);
    }

    @Test
    public void testPurgeRecords() throws RangerServiceException {
        RangerClient client        = Mockito.mock(RangerClient.class);
        String       recordType    = "login_records";
        int          retentionDays = 180;

        when(client.purgeRecords(recordType, retentionDays)).thenReturn(Collections.emptyList());

        List<RangerPurgeResult> purgeResults = client.purgeRecords(recordType, retentionDays);

        Assertions.assertEquals(Collections.emptyList(), purgeResults);
    }
}