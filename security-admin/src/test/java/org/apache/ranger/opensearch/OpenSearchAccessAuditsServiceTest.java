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

import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.opensearch.OpenSearchUtil.OpenSearchSearchResult;
import org.apache.ranger.view.VXAccessAuditList;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.WebApplicationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenSearchAccessAuditsServiceTest {
    @InjectMocks
    OpenSearchAccessAuditsService service;

    @Mock
    OpenSearchMgr openSearchMgr;

    @Mock
    OpenSearchUtil openSearchUtil;

    @Mock
    RESTErrorUtil restErrorUtil;

    @Mock
    RangerDaoManager daoManager;

    @Mock
    XXServiceDao xxServiceDao;

    @Mock
    XXServiceDefDao xxServiceDefDao;

    @Mock
    RestClient restClient;

    @BeforeEach
    void setUp() throws Exception {
        service.setRestErrorUtil(restErrorUtil);

        java.lang.reflect.Field daoField = org.apache.ranger.AccessAuditsService.class.getDeclaredField("daoManager");

        daoField.setAccessible(true);
        daoField.set(service, daoManager);
    }

    @Test
    void searchXAccessAudits_clientNull_throwsException() {
        when(openSearchMgr.getClient()).thenReturn(null);
        when(restErrorUtil.createRESTException(anyString(), any(MessageEnums.class)))
                .thenReturn(new WebApplicationException(500));

        SearchCriteria criteria = new SearchCriteria();

        assertThrows(WebApplicationException.class, () -> service.searchXAccessAudits(criteria));
    }

    @Test
    void searchXAccessAudits_ioException_throwsException() throws Exception {
        when(openSearchMgr.getClient()).thenReturn(restClient);
        when(openSearchMgr.getIndex()).thenReturn("ranger_audits");
        when(openSearchUtil.searchResources(any(), any(), any(), eq(restClient), eq("ranger_audits")))
                .thenThrow(new IOException("Connection refused"));
        when(restErrorUtil.createRESTException(anyString(), any(MessageEnums.class)))
                .thenReturn(new WebApplicationException(500));

        SearchCriteria criteria = new SearchCriteria();

        assertThrows(WebApplicationException.class, () -> service.searchXAccessAudits(criteria));
    }

    @Test
    void searchXAccessAudits_success() throws Exception {
        when(openSearchMgr.getClient()).thenReturn(restClient);
        when(openSearchMgr.getIndex()).thenReturn("ranger_audits");

        Map<String, Object> doc1 = new HashMap<>();

        doc1.put("id", "test-id-001");
        doc1.put("reqUser", "testuser1");
        doc1.put("resource", "/tmp/test");
        doc1.put("access", "read");
        doc1.put("result", 1);
        doc1.put("repo", "dev_hdfs");
        doc1.put("repoType", 1);
        doc1.put("action", "read");

        List<Map<String, Object>> sources = new ArrayList<>();

        sources.add(doc1);

        OpenSearchSearchResult mockResult = new OpenSearchSearchResult(1, sources);

        when(openSearchUtil.searchResources(any(), any(), any(), eq(restClient), anyString()))
                .thenReturn(mockResult);
        when(daoManager.getXXService()).thenReturn(xxServiceDao);
        when(daoManager.getXXServiceDef()).thenReturn(xxServiceDefDao);

        SearchCriteria criteria = new SearchCriteria();

        criteria.setMaxRows(25);
        criteria.setStartIndex(0);

        VXAccessAuditList result = service.searchXAccessAudits(criteria);

        assertNotNull(result);
        assertEquals(1, result.getTotalCount());
        assertEquals(1, result.getResultSize());
        assertNotNull(result.getVXAccessAudits());
        assertEquals(1, result.getVXAccessAudits().size());
        assertEquals("testuser1", result.getVXAccessAudits().get(0).getRequestUser());
        assertEquals("/tmp/test", result.getVXAccessAudits().get(0).getResourcePath());
        assertEquals("read", result.getVXAccessAudits().get(0).getAccessType());
    }

    @Test
    void searchXAccessAudits_emptyResults() throws Exception {
        when(openSearchMgr.getClient()).thenReturn(restClient);
        when(openSearchMgr.getIndex()).thenReturn("ranger_audits");

        OpenSearchSearchResult mockResult = new OpenSearchSearchResult(0, new ArrayList<>());

        when(openSearchUtil.searchResources(any(), any(), any(), eq(restClient), anyString()))
                .thenReturn(mockResult);

        SearchCriteria criteria = new SearchCriteria();

        criteria.setMaxRows(25);
        criteria.setStartIndex(0);

        VXAccessAuditList result = service.searchXAccessAudits(criteria);

        assertNotNull(result);
        assertEquals(0, result.getTotalCount());
        assertEquals(0, result.getResultSize());
        assertTrue(result.getVXAccessAudits().isEmpty());
    }

    private static void assertTrue(boolean condition) {
        org.junit.jupiter.api.Assertions.assertTrue(condition);
    }
}
