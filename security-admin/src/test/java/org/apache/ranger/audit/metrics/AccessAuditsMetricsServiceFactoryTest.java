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

package org.apache.ranger.audit.metrics;

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.opensearch.OpenSearchAccessAuditsService;
import org.apache.ranger.solr.SolrAccessAuditsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AccessAuditsMetricsServiceFactoryTest {
    @Mock
    RangerBizUtil rangerBizUtil;

    @Mock
    SolrAccessAuditsService solrAccessAuditsService;

    @Mock
    OpenSearchAccessAuditsService openSearchAccessAuditsService;

    @InjectMocks
    AccessAuditsMetricsServiceFactory accessAuditsMetricsServiceFactory;

    @BeforeEach
    void setUp() {
        when(rangerBizUtil.getAuditDBType()).thenReturn(RangerBizUtil.AUDIT_STORE_SOLR);
    }

    @Test
    void getAccessAuditsMetricsService_returnsSolrByDefault() {
        AccessAuditsMetricsService service = accessAuditsMetricsServiceFactory.getAccessAuditsMetricsService();

        assertSame(solrAccessAuditsService, service);
    }

    @Test
    void getAccessAuditsMetricsService_returnsOpenSearchWhenConfigured() {
        when(rangerBizUtil.getAuditDBType()).thenReturn(RangerBizUtil.AUDIT_STORE_OPENSEARCH);

        AccessAuditsMetricsService service = accessAuditsMetricsServiceFactory.getAccessAuditsMetricsService();

        assertSame(openSearchAccessAuditsService, service);
    }
}
