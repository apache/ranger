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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AccessAuditsMetricsServiceFactory {
    @Autowired
    RangerBizUtil rangerBizUtil;

    @Autowired
    SolrAccessAuditsService solrAccessAuditsService;

    @Autowired
    OpenSearchAccessAuditsService openSearchAccessAuditsService;

    public AccessAuditsMetricsService getAccessAuditsMetricsService() {
        AccessAuditsMetricsService ret = solrAccessAuditsService;

        if (RangerBizUtil.AUDIT_STORE_OPENSEARCH.equalsIgnoreCase(rangerBizUtil.getAuditDBType())) {
            ret = openSearchAccessAuditsService;
        }

        return ret;
    }
}
