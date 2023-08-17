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

package org.apache.ranger.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.ranger.metrics.source.RangerAdminMetricsSourceContextEnricher;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourceDenyConditions;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourcePolicyMasking;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourcePolicyResourceAccess;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourcePolicyRowFiltering;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourceService;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourceUserGroup;
import org.apache.ranger.metrics.wrapper.RangerMetricsSourceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RangerAdminMetricsWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAdminMetricsWrapper.class);
    private static final String context = "admin";

    private final RangerMetricsSystemWrapper rangerMetricsSystemWrapper = new RangerMetricsSystemWrapper();

    @Autowired
    private RangerAdminMetricsSourceUserGroup userGroupSource;

    @Autowired
    private RangerAdminMetricsSourceService serviceSource;

    @Autowired
    private RangerAdminMetricsSourcePolicyResourceAccess policyResourceAccessSource;

    @Autowired
    private RangerAdminMetricsSourcePolicyRowFiltering policyRowFilteringSource;

    @Autowired
    private RangerAdminMetricsSourcePolicyMasking policyMaskingSource;

    @Autowired
    private RangerAdminMetricsSourceContextEnricher contextEnricherSource;

    @Autowired
    private RangerAdminMetricsSourceDenyConditions denyConditionSource;

    @PostConstruct
    public void init() {
        LOG.info("===>> RangerAdminMetricsWrapper.init()");
        try {
            //Source
            List<RangerMetricsSourceWrapper> sourceWrappers = new ArrayList<>();

            //Source: UserGroup
            sourceWrappers.add(new RangerMetricsSourceWrapper("UserGroup", "UserGroup metrics in Ranger Admin", context, userGroupSource));

            //Source: Service
            sourceWrappers.add(new RangerMetricsSourceWrapper("RangerAdminMetricsSourceService", "Service metrics in Ranger Admin", context, serviceSource));

            //Source: Policy
            sourceWrappers.add(new RangerMetricsSourceWrapper("RangerAdminMetricsSourcePolicyResourceAccess", "Resource Policy Access metrics in Ranger Admin", context, policyResourceAccessSource));
            sourceWrappers.add(new RangerMetricsSourceWrapper("RangerAdminMetricsSourcePolicyRowFiltering", "Row Filtering Policy Access metrics in Ranger Admin", context, policyRowFilteringSource));
            sourceWrappers.add(new RangerMetricsSourceWrapper("RangerAdminMetricsSourcePolicyMasking", "Masking Policy Access metrics in Ranger Admin", context, policyMaskingSource));

            //Source: ContextEnricher
            sourceWrappers.add(new RangerMetricsSourceWrapper("RangerAdminMetricsSourceContextEnricher", "Context Enricher metrics in Ranger Admin", context, contextEnricherSource));

            //Source: DenyConditionService
            sourceWrappers.add(new RangerMetricsSourceWrapper("RangerAdminMetricsSourceDenyConditionService", "Deny Condition in Ranger Admin", context, denyConditionSource));

            rangerMetricsSystemWrapper.init(context, sourceWrappers, Collections.emptyList());
        } catch (Exception e) {
            LOG.error("RangerAdminMetricsWrapper: Exception occured while initializing Metric Starter:", e);
        }

        LOG.info("<<=== RangerAdminMetricsWrapper.init()");
    }

    public String getRangerMetricsInPrometheusFormat() throws Exception {
        return rangerMetricsSystemWrapper.getRangerMetricsInPrometheusFormat();
    }

    public Map<String, Map<String, Object>> getRangerMetrics() {
        return rangerMetricsSystemWrapper.getRangerMetrics();
    }
}
