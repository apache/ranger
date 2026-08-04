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

package org.apache.ranger.authentication.metrics;

import org.apache.ranger.authentication.metrics.source.RangerUserSyncSourceApis;
import org.apache.ranger.authentication.metrics.source.RangerUserSyncSourceCache;
import org.apache.ranger.authentication.metrics.source.RangerUserSyncSourceRoleStatus;
import org.apache.ranger.authentication.metrics.source.RangerUserSyncSourceSyncSource;
import org.apache.ranger.metrics.RangerMetricsSystemWrapper;
import org.apache.ranger.metrics.wrapper.RangerMetricsSourceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class UserSyncMetricsWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(UserSyncMetricsWrapper.class);

    private static final String context = "usersync";

    private final RangerMetricsSystemWrapper rangerMetricsSystemWrapper = new RangerMetricsSystemWrapper();

    @Autowired
    private RangerUserSyncSourceApis rangerUserSyncSourceApis;

    @Autowired
    private RangerUserSyncSourceCache rangerUserSyncSourceCache;

    @Autowired
    private RangerUserSyncSourceSyncSource rangerUserSyncSourceSyncSource;

    @Autowired
    private RangerUserSyncSourceRoleStatus rangerUserSyncSourceRoleStatus;

    @PostConstruct
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>> UserSyncMetricsWrapper.init()");
        }

        // Source
        List<RangerMetricsSourceWrapper> sourceWrappers = new ArrayList<>();
        sourceWrappers.add(new RangerMetricsSourceWrapper("UserSyncSource", "Ranger UserSync metrics", context,
                rangerUserSyncSourceApis));

        sourceWrappers.add(new RangerMetricsSourceWrapper("UserSyncSourceCache", "Ranger UserSync Cache metrics", context,
                rangerUserSyncSourceCache));

        sourceWrappers.add(new RangerMetricsSourceWrapper("UserSyncSourceSyncSource", "Ranger UserSync Sync Source metrics", context,
                rangerUserSyncSourceSyncSource));

        sourceWrappers.add(new RangerMetricsSourceWrapper("UserSyncSourceCommon", "Ranger UserSync Role Status metrics", context,
                rangerUserSyncSourceRoleStatus));

        rangerMetricsSystemWrapper.init(context, sourceWrappers, Collections.emptyList());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<<=== UserSyncMetricsWrapper.init()");
        }
    }

    public String getRangerMetricsInPrometheusFormat() throws Exception {
        LOG.info("===>>getRangerMetricsInPrometheusFormat rangerMetricsSystemWrapper = " + rangerMetricsSystemWrapper);
        return rangerMetricsSystemWrapper.getRangerMetricsInPrometheusFormat();
    }

    public Map<String, Map<String, Object>> getRangerMetricsInJsonFormat() {
        LOG.info("===>>getRangerMetricsInJsonFormat rangerMetricsSystemWrapper = " + rangerMetricsSystemWrapper);
        return rangerMetricsSystemWrapper.getRangerMetrics();
    }
}
