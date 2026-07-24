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

package org.apache.ranger.tagsync.metrics;

import org.apache.ranger.metrics.RangerMetricsSystemWrapper;
import org.apache.ranger.metrics.wrapper.RangerMetricsSourceWrapper;
import org.apache.ranger.tagsync.metrics.source.RangerTagSyncMetricsSourceTags;
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
public class TagSyncMetricsWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(TagSyncMetricsWrapper.class);

    private static final String context = "tagsync";
    private final RangerMetricsSystemWrapper rangerMetricsSystemWrapper = new RangerMetricsSystemWrapper();
    @Autowired
    private RangerTagSyncMetricsSourceTags rangerTagSyncMetricsSourceTags;

    @PostConstruct
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>> TagSyncMetricsWrapper.init()");
        }

        // Source
        List<RangerMetricsSourceWrapper> sourceWrappers = new ArrayList<>();
        sourceWrappers.add(new RangerMetricsSourceWrapper("TagsyncSource", "Ranger TAGSYNC metrics", context,
                rangerTagSyncMetricsSourceTags));

        rangerMetricsSystemWrapper.init(context, sourceWrappers, Collections.emptyList());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<<=== TagSyncMetricsWrapper.init()");
        }
    }

    public String getRangerMetricsInPrometheusFormat() throws Exception {
        return rangerMetricsSystemWrapper.getRangerMetricsInPrometheusFormat();
    }

    public Map<String, Map<String, Object>> getRangerMetricsInJsonFormat() {
        return rangerMetricsSystemWrapper.getRangerMetrics();
    }
}
