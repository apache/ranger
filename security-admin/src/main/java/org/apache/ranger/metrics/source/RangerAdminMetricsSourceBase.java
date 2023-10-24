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

package  org.apache.ranger.metrics.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.ranger.metrics.RangerMetricsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RangerAdminMetricsSourceBase extends RangerMetricsSource {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAdminMetricsSourceBase.class);

    final Map<String, Long> metricsMap = new HashMap<>();

    final String context;
    final String record;

    public RangerAdminMetricsSourceBase(String context, String record) {
        this.context = context;
        this.record  = record;
    }

    @Override
    protected void update(MetricsCollector collector, boolean all) {
        MetricsRecordBuilder builder = collector.addRecord(this.record)
        .setContext(this.context);

        for (String key: metricsMap.keySet() ) {
            builder.addGauge(new RangerMetricsInfo(key, ""), metricsMap.get(key));
        }
    }

    protected void addMetricEntry(String prefix, String suffix, Long value) {
        if (Objects.nonNull(prefix)) {
            if (Objects.isNull(suffix) || suffix.isEmpty() || suffix.equalsIgnoreCase("Total")) {
                metricsMap.put(prefix, value);
            } else {
                metricsMap.put(prefix + (suffix).toUpperCase(), value);
            }
        } else {
            LOG.warn("===>> RangerAdminMetricsSourceBase.addMetricEntry(): Metrics prefix can not be null.");
        }
    }

    protected void addMetricEntries(String prefix, Map<String, Long> metrics) {
        for (Map.Entry<String, Long> entry : metrics.entrySet()) {
            addMetricEntry(prefix, entry.getKey(), entry.getValue());
        }
    }
}
