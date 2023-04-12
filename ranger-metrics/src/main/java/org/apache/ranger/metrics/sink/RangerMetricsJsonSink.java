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

package org.apache.ranger.metrics.sink;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerMetricsJsonSink implements MetricsSink {
    private static final Logger LOG = LoggerFactory.getLogger(RangerMetricsJsonSink.class);

    private final Set<String> contexts;
    private final Map<String, Map<String, Object>> metricsJson = new HashMap<>();

    public RangerMetricsJsonSink(Set<String> contexts) {
        this.contexts = contexts;
    }

    @Override
    public void init(SubsetConfiguration conf) {
     // Implementation not needed
    }

    @Override
    public void putMetrics(MetricsRecord metricsRecord) {
        try {
            if (contexts.contains(metricsRecord.context())) {
                for (AbstractMetric metrics : metricsRecord.metrics()) {
                    if (metrics.type() == MetricType.COUNTER || metrics.type() == MetricType.GAUGE) {
                        String recordName =  metricsRecord.name();
                        Map<String, Object> record = metricsJson.get(recordName);

                        if (Objects.isNull(record)) {
                            record = new HashMap<> ();
                        }

                        record.put(metrics.name(), metrics.value());
                        metricsJson.put(recordName, record);
                    }
                }
            } else {
                if (LOG.isDebugEnabled()) {
		    LOG.debug("=== RangerMetricsJsonSink:putMetrics(): skipping... "+ metricsRecord.context());
		}
            }
        } catch (Exception e) {
            LOG.error("Exception occured while converting metrics into json.", e);
        }
    }

    @Override
    public void flush() {
     // Implementation not needed
    }

    public Map<String, Map<String, Object>> getMetrics() {
        return metricsJson;
    }
}
