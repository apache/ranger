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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ranger.metrics.sink.RangerMetricsJsonSink;
import org.apache.ranger.metrics.sink.RangerMetricsPrometheusSink;
import org.apache.ranger.metrics.source.RangerMetricsContainerSource;
import org.apache.ranger.metrics.source.RangerMetricsJvmSource;
import org.apache.ranger.metrics.wrapper.RangerMetricsSinkWrapper;
import org.apache.ranger.metrics.wrapper.RangerMetricsSourceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerMetricsSystemWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(RangerMetricsSystemWrapper.class);

    private RangerMetricsPrometheusSink rangerMetricsPrometheusSink;
    private RangerMetricsJsonSink rangerMetricsJsonSink;

    /**
     * Initialized metric system.
     * @param serviceName
     * @param sourceWrappers
     * @param sinkWrappers
     */
    public void init(String serviceName, List<RangerMetricsSourceWrapper> sourceWrappers, List<RangerMetricsSinkWrapper> sinkWrappers) {
        // Initialize metrics system
        MetricsSystem metricsSystem = DefaultMetricsSystem.initialize(serviceName);
        Set<String> sourceContexts = new HashSet<String>();
        sourceContexts.add(serviceName);

        // Ranger Source
        if (Objects.isNull(sourceWrappers) || sourceWrappers.isEmpty()) {
            sourceWrappers = new ArrayList<RangerMetricsSourceWrapper>();
        }
        sourceWrappers.add(new RangerMetricsSourceWrapper("RangerJVM", "Ranger common metric source (RangerMetricsJvmSource)", serviceName, new RangerMetricsJvmSource(serviceName)));
        sourceWrappers.add(new RangerMetricsSourceWrapper("RangerContainer", "Ranger web container metric source (RangerMetricsContainerSource)", serviceName, new RangerMetricsContainerSource(serviceName)));

        for (RangerMetricsSourceWrapper sourceWrapper: sourceWrappers) {
            metricsSystem.register(sourceWrapper.getName(), sourceWrapper.getDescription(), sourceWrapper.getSource());
            sourceContexts.add(sourceWrapper.getContext());
        }

        // Ranger Sink
        if (Objects.isNull(sinkWrappers) || sinkWrappers.isEmpty()) {
            sinkWrappers = new ArrayList<RangerMetricsSinkWrapper>();
        }

        // Prometheus
        rangerMetricsPrometheusSink = new RangerMetricsPrometheusSink(sourceContexts);
        sinkWrappers.add(new RangerMetricsSinkWrapper("Prometheus", "Ranger common metric sink (RangerMetricsPrometheusSink)", rangerMetricsPrometheusSink));

        // JSON
        rangerMetricsJsonSink = new RangerMetricsJsonSink(sourceContexts);
        sinkWrappers.add(new RangerMetricsSinkWrapper("Json", "Ranger common metric sink (RangerMetricsJsonSink)", rangerMetricsJsonSink));

        for (RangerMetricsSinkWrapper sinkWrapper: sinkWrappers) {
            metricsSystem.register(sinkWrapper.getName(), sinkWrapper.getDescription(), sinkWrapper.getSink());
        }

        LOG.info("===>> Ranger Metric system initialized successfully.");
    }

    public String getRangerMetricsInPrometheusFormat() throws IOException {
        StringWriter stringWriter = new StringWriter();
        rangerMetricsPrometheusSink.writeMetrics(stringWriter);
        return stringWriter.toString();
    }

    public Map<String, Map<String, Object>> getRangerMetrics() {
        return rangerMetricsJsonSink.getMetrics();
    }
}
