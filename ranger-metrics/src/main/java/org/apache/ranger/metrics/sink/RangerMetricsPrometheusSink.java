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

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

public class RangerMetricsPrometheusSink implements MetricsSink {
    private static final Pattern SPLIT_PATTERN = Pattern.compile("(?<!(^|[A-Z_]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])");
    private static final Pattern DELIMITERS    = Pattern.compile("[^a-zA-Z0-9]+");

    /**
     * Cached output lines for each metrics.
     */
    private final Map<String, String> metricLines = new ConcurrentHashMap<>();
    private final Set<String> contexts;

    public RangerMetricsPrometheusSink(Set<String> metricsContexts) {
        if (Objects.isNull(metricsContexts)) {
            this.contexts = Collections.emptySet();
        } else {
            this.contexts = metricsContexts;
        }
    }

    @Override
    public void init(SubsetConfiguration conf) {
        // Implementation not needed
    }

    @Override
    public void putMetrics(MetricsRecord metricsRecord) {
        if (contexts.contains(metricsRecord.context())) {
            for (AbstractMetric metrics : metricsRecord.metrics()) {
                if (metrics.type() == MetricType.COUNTER || metrics.type() == MetricType.GAUGE) {

                    String key = prometheusName(metricsRecord.name(), metrics.name());

                    StringBuilder builder = new StringBuilder();
                    builder.append("# TYPE ").append(key).append(" ").append(metrics.type().toString().toLowerCase()).append("\n").append(key).append("{");
                    String sep = "";

                    // add tags
                    for (MetricsTag tag : metricsRecord.tags()) {
                        String tagName = tag.name().toLowerCase();

                        // ignore specific tag which includes sub-hierarchy
                        if (!tagName.equals("numopenconnectionsperuser")) {
                            builder.append(sep).append(tagName).append("=\"").append(tag.value()).append("\"");
                            sep = ",";
                        }
                    }
                    builder.append("} ");
                    builder.append(metrics.value());
                    builder.append("\n");
                    metricLines.put(key, builder.toString());
                }
            }
        } 
    }

    /**
     * Convert CamelCase based names to lower-case names where the separator is the
     * underscore, to follow prometheus naming conventions.
     */
    public String prometheusName(String recordName, String metricName) {
        String   baseName = StringUtils.capitalize(recordName) + StringUtils.capitalize(metricName);
        String[] parts    = SPLIT_PATTERN.split(baseName);
        String   joined   = String.join("_", parts).toLowerCase();
        return DELIMITERS.matcher(joined).replaceAll("_");
    }

    @Override
    public void flush() {
        // Implementation not needed
    }

    public void writeMetrics(Writer writer) throws IOException {
        for (String line : metricLines.values()) {
            writer.write(line);
        }
    }
}
