/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.kms.metrics;

import org.apache.ranger.kms.metrics.collector.KMSMetricsCollector;
import org.apache.ranger.kms.metrics.source.KMSMetricSource;
import org.apache.ranger.metrics.RangerMetricsSystemWrapper;
import org.apache.ranger.metrics.wrapper.RangerMetricsSourceWrapper;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KMSMetricWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(KMSMetricWrapper.class);

    private static final Logger KMS_METRICS_LOGGER = LoggerFactory.getLogger("kms-metrics");

    private KMSMetricSource kmsMetricSource;

    private final RangerMetricsSystemWrapper rangerMetricsSystemWrapper;

    private final KMSMetricsCollector kmsMetricsCollector;

    private static volatile KMSMetricWrapper instance;

    private KMSMetricWrapper(boolean isMetricCollectionThreadSafe) {

        LOG.info("Creating KMSMetricWrapper with thread-safe value=" + isMetricCollectionThreadSafe);
        this.rangerMetricsSystemWrapper = new RangerMetricsSystemWrapper();
        this.kmsMetricsCollector = KMSMetricsCollector.getInstance(isMetricCollectionThreadSafe);
        this.kmsMetricSource = new KMSMetricSource(KMSMetrics.KMS_METRICS_CONTEXT, KMSMetrics.KMS_METRIC_RECORD, kmsMetricsCollector);
        init();
    }

    public static KMSMetricWrapper getInstance( boolean isMetricCollectionThreadSafe ) {
        if (null == instance) {

            synchronized (KMSMetricWrapper.class) {
                if (null == instance) {
                    instance = new KMSMetricWrapper(isMetricCollectionThreadSafe);
                }
            }

        }

        return instance;
    }


    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>> KMSMetricWrapper.init()");
        }

        // Source
        List<RangerMetricsSourceWrapper> sourceWrappers = new ArrayList<>();
        sourceWrappers.add(new RangerMetricsSourceWrapper("KMSMetricSource", "KMS metrics", KMSMetrics.KMS_METRICS_CONTEXT, kmsMetricSource));

        rangerMetricsSystemWrapper.init(KMSMetrics.KMS_METRICS_CONTEXT, sourceWrappers, null);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<<=== KMSMetricWrapper.init()");
        }
    }

    public String getRangerMetricsInPrometheusFormat() throws Exception {
        return rangerMetricsSystemWrapper.getRangerMetricsInPrometheusFormat();
    }

    public Map<String, Map<String, Object>> getRangerMetricsInJsonFormat() {
        return rangerMetricsSystemWrapper.getRangerMetrics();
    }

    public KMSMetricsCollector getKmsMetricsCollector()
    {
        return this.kmsMetricsCollector;
    }

    public void writeJsonMetricsToFile()
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>> KMSMetricWrapper.writeJsonMetricsToFile()");
        }
        try {
            KMS_METRICS_LOGGER.info(JsonUtilsV2.mapToJson(this.getRangerMetricsInJsonFormat()));
        } catch (Exception e) {
            LOG.error("Error while writing metrics to metrics-log file", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<<=== KMSMetricWrapper.writeJsonMetricsToFile()");
        }
    }
}
