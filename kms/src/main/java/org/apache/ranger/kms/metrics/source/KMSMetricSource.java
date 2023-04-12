/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.kms.metrics.source;

import org.apache.hadoop.crypto.key.kms.server.KMSWebApp;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.ranger.kms.metrics.KMSMetrics;
import com.codahale.metrics.Meter;
import org.apache.ranger.kms.metrics.collector.KMSMetricsCollector;
import org.apache.ranger.metrics.RangerMetricsInfo;
import org.apache.ranger.metrics.source.RangerMetricsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class KMSMetricSource extends RangerMetricsSource {

    private static final Logger LOG     = LoggerFactory.getLogger(KMSMetricSource.class);

    private final String    context;
    private final String    record;

    private final KMSMetricsCollector kmsMetricsCollector;

    public KMSMetricSource(String context, String record, KMSMetricsCollector kmsMetricsCollector)
    {
        this.context = context;
        this.record = record;
        this.kmsMetricsCollector = kmsMetricsCollector;
    }
    @Override
    protected void refresh() {
        // not required.
    }

    @Override
    protected void update(MetricsCollector collector, boolean all) {

        MetricsRecordBuilder builder = collector.addRecord(this.record).setContext(this.context);

        boolean isCollectionThreadSafe = this.kmsMetricsCollector.isCollectionThreadSafe();
        Map<KMSMetrics.KMSMetric, Long> collectorMetricsMap = null;

        if( ! isCollectionThreadSafe ){
            collectorMetricsMap = this.kmsMetricsCollector.getMetricsMap();
        }

        for (KMSMetrics.KMSMetric metric : KMSMetrics.KMSMetric.values()) {

            if( LOG.isDebugEnabled()){
                LOG.debug("KMSMetricSource: key=" + metric.getKey() + "  , value="+ metric.getValue() + " , type=" + metric.getType());
            }

            Long metricVal = isCollectionThreadSafe ? metric.getValue() : Objects.isNull(collectorMetricsMap.get(metric))? 0L : collectorMetricsMap.get(metric);
            switch (metric.getType())
            {
                case COUNTER:
                    builder.addCounter(new RangerMetricsInfo(metric.getKey(), ""), Objects.isNull(metricVal) ? 0L : metricVal);
                    break;
                case GAUGE:
                    builder.addGauge(new RangerMetricsInfo(metric.getKey(), ""), Objects.isNull(metricVal) ? 0L : metricVal);
                    break;
                default:
                    LOG.warn("Unsupported metric type found, it is being ignored. Current metric type "+metric.getType());
            }

        }

        collectAndUpdateUserAccessMetrics(builder);


    }

    private void collectAndUpdateUserAccessMetrics( MetricsRecordBuilder builder){

        Meter meter  = KMSWebApp.getUnauthenticatedCallsMeter();

        if( null != meter ){
            Long metricVal = KMSWebApp.getUnauthenticatedCallsMeter().getCount();
            builder.addCounter(new RangerMetricsInfo(KMSMetrics.KMSMetric.UNAUTHENTICATED_CALLS_COUNT.getKey(), ""), Objects.isNull(metricVal) ? 0L : metricVal);
        }

        meter  = KMSWebApp.getUnauthorizedCallsMeter();

        if( null != meter){

            Long metricVal = KMSWebApp.getUnauthorizedCallsMeter().getCount();
            builder.addCounter(new RangerMetricsInfo(KMSMetrics.KMSMetric.UNAUTHORIZED_CALLS_COUNT.getKey(),  ""), Objects.isNull(metricVal) ? 0L : metricVal);
        }
    }


}
