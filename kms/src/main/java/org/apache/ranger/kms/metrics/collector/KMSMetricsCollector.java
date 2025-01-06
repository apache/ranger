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

package org.apache.ranger.kms.metrics.collector;

import org.apache.hadoop.thirdparty.com.google.common.base.Stopwatch;
import org.apache.ranger.kms.metrics.KMSMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KMSMetricsCollector {
    static final Logger logger = LoggerFactory.getLogger(KMSMetricsCollector.class);

    private static volatile  KMSMetricsCollector kmsMetricsCollector;

    private final boolean isCollectionThreadSafe;
    private final Map<KMSMetrics.KMSMetric, Long> metrics = new HashMap<>();

    private KMSMetricsCollector(boolean isCollectionThreadSafe) {
        this.isCollectionThreadSafe = isCollectionThreadSafe;
    }

    public static KMSMetricsCollector getInstance( boolean isCollectionThreadSafe) {
        KMSMetricsCollector metricsCollector = kmsMetricsCollector;

        if (metricsCollector == null) {
            synchronized(KMSMetricsCollector.class) {
                metricsCollector = kmsMetricsCollector;
                if (metricsCollector == null) {
                    metricsCollector    = new KMSMetricsCollector(isCollectionThreadSafe);
                    kmsMetricsCollector = metricsCollector;
                }
            }
        }

        return metricsCollector;
    }

    public boolean isCollectionThreadSafe() {
        return this.isCollectionThreadSafe;
    }

    public Map<KMSMetrics.KMSMetric, Long> getMetricsMap() {
        return Collections.unmodifiableMap(this.metrics);
    }

    public void incrementCounter(KMSMetrics.KMSMetric metric) {
        if (isCollectionThreadSafe) {
            if (KMSMetrics.Type.COUNTER.equals(metric.getType())) {
                metric.incrementCounter();
            } else {
                logger.warn("Only Counter metric can be incremented. Current metric type is {}", metric.getType());
            }
        } else {

            this.metrics.compute(metric, (k,v) -> null == v ? 1 : v+1);
        }
    }

    public void updateMetric(KMSMetrics.KMSMetric metric, long val) {
        if(this.isCollectionThreadSafe) {
            metric.updateValue(val);
        }
        else {
            this.metrics.compute(metric, (k,v) -> null == v ? val : v + val);
        }
    }

    public APIMetric createAPIMetric(KMSMetrics.KMSMetric counter, KMSMetrics.KMSMetric elapsedTime) {
        return new APIMetric(counter, elapsedTime);
    }

    /**
     * This method starts the Stopwatch to capture the elapsed time but KMSMetrics is not yet set.
     * It may be used to address the use cases where exact metric name would be known after some processing,
     * in such cases, Initially StopWatch can be started and specific metric can be set once known.
     *
     * If metric is not set, this StopWatch will be stopped and ignored. A warn log message will be logged.
     * @return : APIMetric
     */
    public APIMetric captureElapsedTime() {
        return new APIMetric();
    }

    public class APIMetric implements AutoCloseable {
        private KMSMetrics.KMSMetric counter;
        private KMSMetrics.KMSMetric elapsedTime;
        private final Stopwatch            sw;

        private APIMetric(KMSMetrics.KMSMetric counter, KMSMetrics.KMSMetric elapsedTime) {
            this();
            this.counter     = counter;
            this.elapsedTime = elapsedTime;
        }

        private APIMetric() {
            this.sw = Stopwatch.createStarted();
        }

        public void setMetrics(KMSMetrics.KMSMetric counter, KMSMetrics.KMSMetric elapsedTime) {
            this.counter = counter;
            this.elapsedTime = elapsedTime;
        }

        @Override
        public void close() {
            if (null != counter && null != elapsedTime) {
                incrementCounter(counter);
                updateMetric(elapsedTime, sw.stop().elapsed(TimeUnit.MILLISECONDS));
            } else {
                long elapsedTime = sw.stop().elapsed(TimeUnit.MILLISECONDS);
                logger.warn("API metric started to capture elapsed time but Elapsed metric was not set. Elapsed time(in MS) {}", elapsedTime);
            }
        }
    }
}
