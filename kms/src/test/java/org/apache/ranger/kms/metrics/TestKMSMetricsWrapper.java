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

package org.apache.ranger.kms.metrics;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ranger.kms.metrics.collector.KMSMetricsCollector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

public class TestKMSMetricsWrapper {

    private static KMSMetricWrapper kmsMetricWrapper;

    private static KMSMetricsCollector kmsMetricsCollector;

    private static final boolean isMetricCollectionThreadsafe = false;

    public TestKMSMetricsWrapper()
    {
        // Initialize hadoop-metric2 system & create KMSMetricWrapper

        System.setProperty("hadoop.home.dir", "./");


        kmsMetricWrapper = KMSMetricWrapper.getInstance(isMetricCollectionThreadsafe);
        kmsMetricsCollector = kmsMetricWrapper.getKmsMetricsCollector();
    }

    @Test
    public void testNonThreadsafeCounterMetrics() throws NoSuchFieldException, IllegalAccessException {

        // set the "isMetricCollectionThreadsafe" value to true to verify different flow.
        setKmsMetricsCollectorThreadSafelyFlag(false);

        // For COUNTER type
        kmsMetricsCollector.incrementCounter(KMSMetrics.KMSMetric.KEY_CREATE_COUNT);

        // For Gauge type
        kmsMetricsCollector.updateMetric(KMSMetrics.KMSMetric.KEY_CREATE_ELAPSED_TIME, 100L);

        DefaultMetricsSystem.instance().publishMetricsNow();

        Assertions.assertEquals(1L,  kmsMetricWrapper.getRangerMetricsInJsonFormat().get("KMS").get(KMSMetrics.KMSMetric.KEY_CREATE_COUNT.getKey()));
        Assertions.assertEquals(100L,  kmsMetricWrapper.getRangerMetricsInJsonFormat().get("KMS").get(KMSMetrics.KMSMetric.KEY_CREATE_ELAPSED_TIME.getKey()));
    }

    @Test
    public void testThreadsafeCounterMetrics() throws NoSuchFieldException, IllegalAccessException {

        // set the "isMetricCollectionThreadsafe" value to true to verify different flow.

        setKmsMetricsCollectorThreadSafelyFlag(true);

        // For COUNTER type
        kmsMetricsCollector.incrementCounter(KMSMetrics.KMSMetric.KEY_CREATE_COUNT);

        // For Gauge type
        kmsMetricsCollector.updateMetric(KMSMetrics.KMSMetric.KEY_CREATE_ELAPSED_TIME, 200L);

        DefaultMetricsSystem.instance().publishMetricsNow();

        Assertions.assertEquals(1L,  kmsMetricWrapper.getRangerMetricsInJsonFormat().get("KMS").get(KMSMetrics.KMSMetric.KEY_CREATE_COUNT.getKey()));
        Assertions.assertEquals(200L,  kmsMetricWrapper.getRangerMetricsInJsonFormat().get("KMS").get(KMSMetrics.KMSMetric.KEY_CREATE_ELAPSED_TIME.getKey()));
    }

    private void setKmsMetricsCollectorThreadSafelyFlag(boolean isMetricCollectionThreadsafe) throws IllegalAccessException, NoSuchFieldException {

        Field isCollectionThreadSafeField = kmsMetricsCollector.getClass().getDeclaredField("isCollectionThreadSafe");
        isCollectionThreadSafeField.setAccessible(true);
        isCollectionThreadSafeField.setBoolean(kmsMetricsCollector, isMetricCollectionThreadsafe);
    }
}
