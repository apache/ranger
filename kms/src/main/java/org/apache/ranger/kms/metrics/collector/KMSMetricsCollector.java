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

import org.apache.ranger.kms.metrics.KMSMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KMSMetricsCollector {

    static final Logger logger = LoggerFactory.getLogger(KMSMetricsCollector.class);

    private boolean isCollectionThreadSafe;

    private Map<KMSMetrics.KMSMetric, Long> metrics = new HashMap<>();

    private static volatile  KMSMetricsCollector kmsMetricsCollector;

    private KMSMetricsCollector(boolean isCollectionThreadSafe) {
        this.isCollectionThreadSafe = isCollectionThreadSafe;
    }

    public static KMSMetricsCollector getInstance( boolean isCollectionThreadSafe)
    {
        if( null == kmsMetricsCollector)
        {
            synchronized(KMSMetricsCollector.class)
            {
                if(null == kmsMetricsCollector){
                    kmsMetricsCollector = new KMSMetricsCollector(isCollectionThreadSafe);
                }
            }
        }

        return kmsMetricsCollector;
    }

    public boolean isCollectionThreadSafe()
    {
        return this.isCollectionThreadSafe;
    }

    public Map<KMSMetrics.KMSMetric, Long> getMetricsMap()
    {
        return Collections.unmodifiableMap(this.metrics);
    }

    public void incrementCounter(KMSMetrics.KMSMetric metric) {

        if (isCollectionThreadSafe) {
            if (KMSMetrics.Type.COUNTER.equals(metric.getType())) {
                metric.incrementCounter();
            } else {
                logger.warn("Only Counter metric can be incremented. Current metric type is " + metric.getType());
            }
        } else {

            this.metrics.compute(metric, (k,v) -> null == v ? 1 : v+1);
        }


    }

    public void updateMetric(KMSMetrics.KMSMetric metric, long val) {

        if(this.isCollectionThreadSafe)
        {
            metric.updateValue(val);
        }
        else{
            this.metrics.compute(metric, (k,v) -> null == v ? val : v + val);
        }
    }

}
