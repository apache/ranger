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

package org.apache.ranger.rest;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ranger.plugin.model.RangerMetrics;
import org.apache.ranger.util.RangerMetricsUtil;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestMetricsREST {
    @InjectMocks
    MetricsREST metricsREST = new MetricsREST();

    @Mock
    RangerMetricsUtil jvmMetricUtil;


    @Test
    public void testGetStatus() throws Exception {
        Map<String, Object> rangerMetricsValues = getRangerMetricsValues();

        Mockito.when(jvmMetricUtil.getValues()).thenReturn(rangerMetricsValues);

        RangerMetrics rangerMetrics = metricsREST.getStatus();

        Assert.assertNotNull(rangerMetrics);
        Assert.assertNotNull(rangerMetrics.getData());
        Assert.assertNotNull(rangerMetrics.getData().get("jvm"));

        Map<String, Object> jvmMetricsMap = (Map<String, Object>)rangerMetrics.getData().get("jvm");

        Assert.assertNotNull(jvmMetricsMap.get("JVM Machine Actual Name"));
        Assert.assertNotNull(jvmMetricsMap.get("version"));
        Assert.assertNotNull(jvmMetricsMap.get("JVM Vendor Name"));
        Assert.assertEquals("Mac OS X, x86_64, 12.6.3", jvmMetricsMap.get("os.spec"));
        Assert.assertEquals("8", jvmMetricsMap.get("os.vcpus"));
        Assert.assertNotNull(jvmMetricsMap.get("memory"));

        Map<String, Object> memoryDetailsMap = (Map<String, Object>)jvmMetricsMap.get("memory");

        Assert.assertEquals("7635730432", memoryDetailsMap.get("heapMax"));
        Assert.assertEquals("40424768", memoryDetailsMap.get("heapUsed"));
    }


    private Map<String, Object> getRangerMetricsValues() {
        Map<String, Object>  rangerMetricsMap = new LinkedHashMap<>();
        rangerMetricsMap.put("os.spec", "Mac OS X, x86_64, 12.6.3");
        rangerMetricsMap.put("os.vcpus", "8");

        Map<String, Object> memoryDetailsMap  = new LinkedHashMap<>();
        memoryDetailsMap.put("heapMax", String.valueOf(7635730432L));
        memoryDetailsMap.put("heapCommitted", String.valueOf(514850816L));
        memoryDetailsMap.put("heapUsed", String.valueOf(40424768L));
        rangerMetricsMap.put("memory", memoryDetailsMap);

        return rangerMetricsMap;
    }
}
