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

package org.apache.ranger.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;

import java.util.Map;

public class TestRangerMetricsUtil {
    @InjectMocks
    RangerMetricsUtil rangerMetricsUtil = new RangerMetricsUtil();

    @Test
    public void testGetRangerMetricsValues() {
        Map<String, Object> rangerMetricsMap = rangerMetricsUtil.getValues();

        Assertions.assertNotNull(rangerMetricsMap);
        Assertions.assertNotNull(rangerMetricsMap.get("os.spec"));
        Assertions.assertNotNull(rangerMetricsMap.get("os.vcpus"));
        Assertions.assertNotNull(rangerMetricsMap.get("memory"));

        Map<String, Object> memoryDetailsMap = (Map<String, Object>) rangerMetricsMap.get("memory");

        Assertions.assertNotNull(memoryDetailsMap.get("heapMax"));
        Assertions.assertNotNull(memoryDetailsMap.get("heapCommitted"));
        Assertions.assertNotNull(memoryDetailsMap.get("heapUsed"));
        Assertions.assertNotNull(memoryDetailsMap.get("memory_pool_usages"));

        Map<String, Object> poolDivisionDetailsMap = (Map<String, Object>) memoryDetailsMap.get("memory_pool_usages");

        Assertions.assertTrue(poolDivisionDetailsMap.size() > 0);
    }
}
