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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MetricCacheUtil {
    public static final String ADD_USER_COUNT_SUCCESS = "AddUserCountSUCCESS";
    public static final String ADD_USER_COUNT_FAIL    = "AddUserCountFAIL";
    public static final String GROUP_USER_COUNT_SUCCESS = "GroupUserCountSUCCESS";
    public static final String GROUP_USER_COUNT_FAIL    = "GroupUserCountFAIL";
    public static final String ADD_GROUP_COUNT_SUCCESS = "AddGroupCountSUCCESS";
    public static final String ADD_GROUP_COUNT_FAIL    = "AddGroupCountFAIL";
    public static final String DELETE_USER_COUNT_SUCCESS = "DeleteUserCountSUCCESS";
    public static final String DELETE_USER_COUNT_FAIL    = "DeleteUserCountFAIL";
    public static final String DELETE_GROUP_COUNT_SUCCESS = "DeleteGroupCountSUCCESS";
    public static final String DELETE_GROUP_COUNT_FAIL    = "DeleteGroupCountFAIL";
    public static final String AUDIT_COUNT_SUCCESS = "AuditCountSUCCESS";
    public static final String AUDIT_COUNT_FAIL    = "AuditCountFAIL";
    public static final String NO_OF_CACHED_USERS        = "CountUSER";
    public static final String NO_OF_CACHED_GROUPS       = "CountGROUP";
    public static final String NO_OF_CACHED_GROUPS_USERS = "CountGROUPUSER";
    private static MetricCacheUtil metricCacheUtil;
    private Map<String, Long> apiMetrics    = new HashMap<>();
    private Map<String, Long> cacheMetrics  = new HashMap<>();
    private Map<String, Long> sourceMetrics = new HashMap<>();

    private MetricCacheUtil() {
    }

    public static MetricCacheUtil getInstance() {
        if (Objects.isNull(metricCacheUtil)) {
            synchronized (MetricCacheUtil.class) {
                if (Objects.isNull(metricCacheUtil)) {
                    metricCacheUtil = new MetricCacheUtil();
                }
            }
        }
        return metricCacheUtil;
    }

    public void incrementMetric(MetricType metricType, String metricName, Long newValue) {
        Map<String, Long> metric = getMetric(metricType);

        if (Objects.nonNull(metric)) {
            Long originalValue = metric.get(metricName);
            if (Objects.isNull(originalValue)) {
                originalValue = 0L;
            }
            if (metricType.equals(MetricType.CACHE) || metricType.equals(MetricType.SYNCSOURCE)) {
                originalValue = newValue;
            } else {
                originalValue += newValue;
            }
            metric.put(metricName, originalValue);
        }
    }

    public Map<String, Long> getMetric(MetricType metricType) {
        Map<String, Long> metric = null;
        switch (metricType) {
            case API:
                metric = apiMetrics;
                break;
            case CACHE:
                metric = cacheMetrics;
                break;
            case SYNCSOURCE:
                metric = sourceMetrics;
                break;

            default:
                break;
        }
        return metric;
    }

    public static enum MetricType {
        API, CACHE, SYNCSOURCE;
    }
}
