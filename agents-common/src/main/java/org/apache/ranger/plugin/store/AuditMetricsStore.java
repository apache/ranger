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

package org.apache.ranger.plugin.store;

import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.List;

public interface AuditMetricsStore {
    void init() throws Exception;

    RangerAuditMetrics createRangerAuditMetrics(RangerAuditMetrics rangerAuditMetrics) throws RuntimeException;

    RangerAuditMetrics getRangerAuditMetrics(Long id) throws RuntimeException;

    RangerAuditMetrics getLatestRangerAuditMetrics(String serviceType, String serviceName) throws RuntimeException;

    List<RangerAuditMetrics> getAllLatestRangerAuditMetrics(SearchFilter searchFilter) throws RuntimeException;

    List<RangerAuditMetricsByHours> getRangerAuditMetricsByHours(SearchFilter searchFilter) throws RuntimeException;

    List<RangerAuditMetricsByDays> getRangerAuditMetricsByDays(Integer olderThanInDays, SearchFilter searchFilter) throws RuntimeException;

    void deleteRangerAuditMetrics(Integer olderThanInDays, String serviceName, String serviceType) throws RuntimeException;
}
