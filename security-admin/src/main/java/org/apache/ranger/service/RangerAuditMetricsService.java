/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.service;

import org.apache.ranger.entity.XXAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class RangerAuditMetricsService extends RangerAuditMetricsServiceBase<XXAuditMetrics, RangerAuditMetrics> {
    public RangerAuditMetricsService() {
        super();
    }

    @Override
    protected void validateForCreate(RangerAuditMetrics vObj) {
    }

    @Override
    protected void validateForUpdate(RangerAuditMetrics vObj, XXAuditMetrics entityObj) {
    }

    @Override
    protected XXAuditMetrics mapViewToEntityBean(RangerAuditMetrics rangerAuditMetrics, XXAuditMetrics xxAuditMetrics, int operationContext) {
        XXAuditMetrics ret = super.mapViewToEntityBean(rangerAuditMetrics, xxAuditMetrics, operationContext);
        return ret;
    }

    @Override
    protected RangerAuditMetrics mapEntityToViewBean(RangerAuditMetrics rangerAuditMetrics, XXAuditMetrics xxAuditMetrics) {
        RangerAuditMetrics ret = super.mapEntityToViewBean(rangerAuditMetrics, xxAuditMetrics);
        return ret;
    }
}
