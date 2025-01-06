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

import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.service.XGroupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class RangerMetricsFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(RangerMetricsFetcher.class);

    @Autowired
    private XUserMgr xUserMgr;

    @Autowired
    private ServiceDBStore svcStore;

    @Autowired
    private XGroupService groupService;

    public Long getGroupCount() {
        return groupService.getAllGroupCount();
    }

    public Map<String, Long> getUserMetrics() {
        Map<String, Long> ret   = new HashMap<>();
        long              total = 0L;

        for (Map.Entry<String, Long> entry : xUserMgr.getUserCountByRole().entrySet()) {
            String role = entry.getKey();
            switch (role) {
                case RangerConstants.ROLE_SYS_ADMIN:
                    ret.put("SysAdmin", entry.getValue());
                    break;
                case RangerConstants.ROLE_ADMIN_AUDITOR:
                    ret.put("AdminAuditor", entry.getValue());
                    break;
                case RangerConstants.ROLE_KEY_ADMIN:
                    ret.put("KeyAdmin", entry.getValue());
                    break;
                case RangerConstants.ROLE_KEY_ADMIN_AUDITOR:
                    ret.put("KeyAdminAuditor", entry.getValue());
                    break;
                case RangerConstants.ROLE_USER:
                    ret.put("User", entry.getValue());
                    break;
                default:
                    LOG.warn("===>> RangerMetricsFetcher.getUserMetrics(): invalid role [{}] type.", role);
                    break;
            }

            total += entry.getValue();
        }

        ret.put("Total", total);

        return ret;
    }

    public Map<String, Long> getRangerServiceMetrics() {
        Map<String, Long> ret   = new HashMap<>();
        long              total = 0L;

        for (Map.Entry<String, Long> entry : svcStore.getServiceCountByType().entrySet()) {
            ret.put(entry.getKey(), entry.getValue());

            total += entry.getValue();
        }

        ret.put("Total", total);

        return ret;
    }

    public Map<String, Long> getPolicyMetrics(Integer policyType) {
        requireNonNull(policyType, "Policy type must not be null to get policy metrics.");

        Map<String, Long> ret   = new HashMap<>();
        long              total = 0L;

        for (Map.Entry<String, Long> entry : svcStore.getPolicyCountByTypeAndServiceType(policyType).entrySet()) {
            ret.put(entry.getKey(), entry.getValue());

            total += entry.getValue();
        }

        ret.put("Total", total);

        return ret;
    }

    public Map<String, Long> getDenyConditionsMetrics() {
        Map<String, Long> ret   = new HashMap<>();
        long              total = 0L;

        for (Map.Entry<String, Long> entry : svcStore.getPolicyCountByDenyConditionsAndServiceDef().entrySet()) {
            ret.put(entry.getKey(), entry.getValue());

            total += entry.getValue();
        }

        ret.put("Total", total);

        return ret;
    }

    public Map<String, Long> getContextEnrichersMetrics() {
        Map<String, Long> ret   = new HashMap<>();
        long              total = 0L;

        for (String serviceDef : svcStore.findAllServiceDefNamesHavingContextEnrichers()) {
            ret.put(serviceDef, 1L);

            total++;
        }

        ret.put("Total", total);

        return ret;
    }
}
