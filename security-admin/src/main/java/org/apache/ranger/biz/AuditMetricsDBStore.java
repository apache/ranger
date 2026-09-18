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

package org.apache.ranger.biz;

import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.solr.SolrAccessAuditsService;
import org.apache.ranger.view.RangerAuditAdminMetricsByDays;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class AuditMetricsDBStore {
    private static final String  PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS         = "ranger.audit.metrics.max.supported.days";
    private static final Integer PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS_DEFAULT   = 90;

    private static final Set<Integer> ALLOWED_OBJECT_CLASS_TYPES = new HashSet<>(Arrays.asList(1002, 1003, 1020, 1030, 1057));
    private static final Set<String>  ALLOWED_ACTIONS          = Arrays.stream(Action.values()).map(a -> a.name().toLowerCase()).collect(Collectors.toSet());

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    SolrAccessAuditsService solrAccessAuditsService;

    @Autowired
    RESTErrorUtil restErrorUtil;

    RangerAdminConfig config;

    @PostConstruct
    public void initStore() {
        config = RangerAdminConfig.getInstance();
    }

    public List<RangerAuditAdminMetricsByDays> getRangerAuditAdminMetricsByDays(Integer olderThanInDays, List<String> objectClassTypes, List<String> actions, String timezone) throws RuntimeException {
        Integer maxAllowedDays = config.getInt(PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS, PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS_DEFAULT);

        if (olderThanInDays <= 0 || olderThanInDays > maxAllowedDays) {
            throw restErrorUtil.createRESTException("Invalid parameter: olderThanInDays must be between 1 and " + maxAllowedDays, MessageEnums.INVALID_INPUT_DATA);
        }

        List<String> validatedActions           = validateActions(actions);
        Set<Integer> validatedObjectClassTypes  = validateObjectClassType(objectClassTypes);
        ZoneId zoneId                           = getZoneId(timezone);

        return daoMgr.getXXTrxLogV2().getRangerAuditAdminMetricsByDays(olderThanInDays, validatedObjectClassTypes, validatedActions, zoneId);
    }

    public List<Map<String, Object>> getRangerAuditAccessMetricsByDays(Integer olderThanInDays, String timezone) throws RuntimeException {
        Integer maxAllowedDays = config.getInt(PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS, PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS_DEFAULT);

        if (olderThanInDays <= 0 || olderThanInDays > maxAllowedDays) {
            throw restErrorUtil.createRESTException("Invalid parameter: olderThanInDays must be between 1 and " + maxAllowedDays, MessageEnums.INVALID_INPUT_DATA);
        }

        getZoneId(timezone);

        return solrAccessAuditsService.getAuditAccessMetricsByDays(olderThanInDays, timezone);
    }

    private List<String> validateActions(List<String> actions) {
        if (actions == null || actions.isEmpty()) {
            return new ArrayList<>(ALLOWED_ACTIONS);
        }

        if (!ALLOWED_ACTIONS.containsAll(actions)) {
            throw restErrorUtil.createRESTException("Invalid parameter: actions. Allowed values: " + ALLOWED_ACTIONS, MessageEnums.INVALID_INPUT_DATA);
        }

        return actions;
    }

    private Set<Integer> validateObjectClassType(List<String> objectClassTypes) {
        Set<Integer> ret;

        if (objectClassTypes == null || objectClassTypes.isEmpty()) {
            ret = new HashSet<>(ALLOWED_OBJECT_CLASS_TYPES);
        } else {
            try {
                ret = objectClassTypes.stream().map(Integer::valueOf).collect(Collectors.toSet());
            } catch (NumberFormatException e) {
                throw restErrorUtil.createRESTException("Invalid parameter: objectClassTypes must contain numeric values only", MessageEnums.INVALID_INPUT_DATA);
            }

            if (!ALLOWED_OBJECT_CLASS_TYPES.containsAll(ret)) {
                throw restErrorUtil.createRESTException("Invalid parameter: objectClassTypes. Allowed values: " + ALLOWED_OBJECT_CLASS_TYPES, MessageEnums.INVALID_INPUT_DATA);
            }
        }

        return ret;
    }

    private ZoneId getZoneId(String timezone) {
        ZoneId zoneId;

        try {
            zoneId = ZoneId.of(timezone == null || timezone.isEmpty() ? "UTC" : timezone);
        } catch (DateTimeException e) {
            throw restErrorUtil.createRESTException("Invalid parameter: timezone", MessageEnums.INVALID_INPUT_DATA);
        }

        return zoneId;
    }

    public enum Action {
        CREATE, UPDATE, DELETE
    }
}
