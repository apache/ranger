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

import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.entity.XXAuditMetrics;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsText;
import org.apache.ranger.plugin.util.SearchFilter;

public abstract class RangerAuditMetricsServiceBase<T extends XXAuditMetrics, V extends RangerAuditMetrics> extends RangerBaseModelService<T, V> {
    public RangerAuditMetricsServiceBase() {
        super();

        searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE, "obj.serviceType", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "obj.serviceName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

        sortFields.add(new SortField(SearchFilter.CREATE_TIME, "obj.createTime"));
        sortFields.add(new SortField(SearchFilter.UPDATE_TIME, "obj.updateTime"));
        sortFields.add(new SortField(SearchFilter.AUDIT_METRICS_ID, "obj.id", true, SortField.SORT_ORDER.ASC));
    }

    @Override
    protected T mapViewToEntityBean(V vObj, T xObj, int operationContext) {
        XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(vObj.getServiceType());
        if (xServiceDef == null) {
            throw restErrorUtil.createRESTException("No ServiceDefinition found with name :" + vObj.getServiceName(), MessageEnums.INVALID_INPUT_DATA);
        }
        xObj.setServiceType(xServiceDef.getId());
        xObj.setServiceName(vObj.getServiceName());
        xObj.setAppID(vObj.getAppId());
        xObj.setClusterName(vObj.getClusterName());
        xObj.setClientIP(vObj.getclientIP());
        xObj.setThroughPutUnit(vObj.getThroughPutUnit());
        xObj.setNumberOfAudits(vObj.getNumberOfAudits());
        xObj.setMetricsText(JsonUtils.objectToJson(vObj.getMetricsText()));
        return xObj;
    }

    @Override
    protected V mapEntityToViewBean(V vObj, T xObj) {
        XXServiceDef xServiceDef = daoMgr.getXXServiceDef().getById(xObj.getServiceType());
        vObj.setServiceType(xServiceDef.getName());
        vObj.setServiceName(xObj.getServiceName());
        vObj.setAppId(xObj.getAppID());
        vObj.setClusterName(xObj.getClusterName());
        vObj.setclientIP(xObj.getClientIP());
        vObj.setThroughPutUnit(xObj.getThroughPutUnit());
        vObj.setNumberOfAudits(xObj.getNumberOfAudits());
        String                 metricsText            = xObj.getMetricsText();
        RangerAuditMetricsText rangerAuditMetricsText = JsonUtils.jsonToObject(metricsText, RangerAuditMetricsText.class);
        vObj.setMetricsText(rangerAuditMetricsText);
        return vObj;
    }
}
