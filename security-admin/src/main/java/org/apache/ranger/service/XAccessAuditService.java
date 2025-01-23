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

package org.apache.ranger.service;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.SortField.SORT_ORDER;
import org.apache.ranger.entity.XXAccessAudit;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.view.VXAccessAudit;
import org.apache.ranger.view.VXAccessAuditList;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

@Service
@Scope("singleton")
public class XAccessAuditService extends XAccessAuditServiceBase<XXAccessAudit, VXAccessAudit> {
    public static final String NAME = "XAccessAudit";

    public XAccessAuditService() {
        searchFields.add(new SearchField("accessType", "obj.accessType", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField("aclEnforcer", "obj.aclEnforcer", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField("agentId", "obj.agentId", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField("repoName", "obj.repoName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField("sessionId", "obj.sessionId", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField("requestUser", "obj.requestUser", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField("requestData", "obj.requestData", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField("resourcePath", "obj.resourcePath", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField("clientIP", "obj.clientIP", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));

        searchFields.add(new SearchField("auditType", "obj.auditType", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField("accessResult", "obj.accessResult", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField("assetId", "obj.assetId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField("policyId", "obj.policyId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField("repoType", "obj.repoType", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));

        searchFields.add(new SearchField("startDate", "obj.eventTime", DATA_TYPE.DATE, SEARCH_TYPE.GREATER_EQUAL_THAN));
        searchFields.add(new SearchField("endDate", "obj.eventTime", DATA_TYPE.DATE, SEARCH_TYPE.LESS_EQUAL_THAN));
        searchFields.add(new SearchField("tags", "obj.tags", DATA_TYPE.STRING, SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField("cluster", "obj.cluster", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField("zoneName", "obj.zoneName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField("agentHost", "obj.agentHost", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));

        sortFields.add(new SortField("eventTime", "obj.evtTime", true, SORT_ORDER.DESC));
        sortFields.add(new SortField("policyId", "obj.policy", false, SORT_ORDER.ASC));
        sortFields.add(new SortField("requestUser", "obj.reqUser", false, SORT_ORDER.ASC));
        sortFields.add(new SortField("resourceType", "obj.resType", false, SORT_ORDER.ASC));
        sortFields.add(new SortField("accessType", "obj.access", false, SORT_ORDER.ASC));
        sortFields.add(new SortField("action", "obj.action", false, SORT_ORDER.ASC));
        sortFields.add(new SortField("aclEnforcer", "obj.enforcer", false, SORT_ORDER.ASC));
        sortFields.add(new SortField("zoneName", "obj.zoneName", false, SORT_ORDER.ASC));
        sortFields.add(new SortField("clientIP", "obj.cliIP", false, SORT_ORDER.ASC));
    }

    /**
     * @param searchCriteria
     * @return
     */
    public VXAccessAuditList searchXAccessAudits(SearchCriteria searchCriteria) {
        VXAccessAuditList   returnList          = new VXAccessAuditList();
        List<VXAccessAudit> xAccessAuditList    = new ArrayList<>();
        List<XXAccessAudit> resultList          = searchResources(searchCriteria, searchFields, sortFields, returnList);
        final boolean       hiveQueryVisibility = PropertiesUtil.getBooleanProperty("ranger.audit.hive.query.visibility", true);

        // Iterate over the result list and create the return list
        for (XXAccessAudit gjXAccessAudit : resultList) {
            VXAccessAudit vXAccessAudit = populateViewBean(gjXAccessAudit);

            if (vXAccessAudit != null) {
                if (StringUtils.equalsIgnoreCase(vXAccessAudit.getAclEnforcer(), RangerHadoopConstants.DEFAULT_XASECURE_MODULE_ACL_NAME)) {
                    vXAccessAudit.setAclEnforcer(RangerHadoopConstants.DEFAULT_RANGER_MODULE_ACL_NAME);
                }

                if (!hiveQueryVisibility && "hive".equalsIgnoreCase(vXAccessAudit.getServiceType())) {
                    vXAccessAudit.setRequestData(null);
                } else if ("hive".equalsIgnoreCase(vXAccessAudit.getServiceType()) && ("grant".equalsIgnoreCase(vXAccessAudit.getAccessType()) || "revoke".equalsIgnoreCase(vXAccessAudit.getAccessType()))) {
                    try {
                        vXAccessAudit.setRequestData(java.net.URLDecoder.decode(vXAccessAudit.getRequestData(), "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        logger.warn("Error while encoding request data");
                    }
                }

                xAccessAuditList.add(vXAccessAudit);
            }
        }

        returnList.setVXAccessAudits(xAccessAuditList);

        return returnList;
    }

    protected XXAccessAudit mapViewToEntityBean(VXAccessAudit vObj, XXAccessAudit mObj, int operationContext) {
        mObj.setId(vObj.getId());
        mObj.setAuditType(vObj.getAuditType());
        mObj.setAccessResult(vObj.getAccessResult());
        mObj.setAccessType(vObj.getAccessType());
        mObj.setAclEnforcer(vObj.getAclEnforcer());
        mObj.setAgentId(vObj.getAgentId());
        mObj.setPolicyId(vObj.getPolicyId());
        mObj.setRepoName(vObj.getRepoName());
        mObj.setRepoType(vObj.getRepoType());
        mObj.setResultReason(vObj.getResultReason());
        mObj.setSessionId(vObj.getSessionId());
        mObj.setEventTime(vObj.getEventTime());
        mObj.setRequestUser(vObj.getRequestUser());
        mObj.setRequestData(vObj.getRequestData());
        mObj.setResourcePath(vObj.getResourcePath());
        mObj.setResourceType(vObj.getResourceType());
        mObj.setClientIP(vObj.getClientIP());
        mObj.setClientType(vObj.getClientType());
        mObj.setSequenceNumber(vObj.getSequenceNumber());
        mObj.setEventCount(vObj.getEventCount());
        mObj.setEventDuration(vObj.getEventDuration());
        mObj.setTags(vObj.getTags());

        return mObj;
    }

    protected VXAccessAudit mapEntityToViewBean(VXAccessAudit vObj, XXAccessAudit mObj) {
        vObj.setAuditType(mObj.getAuditType());
        vObj.setAccessResult(mObj.getAccessResult());
        vObj.setAccessType(mObj.getAccessType());
        vObj.setAclEnforcer(mObj.getAclEnforcer());
        vObj.setAgentId(mObj.getAgentId());
        vObj.setPolicyId(mObj.getPolicyId());
        vObj.setRepoName(mObj.getRepoName());
        vObj.setRepoType(mObj.getRepoType());
        vObj.setResultReason(mObj.getResultReason());
        vObj.setSessionId(mObj.getSessionId());
        vObj.setEventTime(mObj.getEventTime());
        vObj.setRequestUser(mObj.getRequestUser());
        vObj.setRequestData(mObj.getRequestData());
        vObj.setResourcePath(mObj.getResourcePath());
        vObj.setResourceType(mObj.getResourceType());
        vObj.setClientIP(mObj.getClientIP());
        vObj.setClientType(mObj.getClientType());
        vObj.setSequenceNumber(mObj.getSequenceNumber());
        vObj.setEventCount(mObj.getEventCount());
        vObj.setEventDuration(mObj.getEventDuration());
        vObj.setTags(mObj.getTags());

        XXService xService = daoManager.getXXService().findByName(mObj.getRepoName());

        if (xService != null) {
            vObj.setRepoDisplayName(xService.getDisplayName());

            XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());

            if (xServiceDef != null) {
                vObj.setServiceType(xServiceDef.getName());
                vObj.setServiceTypeDisplayName(xServiceDef.getDisplayName());
            }
        }

        return vObj;
    }

    public VXAccessAudit populateViewBean(XXAccessAudit gjXAccessAudit) {
        VXAccessAudit vXAccessAudit = new VXAccessAudit();

        return mapEntityToViewBean(vXAccessAudit, gjXAccessAudit);
    }

    @Override
    protected void validateForCreate(VXAccessAudit viewBaseBean) {
        // TODO Auto-generated method stub
    }

    @Override
    protected void validateForUpdate(VXAccessAudit viewBaseBean, XXAccessAudit t) {
        // TODO Auto-generated method stub
    }
}
