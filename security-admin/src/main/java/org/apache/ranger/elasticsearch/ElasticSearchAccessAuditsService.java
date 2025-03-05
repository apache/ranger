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

package org.apache.ranger.elasticsearch;

import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.view.VXAccessAudit;
import org.apache.ranger.view.VXAccessAuditList;
import org.apache.ranger.view.VXLong;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Scope("singleton")
public class ElasticSearchAccessAuditsService extends org.apache.ranger.AccessAuditsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchAccessAuditsService.class);

    @Autowired
    ElasticSearchMgr elasticSearchMgr;

    @Autowired
    ElasticSearchUtil elasticSearchUtil;

    public VXAccessAuditList searchXAccessAudits(SearchCriteria searchCriteria) {
        RestHighLevelClient client              = elasticSearchMgr.getClient();
        final boolean       hiveQueryVisibility = PropertiesUtil.getBooleanProperty("ranger.audit.hive.query.visibility", true);

        if (client == null) {
            LOGGER.warn("ElasticSearch client is null, so not running the query.");

            throw restErrorUtil.createRESTException("Error connecting to search engine", MessageEnums.ERROR_SYSTEM);
        }

        List<VXAccessAudit> xAccessAuditList = new ArrayList<>();
        Map<String, Object> paramList        = searchCriteria.getParamList();

        updateUserExclusion(paramList);

        SearchResponse response;

        try {
            response = elasticSearchUtil.searchResources(searchCriteria, searchFields, sortFields, client, elasticSearchMgr.index);
        } catch (IOException e) {
            LOGGER.warn("ElasticSearch query failed: {}", e.getMessage());

            throw restErrorUtil.createRESTException("Error querying search engine", MessageEnums.ERROR_SYSTEM);
        }

        MultiGetItemResponse[] docs;

        try {
            docs = elasticSearchUtil.fetch(client, elasticSearchMgr.index, response.getHits().getHits());
        } catch (IOException e) {
            LOGGER.warn("ElasticSearch fetch failed: {}", e.getMessage());

            throw restErrorUtil.createRESTException("Error querying search engine", MessageEnums.ERROR_SYSTEM);
        }

        for (MultiGetItemResponse doc : docs) {
            VXAccessAudit vXAccessAudit = populateViewBean(doc.getResponse());
            String        serviceType   = vXAccessAudit.getServiceType();
            boolean       isHive        = "hive".equalsIgnoreCase(serviceType);

            if (!hiveQueryVisibility && isHive) {
                vXAccessAudit.setRequestData(null);
            } else if (isHive) {
                String accessType = vXAccessAudit.getAccessType();

                if ("grant".equalsIgnoreCase(accessType) || "revoke".equalsIgnoreCase(accessType)) {
                    String requestData = vXAccessAudit.getRequestData();

                    if (requestData != null) {
                        try {
                            vXAccessAudit.setRequestData(java.net.URLDecoder.decode(requestData, "UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            LOGGER.warn("Error while encoding request data: {}", requestData, e);
                        }
                    } else {
                        LOGGER.warn("Error in request data of audit from elasticSearch. AuditData: {}", vXAccessAudit);
                    }
                }
            }

            xAccessAuditList.add(vXAccessAudit);
        }

        VXAccessAuditList returnList = new VXAccessAuditList();

        returnList.setPageSize(searchCriteria.getMaxRows());
        returnList.setResultSize(response.getHits().getHits().length);

        if (response.getHits().getTotalHits() != null) {
            returnList.setTotalCount(response.getHits().getTotalHits().value);
        }

        returnList.setStartIndex(searchCriteria.getStartIndex());
        returnList.setVXAccessAudits(xAccessAuditList);

        return returnList;
    }

    public void setRestErrorUtil(RESTErrorUtil restErrorUtil) {
        this.restErrorUtil = restErrorUtil;
    }

    /**
     * @param searchCriteria
     * @return
     */
    public VXLong getXAccessAuditSearchCount(SearchCriteria searchCriteria) {
        long   count  = 100;
        VXLong vXLong = new VXLong();

        vXLong.setValue(count);

        return vXLong;
    }

    /**
     * @param doc
     * @return
     */
    private VXAccessAudit populateViewBean(GetResponse doc) {
        LOGGER.debug("doc={}", doc);

        VXAccessAudit       accessAudit = new VXAccessAudit();
        Map<String, Object> source      = doc.getSource();
        Object              value;

        value = source.get("id");
        if (value != null) {
            // TODO: Converting ID to hashcode for now
            accessAudit.setId((long) value.hashCode());
        }

        value = source.get("cluster");
        if (value != null) {
            accessAudit.setClusterName(value.toString());
        }

        value = source.get("zoneName");
        if (value != null) {
            accessAudit.setZoneName(value.toString());
        }

        value = source.get("agentHost");
        if (value != null) {
            accessAudit.setAgentHost(value.toString());
        }

        value = source.get("policyVersion");
        if (value != null) {
            accessAudit.setPolicyVersion(MiscUtil.toLong(value));
        }

        value = source.get("access");
        if (value != null) {
            accessAudit.setAccessType(value.toString());
        }

        value = source.get("enforcer");
        if (value != null) {
            accessAudit.setAclEnforcer(value.toString());
        }

        value = source.get("agent");
        if (value != null) {
            accessAudit.setAgentId(value.toString());
        }

        value = source.get("repo");
        if (value != null) {
            accessAudit.setRepoName(value.toString());

            XXService xxService = daoManager.getXXService().findByName(accessAudit.getRepoName());

            if (xxService != null) {
                accessAudit.setRepoDisplayName(xxService.getDisplayName());
            }
        }

        value = source.get("sess");
        if (value != null) {
            accessAudit.setSessionId(value.toString());
        }

        value = source.get("reqUser");
        if (value != null) {
            accessAudit.setRequestUser(value.toString());
        }

        value = source.get("reqData");
        if (value != null) {
            accessAudit.setRequestData(value.toString());
        }

        value = source.get("resource");
        if (value != null) {
            accessAudit.setResourcePath(value.toString());
        }

        value = source.get("cliIP");
        if (value != null) {
            accessAudit.setClientIP(value.toString());
        }

        // TODO: Need to see what logType maps to in UI
        //value = source.get("logType");
        //if (value != null) {
        //    accessAudit.setAuditType(solrUtil.toInt(value));
        //}

        value = source.get("result");
        if (value != null) {
            accessAudit.setAccessResult(MiscUtil.toInt(value));
        }

        value = source.get("policy");
        if (value != null) {
            accessAudit.setPolicyId(MiscUtil.toLong(value));
        }

        value = source.get("repoType");
        if (value != null) {
            accessAudit.setRepoType(MiscUtil.toInt(value));

            if (null != daoManager) {
                XXServiceDefDao xxServiceDef = daoManager.getXXServiceDef();

                if (xxServiceDef != null) {
                    XXServiceDef xServiceDef = xxServiceDef.getById((long) accessAudit.getRepoType());

                    if (xServiceDef != null) {
                        accessAudit.setServiceType(xServiceDef.getName());
                        accessAudit.setServiceTypeDisplayName(xServiceDef.getDisplayName());
                    }
                }
            }
        }

        value = source.get("resType");
        if (value != null) {
            accessAudit.setResourceType(value.toString());
        }

        value = source.get("reason");
        if (value != null) {
            accessAudit.setResultReason(value.toString());
        }

        value = source.get("action");
        if (value != null) {
            accessAudit.setAction(value.toString());
        }

        value = source.get("evtTime");
        if (value != null) {
            accessAudit.setEventTime(MiscUtil.toLocalDate(value));
        }

        value = source.get("seq_num");
        if (value != null) {
            accessAudit.setSequenceNumber(MiscUtil.toLong(value));
        }

        value = source.get("event_count");
        if (value != null) {
            accessAudit.setEventCount(MiscUtil.toLong(value));
        }

        value = source.get("event_dur_ms");
        if (value != null) {
            accessAudit.setEventDuration(MiscUtil.toLong(value));
        }

        value = source.get("tags");
        if (value != null) {
            accessAudit.setTags(value.toString());
        }

        value = source.get("datasets");
        if (value != null) {
            try {
                accessAudit.setDatasets(JsonUtilsV2.nonSerializableObjToJson(value));
            } catch (Exception e) {
                LOGGER.warn("Failed to convert datasets to json", e);
            }
        }

        value = source.get("projects");
        if (value != null) {
            try {
                accessAudit.setProjects(JsonUtilsV2.nonSerializableObjToJson(value));
            } catch (Exception e) {
                LOGGER.warn("Failed to convert projects to json", e);
            }
        }

        return accessAudit;
    }
}
