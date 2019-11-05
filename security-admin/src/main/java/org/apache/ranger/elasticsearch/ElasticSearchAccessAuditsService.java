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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.common.*;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.common.SortField.SORT_ORDER;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.view.VXAccessAudit;
import org.apache.ranger.view.VXAccessAuditList;
import org.apache.ranger.view.VXLong;
//import org.apache.solr.client.solrj.SolrClient;
//import org.apache.solr.client.solrj.response.QueryResponse;
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrDocumentList;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

@Service
@Scope("singleton")
public class ElasticSearchAccessAuditsService {
	private static final Logger LOGGER = Logger.getLogger(ElasticSearchAccessAuditsService.class);

	@Autowired
	ElasticSearchMgr elasticSearchMgr;

	@Autowired
	ElasticSearchUtil elasticSearchUtil;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	RangerDaoManager daoManager;

	private List<SortField> sortFields = new ArrayList<SortField>();
	private List<SearchField> searchFields = new ArrayList<SearchField>();

	public ElasticSearchAccessAuditsService() {

		searchFields.add(new SearchField("id", "id",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("accessType", "access",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("aclEnforcer", "enforcer",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("agentId", "agent",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("repoName", "repo",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("sessionId", "sess",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("requestUser", "reqUser",
			DATA_TYPE.STR_LIST, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("excludeUser", "exlUser",
			DATA_TYPE.STR_LIST, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("requestData", "reqData", DATA_TYPE.STRING,
				SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("resourcePath", "resource", DATA_TYPE.STRING,
				SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("clientIP", "cliIP",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));

		searchFields.add(new SearchField("auditType", "logType",
				DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("accessResult", "result",
				DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		// searchFields.add(new SearchField("assetId", "obj.assetId",
		// SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("policyId", "policy",
				DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("repoType", "repoType",
				DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
                searchFields.add(new SearchField("-repoType", "-repoType",
        DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
                searchFields.add(new SearchField("-requestUser", "-reqUser",
        		DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("resourceType", "resType",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("reason", "reason",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("action", "action",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));

		searchFields.add(new SearchField("startDate", "evtTime",
				DATA_TYPE.DATE, SEARCH_TYPE.GREATER_EQUAL_THAN));
		searchFields.add(new SearchField("endDate", "evtTime", DATA_TYPE.DATE,
				SEARCH_TYPE.LESS_EQUAL_THAN));

		searchFields.add(new SearchField("tags", "tags", DATA_TYPE.STRING, SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("cluster", "cluster",
				DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("zoneName", "zoneName",
				DATA_TYPE.STR_LIST, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("agentHost", "agentHost",
				DATA_TYPE.STRING, SEARCH_TYPE.PARTIAL));

		sortFields.add(new SortField("eventTime", "evtTime", true,
				SORT_ORDER.DESC));
	}


	public VXAccessAuditList searchXAccessAudits(SearchCriteria searchCriteria) {

		RestHighLevelClient client = elasticSearchMgr.getClient();
		final boolean hiveQueryVisibility = PropertiesUtil.getBooleanProperty("ranger.audit.hive.query.visibility", true);
		if (client == null) {
			LOGGER.warn("ElasticSearch client is null, so not running the query.");
			throw restErrorUtil.createRESTException(
					"Error connecting to search engine",
					MessageEnums.ERROR_SYSTEM);
		}
		List<VXAccessAudit> xAccessAuditList = new ArrayList<VXAccessAudit>();
		Map<String, Object> paramList = searchCriteria.getParamList();
		updateUserExclusion(paramList);

		SearchResponse response;
		try {
			response = elasticSearchUtil.searchResources(searchCriteria, searchFields, sortFields, client);
		} catch (IOException e) {
			LOGGER.warn("ElasticSearch query failed.");
			throw restErrorUtil.createRESTException(
					"Error querying search engine",
					MessageEnums.ERROR_SYSTEM);
		}
		SearchHit[] docs = response.getHits().getHits();
		for (int i = 0; i < docs.length; i++) {
			SearchHit doc = docs[i];
			VXAccessAudit vXAccessAudit = populateViewBean(doc);
                        if (vXAccessAudit != null) {
                                if (!hiveQueryVisibility && "hive".equalsIgnoreCase(vXAccessAudit.getServiceType())) {
                                        vXAccessAudit.setRequestData(null);
                                }
                                else if("hive".equalsIgnoreCase(vXAccessAudit.getServiceType()) && ("grant".equalsIgnoreCase(vXAccessAudit.getAccessType()) || "revoke".equalsIgnoreCase(vXAccessAudit.getAccessType()))){
                                        try {
                                            if (vXAccessAudit.getRequestData() != null) {
                                                vXAccessAudit.setRequestData(java.net.URLDecoder.decode(vXAccessAudit.getRequestData(), "UTF-8"));
                                            } else {
                                                LOGGER.warn("Error in request data of audit from solr. AuditData: "  + vXAccessAudit.toString());
                                            }
                                        } catch (UnsupportedEncodingException e) {
                                                LOGGER.warn("Error while encoding request data");
                                        }
                                }
                        }
                        xAccessAuditList.add(vXAccessAudit);
		}

		VXAccessAuditList returnList = new VXAccessAuditList();
		returnList.setPageSize(searchCriteria.getMaxRows());
		returnList.setResultSize(docs.length);
		
		returnList.setTotalCount((int) response.getHits().getTotalHits().value);
		//returnList.setStartIndex((int) response.getHits().getTotalHits().value);
		returnList.setVXAccessAudits(xAccessAuditList);
		return returnList;
	}


	private void updateUserExclusion(Map<String, Object> paramList) {
		String val = (String) paramList.get("excludeServiceUser");

		if (val != null && Boolean.valueOf(val.trim())) { // add param to negate requestUsers which will be added as
			// filter query in solr
			List<String> excludeUsersList = getExcludeUsersList();
			if (CollectionUtils.isNotEmpty(excludeUsersList)) {
				Object oldUserExclusions = paramList.get("-requestUser");
				if (oldUserExclusions instanceof Collection && (!((Collection<?>)oldUserExclusions).isEmpty())) {
					excludeUsersList.addAll((Collection<String>)oldUserExclusions);
					paramList.put("-requestUser", excludeUsersList);
				} else {
					paramList.put("-requestUser", excludeUsersList);
				}
			}
		}
	}

	private List<String> getExcludeUsersList() {
		//for excluding serviceUsers using existing property in ranger-admin-site
		List<String> excludeUsersList = new ArrayList<String>(getServiceUserList());

		//for excluding additional users using new property in ranger-admin-site
		String additionalExcludeUsers = PropertiesUtil.getProperty("ranger.accesslogs.exclude.users.list");
		List<String> additionalExcludeUsersList = null;
		if (StringUtils.isNotBlank(additionalExcludeUsers)) {
			additionalExcludeUsersList = new ArrayList<>(Arrays.asList(StringUtils.split(additionalExcludeUsers, ",")));
			for (String serviceUser : additionalExcludeUsersList) {
				if (StringUtils.isNotBlank(serviceUser) && !excludeUsersList.contains(serviceUser.trim())) {
					excludeUsersList.add(serviceUser);
				}
			}
		}
		return excludeUsersList;
	}

	private List<String> getServiceUserList() {
		String components = EmbeddedServiceDefsUtil.DEFAULT_BOOTSTRAP_SERVICEDEF_LIST;
		List<String> serviceUsersList = new ArrayList<String>();
		List<String> componentNames =  Arrays.asList(StringUtils.split(components,","));
		for(String componentName : componentNames) {
			String serviceUser = PropertiesUtil.getProperty("ranger.plugins."+componentName+".serviceuser");
			if(StringUtils.isNotBlank(serviceUser)) {
				serviceUsersList.add(serviceUser);
			}
		}
		return serviceUsersList;
	}

	/**
	 * @param doc
	 * @return
	 */
	private VXAccessAudit populateViewBean(SearchHit doc) {
		VXAccessAudit accessAudit = new VXAccessAudit();

		Object value = null;
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("doc=" + doc.toString());
		}

		value = doc.field("id");
		if (value != null) {
			// TODO: Converting ID to hashcode for now
			accessAudit.setId((long) value.hashCode());
		}

		value = doc.field("cluster");
		if (value != null) {
			accessAudit.setClusterName(value.toString());
		}

		value = doc.field("zoneName");
		if (value != null) {
			accessAudit.setZoneName(value.toString());
		}

		value = doc.field("agentHost");
		if (value != null) {
			accessAudit.setAgentHost(value.toString());
		}

		value = doc.field("policyVersion");
		if (value != null) {
			accessAudit.setPolicyVersion(elasticSearchUtil.toLong(value));
		}

		value = doc.field("access");
		if (value != null) {
			accessAudit.setAccessType(value.toString());
		}

		value = doc.field("enforcer");
		if (value != null) {
			accessAudit.setAclEnforcer(value.toString());
		}
		value = doc.field("agent");
		if (value != null) {
			accessAudit.setAgentId(value.toString());
		}
		value = doc.field("repo");
		if (value != null) {
			accessAudit.setRepoName(value.toString());
		}
		value = doc.field("sess");
		if (value != null) {
			accessAudit.setSessionId(value.toString());
		}
		value = doc.field("reqUser");
		if (value != null) {
			accessAudit.setRequestUser(value.toString());
		}
		value = doc.field("reqData");
		if (value != null) {
			accessAudit.setRequestData(value.toString());
		}
		value = doc.field("resource");
		if (value != null) {
			accessAudit.setResourcePath(value.toString());
		}
		value = doc.field("cliIP");
		if (value != null) {
			accessAudit.setClientIP(value.toString());
		}
		value = doc.field("logType");
		//if (value != null) {
			// TODO: Need to see what logType maps to in UI
//			accessAudit.setAuditType(solrUtil.toInt(value));
		//}
		value = doc.field("result");
		if (value != null) {
			accessAudit.setAccessResult(elasticSearchUtil.toInt(value));
		}
		value = doc.field("policy");
		if (value != null) {
			accessAudit.setPolicyId(elasticSearchUtil.toLong(value));
		}
		value = doc.field("repoType");
		if (value != null) {
			accessAudit.setRepoType(elasticSearchUtil.toInt(value));
			XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById((long) accessAudit.getRepoType());
			if (xServiceDef != null) {
				accessAudit.setServiceType(xServiceDef.getName());
			}
		}
		value = doc.field("resType");
		if (value != null) {
			accessAudit.setResourceType(value.toString());
		}
		value = doc.field("reason");
		if (value != null) {
			accessAudit.setResultReason(value.toString());
		}
		value = doc.field("action");
		if (value != null) {
			accessAudit.setAction(value.toString());
		}
		value = doc.field("evtTime");
		if (value != null) {
			accessAudit.setEventTime(elasticSearchUtil.toDate(value));
		}
		value = doc.field("seq_num");
		if (value != null) {
			accessAudit.setSequenceNumber(elasticSearchUtil.toLong(value));
		}
		value = doc.field("event_count");
		if (value != null) {
			accessAudit.setEventCount(elasticSearchUtil.toLong(value));
		}
		value = doc.field("event_dur_ms");
		if (value != null) {
			accessAudit.setEventDuration(elasticSearchUtil.toLong(value));
		}
		value = doc.field("tags");
		if (value != null) {
			accessAudit.setTags(value.toString());
		}
		return accessAudit;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXLong getXAccessAuditSearchCount(SearchCriteria searchCriteria) {
		long count = 100;

		VXLong vXLong = new VXLong();
		vXLong.setValue(count);
		return vXLong;
	}

}
