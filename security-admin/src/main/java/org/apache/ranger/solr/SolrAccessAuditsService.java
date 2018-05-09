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

package org.apache.ranger.solr;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.common.SortField.SORT_ORDER;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.view.VXAccessAudit;
import org.apache.ranger.view.VXAccessAuditList;
import org.apache.ranger.view.VXLong;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class SolrAccessAuditsService {
	private static final Logger logger = Logger.getLogger(SolrAccessAuditsService.class);

	@Autowired
	SolrMgr solrMgr;

	@Autowired
	SolrUtil solrUtil;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	RangerDaoManager daoManager;

	public List<SortField> sortFields = new ArrayList<SortField>();
	public List<SearchField> searchFields = new ArrayList<SearchField>();

	public SolrAccessAuditsService() {

		searchFields.add(new SearchField("id", "id",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("accessType", "access",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("aclEnforcer", "enforcer",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("agentId", "agent",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("repoName", "repo",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("sessionId", "sess",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("requestUser", "reqUser",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("requestData", "reqData",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("resourcePath", "resource",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("clientIP", "cliIP",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

		searchFields.add(new SearchField("auditType", "logType",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("accessResult", "result",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		// searchFields.add(new SearchField("assetId", "obj.assetId",
		// SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("policyId", "policy",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("repoType", "repoType",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
                searchFields.add(new SearchField("-repoType", "-repoType",
                                SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("resourceType", "resType",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("reason", "reason",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("action", "action",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

		searchFields.add(new SearchField("startDate", "evtTime",
				DATA_TYPE.DATE, SEARCH_TYPE.GREATER_EQUAL_THAN));
		searchFields.add(new SearchField("endDate", "evtTime", DATA_TYPE.DATE,
				SEARCH_TYPE.LESS_EQUAL_THAN));

		searchFields.add(new SearchField("tags", "tags", DATA_TYPE.STRING, SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("cluster", "cluster",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

		sortFields.add(new SortField("eventTime", "evtTime", true,
				SORT_ORDER.DESC));
	}

	public VXAccessAuditList searchXAccessAudits(SearchCriteria searchCriteria) {

		// Make call to Solr
		SolrClient solrClient = solrMgr.getSolrClient();
                final boolean hiveQueryVisibility = PropertiesUtil.getBooleanProperty("ranger.audit.hive.query.visibility", true);
		if (solrClient == null) {
			logger.warn("Solr client is null, so not running the query.");
			throw restErrorUtil.createRESTException(
					"Error connecting to search engine",
					MessageEnums.ERROR_SYSTEM);
		}

		List<VXAccessAudit> xAccessAuditList = new ArrayList<VXAccessAudit>();

		QueryResponse response = solrUtil.searchResources(searchCriteria,
				searchFields, sortFields, solrClient);
		SolrDocumentList docs = response.getResults();
		for (int i = 0; i < docs.size(); i++) {
			SolrDocument doc = docs.get(i);
			VXAccessAudit vXAccessAudit = populateViewBean(doc);
                        if (vXAccessAudit != null) {
                                if (!hiveQueryVisibility && "hive".equalsIgnoreCase(vXAccessAudit.getServiceType())) {
                                        vXAccessAudit.setRequestData(null);
                                }
                                else if("hive".equalsIgnoreCase(vXAccessAudit.getServiceType()) && "grant".equalsIgnoreCase(vXAccessAudit.getAccessType()) || "revoke".equalsIgnoreCase(vXAccessAudit.getAccessType())){
                                        try {
                                                vXAccessAudit.setRequestData(java.net.URLDecoder.decode(vXAccessAudit.getRequestData(), "UTF-8"));
                                        } catch (UnsupportedEncodingException e) {
                                                logger.warn("Error while encoding request data");
                                        }
                                }
                        }
			xAccessAuditList.add(vXAccessAudit);
		}

		VXAccessAuditList returnList = new VXAccessAuditList();
		returnList.setPageSize(searchCriteria.getMaxRows());
		returnList.setResultSize(docs.size());
		returnList.setTotalCount((int) docs.getNumFound());
		returnList.setStartIndex((int) docs.getStart());
		returnList.setVXAccessAudits(xAccessAuditList);
		return returnList;
	}

	/**
	 * @param doc
	 * @return
	 */
	private VXAccessAudit populateViewBean(SolrDocument doc) {
		VXAccessAudit accessAudit = new VXAccessAudit();
		Object value = null;
		if(logger.isDebugEnabled()) {
			logger.debug("doc=" + doc.toString());
		}

		value = doc.getFieldValue("id");
		if (value != null) {
			// TODO: Converting ID to hashcode for now
			accessAudit.setId((long) value.hashCode());
		}
		
		value = doc.getFieldValue("cluster");
		if (value != null) {
			accessAudit.setClusterName(value.toString());
		}
		
		value = doc.getFieldValue("access");
		if (value != null) {
			accessAudit.setAccessType(value.toString());
		}

		value = doc.getFieldValue("enforcer");
		if (value != null) {
			accessAudit.setAclEnforcer(value.toString());
		}
		value = doc.getFieldValue("agent");
		if (value != null) {
			accessAudit.setAgentId(value.toString());
		}
		value = doc.getFieldValue("repo");
		if (value != null) {
			accessAudit.setRepoName(value.toString());
		}
		value = doc.getFieldValue("sess");
		if (value != null) {
			accessAudit.setSessionId(value.toString());
		}
		value = doc.getFieldValue("reqUser");
		if (value != null) {
			accessAudit.setRequestUser(value.toString());
		}
		value = doc.getFieldValue("reqData");
		if (value != null) {
			accessAudit.setRequestData(value.toString());
		}
		value = doc.getFieldValue("resource");
		if (value != null) {
			accessAudit.setResourcePath(value.toString());
		}
		value = doc.getFieldValue("cliIP");
		if (value != null) {
			accessAudit.setClientIP(value.toString());
		}
		value = doc.getFieldValue("logType");
		//if (value != null) {
			// TODO: Need to see what logType maps to in UI
//			accessAudit.setAuditType(solrUtil.toInt(value));
		//}
		value = doc.getFieldValue("result");
		if (value != null) {
			accessAudit.setAccessResult(solrUtil.toInt(value));
		}
		value = doc.getFieldValue("policy");
		if (value != null) {
			accessAudit.setPolicyId(solrUtil.toLong(value));
		}
		value = doc.getFieldValue("repoType");
		if (value != null) {
			accessAudit.setRepoType(solrUtil.toInt(value));
			XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById((long) accessAudit.getRepoType());
			if (xServiceDef != null) {
				accessAudit.setServiceType(xServiceDef.getName());
			}
		}
		value = doc.getFieldValue("resType");
		if (value != null) {
			accessAudit.setResourceType(value.toString());
		}
		value = doc.getFieldValue("reason");
		if (value != null) {
			accessAudit.setResultReason(value.toString());
		}
		value = doc.getFieldValue("action");
		if (value != null) {
			accessAudit.setAction(value.toString());
		}
		value = doc.getFieldValue("evtTime");
		if (value != null) {
			accessAudit.setEventTime(solrUtil.toDate(value));
		}
		value = doc.getFieldValue("seq_num");
		if (value != null) {
			accessAudit.setSequenceNumber(solrUtil.toLong(value));
		}
		value = doc.getFieldValue("event_count");
		if (value != null) {
			accessAudit.setEventCount(solrUtil.toLong(value));
		}
		value = doc.getFieldValue("event_dur_ms");
		if (value != null) {
			accessAudit.setEventDuration(solrUtil.toLong(value));
		}
		value = doc.getFieldValue("tags");
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
