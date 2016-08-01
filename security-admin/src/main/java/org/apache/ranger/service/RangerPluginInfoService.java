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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPluginInfo;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.Query;

@Service
public class RangerPluginInfoService {

	@Autowired
	RangerSearchUtil searchUtil;

	@Autowired
	RangerBizUtil bizUtil;

	@Autowired
	JSONUtil jsonUtil;

	@Autowired
	RangerDaoManager daoManager;

	private List<SortField> sortFields = new ArrayList<SortField>();
	private List<SearchField> searchFields = new ArrayList<SearchField>();

	RangerPluginInfoService() {

		searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "obj.serviceName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.PLUGIN_HOST_NAME, "obj.hostName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.PLUGIN_APP_TYPE, "obj.appType", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.PLUGIN_IP_ADDRESS, "obj.ipAddress", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

		sortFields.add(new SortField(SearchFilter.SERVICE_NAME, "obj.serviceName", true, SortField.SORT_ORDER.ASC));
		sortFields.add(new SortField(SearchFilter.PLUGIN_HOST_NAME, "obj.hostName", true, SortField.SORT_ORDER.ASC));
		sortFields.add(new SortField(SearchFilter.PLUGIN_APP_TYPE, "obj.appType", true, SortField.SORT_ORDER.ASC));

	}

	public List<SearchField> getSearchFields() {
		return searchFields;
	}

	public List<SortField> getSortFields() {
		return sortFields;
	}

	public PList<RangerPluginInfo> searchRangerPluginInfo(SearchFilter searchFilter) {
		PList<RangerPluginInfo> retList = new PList<RangerPluginInfo>();
		List<RangerPluginInfo> objList = new ArrayList<RangerPluginInfo>();

		List<XXPluginInfo> xObjList = searchRangerObjects(searchFilter, searchFields, sortFields, retList);

		for (XXPluginInfo xObj : xObjList) {
			RangerPluginInfo obj = populateViewObject(xObj);
			objList.add(obj);
		}

		retList.setList(objList);

		return retList;
	}

	public RangerPluginInfo populateViewObject(XXPluginInfo xObj) {
		RangerPluginInfo ret = new RangerPluginInfo();
		ret.setId(xObj.getId());
		ret.setCreateTime(xObj.getCreateTime());
		ret.setUpdateTime(xObj.getUpdateTime());
		ret.setServiceName(xObj.getServiceName());
		ret.setHostName(xObj.getHostName());
		ret.setAppType(xObj.getAppType());
		ret.setIpAddress(xObj.getIpAddress());
		ret.setInfo(jsonStringToMap(xObj.getInfo()));
		return ret;
	}

	public XXPluginInfo populateDBObject(RangerPluginInfo modelObj) {
		XXPluginInfo ret = new XXPluginInfo();
		ret.setId(modelObj.getId());
		ret.setCreateTime(modelObj.getCreateTime());
		ret.setUpdateTime(modelObj.getUpdateTime());
		ret.setServiceName(modelObj.getServiceName());
		ret.setHostName(modelObj.getHostName());
		ret.setAppType(modelObj.getAppType());
		ret.setIpAddress(modelObj.getIpAddress());
		ret.setInfo(mapToJsonString(modelObj.getInfo()));
		return ret;
	}
	private List<XXPluginInfo> searchRangerObjects(SearchFilter searchCriteria, List<SearchField> searchFieldList, List<SortField> sortFieldList, PList<RangerPluginInfo> pList) {

		// Get total count of the rows which meet the search criteria
		long count = -1;
		if (searchCriteria.isGetCount()) {
			count = getCountForSearchQuery(searchCriteria, searchFieldList);
			if (count == 0) {
				return Collections.emptyList();
			}
		}

		String sortClause = searchUtil.constructSortClause(searchCriteria, sortFieldList);

		String queryStr = "SELECT obj FROM " + XXPluginInfo.class.getName() + " obj ";
		Query query = createQuery(queryStr, sortClause, searchCriteria, searchFieldList, false);

		List<XXPluginInfo> resultList = daoManager.getXXPluginInfo().executeQueryInSecurityContext(XXPluginInfo.class, query);

		if (pList != null) {
			pList.setResultSize(resultList.size());
			pList.setPageSize(query.getMaxResults());
			pList.setSortBy(searchCriteria.getSortBy());
			pList.setSortType(searchCriteria.getSortType());
			pList.setStartIndex(query.getFirstResult());
			pList.setTotalCount(count);
		}
		return resultList;
	}

	private Query createQuery(String searchString, String sortString, SearchFilter searchCriteria,
								List<SearchField> searchFieldList, boolean isCountQuery) {

		EntityManager em = daoManager.getEntityManager();
		return searchUtil.createSearchQuery(em, searchString, sortString, searchCriteria,
				searchFieldList, bizUtil.getClassType(XXPluginInfo.class), false, isCountQuery);
	}

	private long getCountForSearchQuery(SearchFilter searchCriteria, List<SearchField> searchFieldList) {

		String countQueryStr = "SELECT COUNT(obj) FROM " + XXPluginInfo.class.getName() + " obj ";

		Query query = createQuery(countQueryStr, null, searchCriteria, searchFieldList, true);
		Long count = daoManager.getXXPluginInfo().executeCountQueryInSecurityContext(XXPluginInfo.class, query);

		if (count == null) {
			return 0;
		}
		return count;
	}

	private String mapToJsonString(Map<String, String> map) {
		String ret = null;

		if(map != null) {
			try {
				ret = jsonUtil.readMapToString(map);
			} catch(Exception excp) {
			}
		}

		return ret;
	}

	private Map<String, String> jsonStringToMap(String jsonStr) {
		Map<String, String> ret = null;

			try {
				ret = jsonUtil.jsonToMap(jsonStr);
			} catch(Exception excp) {

			}


		return ret;
	}
}