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

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPluginServiceVersionInfo;
import org.apache.ranger.plugin.model.RangerPluginServiceVersionInfo;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.Query;

@Service
public class RangerPluginServiceVersionInfoService {

	@Autowired
	RangerSearchUtil searchUtil;

	@Autowired
	RangerBizUtil bizUtil;

	@Autowired
	RangerDaoManager daoManager;

	private List<SortField> sortFields = new ArrayList<SortField>();
	private List<SearchField> searchFields = new ArrayList<SearchField>();

	RangerPluginServiceVersionInfoService() {

		searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "obj.serviceName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.PLUGIN_HOST_NAME, "obj.hostName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.PLUGIN_APP_TYPE, "obj.appType", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.PLUGIN_ENTITY_TYPE, "obj.entityType", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.PLUGIN_IP_ADDRESS, "obj.ipAddress", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

		sortFields.add(new SortField(SearchFilter.SERVICE_NAME, "obj.serviceName", true, SortField.SORT_ORDER.ASC));
		sortFields.add(new SortField(SearchFilter.PLUGIN_HOST_NAME, "obj.hostName", true, SortField.SORT_ORDER.ASC));
		sortFields.add(new SortField(SearchFilter.PLUGIN_ENTITY_TYPE, "obj.entityType", true, SortField.SORT_ORDER.ASC));

	}

	public List<SearchField> getSearchFields() {
		return searchFields;
	}

	public List<SortField> getSortFields() {
		return sortFields;
	}

	public PList<RangerPluginServiceVersionInfo> searchRangerPluginServiceVersionInfo(SearchFilter searchFilter) {
		PList<RangerPluginServiceVersionInfo> retList = new PList<RangerPluginServiceVersionInfo>();
		List<RangerPluginServiceVersionInfo> objList = new ArrayList<RangerPluginServiceVersionInfo>();

		List<XXPluginServiceVersionInfo> xObjList = searchRangerObjects(searchFilter, searchFields, sortFields, retList);

		for (XXPluginServiceVersionInfo xObj : xObjList) {
			RangerPluginServiceVersionInfo obj = populateViewObject(xObj);
			objList.add(obj);
		}

		retList.setList(objList);

		return retList;
	}

	private RangerPluginServiceVersionInfo populateViewObject(XXPluginServiceVersionInfo xObj) {
		RangerPluginServiceVersionInfo ret = new RangerPluginServiceVersionInfo();
		ret.setId(xObj.getId());
		ret.setCreateTime(xObj.getCreateTime());
		ret.setServiceName(xObj.getServiceName());
		ret.setHostName(xObj.getHostName());
		ret.setAppType(xObj.getAppType());
		ret.setIpAddress(xObj.getIpAddress());
		ret.setEntityType(xObj.getEntityType());
		ret.setDownloadedVersion(xObj.getDownloadedVersion());
		ret.setDownloadTime(xObj.getDownloadTime());
		ret.setActiveVersion(xObj.getActiveVersion());
		ret.setActivationTime(xObj.getActivationTime());
		return ret;
	}

	static public XXPluginServiceVersionInfo populateDBObject(RangerPluginServiceVersionInfo modelObj) {
		XXPluginServiceVersionInfo ret = new XXPluginServiceVersionInfo();
		ret.setServiceName(modelObj.getServiceName());
		ret.setHostName(modelObj.getHostName());
		ret.setAppType(modelObj.getAppType());
		ret.setIpAddress(modelObj.getIpAddress());
		ret.setEntityType(modelObj.getEntityType());
		ret.setDownloadedVersion(modelObj.getDownloadedVersion());
		ret.setDownloadTime(modelObj.getDownloadTime());
		ret.setActiveVersion(modelObj.getActiveVersion());
		ret.setActivationTime(modelObj.getActivationTime());
		return ret;
	}
	private List<XXPluginServiceVersionInfo> searchRangerObjects(SearchFilter searchCriteria, List<SearchField> searchFieldList, List<SortField> sortFieldList, PList<RangerPluginServiceVersionInfo> pList) {

		// Get total count of the rows which meet the search criteria
		long count = -1;
		if (searchCriteria.isGetCount()) {
			count = getCountForSearchQuery(searchCriteria, searchFieldList);
			if (count == 0) {
				return Collections.emptyList();
			}
		}

		String sortClause = searchUtil.constructSortClause(searchCriteria, sortFieldList);

		String queryStr = "SELECT obj FROM " + XXPluginServiceVersionInfo.class.getName() + " obj ";
		Query query = createQuery(queryStr, sortClause, searchCriteria, searchFieldList, false);

		List<XXPluginServiceVersionInfo> resultList = daoManager.getXXPluginServiceVersionInfo().executeQueryInSecurityContext(XXPluginServiceVersionInfo.class, query);

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
				searchFieldList, bizUtil.getClassType(XXPluginServiceVersionInfo.class), false, isCountQuery);
	}

	private long getCountForSearchQuery(SearchFilter searchCriteria, List<SearchField> searchFieldList) {

		String countQueryStr = "SELECT COUNT(obj) FROM " + XXPluginServiceVersionInfo.class.getName() + " obj ";

		Query query = createQuery(countQueryStr, null, searchCriteria, searchFieldList, true);
		Long count = daoManager.getXXPluginServiceVersionInfo().executeCountQueryInSecurityContext(XXPluginServiceVersionInfo.class, query);

		if (count == null) {
			return 0;
		}
		return count;
	}
}