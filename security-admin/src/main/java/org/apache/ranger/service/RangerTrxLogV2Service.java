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

/**
 *
 */

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.common.*;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXTrxLogV2;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.view.VXTrxLogV2;
import org.apache.ranger.view.VXTrxLogV2.ObjectChangeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Scope("singleton")
public class RangerTrxLogV2Service {
	private static final Logger LOG = LoggerFactory.getLogger(RangerTrxLogV2Service.class);

	@Autowired
	RangerSearchUtil searchUtil;

	@Autowired
	RangerDaoManager daoManager;

	@Autowired
	RESTErrorUtil restErrorUtil;

	private final List<SortField>   sortFields   = new ArrayList<>();
	private final List<SearchField> searchFields = new ArrayList<>();

	public RangerTrxLogV2Service() {
		searchFields.add(new SearchField("attributeName",   "obj.changeInfo",      SearchField.DATA_TYPE.STRING,   SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("action",          "obj.action",          SearchField.DATA_TYPE.STRING,   SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("sessionId",       "obj.sessionId",       SearchField.DATA_TYPE.STRING,   SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("startDate",       "obj.createTime",      SearchField.DATA_TYPE.DATE,     SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN));
		searchFields.add(new SearchField("endDate",         "obj.createTime",      SearchField.DATA_TYPE.DATE,     SearchField.SEARCH_TYPE.LESS_EQUAL_THAN));
		searchFields.add(new SearchField("owner",           "obj.addedByUserId",   SearchField.DATA_TYPE.INT_LIST, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("objectClassType", "obj.objectClassType", SearchField.DATA_TYPE.INT_LIST, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("objectId",        "obj.objectId",        SearchField.DATA_TYPE.INT_LIST, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("parentObjectId",  "obj.parentObjectId",  SearchField.DATA_TYPE.INT_LIST, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("parentObjectName","obj.parentObjectName",SearchField.DATA_TYPE.STRING,   SearchField.SEARCH_TYPE.FULL));

		sortFields.add(new SortField("id", "obj.id", true, SortField.SORT_ORDER.DESC));
		sortFields.add(new SortField("createDate", "obj.createTime", true, SortField.SORT_ORDER.DESC));
	}

	public List<SearchField> getSearchFields() {
		return searchFields;
	}

	public List<SortField> getSortFields() {
		return sortFields;
	}

	public PList<VXTrxLogV2> searchTrxLogs(SearchCriteria searchCriteria) {
		PList<VXTrxLogV2> ret          = new PList<>();
		List<XXTrxLogV2>  resultList   = searchTrxLogs(searchCriteria, ret);
		Map<Long, String> uidNameCache = new HashMap<>();
		List<VXTrxLogV2>  objList      = resultList.stream().map(xTrxLog -> toViewObject(xTrxLog, uidNameCache)).collect(Collectors.toList());

		ret.setList(objList);

		return ret;
	}

	public long getTrxLogsCount(SearchCriteria searchCriteria) {
		Map<String, Object> params = new HashMap<>();
		UserSessionBase session = ContextUtil.getCurrentUserSession();

		if (session != null && (session.isKeyAdmin() || session.isAuditKeyAdmin())) {
			searchFields.stream().filter(field -> "parentObjectName".equals(field.getClientFieldName())).findFirst()
					.ifPresent(parentObjNameField -> parentObjNameField.setCustomCondition(applyKeyAdminAccessFilters(params)));
			searchCriteria.addParam("parentObjectName", "");
		}

		String countQueryStr = "SELECT COUNT(obj) FROM " + XXTrxLogV2.class.getName() + " obj ";
		Query  query         = createQuery(countQueryStr, null, searchCriteria, searchFields, true);

		if (!params.isEmpty()) {
			params.forEach(query::setParameter);
		}

		Long   count         = daoManager.getXXTrxLogV2().executeCountQueryInSecurityContext(XXTrxLogV2.class, query);

		return count == null ? 0 : count;
	}

	public List<VXTrxLogV2> findByTransactionId(String transactionId) {
		final List<VXTrxLogV2> ret;
		final List<XXTrxLogV2> trxLogsV2 = daoManager.getXXTrxLogV2().findByTransactionId(transactionId);

		if (trxLogsV2 != null && !trxLogsV2.isEmpty()) {
			Map<Long, String> uidNameCache = new HashMap<>();

			UserSessionBase session = ContextUtil.getCurrentUserSession();
			if (session != null && (session.isKeyAdmin() || session.isAuditKeyAdmin())) {
				ret = trxLogsV2.stream().filter(xTrxLog -> getValidTrxLogsForKeyAdminAndAuditor(xTrxLog))
						.map(xTrxLog -> toViewObject(xTrxLog, uidNameCache)).collect(Collectors.toList());
			} else {
				ret = trxLogsV2.stream().map(xTrxLog -> toViewObject(xTrxLog, uidNameCache))
						.collect(Collectors.toList());
			}
		} else {
			ret = Collections.emptyList();
		}

		return ret;
	}

	public VXTrxLogV2 createResource(VXTrxLogV2 trxLog) {
		XXTrxLogV2 dbObj    = trxLog != null ? toDBObject(trxLog) : null;
		XXTrxLogV2 savedObj = dbObj != null ? daoManager.getXXTrxLogV2().create(dbObj) : null;
		VXTrxLogV2 ret      = savedObj != null ? toViewObject(savedObj, null) : null;

		if (LOG.isDebugEnabled()) {
			LOG.debug("createResource(" + trxLog + "): ret=" + ret);
		}

		return ret;
	}

	public VXTrxLogV2 readResource(Long id) {
		XXTrxLogV2 dbObj = id != null ? daoManager.getXXTrxLogV2().getById(id) : null;
		VXTrxLogV2 ret   = dbObj != null ? toViewObject(dbObj, null) : null;

		if (ret != null) {
			UserSessionBase session = ContextUtil.getCurrentUserSession();
			if (session != null && (session.isKeyAdmin() || session.isAuditKeyAdmin())) {
				if (!getValidTrxLogsForKeyAdminAndAuditor(dbObj)) {
					return null;
				}
			}
		} else {
			throw restErrorUtil.create404RESTException("Object not found", MessageEnums.DATA_NOT_FOUND, id, null, "readResource : No Object found with given id.");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("readResource(" + id + "): ret=" + ret);
		}

		return ret;
	}

	public VXTrxLogV2 updateResource(VXTrxLogV2 trxLog) {
		XXTrxLogV2 dbObj    = trxLog != null ? toDBObject(trxLog) : null;
		XXTrxLogV2 savedObj = dbObj != null ? daoManager.getXXTrxLogV2().update(dbObj) : null;
		VXTrxLogV2 ret      = savedObj != null ? toViewObject(savedObj, null) : null;

		if (LOG.isDebugEnabled()) {
			LOG.debug("updateResource(" + trxLog + "): ret=" + ret);
		}

		return ret;
	}

	public boolean deleteResource(Long id) {
		boolean ret = id != null && daoManager.getXXTrxLogV2().remove(id);

		if (LOG.isDebugEnabled()) {
			LOG.debug("deleteResource(" + id + "): ret=" + ret);
		}

		return ret;
	}

	private List<XXTrxLogV2> searchTrxLogs(SearchCriteria searchCriteria, PList<VXTrxLogV2> pList) {
		// Get total count of the rows which meet the search criteria
		long count = -1;

		if (searchCriteria.isGetCount()) {
			count = getTrxLogsCount(searchCriteria);

			if (count == 0) {
				return Collections.emptyList();
			}
		}

		Map<String, Object> params = new HashMap<>();
		UserSessionBase session = ContextUtil.getCurrentUserSession();

		if (session != null && (session.isKeyAdmin() || session.isAuditKeyAdmin())) {
			searchFields.stream().filter(field -> "parentObjectName".equals(field.getClientFieldName())).findFirst()
					.ifPresent(parentObjNameField -> parentObjNameField.setCustomCondition(applyKeyAdminAccessFilters(params)));
			searchCriteria.addParam("parentObjectName", "");
		}

		String sortClause = searchUtil.constructSortClause(searchCriteria, sortFields);
		String queryStr   = "SELECT obj FROM " + XXTrxLogV2.class.getName() + " obj ";
		Query  query      = createQuery(queryStr, sortClause, searchCriteria, searchFields, false);

		if (!params.isEmpty()) {
			params.forEach(query::setParameter);
		}

		List<XXTrxLogV2> ret = daoManager.getXXTrxLogV2().executeQueryInSecurityContext(XXTrxLogV2.class, query);

		if (pList != null) {
			pList.setResultSize(ret.size());
			pList.setPageSize(query.getMaxResults());
			pList.setSortBy(searchCriteria.getSortBy());
			pList.setSortType(searchCriteria.getSortType());
			pList.setStartIndex(query.getFirstResult());
			pList.setTotalCount(count);
		}

		return ret;
	}

	private Query createQuery(String searchString, String sortString, SearchCriteria searchCriteria, List<SearchField> searchFieldList, boolean isCountQuery) {
		EntityManager em = daoManager.getEntityManager();

		return searchUtil.createSearchQuery(em, searchString, sortString, searchCriteria, searchFieldList, false, isCountQuery);
	}

	private XXTrxLogV2 toDBObject(VXTrxLogV2 vObj) {
		XXTrxLogV2 ret = new XXTrxLogV2(vObj.getObjectClassType(), vObj.getObjectId(), vObj.getObjectName(), vObj.getParentObjectClassType(), vObj.getParentObjectId(), vObj.getParentObjectName(), vObj.getAction());

		ret.setChangeInfo(toJson(vObj.getChangeInfo()));
		ret.setTransactionId(vObj.getTransactionId());
		ret.setAction(vObj.getAction());
		ret.setSessionId(vObj.getSessionId());
		ret.setRequestId(vObj.getRequestId());
		ret.setSessionType(vObj.getSessionType());

		return ret;
	}

	private VXTrxLogV2 toViewObject(XXTrxLogV2 dbObj, Map<Long, String> userIdNameCache) {
		VXTrxLogV2 ret = new VXTrxLogV2();

		ret.setId(dbObj.getId());
		ret.setCreateDate(dbObj.getCreateTime());
		ret.setCreatedBy(toUserName(dbObj.getAddedByUserId(), userIdNameCache));
		ret.setObjectClassType(dbObj.getObjectClassType());
		ret.setObjectId(dbObj.getObjectId());
		ret.setObjectName(dbObj.getObjectName());
		ret.setParentObjectClassType(dbObj.getParentObjectClassType());
		ret.setParentObjectId(dbObj.getParentObjectId());
		ret.setParentObjectName(dbObj.getParentObjectName());
		ret.setChangeInfo(toObjectChangeInfo(dbObj.getChangeInfo()));
		ret.setTransactionId(dbObj.getTransactionId());
		ret.setAction(dbObj.getAction());
		ret.setSessionId(dbObj.getSessionId());
		ret.setRequestId(dbObj.getRequestId());
		ret.setSessionType(dbObj.getSessionType());

		return ret;
	}

	private String toJson(ObjectChangeInfo changeInfo) {
		String ret = null;

		try {
			ret = JsonUtilsV2.objToJson(changeInfo);
		} catch (Exception excp) {
			// ignore
		}

		return ret;
	}

	private ObjectChangeInfo toObjectChangeInfo(String json) {
		ObjectChangeInfo ret = null;

		try {
			ret = JsonUtilsV2.jsonToObj(json, ObjectChangeInfo.class);
		} catch (Exception excp) {
			// ignore
		}

		return ret;
	}

	private String toUserName(Long userId, Map<Long, String> userIdNameCache) {
		String ret = null;

		if(userId != null) {
			ret = userIdNameCache != null ? userIdNameCache.get(userId) : null;

			if(ret == null) {
				XXPortalUser user = daoManager.getXXPortalUser().findById(userId);

				if(user != null) {
					ret = user.getPublicScreenName();

					if (StringUtil.isEmpty(ret)) {
						ret = user.getFirstName();

						if(StringUtil.isEmpty(ret)) {
							ret = user.getLoginId();
						} else {
							if(StringUtils.isNotEmpty(user.getLastName())) {
								ret += (" " + user.getLastName());
							}
						}
					}

					if (ret != null && userIdNameCache != null) {
						userIdNameCache.put(userId, ret);
					}
				}
			}
		}

		return ret;
	}

	public String applyKeyAdminAccessFilters(Map<String, Object> parameters) {
		StringBuilder filterClause = new StringBuilder();

		List<XXPortalUser> listXXPortalUser = daoManager.getXXPortalUser().findByRole(RangerConstants.ROLE_KEY_ADMIN);
		listXXPortalUser.addAll(daoManager.getXXPortalUser().findByRole(RangerConstants.ROLE_KEY_ADMIN_AUDITOR));
		List<Long> addedByUserId = listXXPortalUser.stream().map(XXPortalUser::getId).collect(Collectors.toList());

		if (!addedByUserId.isEmpty()) {
			filterClause.append("obj.addedByUserId IN :addedByUserId");
			parameters.put("addedByUserId", addedByUserId);
		}

		if (filterClause.length() > 0) filterClause.append(" OR ");
		String parentObjectName = EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_KMS_NAME;
		filterClause.append("obj.parentObjectName = :parentObjectName");
		parameters.put("parentObjectName", parentObjectName);

		XXServiceDef xxServiceDef = daoManager.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_KMS_NAME);
		if (xxServiceDef != null) {
			List<Long> parentObjectId = daoManager.getXXService().findByServiceDefId(xxServiceDef.getId()).stream()
					.map(XXService::getId).collect(Collectors.toList());

			if (!parentObjectId.isEmpty()) {
				if (filterClause.length() > 0) filterClause.append(" OR ");
				filterClause.append("obj.parentObjectId IN :parentObjectId");
				parameters.put("parentObjectId", parentObjectId);
			}
		}

		if (filterClause.length() > 0) {
			filterClause.insert(0, "(").append(")");
		}
		return filterClause.toString();
	}

	public boolean getValidTrxLogsForKeyAdminAndAuditor(XXTrxLogV2 xXTrxLog) {
		Map<String, Object> params = new HashMap<>();
		applyKeyAdminAccessFilters(params);

		List<Long> addedByUserIdList  = (List<Long>) params.get("addedByUserId");
		List<Long> parentObjectIdList = (List<Long>) params.get("parentObjectId");
		String     parentObjectName   = (String) params.get("parentObjectName");

		if (addedByUserIdList == null || parentObjectIdList == null || parentObjectName == null) {
			return false;
		}

		boolean isValid = addedByUserIdList.contains(xXTrxLog.getAddedByUserId()) || parentObjectIdList.contains(xXTrxLog.getParentObjectId())
				|| parentObjectName.equals(xXTrxLog.getParentObjectName());

		return isValid;
	}
}
