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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.SortField.SORT_ORDER;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.view.VXXTrxLog;
import org.apache.ranger.view.VXTrxLog;
import org.apache.ranger.view.VXTrxLogList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XTrxLogService extends XTrxLogServiceBase<XXTrxLog, VXTrxLog> {
	@Autowired
	RangerDaoManager rangerDaoManager;
	public XTrxLogService(){
		searchFields.add(new SearchField("attributeName", "obj.attributeName",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("action", "obj.action",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField("sessionId", "obj.sessionId",
				SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("startDate", "obj.createTime",
				SearchField.DATA_TYPE.DATE, SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN));	
		searchFields.add(new SearchField("endDate", "obj.createTime",
				SearchField.DATA_TYPE.DATE, SearchField.SEARCH_TYPE.LESS_EQUAL_THAN));	
		searchFields.add(new SearchField("owner", "obj.addedByUserId",
				SearchField.DATA_TYPE.INT_LIST, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("objectClassType", "obj.objectClassType",
				SearchField.DATA_TYPE.INT_LIST, SearchField.SEARCH_TYPE.FULL));
		
		sortFields.add(new SortField("createDate", "obj.createTime", true, SORT_ORDER.DESC));
		}

	@Override
	protected void validateForCreate(VXTrxLog vObj) {}

	@Override
	protected void validateForUpdate(VXTrxLog vObj, XXTrxLog mObj) {}

	@Override
	public VXTrxLogList searchXTrxLogs(SearchCriteria searchCriteria) {		
			
		EntityManager em = daoMgr.getEntityManager();
		CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
		Metamodel entityMetaModel = em.getMetamodel();
		Class<VXXTrxLog> klass = VXXTrxLog.class;
		EntityType<VXXTrxLog> entityType = entityMetaModel.entity(klass);
		CriteriaQuery<VXXTrxLog> selectCQ = criteriaBuilder.createQuery(klass);
		Root<VXXTrxLog> rootEntityType = selectCQ.from(klass);
		Predicate predicate = criteriaBuilder.conjunction();
		String fieldName=null;
		String clientFieldName =null;
		Object paramValue = null;
		boolean isListValue = false;
		SingularAttribute attr =null;
		Collection<Number> intValueList = null;
		Date fieldValue =null;
		Predicate stringPredicate =null;		
		Predicate intPredicate = null;		
		Predicate datePredicate =null;
		Map<String, Object> paramList = searchCriteria.getParamList();	
		for(String key : paramList.keySet()){
			for(SearchField searchField : searchFields){				
				fieldName = searchField.getFieldName();
				clientFieldName = searchField.getClientFieldName();				
				paramValue = paramList.get(key);
				isListValue = false;				
				if (paramValue != null && paramValue instanceof Collection) {
					isListValue = true;
				}				
				if(fieldName != null){
					fieldName = fieldName.contains(".") ? fieldName.substring(fieldName.indexOf(".") + 1) : fieldName;
				}				
				if(key.equalsIgnoreCase(clientFieldName)){
					// build where clause depending upon given parameters
					if(searchField.getDataType() == SearchField.DATA_TYPE.STRING) {
						// build where clause for String datatypes
						attr = entityType.getSingularAttribute(fieldName);
						if(attr != null){
							stringPredicate = criteriaBuilder.equal(rootEntityType.get(attr), paramValue);
							if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.PARTIAL)) {
								String val = "%" + paramValue + "%";
								stringPredicate = criteriaBuilder.like(rootEntityType.get(attr), val);
							}
							predicate = criteriaBuilder.and(predicate, stringPredicate);
							
						}	
					} else if (searchField.getDataType() == SearchField.DATA_TYPE.INT_LIST || 
							isListValue && searchField.getDataType() == SearchField.DATA_TYPE.INTEGER) {
						// build where clause for integer lists or integers datatypes
						intValueList = null;
						if (paramValue != null && (paramValue instanceof Integer || paramValue instanceof Long)) {
							intValueList = new ArrayList<Number>();
							intValueList.add((Number) paramValue);
						} else {
							intValueList = (Collection<Number>) paramValue;
						}
						for(Number value : intValueList){
							attr = entityType.getSingularAttribute(fieldName);
							if(attr != null){
								intPredicate = criteriaBuilder.equal(rootEntityType.get(attr), value);
								predicate = criteriaBuilder.and(predicate, intPredicate);
							}							
						}
						
					} else if (searchField.getDataType() == SearchField.DATA_TYPE.DATE){
						// build where clause for date datatypes
						fieldValue = (Date) paramList.get(searchField
								.getClientFieldName());
						attr = entityType.getSingularAttribute(fieldName);
						if (fieldValue != null) {
							if (searchField.getCustomCondition() == null) {							
								datePredicate = criteriaBuilder.equal(rootEntityType.get(attr), fieldValue);
								if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.LESS_THAN)) {
									datePredicate = criteriaBuilder.lessThan(rootEntityType.get(attr), fieldValue);
								} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.LESS_EQUAL_THAN)) {
									datePredicate = criteriaBuilder.lessThanOrEqualTo(rootEntityType.get(attr), fieldValue);
								} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.GREATER_THAN)) {
									datePredicate = criteriaBuilder.greaterThan(rootEntityType.get(attr), fieldValue);
								} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN)) {
									datePredicate = criteriaBuilder.greaterThanOrEqualTo(rootEntityType.get(attr), fieldValue);
								} else {
									datePredicate = criteriaBuilder.equal(rootEntityType.get(attr), fieldValue);
								}
								predicate = criteriaBuilder.and(predicate, datePredicate);
							} 
						}
						
					}
				}
			}
		}
		
		selectCQ.where(predicate);
		if(searchCriteria.getSortType()!=null && searchCriteria.getSortType().equalsIgnoreCase("asc")){
			selectCQ.orderBy(criteriaBuilder.asc(rootEntityType.get("createTime")));
		}else{
			selectCQ.orderBy(criteriaBuilder.desc(rootEntityType.get("createTime")));
		}
		int startIndex = searchCriteria.getStartIndex();
		int pageSize = searchCriteria.getMaxRows();
		List<VXXTrxLog> resultList = em.createQuery(selectCQ).setFirstResult(startIndex).setMaxResults(pageSize).getResultList();
		VXTrxLogList vxTrxLogList = new VXTrxLogList();		
		vxTrxLogList.setStartIndex(startIndex);
		vxTrxLogList.setPageSize(pageSize);		
		List<VXTrxLog> trxLogList = new ArrayList<VXTrxLog>();
		XXPortalUser xXPortalUser=null;
		for(VXXTrxLog xTrxLog : resultList){
			VXTrxLog trxLog = mapCustomViewToViewObj(xTrxLog);
			xXPortalUser=null;
			if(trxLog.getUpdatedBy()!=null){
				xXPortalUser= rangerDaoManager.getXXPortalUser().getById(
						Long.parseLong(trxLog.getUpdatedBy()));
			}			
			if(xXPortalUser!=null){
				trxLog.setOwner(xXPortalUser.getLoginId());
			}
			trxLogList.add(trxLog);
		}			
		//vxTrxLogList.setTotalCount(count);
		vxTrxLogList.setVXTrxLogs(trxLogList);
		return vxTrxLogList;
	}
	
	public Long searchXTrxLogsCount(SearchCriteria searchCriteria) {		
		
		EntityManager em = daoMgr.getEntityManager();
		CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();		
		Class<VXXTrxLog> klass = VXXTrxLog.class;		
		CriteriaQuery<VXXTrxLog> criteriaQuery = criteriaBuilder.createQuery(klass);
		Root<VXXTrxLog> rootEntityType = criteriaQuery.from(klass);
		Metamodel entityMetaModel = em.getMetamodel();
		EntityType<VXXTrxLog> entityType = entityMetaModel.entity(klass);
		Map<String, Object> paramList = searchCriteria.getParamList();
		CriteriaQuery<Long> countCQ = criteriaBuilder.createQuery(Long.class);
		Predicate predicate = criteriaBuilder.conjunction();		
		String fieldName=null;
		String clientFieldName =null;
		Object paramValue = null;
		boolean isListValue = false;
		SingularAttribute attr =null;
		Collection<Number> intValueList = null;
		Date fieldValue =null;
		Predicate stringPredicate =null;		
		Predicate intPredicate = null;		
		Predicate datePredicate =null;		
		for(String key : paramList.keySet()){
			for(SearchField searchField : searchFields){				
				fieldName = searchField.getFieldName();
				clientFieldName = searchField.getClientFieldName();				
				paramValue = paramList.get(key);
				isListValue = false;				
				if (paramValue != null && paramValue instanceof Collection) {
					isListValue = true;
				}				
				if(fieldName != null){
					fieldName = fieldName.contains(".") ? fieldName.substring(fieldName.indexOf(".") + 1) : fieldName;
				}				
				if(key.equalsIgnoreCase(clientFieldName)){
					// build where clause depending upon given parameters
					if(searchField.getDataType() == SearchField.DATA_TYPE.STRING) {
						// build where clause for String datatypes
						attr = entityType.getSingularAttribute(fieldName);
						if(attr != null){
							stringPredicate = criteriaBuilder.equal(rootEntityType.get(attr), paramValue);
							if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.PARTIAL)) {
								String val = "%" + paramValue + "%";
								stringPredicate = criteriaBuilder.like(rootEntityType.get(attr), val);
							}
							predicate = criteriaBuilder.and(predicate, stringPredicate);
							
						}	
					} else if (searchField.getDataType() == SearchField.DATA_TYPE.INT_LIST || 
							isListValue && searchField.getDataType() == SearchField.DATA_TYPE.INTEGER) {
						// build where clause for integer lists or integers datatypes
						intValueList = null;
						if (paramValue != null && (paramValue instanceof Integer || paramValue instanceof Long)) {
							intValueList = new ArrayList<Number>();
							intValueList.add((Number) paramValue);
						} else {
							intValueList = (Collection<Number>) paramValue;
						}
						for(Number value : intValueList){
							attr = entityType.getSingularAttribute(fieldName);
							if(attr != null){
								intPredicate = criteriaBuilder.equal(rootEntityType.get(attr), value);
								predicate = criteriaBuilder.and(predicate, intPredicate);
							}							
						}
						
					} else if (searchField.getDataType() == SearchField.DATA_TYPE.DATE){
						// build where clause for date datatypes
						fieldValue = (Date) paramList.get(searchField
								.getClientFieldName());
						attr = entityType.getSingularAttribute(fieldName);
						if (fieldValue != null) {
							if (searchField.getCustomCondition() == null) {							
								datePredicate = criteriaBuilder.equal(rootEntityType.get(attr), fieldValue);
								if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.LESS_THAN)) {
									datePredicate = criteriaBuilder.lessThan(rootEntityType.get(attr), fieldValue);
								} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.LESS_EQUAL_THAN)) {
									datePredicate = criteriaBuilder.lessThanOrEqualTo(rootEntityType.get(attr), fieldValue);
								} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.GREATER_THAN)) {
									datePredicate = criteriaBuilder.greaterThan(rootEntityType.get(attr), fieldValue);
								} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN)) {
									datePredicate = criteriaBuilder.greaterThanOrEqualTo(rootEntityType.get(attr), fieldValue);
								} else {
									datePredicate = criteriaBuilder.equal(rootEntityType.get(attr), fieldValue);
								}
								predicate = criteriaBuilder.and(predicate, datePredicate);
							} 
						}
						
					}
				}
			}
		}		
		countCQ.select(criteriaBuilder.count(rootEntityType)).where(predicate);		
		List<Long> countList = em.createQuery(countCQ).getResultList();		
		Long count = 0L;
		if(countList != null && countList.size() > 0) {
			count = countList.get(0);
			if(count == null) {
				count = 0L;
			}
		}	
		return count;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Predicate buildWhereClause(Predicate predicate, Map<String, Object> paramList, EntityType<XXTrxLog> trxLogEntity, 
			CriteriaBuilder criteriaBuilder, Root<XXTrxLog> root){
		
		for(String key : paramList.keySet()) {
			for(SearchField searchField : searchFields){
				
				String fieldName = searchField.getFieldName();
				String clientFieldName = searchField.getClientFieldName();
				
				Object paramValue = paramList.get(key);
				boolean isListValue = false;
				
				if (paramValue != null && paramValue instanceof Collection) {
					isListValue = true;
				}
				
				if(fieldName != null){
					fieldName = fieldName.contains(".") ? fieldName.substring(fieldName.indexOf(".") + 1) : fieldName;
				}
				
				if(key.equalsIgnoreCase(clientFieldName)){
					// build where clause depending upon given parameters
					if(searchField.getDataType() == SearchField.DATA_TYPE.STRING) {
						// build where clause for String datatypes
						SingularAttribute attr = trxLogEntity.getSingularAttribute(fieldName);
						if(attr != null){
							Predicate stringPredicate = criteriaBuilder.equal(root.get(attr), paramValue);
							predicate = criteriaBuilder.and(predicate, stringPredicate);
						}	
					} else if (searchField.getDataType() == SearchField.DATA_TYPE.INT_LIST || 
							isListValue && searchField.getDataType() == SearchField.DATA_TYPE.INTEGER) {
						// build where clause for integer lists or integers datatypes
						Collection<Number> intValueList = null;
						if (paramValue != null && (paramValue instanceof Integer || paramValue instanceof Long)) {
							intValueList = new ArrayList<Number>();
							intValueList.add((Number) paramValue);
						} else {
							intValueList = (Collection<Number>) paramValue;
						}
						for(Number value : intValueList){
							SingularAttribute attr = trxLogEntity.getSingularAttribute(fieldName);
							if(attr != null){
								Predicate intPredicate = criteriaBuilder.equal(root.get(attr), value);
								predicate = criteriaBuilder.and(predicate, intPredicate);
							}							
						}
					} else if (searchField.getDataType() == SearchField.DATA_TYPE.DATE){
						// build where clause for date datatypes
						Date fieldValue = (Date) paramList.get(searchField
								.getClientFieldName());
						SingularAttribute attr = trxLogEntity.getSingularAttribute(fieldName);
						if (fieldValue != null) {
							if (searchField.getCustomCondition() == null) {
								Predicate datePredicate = criteriaBuilder.equal(root.get(attr), fieldValue);
								if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.LESS_THAN)) {
									datePredicate = criteriaBuilder.lessThan(root.get(attr), fieldValue);
								} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.LESS_EQUAL_THAN)) {
									datePredicate = criteriaBuilder.lessThanOrEqualTo(root.get(attr), fieldValue);
								} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.GREATER_THAN)) {
									datePredicate = criteriaBuilder.greaterThan(root.get(attr), fieldValue);
								} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN)) {
									datePredicate = criteriaBuilder.greaterThanOrEqualTo(root.get(attr), fieldValue);
								} else {
									datePredicate = criteriaBuilder.equal(root.get(attr), fieldValue);
								}
								predicate = criteriaBuilder.and(predicate, datePredicate);
							} 
						}
					}
				}
			}
		}
		return predicate;
	}

	private VXTrxLog mapCustomViewToViewObj(VXXTrxLog vXXTrxLog){
		VXTrxLog vXTrxLog = new VXTrxLog();
		vXTrxLog.setId(vXXTrxLog.getId());
		vXTrxLog.setAction(vXXTrxLog.getAction());
		vXTrxLog.setAttributeName(vXXTrxLog.getAttributeName());
		vXTrxLog.setCreateDate(vXXTrxLog.getCreateTime());
		vXTrxLog.setNewValue(vXXTrxLog.getNewValue());
		vXTrxLog.setPreviousValue(vXXTrxLog.getPreviousValue());
		vXTrxLog.setSessionId(vXXTrxLog.getSessionId());
		if(vXXTrxLog.getUpdatedByUserId()==null || vXXTrxLog.getUpdatedByUserId()==0){
			vXTrxLog.setUpdatedBy(null);
		}else{
			vXTrxLog.setUpdatedBy(String.valueOf(vXXTrxLog.getUpdatedByUserId()));
		}
		//We will have to get this from XXUser
		//vXTrxLog.setOwner(vXXTrxLog.getAddedByUserName());
		vXTrxLog.setParentObjectClassType(vXXTrxLog.getParentObjectClassType());
		vXTrxLog.setParentObjectName(vXXTrxLog.getParentObjectName());
		vXTrxLog.setObjectClassType(vXXTrxLog.getObjectClassType());
		vXTrxLog.setObjectId(vXXTrxLog.getObjectId());
		vXTrxLog.setObjectName(vXXTrxLog.getObjectName());
		vXTrxLog.setTransactionId(vXXTrxLog.getTransactionId());
		return vXTrxLog;
	}
	
	@Override
	protected XXTrxLog mapViewToEntityBean(VXTrxLog vObj, XXTrxLog mObj, int OPERATION_CONTEXT) {
		if(vObj!=null && mObj!=null){
			super.mapViewToEntityBean(vObj, mObj, OPERATION_CONTEXT);
			XXPortalUser xXPortalUser=null;
			if(mObj.getAddedByUserId()==null || mObj.getAddedByUserId()==0){
				if(!stringUtil.isEmpty(vObj.getOwner())){
					xXPortalUser=rangerDaoManager.getXXPortalUser().findByLoginId(vObj.getOwner());	
					if(xXPortalUser!=null){
						mObj.setAddedByUserId(xXPortalUser.getId());
					}
				}
			}
			if(mObj.getUpdatedByUserId()==null || mObj.getUpdatedByUserId()==0){
				if(!stringUtil.isEmpty(vObj.getUpdatedBy())){
					xXPortalUser= rangerDaoManager.getXXPortalUser().findByLoginId(vObj.getUpdatedBy());			
					if(xXPortalUser!=null){
						mObj.setUpdatedByUserId(xXPortalUser.getId());
					}		
				}
			}
		}
		return mObj;
	}

	@Override
	protected VXTrxLog mapEntityToViewBean(VXTrxLog vObj, XXTrxLog mObj) {
        if(mObj!=null && vObj!=null){
            super.mapEntityToViewBean(vObj, mObj);
            XXPortalUser xXPortalUser=null;
            if(stringUtil.isEmpty(vObj.getOwner())){
                xXPortalUser= rangerDaoManager.getXXPortalUser().getById(mObj.getAddedByUserId());
                if(xXPortalUser!=null){
                    vObj.setOwner(xXPortalUser.getLoginId());
                }
            }
            if(stringUtil.isEmpty(vObj.getUpdatedBy())){
                xXPortalUser= rangerDaoManager.getXXPortalUser().getById(mObj.getUpdatedByUserId());
                if(xXPortalUser!=null){
                    vObj.setUpdatedBy(xXPortalUser.getLoginId());
                }
            }
        }
        return vObj;
	}
}
