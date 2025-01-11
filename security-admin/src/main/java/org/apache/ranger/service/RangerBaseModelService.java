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

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.common.view.VList;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class RangerBaseModelService<T extends XXDBBase, V extends RangerBaseModelObject> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerBaseModelService.class);

    public static final int OPERATION_CREATE_CONTEXT        = 1;
    public static final int OPERATION_UPDATE_CONTEXT        = 2;
    public static final int OPERATION_DELETE_CONTEXT        = 3;
    public static final int OPERATION_IMPORT_CREATE_CONTEXT = 4;
    public static final int OPERATION_IMPORT_DELETE_CONTEXT = 5;

    public final List<SortField>   sortFields   = new ArrayList<>();
    public final List<SearchField> searchFields = new ArrayList<>();

    protected final Class<T> tEntityClass;
    protected final Class<V> tViewClass;
    protected final String   tClassName;
    protected final String   countQueryStr;
    protected final String   distinctCountQueryStr;
    protected final String   queryStr;
    protected final String   distinctQueryStr;

    @Autowired
    protected RangerDaoManager daoMgr;

    @Autowired
    protected StringUtil stringUtil;

    @Autowired
    protected RESTErrorUtil restErrorUtil;

    @Autowired
    protected RangerSearchUtil searchUtil;

    @Autowired
    protected BaseDao<T> entityDao;

    @Autowired
    RangerBizUtil bizUtil;

    private Boolean populateExistingBaseFields;

    @SuppressWarnings("unchecked")
    public RangerBaseModelService() {
        Class                    klass             = getClass();
        ParameterizedType        genericSuperclass = (ParameterizedType) klass.getGenericSuperclass();
        TypeVariable<Class<?>>[] var               = klass.getTypeParameters();

        if (genericSuperclass.getActualTypeArguments()[0] instanceof Class) {
            tEntityClass = (Class<T>) genericSuperclass.getActualTypeArguments()[0];
            tViewClass   = (Class<V>) genericSuperclass.getActualTypeArguments()[1];
        } else if (var.length > 0) {
            tEntityClass = (Class<T>) var[0].getBounds()[0];
            tViewClass   = (Class<V>) var[1].getBounds()[0];
        } else {
            LOG.error("Cannot find class for template", new Throwable());

            tEntityClass = null;
            tViewClass   = null;
        }

        tClassName                 = (tEntityClass != null) ? tEntityClass.getName() : "XXDBBase";
        populateExistingBaseFields = false;
        countQueryStr              = "SELECT COUNT(obj) FROM " + tClassName + " obj ";
        distinctCountQueryStr      = "SELECT COUNT(distinct obj.id) FROM " + tClassName + " obj ";
        queryStr                   = "SELECT obj FROM " + tClassName + " obj ";
        distinctQueryStr           = "SELECT DISTINCT obj FROM " + tClassName + " obj ";
    }

    public T preCreate(V vObj) {
        validateForCreate(vObj);

        T entityObj = createEntityObject();

        return populateEntityBeanForCreate(entityObj, vObj);
    }

    public V postCreate(T xObj) {
        return populateViewBean(xObj);
    }

    public V create(V vObj) {
        T resource = preCreate(vObj);

        resource = getDao().create(resource);
        vObj     = postCreate(resource);

        return vObj;
    }

    public V create(V vObj, boolean flush) {
        T resource = preCreate(vObj);

        resource = getDao().create(resource, flush);
        vObj     = postCreate(resource);

        return vObj;
    }

    public V read(Long id) {
        T resource = getDao().getById(id);

        if (resource == null) {
            throw restErrorUtil.createRESTException(tViewClass.getName() + " :Data Not Found for given Id", MessageEnums.DATA_NOT_FOUND, id, null, "readResource : No Object found with given id.");
        }

        return populateViewBean(resource);
    }

    public V update(V viewBaseBean) {
        T resource = preUpdate(viewBaseBean);

        resource = getDao().update(resource);

        return postUpdate(resource);
    }

    public V postUpdate(T resource) {
        return populateViewBean(resource);
    }

    public T preUpdate(V viewBaseBean) {
        T resource = getDao().getById(viewBaseBean.getId());

        if (resource == null) {
            throw restErrorUtil.createRESTException(tEntityClass.getSimpleName() + " not found", MessageEnums.DATA_NOT_FOUND, viewBaseBean.getId(), null, "preUpdate: id not found.");
        }

        validateForUpdate(viewBaseBean, resource);

        return populateEntityBeanForUpdate(resource, viewBaseBean);
    }

    public boolean delete(V vObj) {
        boolean result;
        Long    id       = vObj.getId();
        T       resource = preDelete(id);

        if (resource == null) {
            throw restErrorUtil.createRESTException(tEntityClass.getSimpleName() + " not found", MessageEnums.DATA_NOT_FOUND, id, null, tEntityClass.getSimpleName() + ":" + id);
        }

        try {
            result = getDao().remove(resource);
        } catch (Exception e) {
            LOG.error("Error deleting {}. Id={}", tEntityClass.getSimpleName(), id, e);

            throw restErrorUtil.createRESTException(tEntityClass.getSimpleName() + " can't be deleted", MessageEnums.OPER_NOT_ALLOWED_FOR_STATE, id, null, id + ", error=" + e.getMessage());
        }

        return result;
    }

    public boolean delete(V vObj, boolean flush) {
        boolean result;
        Long    id       = vObj.getId();
        T       resource = preDelete(id);

        if (resource == null) {
            throw restErrorUtil.createRESTException(tEntityClass.getSimpleName() + " not found", MessageEnums.DATA_NOT_FOUND, id, null, tEntityClass.getSimpleName() + ":" + id);
        }

        try {
            result = getDao().remove(resource);
        } catch (Exception e) {
            LOG.error("Error deleting {}. Id={}", tEntityClass.getSimpleName(), id, e);

            throw restErrorUtil.createRESTException(tEntityClass.getSimpleName() + " can't be deleted", MessageEnums.OPER_NOT_ALLOWED_FOR_STATE, id, null, id + ", error=" + e.getMessage());
        }

        return result;
    }

    public Boolean getPopulateExistingBaseFields() {
        return populateExistingBaseFields;
    }

    public void setPopulateExistingBaseFields(Boolean populateExistingBaseFields) {
        this.populateExistingBaseFields = populateExistingBaseFields;
    }

    public List<T> searchResources(SearchFilter searchCriteria, List<SearchField> searchFieldList, List<SortField> sortFieldList, VList vList) {
        // Get total count of the rows which meet the search criteria
        long count = -1;

        if (searchCriteria.isGetCount()) {
            count = getCountForSearchQuery(searchCriteria, searchFieldList);

            if (count == 0) {
                return Collections.emptyList();
            }
        }

        String  sortClause = searchUtil.constructSortClause(searchCriteria, sortFieldList);
        String  q          = searchCriteria.isDistinct() ? distinctQueryStr : queryStr;
        Query   query      = createQuery(q, sortClause, searchCriteria, searchFieldList, false);
        List<T> resultList = getDao().executeQueryInSecurityContext(tEntityClass, query);

        if (vList != null) {
            vList.setResultSize(resultList.size());
            vList.setPageSize(query.getMaxResults());
            vList.setSortBy(searchCriteria.getSortBy());
            vList.setSortType(searchCriteria.getSortType());
            vList.setStartIndex(query.getFirstResult());
            vList.setTotalCount(count);
        }

        return resultList;
    }

    //If not efficient we need to review this and add jpa_named queries to get the count
    public long getCountForSearchQuery(SearchFilter searchCriteria, List<SearchField> searchFieldList) {
        String q     = searchCriteria.isDistinct() ? distinctCountQueryStr : countQueryStr;
        Query  query = createQuery(q, null, searchCriteria, searchFieldList, true);
        Long   count = getDao().executeCountQueryInSecurityContext(tEntityClass, query);

        return (count == null) ? 0 : count;
    }

    protected abstract T mapViewToEntityBean(V viewBean, T t, int operationContext);

    protected abstract V mapEntityToViewBean(V viewBean, T t);

    protected T createEntityObject() {
        try {
            return tEntityClass.newInstance();
        } catch (Throwable e) {
            LOG.error("Error instantiating entity class. tEntityClass={}", tEntityClass.toString(), e);
        }

        return null;
    }

    protected V createViewObject() {
        try {
            return tViewClass.newInstance();
        } catch (Throwable e) {
            LOG.error("Error instantiating view class. tViewClass={}", tViewClass.toString(), e);
        }

        return null;
    }

    protected BaseDao<T> getDao() {
        if (entityDao == null) {
            throw new NullPointerException("entityDao is not injected by Spring!");
        }

        return entityDao;
    }

    protected V populateViewBean(T entityObj) {
        V vObj = createViewObject();

        vObj.setId(entityObj.getId());
        vObj.setCreateTime(entityObj.getCreateTime());
        vObj.setUpdateTime(entityObj.getUpdateTime());
        vObj.setCreatedBy(getUserScreenName(entityObj.getAddedByUserId()));
        vObj.setUpdatedBy(getUserScreenName(entityObj.getUpdatedByUserId()));

        return mapEntityToViewBean(vObj, entityObj);
    }

    protected T populateEntityBeanForCreate(T entityObj, V vObj) {
        if (!populateExistingBaseFields) {
            entityObj.setCreateTime(DateUtil.getUTCDate());
            entityObj.setUpdateTime(entityObj.getCreateTime());
            entityObj.setAddedByUserId(ContextUtil.getCurrentUserId());
            entityObj.setUpdatedByUserId(entityObj.getAddedByUserId());
        } else {
            XXPortalUser createdByUser = daoMgr.getXXPortalUser().findByLoginId(vObj.getCreatedBy());
            XXPortalUser updByUser     = daoMgr.getXXPortalUser().findByLoginId(vObj.getUpdatedBy());

            entityObj.setId(vObj.getId());
            entityObj.setCreateTime(vObj.getCreateTime() != null ? vObj.getCreateTime() : DateUtil.getUTCDate());
            entityObj.setUpdateTime(vObj.getUpdateTime() != null ? vObj.getUpdateTime() : DateUtil.getUTCDate());
            entityObj.setAddedByUserId(createdByUser != null ? createdByUser.getId() : ContextUtil.getCurrentUserId());
            entityObj.setUpdatedByUserId(updByUser != null ? updByUser.getId() : ContextUtil.getCurrentUserId());
        }

        return mapViewToEntityBean(vObj, entityObj, OPERATION_CREATE_CONTEXT);
    }

    protected T populateEntityBeanForUpdate(T entityObj, V vObj) {
        if (entityObj == null) {
            throw restErrorUtil.createRESTException("No Object found to update.", MessageEnums.DATA_NOT_FOUND);
        }

        T ret = mapViewToEntityBean(vObj, entityObj, OPERATION_UPDATE_CONTEXT);

        if (ret.getCreateTime() == null) {
            ret.setCreateTime(DateUtil.getUTCDate());
        }

        if (ret.getAddedByUserId() == null) {
            ret.setAddedByUserId(ContextUtil.getCurrentUserId());
        }

        if (!populateExistingBaseFields) {
            ret.setUpdateTime(DateUtil.getUTCDate());
            ret.setUpdatedByUserId(ContextUtil.getCurrentUserId());
        }

        return ret;
    }

    protected abstract void validateForCreate(V vObj);

    /*
     * Search Operations
     *
     */

    protected abstract void validateForUpdate(V vObj, T entityObj);

    protected T preDelete(Long id) {
        T resource = getDao().getById(id);

        if (resource == null) {
            // Return without error
            LOG.info("Delete ignored for non-existent Object, id={}", id);
        }

        return resource;
    }

    protected List<T> searchRangerObjects(SearchFilter searchCriteria, List<SearchField> searchFieldList, List<SortField> sortFieldList, PList<V> pList) {
        // Get total count of the rows which meet the search criteria
        long count = -1;

        if (searchCriteria.isGetCount()) {
            count = getCountForSearchQuery(searchCriteria, searchFieldList);

            if (count == 0) {
                return Collections.emptyList();
            }
        }

        String  sortClause = searchUtil.constructSortClause(searchCriteria, sortFieldList);
        String  q          = queryStr;
        Query   query      = createQuery(q, sortClause, searchCriteria, searchFieldList, false);
        List<T> resultList = getDao().executeQueryInSecurityContext(tEntityClass, query);

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

    protected Query createQuery(String searchString, String sortString, SearchFilter searchCriteria, List<SearchField> searchFieldList, boolean isCountQuery) {
        EntityManager em = getDao().getEntityManager();

        return searchUtil.createSearchQuery(em, searchString, sortString, searchCriteria, searchFieldList, false, isCountQuery);
    }

    protected String getUserScreenName(Long userId) {
        String       ret = null;
        XXPortalUser xPortalUser = userId == null ? null : daoMgr.getXXPortalUser().getById(userId);

        if (xPortalUser != null) {
            ret = xPortalUser.getPublicScreenName();

            if (stringUtil.isEmpty(ret)) {
                ret = xPortalUser.getFirstName();

                if (stringUtil.isEmpty(ret)) {
                    ret = xPortalUser.getLoginId();
                } else {
                    if (!stringUtil.isEmpty(xPortalUser.getLastName())) {
                        ret += (" " + xPortalUser.getLastName());
                    }
                }
            }
        }

        return ret;
    }

    protected String getUserName(Long userId) {
        XXPortalUser xPortalUser = userId == null ? null : daoMgr.getXXPortalUser().getById(userId);

        return xPortalUser != null ? xPortalUser.getLoginId() : null;
    }

    protected String getGroupName(Long groupId) {
        XXGroup xGroup = groupId == null ? null : daoMgr.getXXGroup().getById(groupId);

        return xGroup != null ? xGroup.getName() : null;
    }

    protected String getAccessTypeName(Long accessTypeDefId) {
        XXAccessTypeDef accessTypeDef = accessTypeDefId == null ? null : daoMgr.getXXAccessTypeDef().getById(accessTypeDefId);

        return accessTypeDef != null ? accessTypeDef.getName() : null;
    }

    protected String getConditionName(Long conditionDefId) {
        XXPolicyConditionDef conditionDef = conditionDefId == null ? null : daoMgr.getXXPolicyConditionDef().getById(conditionDefId);

        return conditionDef != null ? conditionDef.getName() : null;
    }

    protected String getResourceName(Long resourceDefId) {
        XXResourceDef resourceDef = resourceDefId == null ? null : daoMgr.getXXResourceDef().getById(resourceDefId);

        return resourceDef != null ? resourceDef.getName() : null;
    }
}
