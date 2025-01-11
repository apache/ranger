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

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConfigUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.SortField.SORT_ORDER;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.common.view.VList;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAuthSession;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.security.handler.Permission;
import org.apache.ranger.security.handler.RangerDomainObjectSecurityHandler;
import org.apache.ranger.view.VXDataObject;
import org.apache.ranger.view.VXLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.servlet.http.HttpServletResponse;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.ranger.service.RangerBaseModelService.OPERATION_CREATE_CONTEXT;
import static org.apache.ranger.service.RangerBaseModelService.OPERATION_UPDATE_CONTEXT;

public abstract class AbstractBaseResourceService<T extends XXDBBase, V extends VXDataObject> {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractBaseResourceService.class);

    protected static final Map<Class<?>, String> tEntityValueMap = new HashMap<>();

    public final List<SortField>   sortFields      = new ArrayList<>();
    public final List<SearchField> searchFields    = new ArrayList<>();

    protected final Class<T> tEntityClass;
    protected final Class<V> tViewClass;
    protected final String   className;
    protected final String   viewClassName;
    protected final String   countQueryStr;
    protected final String   queryStr;
    protected final String   distinctCountQueryStr;
    protected final String   distinctQueryStr;

    @Autowired
    protected RangerDaoManager daoManager;

    @Autowired
    protected SearchUtil searchUtil;

    @Autowired
    protected RESTErrorUtil restErrorUtil;

    @Autowired
    BaseDao<T> entityDao;

    @Autowired
    StringUtil stringUtil;

    @Autowired
    RangerDomainObjectSecurityHandler objectSecurityHandler;

    @Autowired
    RangerBizUtil bizUtil;

    @Autowired
    RangerConfigUtil msConfigUtil;

    @SuppressWarnings("unchecked")
    public AbstractBaseResourceService() {
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
            tEntityClass = null;
            tViewClass   = null;

            logger.error("Cannot find class for template", new Throwable());
        }

        if (tEntityClass != null) {
            className = tEntityClass.getName();
        } else {
            className = null;
        }

        if (tViewClass != null) {
            viewClassName = tViewClass.getName();
        } else {
            viewClassName = null;
        }

        // Get total count of the rows which meet the search criteria
        countQueryStr         = "SELECT COUNT(obj) FROM " + className + " obj ";
        queryStr              = "SELECT obj FROM " + className + " obj ";
        distinctCountQueryStr = "SELECT COUNT(distinct obj.id) FROM " + className + " obj ";
        distinctQueryStr      = "SELECT distinct obj FROM " + className + " obj ";

        sortFields.add(new SortField("id", "obj.id", true, SORT_ORDER.ASC));
    }

    public V createResource(V viewBaseBean) {
        T resource = preCreate(viewBaseBean);

        // object security
        if (!objectSecurityHandler.hasAccess(resource, Permission.PermissionType.CREATE)) {
            throw restErrorUtil.create403RESTException(getResourceName() + " access denied. classType=" + resource.getMyClassType() + ", className=" + resource.getClass().getName() + ", objectId=" + resource.getId());
        }

        resource = getDao().create(resource);

        return postCreate(resource);
    }

    public V readResource(Long id) {
        // T resource = preRead(id);

        T resource = getDao().getById(id);

        if (resource == null) {
            // Returns code 404 with DATA_NOT_FOUND as the error message
            throw restErrorUtil.createRESTException(getResourceName() + " not found", MessageEnums.DATA_NOT_FOUND, id, null, "preRead: " + id + " not found.", HttpServletResponse.SC_NOT_FOUND);
        }

        return readResource(resource);
    }

    public V updateResource(V viewBaseBean) {
        T resource = preUpdate(viewBaseBean);

        // object security
        if (!objectSecurityHandler.hasAccess(resource, Permission.PermissionType.UPDATE)) {
            throw restErrorUtil.create403RESTException(getResourceName() + " access denied. classType=" + resource.getMyClassType() + ", className=" + resource.getClass().getName() + ", objectId=" + resource.getId());
        }

        resource = getDao().update(resource);

        return postUpdate(resource);
    }

    public boolean deleteResource(Long id) {
        boolean result;
        T       resource = preDelete(id);

        if (resource == null) {
            throw restErrorUtil.createRESTException(getResourceName() + " not found", MessageEnums.DATA_NOT_FOUND, id, null, getResourceName() + ":" + id);
        }

        // object security
        if (!objectSecurityHandler.hasAccess(resource, Permission.PermissionType.DELETE)) {
            // throw 401
            logger.debug("OBJECT SECURITY");
        }

        // Need to delete all dependent common objects like Notes and UserDataPref
        try {
            result = getDao().remove(resource);
        } catch (Exception e) {
            logger.error("Error deleting {} => Id = {}", getResourceName(), id, e);

            throw restErrorUtil.createRESTException(getResourceName() + " can't be deleted", MessageEnums.OPER_NOT_ALLOWED_FOR_STATE, id, null, "" + id + ", error=" + e.getMessage());
        }

        postDelete(resource);

        return result;
    }

    // ----------------------------------------------------------------------------------
    // mapping view bean attributes
    // ----------------------------------------------------------------------------------
    public V populateViewBean(T resource) {
        V viewBean = createViewObject();

        populateViewBean(resource, viewBean);

        return mapEntityToViewBean(viewBean, resource);
    }

    public VXLong getSearchCount(SearchCriteria searchCriteria, List<SearchField> searchFieldList) {
        long   count  = getCountForSearchQuery(searchCriteria, searchFieldList);
        VXLong vXLong = new VXLong();

        vXLong.setValue(count);

        return vXLong;
    }

    // -------------Criteria Usage--------------------
    // -----------------------------------------------
    public VXLong getSearchCountUsingCriteria(SearchCriteria searchCriteria, List<SearchField> searchFieldList) {
        EntityManager       em              = getDao().getEntityManager();
        CriteriaBuilder     criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<Long> criteria        = criteriaBuilder.createQuery(Long.class);
        Root<T>             from            = criteria.from(tEntityClass);
        Expression<Long>    countExpression = criteriaBuilder.count(from.get("id"));

        criteria.select(countExpression);

        Predicate resourceConditions = buildResourceSpecificConditions(criteriaBuilder, from, searchCriteria);
        Predicate userConditions     = buildUserConditions(searchCriteria.getParamList(), searchFieldList, criteriaBuilder, from);

        if (resourceConditions != null) {
            criteria.where(criteriaBuilder.and(resourceConditions, userConditions));
        } else {
            criteria.where(criteriaBuilder.and(userConditions));
        }

        TypedQuery<Long> countQuery = em.createQuery(criteria);
        long             count      = getDao().executeCountQueryInSecurityContext(tEntityClass, countQuery);
        VXLong           vXLong     = new VXLong();

        vXLong.setValue(count);

        return vXLong;
    }

    public void setSortClause(SearchCriteria searchCriteria, List<SortField> sortFields, CriteriaBuilder criteriaBuilder, CriteriaQuery<?> criteria, Root<? extends XXDBBase> from) {
        String sortBy      = searchCriteria.getSortBy();
        String sortByField = null;

        if (!stringUtil.isEmpty(sortBy)) {
            sortBy = sortBy.trim();

            for (SortField sortField : sortFields) {
                if (sortBy.equalsIgnoreCase(sortField.getParamName())) {
                    sortByField = sortField.getFieldName();

                    // Override the sortBy using the normalized value
                    // searchCriteria.setSortBy(sortByField);
                    break;
                }
            }
        }

        if (sortByField == null) {
            for (SortField sortField : sortFields) {
                if (sortField.isDefault()) {
                    sortByField = sortField.getFieldName();

                    // Override the sortBy using the default value
                    searchCriteria.setSortBy(sortField.getParamName());
                    searchCriteria.setSortType(sortField.getDefaultOrder().name());
                    break;
                }
            }
        }

        if (sortByField != null) {
            int dotIndex = sortByField.indexOf(".");

            if (dotIndex != -1) {
                sortByField = sortByField.substring(dotIndex + 1);
            }

            // Add sort type
            String sortType = searchCriteria.getSortType();

            if ("desc".equalsIgnoreCase(sortType)) {
                criteria.orderBy(criteriaBuilder.desc(from.get(sortByField)));
            } else {
                criteria.orderBy(criteriaBuilder.asc(from.get(sortByField)));
            }
        }
    }

    public Map<Long, V> convertVListToVMap(List<V> vObjList) {
        Map<Long, V> ret = new HashMap<>();

        if (vObjList == null) {
            return ret;
        }

        for (V vObj : vObjList) {
            ret.put(vObj.getId(), vObj);
        }

        return ret;
    }

    // ----------------------------------------------------------------------------------
    // Create Operation
    // ----------------------------------------------------------------------------------

    protected abstract void validateForCreate(V viewBaseBean);

    protected abstract void validateForUpdate(V viewBaseBean, T t);

    protected abstract T mapViewToEntityBean(V viewBean, T t, int operationContext);

    protected abstract V mapEntityToViewBean(V viewBean, T t);

    protected String getResourceName() {
        String resourceName = tEntityValueMap.get(tEntityClass);

        if (resourceName == null || resourceName.isEmpty()) {
            resourceName = "Object";
        }

        return resourceName;
    }

    // ----------------------------------------------------------------------------------
    // Read Operation
    // ----------------------------------------------------------------------------------

    protected BaseDao<T> getDao() {
        if (entityDao == null) {
            throw new NullPointerException("entityDao is not injected by Spring!");
        }

        return entityDao;
    }

    protected T createEntityObject() {
        try {
            return tEntityClass.newInstance();
        } catch (Throwable e) {
            logger.error("Error instantiating entity class. tEntityClass={}", tEntityClass.toString(), e);
        }
        return null;
    }

    protected V createViewObject() {
        try {
            return tViewClass.newInstance();
        } catch (Throwable e) {
            logger.error("Error instantiating view class. tViewClass={}", tViewClass.toString(), e);
        }
        return null;
    }

    /**
     * Create Entity object and populate it from view object. Used in create operation
     */
    protected void mapBaseAttributesToEntityBean(T resource, V viewBean) {
        if (resource.getCreateTime() == null) {
            resource.setCreateTime(DateUtil.getUTCDate());
        }

        resource.setUpdateTime(DateUtil.getUTCDate());

        if (resource.getAddedByUserId() == null) {
            resource.setAddedByUserId(ContextUtil.getCurrentUserId());
        }

        resource.setUpdatedByUserId(ContextUtil.getCurrentUserId());
    }

    // ----------------------------------------------------------------------------------
    // Update Operation
    // ----------------------------------------------------------------------------------
    protected T populateEntityBeanForCreate(T t, V viewBaseBean) {
        mapBaseAttributesToEntityBean(t, viewBaseBean);

        return mapViewToEntityBean(viewBaseBean, t, OPERATION_CREATE_CONTEXT);
    }

    protected T preCreate(V viewBaseBean) {
        validateGenericAttributes(viewBaseBean);
        validateForCreate(viewBaseBean);

        T t = createEntityObject();

        t = populateEntityBeanForCreate(t, viewBaseBean);

        return t;
    }

    protected V postCreate(T resource) {
        return populateViewBean(resource);
    }

    protected T preRead(Long id) {
        return null;
    }

    protected V postRead(T resource) {
        return populateViewBean(resource);
    }

    /**
     * Populate Entity object from view object. Used in update operation
     */
    protected T populateEntityBeanForUpdate(T t, V viewBaseBean) {
        mapBaseAttributesToEntityBean(t, viewBaseBean);

        return mapViewToEntityBean(viewBaseBean, t, OPERATION_UPDATE_CONTEXT);
    }

    protected T preUpdate(V viewBaseBean) {
        T resource = getDao().getById(viewBaseBean.getId());

        if (resource == null) {
            // Returns code 400 with DATA_NOT_FOUND as the error message
            throw restErrorUtil.createRESTException(getResourceName() + " not found", MessageEnums.DATA_NOT_FOUND, viewBaseBean.getId(), null, "preUpdate: id not found.");
        }

        validateForUpdate(viewBaseBean, resource);

        return populateEntityBeanForUpdate(resource, viewBaseBean);
    }

    protected V postUpdate(T resource) {
        return populateViewBean(resource);
    }

    // ----------------------------------------------------------------------------------
    // Delete Operation
    // ----------------------------------------------------------------------------------
    protected T preDelete(Long id) {
        T resource = getDao().getById(id);

        if (resource == null) {
            // Return without error
            logger.info("Delete ignored for non-existent {} id={}", getResourceName(), id);
        }

        return resource;
    }

    protected void postDelete(T resource) {
    }

    // ----------------------------------------------------------------------------------
    // Validation
    // ----------------------------------------------------------------------------------
    protected void validateGenericAttributes(V viewBaseBean) {
    }

    // ----------------------------------------------------------------------------------
    // Search Operation
    // ----------------------------------------------------------------------------------

    protected V populateViewBean(T resource, V viewBean) {
        mapBaseAttributesToViewBean(resource, viewBean);

        // TODO:Current:Open: Need to set original and updated content
        return viewBean;
    }

    protected void mapBaseAttributesToViewBean(T resource, V viewBean) {
        viewBean.setId(resource.getId());

        // TBD: Need to review this change later
        viewBean.setMObj(resource);
        viewBean.setCreateDate(resource.getCreateTime());
        viewBean.setUpdateDate(resource.getUpdateTime());

        Long            ownerId            = resource.getAddedByUserId();
        UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();

        if (currentUserSession == null) {
            return;
        }

        if (ownerId != null) {
            XXPortalUser tUser = daoManager.getXXPortalUser().getById(resource.getAddedByUserId());

            if (tUser != null) {
                if (tUser.getPublicScreenName() != null && !tUser.getPublicScreenName().trim().isEmpty() && !"null".equalsIgnoreCase(tUser.getPublicScreenName().trim())) {
                    viewBean.setOwner(tUser.getPublicScreenName());
                } else {
                    if (tUser.getFirstName() != null && !tUser.getFirstName().trim().isEmpty() && !"null".equalsIgnoreCase(tUser.getFirstName().trim())) {
                        if (tUser.getLastName() != null && !tUser.getLastName().trim().isEmpty() && !"null".equalsIgnoreCase(tUser.getLastName().trim())) {
                            viewBean.setOwner(tUser.getFirstName() + " " + tUser.getLastName());
                        } else {
                            viewBean.setOwner(tUser.getFirstName());
                        }
                    } else {
                        viewBean.setOwner(tUser.getLoginId());
                    }
                }
            }
        }

        if (resource.getUpdatedByUserId() != null) {
            XXPortalUser tUser = daoManager.getXXPortalUser().getById(resource.getUpdatedByUserId());

            if (tUser != null) {
                if (tUser.getPublicScreenName() != null && !tUser.getPublicScreenName().trim().isEmpty() && !"null".equalsIgnoreCase(tUser.getPublicScreenName().trim())) {
                    viewBean.setUpdatedBy(tUser.getPublicScreenName());
                } else {
                    if (tUser.getFirstName() != null && !tUser.getFirstName().trim().isEmpty() && !"null".equalsIgnoreCase(tUser.getFirstName().trim())) {
                        if (tUser.getLastName() != null && !tUser.getLastName().trim().isEmpty() && !"null".equalsIgnoreCase(tUser.getLastName().trim())) {
                            viewBean.setUpdatedBy(tUser.getFirstName() + " " + tUser.getLastName());
                        } else {
                            viewBean.setUpdatedBy(tUser.getFirstName());
                        }
                    } else {
                        viewBean.setUpdatedBy(tUser.getLoginId());
                    }
                }
            }
        }
    }

    protected Query createQuery(String searchString, String sortString, SearchCriteria searchCriteria, List<SearchField> searchFieldList, boolean isCountQuery) {
        EntityManager em = getDao().getEntityManager();

        return searchUtil.createSearchQuery(em, searchString, sortString, searchCriteria, searchFieldList, false, isCountQuery);
    }

    protected long getCountForSearchQuery(SearchCriteria searchCriteria, List<SearchField> searchFieldList) {
        String q = countQueryStr;

        // Get total count of the rows which meet the search criteria
        if (searchCriteria.isDistinct()) {
            q = distinctCountQueryStr;
        }

        // Get total count of the rows which meet the search criteria
        Query query = createQuery(q, null, searchCriteria, searchFieldList, true);

        // Make the database call to get the total count
        Long count = getDao().executeCountQueryInSecurityContext(tEntityClass, query);

        if (count == null) {
            // If no data that meets the criteria, return 0
            return 0;
        }

        return count;
    }

    protected List<T> searchResources(SearchCriteria searchCriteria, List<SearchField> searchFieldList, List<SortField> sortFieldList, VList vList) {
        // Get total count of the rows which meet the search criteria
        long count = -1;

        if (searchCriteria.isGetCount()) {
            count = getCountForSearchQuery(searchCriteria, searchFieldList);

            if (count == 0) {
                return Collections.emptyList();
            }
        }

        // construct the sort clause
        String sortClause = searchUtil.constructSortClause(searchCriteria, sortFieldList);

        String q = queryStr;

        if (searchCriteria.isDistinct()) {
            q = distinctQueryStr;
        }

        // construct the query object for retrieving the data
        Query   query      = createQuery(q, sortClause, searchCriteria, searchFieldList, false);
        List<T> resultList = getDao().executeQueryInSecurityContext(tEntityClass, query);

        if (vList != null) {
            // Set the meta values for the query result
            vList.setPageSize(query.getMaxResults());
            vList.setSortBy(searchCriteria.getSortBy());
            vList.setSortType(searchCriteria.getSortType());
            vList.setStartIndex(query.getFirstResult());
            vList.setTotalCount(count);
            vList.setResultSize(resultList.size());
        }

        return resultList;
    }

    protected List<T> searchResourcesUsingCriteria(SearchCriteria searchCriteria, List<SearchField> searchFieldList, List<SortField> sortFieldList, VList vList) {
        EntityManager   em              = getDao().getEntityManager();
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery   criteria        = criteriaBuilder.createQuery();
        Root<T>         from            = criteria.from(tEntityClass);

        Predicate resourceConditions = buildResourceSpecificConditions(criteriaBuilder, from, searchCriteria);
        Predicate userConditions     = buildUserConditions(searchCriteria.getParamList(), searchFieldList, criteriaBuilder, from);

        if (resourceConditions != null) {
            criteria.where(criteriaBuilder.and(resourceConditions, userConditions));
        } else {
            criteria.where(criteriaBuilder.and(userConditions));
        }

        // Get total count of the rows which meet the search criteria
        long count = -1;
        if (searchCriteria.isGetCount()) {
            Expression<Long> countExpression = criteriaBuilder.count(from.get("id"));

            criteria.select(countExpression);

            TypedQuery<Long> countQuery = em.createQuery(criteria);

            count = getDao().executeCountQueryInSecurityContext(tEntityClass, countQuery);

            if (count == 0) {
                return Collections.emptyList();
            }
        }

        // construct the sort clause
        setSortClause(searchCriteria, sortFieldList, criteriaBuilder, criteria, from);

        criteria.select(from);

        TypedQuery<T> typedQuery = em.createQuery(criteria);

        searchUtil.updateQueryPageSize(typedQuery, searchCriteria);

        List<T> resultList = getDao().executeQueryInSecurityContext(tEntityClass, typedQuery);

        if (vList != null) {
            // Set the meta values for the query result
            vList.setPageSize(typedQuery.getMaxResults());
            vList.setSortBy(searchCriteria.getSortBy());
            vList.setSortType(searchCriteria.getSortType());
            vList.setStartIndex(typedQuery.getFirstResult());
            vList.setTotalCount(count);
        }

        return resultList;
    }

    protected Predicate buildUserConditions(Map<String, Object> paramList, List<SearchField> searchFields, CriteriaBuilder cb, Root<? extends XXDBBase> from) {
        Predicate userConditions = cb.conjunction();

        for (SearchField searchField : searchFields) {
            if (paramList.containsKey(searchField.getClientFieldName())) {
                Path<Object> tableField;
                String       fieldName  = searchField.getFieldName();

                // stuff to handle jpql syntax (e.g. obj.id, obj.city.city etc). There has to be better way of dealing with this. Will look again.
                int dotIndex = fieldName.indexOf(".");

                if (dotIndex != -1) {
                    fieldName = fieldName.substring(dotIndex + 1);
                }

                dotIndex = fieldName.indexOf(".");

                if (dotIndex == -1) {
                    tableField = from.get(fieldName);
                } else {
                    String joinTable = fieldName.substring(0, dotIndex);

                    fieldName  = fieldName.substring(dotIndex + 1);
                    tableField = from.join(joinTable).get(fieldName);
                }

                Object value = paramList.get(searchField.getClientFieldName());

                if (value == null) {
                    userConditions = cb.and(userConditions, cb.isNull(tableField));
                    continue;
                }

                if (searchField.getDataType() == SearchField.DATA_TYPE.INTEGER || searchField.getDataType() == SearchField.DATA_TYPE.BOOLEAN) {
                    userConditions = cb.and(userConditions, cb.equal(tableField, value));
                } else if (searchField.getDataType() == SearchField.DATA_TYPE.STRING) {
                    String             strFieldValue           = (String) value;
                    Expression<String> tableFieldWithLowerExpr = cb.lower(tableField.as(String.class));

                    if (searchField.getSearchType() == SearchField.SEARCH_TYPE.FULL) {
                        Expression<String> literal = cb.lower(cb.literal(strFieldValue));

                        userConditions = cb.and(userConditions, cb.equal(tableFieldWithLowerExpr, literal));
                    } else {
                        Expression<String> literal = cb.lower(cb.literal("%" + strFieldValue + "%"));

                        userConditions = cb.and(userConditions, cb.like(tableFieldWithLowerExpr, literal));
                    }
                } else if (searchField.getDataType() == SearchField.DATA_TYPE.INT_LIST) {
                    @SuppressWarnings("unchecked")
                    Collection<Number> intValueList = (Collection<Number>) value;
                    if (intValueList.size() == 1) {
                        userConditions = cb.and(userConditions, cb.equal(tableField, value));
                    } else if (intValueList.size() > 1) {
                        userConditions = cb.and(userConditions, tableField.in(intValueList));
                    }
                }
            }
        }

        return userConditions;
    }

    protected Predicate buildResourceSpecificConditions(CriteriaBuilder criteriaBuilder, Root<T> from, SearchCriteria sc) {
        return null;
    }

    /**
     * @param resource
     * @return
     */
    private V readResource(T resource) {
        // object security
        if (!objectSecurityHandler.hasAccess(resource, Permission.PermissionType.READ)) {
            throw restErrorUtil.create403RESTException(getResourceName() + " access denied. classType=" + resource.getMyClassType() + ", className=" + resource.getClass().getName() + ", objectId=" + resource.getId() + ", object=" + resource);
        }

        return postRead(resource);
    }

    static {
        tEntityValueMap.put(XXAuthSession.class, "Auth Session");
        tEntityValueMap.put(XXDBBase.class, "Base");
    }
}
