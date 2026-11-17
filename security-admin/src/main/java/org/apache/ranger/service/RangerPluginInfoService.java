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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPluginInfo;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.view.VXXPluginInfo;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class RangerPluginInfoService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPluginInfoService.class);

    private final List<SortField>   sortFields   = new ArrayList<>();
    private final List<SearchField> searchFields = new ArrayList<>();

    @Autowired
    RangerSearchUtil searchUtil;

    @Autowired
    RangerBizUtil bizUtil;

    @Autowired
    JSONUtil jsonUtil;

    @Autowired
    RangerDaoManager daoManager;

    RangerPluginInfoService() {
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "obj.serviceName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.PLUGIN_HOST_NAME, "obj.hostName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.PLUGIN_APP_TYPE, "obj.appType", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.PLUGIN_IP_ADDRESS, "obj.ipAddress", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE, "obj.serviceType", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.CLUSTER_NAME, "obj.clusterName", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));

        sortFields.add(new SortField(SearchFilter.SERVICE_NAME, "obj.serviceName", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField(SearchFilter.PLUGIN_HOST_NAME, "obj.hostName", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField(SearchFilter.PLUGIN_APP_TYPE, "obj.appType", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField(SearchFilter.PLUGIN_IP_ADDRESS, "obj.ipAddress", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField(SearchFilter.SERVICE_TYPE, "obj.serviceType", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField(SearchFilter.CLUSTER_NAME, "obj.clusterName", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("policyDownloadTime", "obj.policyDownloadTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("policyActivationTime", "obj.policyActivationTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("lastPolicyUpdateTime", "obj.lastPolicyUpdateTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("tagDownloadTime", "obj.tagDownloadTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("tagActivationTime", "obj.tagActivationTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("lastTagUpdateTime", "obj.lastTagUpdateTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("gdsDownloadTime", "obj.gdsDownloadTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("gdsActivationTime", "obj.gdsActivationTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("lastGdsUpdateTime", "obj.lastGdsUpdateTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("roleDownloadTime", "obj.roleDownloadTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("roleActivationTime", "obj.roleActivationTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("lastRoleUpdateTime", "obj.lastRoleUpdateTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("userstoreDownloadTime", "obj.userstoreDownloadTime", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField("userstoreActivationTime", "obj.userstoreActivationTime", true, SortField.SORT_ORDER.ASC));
    }

    public List<SearchField> getSearchFields() {
        return searchFields;
    }

    public List<SortField> getSortFields() {
        return sortFields;
    }

    public PList<RangerPluginInfo> searchRangerPluginInfo(SearchFilter searchFilter) {
        PList<RangerPluginInfo> retList = new PList<>();
        List<RangerPluginInfo>  objList = new ArrayList<>();

        List<VXXPluginInfo> xObjList = searchRangerObjects(searchFilter, searchFields, sortFields, retList);

        for (VXXPluginInfo xObj : xObjList) {
            RangerPluginInfo obj = populatePluginViewObject(xObj);

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
        ret.setInfo(jsonStringToMap(xObj.getInfo(), null));

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
        ret.setPolicyDownloadTime(modelObj.getPolicyDownloadTime());
        ret.setPolicyActivationTime(modelObj.getPolicyActivationTime());
        ret.setTagDownloadTime(modelObj.getTagDownloadTime());
        ret.setTagActivationTime(modelObj.getTagActivationTime());
        ret.setGdsDownloadTime(modelObj.getGdsDownloadTime());
        ret.setGdsActivationTime(modelObj.getGdsActivationTime());
        ret.setRoleDownloadTime(modelObj.getRoleDownloadTime());
        ret.setRoleActivationTime(modelObj.getRoleActivationTime());
        ret.setUserstoreDownloadTime(modelObj.getUserStoreDownloadTime());
        ret.setUserstoreActivationTime(modelObj.getUserStoreActivationTime());
        ret.setClusterName(modelObj.getClusterName());

        return ret;
    }

    private RangerPluginInfo populatePluginViewObject(VXXPluginInfo xObj) {
        RangerPluginInfo ret = new RangerPluginInfo();

        ret.setId(xObj.getId());
        ret.setCreateTime(xObj.getCreateTime());
        ret.setUpdateTime(xObj.getUpdateTime());
        ret.setServiceName(xObj.getServiceName());

        String serviceDefName = xObj.getServiceType();

        if (StringUtils.isNotBlank(serviceDefName)) {
            ret.setServiceType(serviceDefName);

            XXServiceDef xxServiceDef = daoManager.getXXServiceDef().findByName(serviceDefName);

            ret.setServiceTypeDisplayName(xxServiceDef.getDisplayName());
        }

        ret.setHostName(xObj.getHostName());
        ret.setAppType(xObj.getAppType());
        ret.setIpAddress(xObj.getIpAddress());
        ret.setInfo(jsonStringToMap(xObj.getInfo(), xObj));

        XXService xxService = daoManager.getXXService().findByName(ret.getServiceName());

        if (xxService != null) {
            ret.setServiceDisplayName(xxService.getDisplayName());
        }

        return ret;
    }

    private List<VXXPluginInfo> searchRangerObjects(SearchFilter searchCriteria, List<SearchField> searchFieldList, List<SortField> sortFieldList, PList<RangerPluginInfo> pList) {
        long count = -1;

        if (searchCriteria.isGetCount()) {
            count = getCountForSearchQuery(searchCriteria, searchFieldList);

            if (count == 0) {
                return Collections.emptyList();
            }
        }

        String      sortClause = searchUtil.constructSortClause(searchCriteria, sortFieldList);
        String      queryStr   = "SELECT obj FROM " + VXXPluginInfo.class.getName() + " obj ";
        Query       query      = createQuery(queryStr, sortClause, searchCriteria, searchFieldList, false);
        List<VXXPluginInfo> resultList = query.getResultList();

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

    private Query createQuery(String searchString, String sortString, SearchFilter searchCriteria, List<SearchField> searchFieldList, boolean isCountQuery) {
        EntityManager em = daoManager.getEntityManager();

        return searchUtil.createSearchQuery(em, searchString, sortString, searchCriteria, searchFieldList, false, isCountQuery);
    }

    private long getCountForSearchQuery(SearchFilter searchCriteria, List<SearchField> searchFieldList) {
        String countQueryStr = "SELECT COUNT(obj) FROM " + VXXPluginInfo.class.getName() + " obj ";
        Query  query         = createQuery(countQueryStr, null, searchCriteria, searchFieldList, true);
        Long   count         = (Long) query.getSingleResult();
        long   result        = 0;

        if (count != null) {
            result = count;
        }

        return result;
    }

    private String mapToJsonString(Map<String, String> map) {
        String ret = null;

        if (map != null) {
            try {
                ret = jsonUtil.readMapToString(map);
            } catch (Exception excp) {
                LOG.error("Failed to convert map to JSON string: {}", map, excp);
            }
        }

        return ret;
    }

    private Map<String, String> jsonStringToMap(String jsonStr, VXXPluginInfo xObj) {
        Map<String, String> ret = null;

        try {
            ret = jsonUtil.jsonToMap(jsonStr);

            if (xObj != null) {
                Long   latestPolicyVersion  = xObj.getLatestPolicyVersion();
                Date   lastPolicyUpdateTime = xObj.getLastPolicyUpdateTime();
                Long   latestTagVersion     = xObj.getLatestTagVersion();
                Date   lastTagUpdateTime    = xObj.getLastTagUpdateTime();
                Long   latestGdsVersion     = xObj.getLatestGdsVersion();
                Date   lastGdsUpdateTime    = xObj.getLastGdsUpdateTime();
                Long   latestRoleVersion    = xObj.getLatestRoleVersion();
                Date   lastRoleUpdateTime   = xObj.getLastRoleUpdateTime();

                ret.put(RangerPluginInfo.RANGER_ADMIN_LATEST_POLICY_VERSION, latestPolicyVersion == null ? "" : Long.toString(latestPolicyVersion));
                ret.put(RangerPluginInfo.RANGER_ADMIN_LAST_POLICY_UPDATE_TIME, lastPolicyUpdateTime == null ? "" : Long.toString(lastPolicyUpdateTime.getTime()));
                ret.put(RangerPluginInfo.RANGER_ADMIN_LATEST_GDS_VERSION, latestGdsVersion == null ? "" : Long.toString(latestGdsVersion));
                ret.put(RangerPluginInfo.RANGER_ADMIN_LAST_GDS_UPDATE_TIME, lastGdsUpdateTime == null ? "" : Long.toString(lastGdsUpdateTime.getTime()));
                ret.put(RangerPluginInfo.RANGER_ADMIN_LATEST_ROLE_VERSION, latestRoleVersion == null ? "" : Long.toString(latestRoleVersion));
                ret.put(RangerPluginInfo.RANGER_ADMIN_LAST_ROLE_UPDATE_TIME, lastRoleUpdateTime == null ? "" : Long.toString(lastRoleUpdateTime.getTime()));

                if (xObj.getLatestTagVersion() != null && Boolean.TRUE.equals(xObj.getIsTagServiceEnabled())) {
                    ret.put(RangerPluginInfo.RANGER_ADMIN_LATEST_TAG_VERSION, Long.toString(latestTagVersion));
                    ret.put(RangerPluginInfo.RANGER_ADMIN_LAST_TAG_UPDATE_TIME, lastTagUpdateTime == null ? "" : Long.toString(lastTagUpdateTime.getTime()));
                } else {
                    ret.remove(RangerPluginInfo.RANGER_ADMIN_LATEST_TAG_VERSION);
                    ret.remove(RangerPluginInfo.RANGER_ADMIN_LAST_TAG_UPDATE_TIME);
                }
            }
        } catch (Exception excp) {
            LOG.error("Failed to convert JSON string to Map: {}", jsonStr, excp);
        }

        return ret;
    }
}
