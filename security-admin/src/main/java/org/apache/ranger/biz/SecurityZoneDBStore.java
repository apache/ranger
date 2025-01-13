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

package org.apache.ranger.biz;

import java.util.*;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerSecurityZoneHeaderInfo;
import org.apache.ranger.plugin.model.RangerServiceHeaderInfo;
import org.apache.ranger.plugin.model.RangerPrincipal.PrincipalType;
import org.apache.ranger.plugin.model.RangerSecurityZone.RangerSecurityZoneService;
import org.apache.ranger.plugin.model.RangerSecurityZone.SecurityZoneSummary;
import org.apache.ranger.plugin.model.RangerSecurityZone.ZoneServiceSummary;
import org.apache.ranger.plugin.store.AbstractPredicateUtil;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.SecurityZonePredicateUtil;
import org.apache.ranger.plugin.store.SecurityZoneStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.rest.SecurityZoneREST;
import org.apache.ranger.service.RangerBaseModelService;
import org.apache.ranger.service.RangerSecurityZoneServiceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SecurityZoneDBStore implements SecurityZoneStore {
    private static final Logger LOG = LoggerFactory.getLogger(SecurityZoneDBStore.class);
    private static final String RANGER_GLOBAL_STATE_NAME = "RangerSecurityZone";

    @Autowired
    RangerSecurityZoneServiceService securityZoneService;

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
	SecurityZoneRefUpdater securityZoneRefUpdater;

    @Autowired
    RangerBizUtil bizUtil;

    AbstractPredicateUtil predicateUtil = null;

    @Autowired
    ServiceMgr serviceMgr;

    public void init() throws Exception {}

    @PostConstruct
    public void initStore() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SecurityZoneDBStore.initStore()");
        }

        predicateUtil = new SecurityZonePredicateUtil();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== SecurityZoneDBStore.initStore()");
        }
    }

    @Override
    public RangerSecurityZone createSecurityZone(RangerSecurityZone securityZone) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SecurityZoneDBStore.createSecurityZone()");
        }

        XXSecurityZone xxSecurityZone = daoMgr.getXXSecurityZoneDao().findByZoneName(securityZone.getName());

        if (xxSecurityZone != null) {
            throw restErrorUtil.createRESTException("security-zone with name: " + securityZone.getName() + " already exists", MessageEnums.ERROR_DUPLICATE_OBJECT);
        }

        daoMgr.getXXGlobalState().onGlobalStateChange(RANGER_GLOBAL_STATE_NAME);

        RangerSecurityZone createdSecurityZone = securityZoneService.create(securityZone);
        if (createdSecurityZone == null) {
            throw restErrorUtil.createRESTException("Cannot create security zone:[" + securityZone + "]");
        }
        securityZoneRefUpdater.createNewZoneMappingForRefTable(createdSecurityZone);
        securityZoneService.createTransactionLog(createdSecurityZone, null, RangerBaseModelService.OPERATION_CREATE_CONTEXT);
        return createdSecurityZone;
    }

    @Override
	public RangerSecurityZone updateSecurityZoneById(RangerSecurityZone securityZone) throws Exception {
        RangerSecurityZone oldSecurityZone = securityZoneService.read(securityZone.getId());

        daoMgr.getXXGlobalState().onGlobalStateChange(RANGER_GLOBAL_STATE_NAME);

        RangerSecurityZone updatedSecurityZone = securityZoneService.update(securityZone);
        if (updatedSecurityZone == null) {
            throw restErrorUtil.createRESTException("Cannot update security zone:[" + securityZone + "]");
        }
        securityZoneRefUpdater.createNewZoneMappingForRefTable(updatedSecurityZone);
        boolean isRenamed = !StringUtils.equals(securityZone.getName(), (null == oldSecurityZone) ? null : oldSecurityZone.getName());
		if (isRenamed) {
			securityZoneRefUpdater.updateResourceSignatureWithZoneName(updatedSecurityZone);
		}
        securityZoneService.createTransactionLog(updatedSecurityZone, oldSecurityZone, RangerBaseModelService.OPERATION_UPDATE_CONTEXT);
        return securityZone;
    }

    @Override
    public void deleteSecurityZoneByName(String zoneName) throws Exception {
        XXSecurityZone xxSecurityZone = daoMgr.getXXSecurityZoneDao().findByZoneName(zoneName);
        if (xxSecurityZone == null) {
            throw restErrorUtil.createRESTException("security-zone with name: " + zoneName + " does not exist");
        }
        RangerSecurityZone securityZone = securityZoneService.read(xxSecurityZone.getId());

        daoMgr.getXXGlobalState().onGlobalStateChange(RANGER_GLOBAL_STATE_NAME);

        securityZoneRefUpdater.cleanupRefTables(securityZone);

        securityZoneService.delete(securityZone);
        securityZoneService.createTransactionLog(securityZone, null, RangerBaseModelService.OPERATION_DELETE_CONTEXT);
        }

    @Override
    public void deleteSecurityZoneById(Long zoneId) throws Exception {
        RangerSecurityZone securityZone = securityZoneService.read(zoneId);

        daoMgr.getXXGlobalState().onGlobalStateChange(RANGER_GLOBAL_STATE_NAME);

        securityZoneRefUpdater.cleanupRefTables(securityZone);

        securityZoneService.delete(securityZone);
        securityZoneService.createTransactionLog(securityZone, null, RangerBaseModelService.OPERATION_DELETE_CONTEXT);
    }

    @Override
    public RangerSecurityZone getSecurityZone(Long id) throws Exception {
        return securityZoneService.read(id);
    }

    @Override
    public RangerSecurityZone getSecurityZoneByName(String name) throws Exception {
        XXSecurityZone xxSecurityZone = daoMgr.getXXSecurityZoneDao().findByZoneName(name);
        if (xxSecurityZone == null) {
            throw restErrorUtil.createRESTException("security-zone with name: " + name + " does not exist");
        }
        return securityZoneService.read(xxSecurityZone.getId());
    }

    @Override
    public List<RangerSecurityZone> getSecurityZones(SearchFilter filter) throws Exception {
        List<RangerSecurityZone> ret = new ArrayList<>();

        List<XXSecurityZone> xxSecurityZones = daoMgr.getXXSecurityZoneDao().getAll();

        for (XXSecurityZone xxSecurityZone : xxSecurityZones) {
            if (!xxSecurityZone.getId().equals(RangerSecurityZone.RANGER_UNZONED_SECURITY_ZONE_ID)) {
                ret.add(securityZoneService.read(xxSecurityZone.getId()));
            }
        }

        if (CollectionUtils.isNotEmpty(ret) && filter != null) {
            List<RangerSecurityZone> copy = new ArrayList<>(ret);

            predicateUtil.applyFilter(copy, filter);
            ret = copy;
        }

        return ret;
    }

    @Override
    public Map<String, RangerSecurityZone.RangerSecurityZoneService> getSecurityZonesForService(String serviceName) {
        Map<String, RangerSecurityZone.RangerSecurityZoneService> ret = null;

        SearchFilter filter = new SearchFilter();
        filter.setParam(SearchFilter.SERVICE_NAME, serviceName);

        try {
            List<RangerSecurityZone> matchingZones = getSecurityZones(filter);

            if (CollectionUtils.isNotEmpty(matchingZones)) {
                ret = new HashMap<>();

                for (RangerSecurityZone matchingZone : matchingZones) {
                    ret.put(matchingZone.getName(), matchingZone.getServices().get(serviceName));
                }
            }
        } catch (Exception excp) {
            LOG.error("Failed to get security zones for service:[" + serviceName + "]", excp);
        }

        return ret;
    }

    public List<RangerSecurityZoneHeaderInfo> getSecurityZoneHeaderInfoList(HttpServletRequest request) {
        String  namePrefix         = request.getParameter(SearchFilter.ZONE_NAME_PREFIX);
        boolean filterByNamePrefix = StringUtils.isNotBlank(namePrefix);

        List<RangerSecurityZoneHeaderInfo> ret = daoMgr.getXXSecurityZoneDao().findAllZoneHeaderInfos();

        if (!ret.isEmpty() && filterByNamePrefix) {
            for (ListIterator<RangerSecurityZoneHeaderInfo> iter = ret.listIterator(); iter.hasNext(); ) {
                RangerSecurityZoneHeaderInfo zoneHeader = iter.next();

                if (!StringUtils.startsWithIgnoreCase(zoneHeader.getName(), namePrefix)) {
                    iter.remove();
                }
            }
        }

        return ret;
    }

    public List<RangerServiceHeaderInfo> getServiceHeaderInfoListByZoneId(Long zoneId, HttpServletRequest request) {
        if (!bizUtil.hasModuleAccess(RangerConstants.MODULE_SECURITY_ZONE)) {
            throw restErrorUtil.createRESTException(SecurityZoneREST.STR_USER_NOT_AUTHORIZED_TO_ACCESS_ZONE, MessageEnums.OPER_NO_PERMISSION);
        }

        String  namePrefix         = request.getParameter(SearchFilter.SERVICE_NAME_PREFIX);
        boolean filterByNamePrefix = StringUtils.isNotBlank(namePrefix);

        List<RangerServiceHeaderInfo> services    = daoMgr.getXXSecurityZoneRefService().findServiceHeaderInfosByZoneId(zoneId);
        List<RangerServiceHeaderInfo> tagServices = daoMgr.getXXSecurityZoneRefTagService().findServiceHeaderInfosByZoneId(zoneId);
        List<RangerServiceHeaderInfo> ret         = new ArrayList<>(services.size() + tagServices.size());

        ret.addAll(services);
        ret.addAll(tagServices);

        if (!ret.isEmpty() && filterByNamePrefix) {
            for (ListIterator<RangerServiceHeaderInfo> iter = ret.listIterator(); iter.hasNext(); ) {
                RangerServiceHeaderInfo serviceHeader = iter.next();

                if (!StringUtils.startsWithIgnoreCase(serviceHeader.getName(), namePrefix)) {
                    iter.remove();
                }
            }
        }

        return ret;
    }

    public List<RangerSecurityZoneHeaderInfo> getSecurityZoneHeaderInfoListByServiceId(Long serviceId, Boolean isTagService, HttpServletRequest request) {
        if (serviceId == null){
            throw restErrorUtil.createRESTException("Invalid value for serviceId", MessageEnums.INVALID_INPUT_DATA);
        }

        String  namePrefix         = request.getParameter(SearchFilter.ZONE_NAME_PREFIX);
        boolean filterByNamePrefix = StringUtils.isNotBlank(namePrefix);

        List<RangerSecurityZoneHeaderInfo> ret = daoMgr.getXXSecurityZoneDao().findAllZoneHeaderInfosByServiceId(serviceId, isTagService);

        if (!ret.isEmpty() && filterByNamePrefix) {
            for (ListIterator<RangerSecurityZoneHeaderInfo> iter = ret.listIterator(); iter.hasNext(); ) {
                RangerSecurityZoneHeaderInfo zoneHeader = iter.next();

                if (!StringUtils.startsWithIgnoreCase(zoneHeader.getName(), namePrefix)) {
                    iter.remove();
                }
            }
        }

        return ret;
    }

    public PList<SecurityZoneSummary> getZonesSummary(SearchFilter filter) throws Exception {
        int maxRows    = filter.getMaxRows();
        int startIndex = filter.getStartIndex();

        filter.setStartIndex(0);
        filter.setMaxRows(0);

        List<RangerSecurityZone>  securityZones = getSecurityZones(filter);
        List<SecurityZoneSummary> summaryList   = new ArrayList<>();

        for (RangerSecurityZone securityZone : securityZones) {
            if (bizUtil.isAdmin() || serviceMgr.isZoneAdmin(securityZone.getName()) || serviceMgr.isZoneAuditor(securityZone.getName())) {
                summaryList.add(toSecurityZoneSummary(securityZone));
            }
        }

        List<SecurityZoneSummary>  paginatedList;

        if (summaryList.size() > startIndex) {
            int endIndex = Math.min((startIndex + maxRows), summaryList.size());

            paginatedList = summaryList.subList(startIndex, endIndex);
        } else {
            paginatedList = Collections.emptyList();
        }

        PList<SecurityZoneSummary> ret = new PList<>(paginatedList, startIndex, maxRows, summaryList.size(), paginatedList.size(), filter.getSortType(), filter.getSortBy());

        return ret;
    }

    private SecurityZoneSummary toSecurityZoneSummary(RangerSecurityZone securityZone) {
        SecurityZoneSummary ret = new SecurityZoneSummary();

        ret.setId(securityZone.getId());
        ret.setName(securityZone.getName());
        ret.setDescription(securityZone.getDescription());
        ret.setGuid(securityZone.getGuid());
        ret.setCreateTime(securityZone.getCreateTime());
        ret.setUpdateTime(securityZone.getUpdateTime());
        ret.setCreatedBy(securityZone.getCreatedBy());
        ret.setUpdatedBy(securityZone.getUpdatedBy());
        ret.setVersion(ret.getVersion());
        ret.setIsEnabled(securityZone.getIsEnabled());
        ret.setTagServices(securityZone.getTagServices());

        Map<PrincipalType, Integer> adminCount   = new HashMap<>();
        Map<PrincipalType, Integer> auditorCount = new HashMap<>();

        adminCount.put(PrincipalType.USER, securityZone.getAdminUsers().size());
        adminCount.put(PrincipalType.GROUP, securityZone.getAdminUserGroups().size());
        adminCount.put(PrincipalType.ROLE, securityZone.getAdminRoles().size());

        auditorCount.put(PrincipalType.USER, securityZone.getAuditUsers().size());
        auditorCount.put(PrincipalType.GROUP, securityZone.getAuditUserGroups().size());
        auditorCount.put(PrincipalType.ROLE, securityZone.getAuditRoles().size());

        ret.setAdminCount(adminCount);
        ret.setAuditorCount(auditorCount);

        List<ZoneServiceSummary> services = getSecurityZoneServiceSummary(securityZone);

        ret.setServices(services);
        ret.setTotalResourceCount(services.stream().mapToLong(ZoneServiceSummary::getResourceCount).sum());

        return ret;
    }

    private List<ZoneServiceSummary> getSecurityZoneServiceSummary(RangerSecurityZone securityZone) {
        List<ZoneServiceSummary> ret = new ArrayList<>();

        if(MapUtils.isNotEmpty(securityZone.getServices())) {
            for(Map.Entry<String, RangerSecurityZoneService> entry : securityZone.getServices().entrySet()) {
                String                    serviceName = entry.getKey();
                RangerSecurityZoneService zoneService = entry.getValue();
                XXService                 xService    = daoMgr.getXXService().findByName(serviceName);
                XXServiceDef              serviceDef  = daoMgr.getXXServiceDef().getById(xService.getType());
                ZoneServiceSummary        summary     = new ZoneServiceSummary();

                summary.setId(xService.getId());
                summary.setName(serviceName);
                summary.setType(serviceDef.getName());
                summary.setDisplayName(xService.getDisplayName());
                summary.setResourceCount((long)zoneService.getResources().size());

                ret.add(summary);
            }
        }

        return ret;
    }
}
