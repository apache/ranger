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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXServiceVersionInfo;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerSecurityZone.RangerSecurityZoneService;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
@Scope("singleton")
public class RangerSecurityZoneServiceService extends RangerSecurityZoneServiceBase<XXSecurityZone, RangerSecurityZone> {
	@Autowired
	ServiceDBStore serviceDBStore;

    boolean compressJsonData = false;

    private static final Logger logger = LoggerFactory.getLogger(RangerSecurityZoneServiceService.class);

    private Map<Long, Set<String>> serviceNamesInZones = new HashMap<>();
    private Map<Long, Set<String>> tagServiceNamesInZones = new HashMap<>();


    public RangerSecurityZoneServiceService() {
        super();
    }

    @PostConstruct
    public void initService() {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerSecurityZoneServiceService.initService()");
        }

        RangerAdminConfig config = RangerAdminConfig.getInstance();

        compressJsonData = config.getBoolean("ranger.admin.store.security.zone.compress.json_data", compressJsonData);

        logger.info("ranger.admin.store.security.zone.compress.json_data={}", compressJsonData);

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerSecurityZoneServiceService.initService()");
        }
    }

    @Override
    protected void validateForCreate(RangerSecurityZone vObj) {
    }

    @Override
    protected void validateForUpdate(RangerSecurityZone vObj, XXSecurityZone entityObj) {
        // Cache service-names in existing zone object
        RangerSecurityZone existingZone = new RangerSecurityZone();
        existingZone = mapEntityToViewBean(existingZone, entityObj);
        serviceNamesInZones.put(entityObj.getId(), existingZone.getServices().keySet());
        tagServiceNamesInZones.put(entityObj.getId(), new HashSet<>(existingZone.getTagServices()));
    }

    @Override
    protected XXSecurityZone mapViewToEntityBean(RangerSecurityZone securityZone, XXSecurityZone xxSecurityZone, int OPERATION_CONTEXT) {
        XXSecurityZone ret = super.mapViewToEntityBean(securityZone, xxSecurityZone, OPERATION_CONTEXT);

        String json = JsonUtils.objectToJson(securityZone);

        if (StringUtils.isNotEmpty(json) && compressJsonData) {
            try {
                ret.setJsonData(null);
                ret.setGzJsonData(StringUtil.gzipCompress(json));
            } catch (IOException excp) {
                logger.error("mapViewToEntityBean(): json compression failed (length="+json.length()+"). Will save uncompressed json", excp);

                ret.setJsonData(json);
                ret.setGzJsonData(null);
            }
        } else {
            ret.setJsonData(json);
            ret.setGzJsonData(null);
        }

        return ret;
    }
    @Override
    protected RangerSecurityZone mapEntityToViewBean(RangerSecurityZone securityZone, XXSecurityZone xxSecurityZone) {
        RangerSecurityZone ret    = super.mapEntityToViewBean(securityZone, xxSecurityZone);
        byte[]             gzJson = xxSecurityZone.getGzJsonData();
        String             json;

        if (gzJson != null) {
            try {
                json = StringUtil.gzipDecompress(gzJson);
            } catch (IOException excp) {
                json = xxSecurityZone.getJsonData();

                logger.error("mapEntityToViewBean(): decompression of x_security_zone.gz_jsonData failed (length={}). Will use contents of x_security_zone.jsonData (length={})", gzJson.length, (json != null ? json.length() : 0), excp);
            }
        } else {
            json = xxSecurityZone.getJsonData();
        }

        if (StringUtils.isNotEmpty(json)) {
            RangerSecurityZone zoneFromJsonData = JsonUtils.jsonToObject(json, RangerSecurityZone.class);

            if (zoneFromJsonData != null) {
                ret.setName(zoneFromJsonData.getName());
                ret.setServices(zoneFromJsonData.getServices());
                ret.setAdminUsers(zoneFromJsonData.getAdminUsers());
                ret.setAdminUserGroups(zoneFromJsonData.getAdminUserGroups());
                ret.setAdminRoles(zoneFromJsonData.getAdminRoles());
                ret.setAuditUsers(zoneFromJsonData.getAuditUsers());
                ret.setAuditUserGroups(zoneFromJsonData.getAuditUserGroups());
                ret.setAuditRoles(zoneFromJsonData.getAuditRoles());
                ret.setTagServices(zoneFromJsonData.getTagServices());
            }
        } else {
            logger.info("Empty string representing jsonData in [" + xxSecurityZone + "]!!");
        }

        return ret;
    }

    @Override
    public RangerSecurityZone postCreate(XXSecurityZone xObj) {
        // Ensure to update ServiceVersionInfo for each service in the zone

        RangerSecurityZone ret = super.postCreate(xObj);
        Set<String> serviceNames = ret.getServices().keySet();

        List<String> tagServiceNames = ret.getTagServices();

        // Create default zone policies
        try {
            serviceDBStore.createZoneDefaultPolicies(serviceNames, ret);
            updateServiceInfos(serviceNames);

            serviceDBStore.createZoneDefaultPolicies(tagServiceNames, ret);
            updateServiceInfos(tagServiceNames);
        } catch (Exception exception) {
            logger.error("postCreate processing failed for security-zone:[" + ret + "]", exception);
            ret = null;
        }

        return ret;
    }

        @Override
    public RangerSecurityZone postUpdate(XXSecurityZone xObj) {
        // Update ServiceVersionInfo for all affected services
        RangerSecurityZone ret = super.postUpdate(xObj);

        Set<String> oldServiceNames = new HashSet(serviceNamesInZones.remove(xObj.getId()));
        Set<String> updatedServiceNames = ret.getServices().keySet();

        Set<String> oldTagServiceNames = new HashSet(tagServiceNamesInZones.remove(xObj.getId()));
        Set<String> updatedTagServiceNames = new HashSet<String>(ret.getTagServices());

        Collection<String> newServiceNames = CollectionUtils.subtract(updatedServiceNames, oldServiceNames);
        Collection<String> deletedServiceNames = CollectionUtils.subtract(oldServiceNames, updatedServiceNames);

        Collection<String> deletedTagServiceNames = CollectionUtils.subtract(oldTagServiceNames, updatedTagServiceNames);

        try {
            serviceDBStore.createZoneDefaultPolicies(newServiceNames, ret);
            serviceDBStore.deleteZonePolicies(deletedServiceNames, ret.getId());

            serviceDBStore.deleteZonePolicies(deletedTagServiceNames, ret.getId());

            oldServiceNames.addAll(updatedServiceNames);
            updateServiceInfos(oldServiceNames);
        } catch (Exception exception) {
            logger.error("postUpdate processing failed for security-zone:[" + ret + "]", exception);
            ret = null;
        }
        return ret;
    }

    @Override
    public XXSecurityZone preDelete(Long id) {
        // Update ServiceVersionInfo for each service in the zone
        XXSecurityZone ret = super.preDelete(id);
        RangerSecurityZone viewObject = new RangerSecurityZone();
        viewObject = mapEntityToViewBean(viewObject, ret);
        Set<String> allServiceNames = new HashSet<>(viewObject.getTagServices());
        allServiceNames.addAll(viewObject.getServices().keySet());

        // Delete default zone policies

        try {
            serviceDBStore.deleteZonePolicies(allServiceNames, id);
            updateServiceInfos(allServiceNames);
        } catch (Exception exception) {
            logger.error("preDelete processing failed for security-zone:[" + viewObject + "]", exception);
            ret = null;
        }

        return ret;
    }

    @Override
    public String getTrxLogAttrValue(RangerSecurityZone obj, VTrxLogAttr trxLogAttr) {
        final String ret;

        if (compressJsonData && obj != null && "services".equalsIgnoreCase(trxLogAttr.getAttribName())) {
            Map<String, RangerSecurityZoneService> servicesSummary = new HashMap<>();

            for (Map.Entry<String, RangerSecurityZoneService> entry : obj.getServices().entrySet()) {
                String                    serviceName    = entry.getKey();
                RangerSecurityZoneService service        = entry.getValue();
                int                       resourceCount  = service != null && service.getResources() != null ? service.getResources().size() : 0;
                RangerSecurityZoneService serviceSummary = new RangerSecurityZoneService();

                serviceSummary.getResources().add((new HashMap<String, List<String>>() {{ put("resourceCount", Collections.singletonList(Integer.toString(resourceCount))); }}));

                servicesSummary.put(serviceName, serviceSummary);
            }

            String summaryJson = null;

            try {
                summaryJson = JsonUtilsV2.mapToJson(servicesSummary);
            } catch (Exception excp) {
                logger.error("getFieldValue(): failed to convert services to JSON", excp);
            }

            ret = summaryJson;
        } else {
            ret = super.getTrxLogAttrValue(obj, trxLogAttr);
        }

        return ret;
    }

    private void updateServiceInfos(Collection<String> services) {
        if(CollectionUtils.isEmpty(services)) {
            return;
        }

        List<XXServiceVersionInfo> serviceVersionInfos = new ArrayList<>(services.size());

        for (String serviceName : services) {
            serviceVersionInfos.add(daoMgr.getXXServiceVersionInfo().findByServiceName(serviceName));
        }

        for(XXServiceVersionInfo serviceVersionInfo : serviceVersionInfos) {
            final RangerDaoManager finaldaoManager 		  = daoMgr;
            final Long 		       finalServiceId  		  = serviceVersionInfo.getServiceId();
            final ServiceDBStore.VERSION_TYPE versionType = ServiceDBStore.VERSION_TYPE.POLICY_VERSION;

            Runnable serviceVersionUpdater = new ServiceDBStore.ServiceVersionUpdater(finaldaoManager, finalServiceId, versionType, null, RangerPolicyDelta.CHANGE_TYPE_SERVICE_CHANGE, null);

            daoMgr.getRangerTransactionSynchronizationAdapter().executeOnTransactionCommit(serviceVersionUpdater);
        }

    }
}
