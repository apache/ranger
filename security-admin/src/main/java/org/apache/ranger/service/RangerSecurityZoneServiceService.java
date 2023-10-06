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
import java.lang.reflect.Field;
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
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXServiceVersionInfo;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerSecurityZone.RangerSecurityZoneService;
import org.apache.ranger.util.RangerEnumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.annotation.PostConstruct;

@Service
@Scope("singleton")
public class RangerSecurityZoneServiceService extends RangerSecurityZoneServiceBase<XXSecurityZone, RangerSecurityZone> {
	@Autowired
	RangerEnumUtil xaEnumUtil;

	@Autowired
	ServiceDBStore serviceDBStore;

    boolean compressJsonData = false;

    private static final Logger logger = LoggerFactory.getLogger(RangerSecurityZoneServiceService.class);
    private static final Gson gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();

    private Map<Long, Set<String>> serviceNamesInZones = new HashMap<>();
    private Map<Long, Set<String>> tagServiceNamesInZones = new HashMap<>();

    static HashMap<String, VTrxLogAttr> trxLogAttrs = new HashMap<String, VTrxLogAttr>();

    static {
		trxLogAttrs.put("name", new VTrxLogAttr("name", "Zone Name", false));
		trxLogAttrs.put("services", new VTrxLogAttr("services", "Zone Services", false));
		trxLogAttrs.put("adminUsers", new VTrxLogAttr("adminUsers", "Zone Admin Users", false));
		trxLogAttrs.put("adminUserGroups", new VTrxLogAttr("adminUserGroups", "Zone Admin User Groups", false));
		trxLogAttrs.put("auditUsers", new VTrxLogAttr("auditUsers", "Zone Audit Users", false));
		trxLogAttrs.put("auditUserGroups", new VTrxLogAttr("auditUserGroups", "Zone Audit User Groups", false));
		trxLogAttrs.put("description", new VTrxLogAttr("description", "Zone Description", false));
                trxLogAttrs.put("tagServices", new VTrxLogAttr("tagServices", "Zone Tag Services", false));
	}

    public RangerSecurityZoneServiceService() {
        super();
    }

    @PostConstruct
    public void initService() {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerSecurityZoneServiceService.initService()");
        }

        RangerAdminConfig config = RangerAdminConfig.getInstance();

        compressJsonData = config.getBoolean("ranger.admin.store.security.zone.compress.json_data", false);

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

        String json = gsonBuilder.toJson(securityZone);

        if (StringUtils.isNotEmpty(json) && compressJsonData) {
            try {
                json = StringUtil.compressString(json);
            } catch (IOException excp) {
                logger.error("mapViewToEntityBean(): json compression failed (length={}). Will save uncompressed json", json.length(), excp);
            }
        }

        ret.setJsonData(json);

        return ret;
    }
    @Override
    protected RangerSecurityZone mapEntityToViewBean(RangerSecurityZone securityZone, XXSecurityZone xxSecurityZone) {
        RangerSecurityZone ret  = super.mapEntityToViewBean(securityZone, xxSecurityZone);
        String             json = xxSecurityZone.getJsonData();

        if (StringUtils.isNotEmpty(json)) {
            try {
                json = StringUtil.decompressString(json);
            } catch (IOException excp) {
                logger.error("mapEntityToViewBean(): json decompression failed (length={}). Will treat as uncompressed json", json.length(), excp);
            }

            RangerSecurityZone zoneFromJsonData = gsonBuilder.fromJson(json, RangerSecurityZone.class);

            if (zoneFromJsonData == null) {
                logger.info("Cannot read jsonData into RangerSecurityZone object in [" + json + "]!!");
            } else {
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

        // Create default zone policies
        try {
            serviceDBStore.createZoneDefaultPolicies(serviceNames, ret);
            updateServiceInfos(serviceNames);
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

	public List<XXTrxLog> getTransactionLog(RangerSecurityZone vSecurityZone, RangerSecurityZone securityZoneDB, String action) {
		if (vSecurityZone == null || action == null  || ("update".equalsIgnoreCase(action) && securityZoneDB == null)) {
			return null;
		}
		List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
		Field[] fields = vSecurityZone.getClass().getDeclaredFields();

		try {
			Field nameField = vSecurityZone.getClass().getDeclaredField("name");
			nameField.setAccessible(true);
			String objectName = "" + nameField.get(vSecurityZone);

			for (Field field : fields) {
				String fieldName = field.getName();
				if (!trxLogAttrs.containsKey(fieldName)) {
					continue;
				}
				field.setAccessible(true);
				VTrxLogAttr vTrxLogAttr = trxLogAttrs.get(fieldName);
				XXTrxLog xTrxLog = new XXTrxLog();
				xTrxLog.setAttributeName(vTrxLogAttr
						.getAttribUserFriendlyName());
				xTrxLog.setAction(action);
				xTrxLog.setObjectId(vSecurityZone.getId());
				xTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_SECURITY_ZONE);
				xTrxLog.setObjectName(objectName);

				String value = null;
				if (vTrxLogAttr.isEnum()) {
					String enumName = XXUser.getEnumName(fieldName);
					int enumValue = field.get(vSecurityZone) == null ? 0 : Integer
							.parseInt("" + field.get(vSecurityZone));
					value = xaEnumUtil.getLabel(enumName, enumValue);
				} else {
					value = "" + field.get(vSecurityZone);
					if ((value == null || "null".equalsIgnoreCase(value))
							&& !"update".equalsIgnoreCase(action)) {
						continue;
					}
				}
				if("services".equalsIgnoreCase(fieldName)) {
					value = toTrxLog(vSecurityZone.getServices());
				}
				if ("create".equalsIgnoreCase(action)) {
					xTrxLog.setNewValue(value);
					trxLogList.add(xTrxLog);
				}
				else if ("delete".equalsIgnoreCase(action)) {
					xTrxLog.setPreviousValue(value);
					trxLogList.add(xTrxLog);
				}
				else if ("update".equalsIgnoreCase(action)) {
					String oldValue = null;
					Field[] mFields = vSecurityZone.getClass().getDeclaredFields();
					for (Field mField : mFields) {
						mField.setAccessible(true);
						String mFieldName = mField.getName();
						if (fieldName.equalsIgnoreCase(mFieldName)) {
							if("services".equalsIgnoreCase(mFieldName)) {
								oldValue = toTrxLog(securityZoneDB.getServices());
							}
							else {
								oldValue = mField.get(securityZoneDB) + "";
							}
							break;
						}
					}
					if (oldValue == null || oldValue.equalsIgnoreCase(value)) {
						continue;
					}
					xTrxLog.setPreviousValue(oldValue);
					xTrxLog.setNewValue(value);
					trxLogList.add(xTrxLog);
				}
			}
			if (trxLogList.isEmpty()) {
				XXTrxLog xTrxLog = new XXTrxLog();
				xTrxLog.setAction(action);
				xTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_SECURITY_ZONE);
				xTrxLog.setObjectId(vSecurityZone.getId());
				xTrxLog.setObjectName(objectName);
				trxLogList.add(xTrxLog);
			}
		} catch (IllegalAccessException e) {
			logger.error("Transaction log failure.", e);
		} catch (NoSuchFieldException e) {
			logger.error("Transaction log failure.", e);
		}
		return trxLogList;
	}

    private String toTrxLog(Map<String, RangerSecurityZoneService> services) {
        String ret;

        if (services == null) {
            services = Collections.emptyMap();
        }

        if (compressJsonData) { // when compression is enabled, summarize services info for trx log
            Map<String, RangerSecurityZoneService> servicesSummary = new HashMap<>(services.size());

            for (Map.Entry<String, RangerSecurityZoneService> entry : services.entrySet()) {
                String                    serviceName        = entry.getKey();
                RangerSecurityZoneService zoneService        = entry.getValue();
                Integer                   resourceCount      = (zoneService != null && zoneService.getResources() != null) ? zoneService.getResources().size() : 0;
                RangerSecurityZoneService zoneServiceSummary = new RangerSecurityZoneService();

                zoneServiceSummary.getResources().add(new HashMap<String, List<String>>() {{ put("resourceCount", Collections.singletonList(resourceCount.toString())); }});

                servicesSummary.put(serviceName, zoneServiceSummary);
            }

            ret = new Gson().toJson(servicesSummary, Map.class);
        } else {
            ret = new Gson().toJson(services, Map.class);
        }

        return ret;
    }
}
