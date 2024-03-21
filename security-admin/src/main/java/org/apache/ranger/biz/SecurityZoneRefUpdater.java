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

package org.apache.ranger.biz;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXSecurityZoneRefGroupDao;
import org.apache.ranger.db.XXSecurityZoneRefResourceDao;
import org.apache.ranger.db.XXSecurityZoneRefRoleDao;
import org.apache.ranger.db.XXSecurityZoneRefServiceDao;
import org.apache.ranger.db.XXSecurityZoneRefTagServiceDao;
import org.apache.ranger.db.XXSecurityZoneRefUserDao;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXRole;
import org.apache.ranger.entity.XXSecurityZoneRefGroup;
import org.apache.ranger.entity.XXSecurityZoneRefResource;
import org.apache.ranger.entity.XXSecurityZoneRefRole;
import org.apache.ranger.entity.XXSecurityZoneRefService;
import org.apache.ranger.entity.XXSecurityZoneRefTagService;
import org.apache.ranger.entity.XXSecurityZoneRefUser;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerSecurityZone.RangerSecurityZoneService;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SecurityZoneRefUpdater {
	private static final Logger LOG = LoggerFactory.getLogger(SecurityZoneRefUpdater.class);

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	RangerAuditFields<?> rangerAuditFields;

	@Autowired
	RangerServiceService svcService;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	ServiceDBStore svcStore;

	@Autowired
	RangerPolicyService policyService;

	public void createNewZoneMappingForRefTable(RangerSecurityZone rangerSecurityZone) throws Exception {

		if(rangerSecurityZone == null) {
			return;
		}

		cleanupRefTables(rangerSecurityZone);

		final Long zoneId = rangerSecurityZone == null ? null : rangerSecurityZone.getId();
		final Map<String, RangerSecurityZoneService> zoneServices = rangerSecurityZone.getServices();

		final Set<String> users = new HashSet<>();
		final Set<String> userGroups = new HashSet<>();
		final Set<String> roles = new HashSet<>();
		final Set<String> tagServices = new HashSet<>();

		users.addAll(rangerSecurityZone.getAdminUsers());
		userGroups.addAll(rangerSecurityZone.getAdminUserGroups());
		roles.addAll(rangerSecurityZone.getAdminRoles());
		users.addAll(rangerSecurityZone.getAuditUsers());
		userGroups.addAll(rangerSecurityZone.getAuditUserGroups());
		roles.addAll(rangerSecurityZone.getAuditRoles());
		tagServices.addAll(rangerSecurityZone.getTagServices());

		for(Map.Entry<String, RangerSecurityZoneService> service : zoneServices.entrySet()) {
			String serviceName = service.getKey();

			if (StringUtils.isBlank(serviceName)) {
				continue;
			}

			XXService xService = daoMgr.getXXService().findByName(serviceName);
			RangerService rService = svcService.getPopulatedViewObject(xService);
			XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(rService.getType());
			XXSecurityZoneRefService xZoneService = rangerAuditFields.populateAuditFieldsForCreate(new XXSecurityZoneRefService());

			xZoneService.setZoneId(zoneId);
			xZoneService.setServiceId(xService.getId());
			xZoneService.setServiceName(serviceName);

			daoMgr.getXXSecurityZoneRefService().create(xZoneService);

			Set<String> resourceDefNames = new HashSet<>();

			for(Map<String, List<String>> resourceMap:service.getValue().getResources()){//add all resourcedefs in pre defined set
				for(Map.Entry<String, List<String>> resource : resourceMap.entrySet()) {
					String resourceName = resource.getKey();
					if (StringUtils.isBlank(resourceName)) {
						continue;
					}

					resourceDefNames.add(resourceName);
				}
			}

			for (String resourceName : resourceDefNames) {
				XXResourceDef xResourceDef = daoMgr.getXXResourceDef().findByNameAndServiceDefId(resourceName, xServiceDef.getId());

				XXSecurityZoneRefResource xZoneResource = rangerAuditFields.populateAuditFieldsForCreate(new XXSecurityZoneRefResource());

				xZoneResource.setZoneId(zoneId);
				xZoneResource.setResourceDefId(xResourceDef.getId());
				xZoneResource.setResourceName(resourceName);

				daoMgr.getXXSecurityZoneRefResource().create(xZoneResource);
			}
		}

                if(CollectionUtils.isNotEmpty(tagServices)) {
                        for(String tagService : tagServices) {

                                if (StringUtils.isBlank(tagService)) {
                                        continue;
                                }

                                XXService xService = daoMgr.getXXService().findByName(tagService);
                                if (xService == null || xService.getType() != RangerConstants.TAG_SERVICE_TYPE) {
                                        throw restErrorUtil.createRESTException("Tag Service named: " + tagService + " does not exist ",
                                                        MessageEnums.INVALID_INPUT_DATA);
                                }

                                XXSecurityZoneRefTagService xZoneTagService = rangerAuditFields.populateAuditFieldsForCreate(new XXSecurityZoneRefTagService());

                                xZoneTagService.setZoneId(zoneId);
                                xZoneTagService.setTagServiceId(xService.getId());
                                xZoneTagService.setTagServiceName(xService.getName());

                                daoMgr.getXXSecurityZoneRefTagService().create(xZoneTagService);
                        }
                }

		if(CollectionUtils.isNotEmpty(users)) {
			for(String user : users) {

				if (StringUtils.isBlank(user)) {
					continue;
				}

				XXUser xUser = daoMgr.getXXUser().findByUserName(user);

				if (xUser == null) {
					throw restErrorUtil.createRESTException("user with name: " + user + " does not exist ",
							MessageEnums.INVALID_INPUT_DATA);
				}

				XXSecurityZoneRefUser xZoneUser = rangerAuditFields.populateAuditFieldsForCreate(new XXSecurityZoneRefUser());

				xZoneUser.setZoneId(zoneId);
				xZoneUser.setUserId(xUser.getId());
				xZoneUser.setUserName(user);
				xZoneUser.setUserType(1);

				daoMgr.getXXSecurityZoneRefUser().create(xZoneUser);
			}
		}

		if(CollectionUtils.isNotEmpty(userGroups)) {
			for(String userGroup : userGroups) {

				if (StringUtils.isBlank(userGroup)) {
					continue;
				}

				XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(userGroup);

				if (xGroup == null) {
					throw restErrorUtil.createRESTException("group with name: " + userGroup + " does not exist ",
							MessageEnums.INVALID_INPUT_DATA);
				}

				XXSecurityZoneRefGroup xZoneGroup = rangerAuditFields.populateAuditFieldsForCreate(new XXSecurityZoneRefGroup());

				xZoneGroup.setZoneId(zoneId);
				xZoneGroup.setGroupId(xGroup.getId());
				xZoneGroup.setGroupName(userGroup);
				xZoneGroup.setGroupType(1);

				daoMgr.getXXSecurityZoneRefGroup().create(xZoneGroup);
			}
		}

		if(CollectionUtils.isNotEmpty(roles)) {
			for(String role : roles) {
				if (StringUtils.isBlank(role)) {
					continue;
				}

				XXRole xRole = daoMgr.getXXRole().findByRoleName(role);

				if (xRole == null) {
					throw restErrorUtil.createRESTException("role with name: " + role + " does not exist ",
							MessageEnums.INVALID_INPUT_DATA);
				}

				XXSecurityZoneRefRole xZoneRole = rangerAuditFields.populateAuditFieldsForCreate(new XXSecurityZoneRefRole());

				xZoneRole.setZoneId(zoneId);
				xZoneRole.setRoleId(xRole.getId());
				xZoneRole.setRoleName(role);

				daoMgr.getXXSecurityZoneRefRole().create(xZoneRole);
			}
		}
	}


	public Boolean cleanupRefTables(RangerSecurityZone rangerSecurityZone) {
		final Long zoneId = rangerSecurityZone == null ? null : rangerSecurityZone.getId();

		if (zoneId == null) {
			return false;
		}

		XXSecurityZoneRefServiceDao    xZoneServiceDao    = daoMgr.getXXSecurityZoneRefService();
		XXSecurityZoneRefTagServiceDao xZoneTagServiceDao = daoMgr.getXXSecurityZoneRefTagService();
		XXSecurityZoneRefResourceDao   xZoneResourceDao   = daoMgr.getXXSecurityZoneRefResource();
		XXSecurityZoneRefUserDao       xZoneUserDao       = daoMgr.getXXSecurityZoneRefUser();
		XXSecurityZoneRefGroupDao      xZoneGroupDao      = daoMgr.getXXSecurityZoneRefGroup();
		XXSecurityZoneRefRoleDao       xZoneRoleDao      = daoMgr.getXXSecurityZoneRefRole();

		for (XXSecurityZoneRefService service : xZoneServiceDao.findByZoneId(zoneId)) {
			xZoneServiceDao.remove(service);
		}

		for (XXSecurityZoneRefTagService service : xZoneTagServiceDao.findByZoneId(zoneId)) {
			xZoneTagServiceDao.remove(service);
		}

		for(XXSecurityZoneRefResource resource : xZoneResourceDao.findByZoneId(zoneId)) {
			xZoneResourceDao.remove(resource);
		}

		for(XXSecurityZoneRefUser user : xZoneUserDao.findByZoneId(zoneId)) {
			xZoneUserDao.remove(user);
		}

		for(XXSecurityZoneRefGroup group : xZoneGroupDao.findByZoneId(zoneId)) {
			xZoneGroupDao.remove(group);
		}

		for(XXSecurityZoneRefRole role : xZoneRoleDao.findByZoneId(zoneId)) {
			xZoneRoleDao.remove(role);
		}

		return true;
	}


	public void updateResourceSignatureWithZoneName(RangerSecurityZone updatedSecurityZone) {
		List<XXPolicy> policyList = daoMgr.getXXPolicy().findByZoneId(updatedSecurityZone.getId());
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> SecurityZoneRefUpdater.updateResourceSignatureWithZoneName() Count of policies with zone id : " +updatedSecurityZone.getId()+ " are : "+ policyList.size());
		}

		for (XXPolicy policy : policyList) {
			RangerPolicy policyToUpdate = policyService.getPopulatedViewObject(policy);
			svcStore.updatePolicySignature(policyToUpdate);
			policyService.update(policyToUpdate);
		}
	}
}
