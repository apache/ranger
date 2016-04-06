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

package org.apache.ranger.plugin.store;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public abstract class AbstractServiceStore implements ServiceStore {
	private static final Log LOG = LogFactory.getLog(AbstractServiceStore.class);

	public static final String COMPONENT_ACCESSTYPE_SEPARATOR = ":";

	private static final int MAX_ACCESS_TYPES_IN_SERVICE_DEF = 1000;


	@Override
	public void updateTagServiceDefForAccessTypes() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.updateTagServiceDefForAccessTypes()");
		}
		List<RangerServiceDef> allServiceDefs = getServiceDefs(new SearchFilter());
		for (RangerServiceDef serviceDef : allServiceDefs) {
			updateTagServiceDefForUpdatingAccessTypes(serviceDef);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.updateTagServiceDefForAccessTypes()");
		}
	}

	@Override
	public void deleteServiceDef(Long id, Boolean forceDelete) throws Exception {
		deleteServiceDef(id);   // Ignore forceDelete flag
	}

	@Override
	public PList<RangerServiceDef> getPaginatedServiceDefs(SearchFilter filter) throws Exception {
		List<RangerServiceDef> resultList = getServiceDefs(filter);

		return CollectionUtils.isEmpty(resultList) ? new PList<RangerServiceDef>() : new PList<RangerServiceDef>(resultList, 0, resultList.size(),
				(long)resultList.size(), resultList.size(), filter.getSortType(), filter.getSortBy());
	}

	@Override
	public PList<RangerService> getPaginatedServices(SearchFilter filter) throws Exception {
		List<RangerService> resultList = getServices(filter);

		return CollectionUtils.isEmpty(resultList) ? new PList<RangerService>() : new PList<RangerService>(resultList, 0, resultList.size(), (long)resultList.size(),
				resultList.size(), filter.getSortType(), filter.getSortBy());
	}

	@Override
	public 	PList<RangerPolicy> getPaginatedPolicies(SearchFilter filter) throws Exception {
		List<RangerPolicy> resultList = getPolicies(filter);

		return CollectionUtils.isEmpty(resultList) ? new PList<RangerPolicy>() : new PList<RangerPolicy>(resultList, 0, resultList.size(), (long)resultList.size(),
				resultList.size(), filter.getSortType(), filter.getSortBy());
	}

	@Override
	public PList<RangerPolicy> getPaginatedServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
		List<RangerPolicy> resultList = getServicePolicies(serviceId, filter);

		return CollectionUtils.isEmpty(resultList) ? new PList<RangerPolicy>() : new PList<RangerPolicy>(resultList, 0, resultList.size(), (long)resultList.size(),
				resultList.size(), filter.getSortType(), filter.getSortBy());
	}

	@Override
	public 	PList<RangerPolicy> getPaginatedServicePolicies(String serviceName, SearchFilter filter) throws Exception {
		List<RangerPolicy> resultList = getServicePolicies(serviceName, filter);

		return CollectionUtils.isEmpty(resultList) ? new PList<RangerPolicy>() : new PList<RangerPolicy>(resultList, 0, resultList.size(), (long)resultList.size(),
				resultList.size(), filter.getSortType(), filter.getSortBy());
	}

	@Override
	public RangerPolicy getPolicyFromEventTime(String eventTimeStr, Long policyId) {
		RangerPolicy ret = null;
		try {
			ret = getPolicy(policyId);
		} catch (Exception e) {
			// Do nothing
		}
		return ret;
	}

	@Override
	public RangerPolicy getPolicyForVersionNumber(Long policyId, Integer versionNo) {
		RangerPolicy ret = null;
		try {
			ret = getPolicy(policyId);
		} catch (Exception e) {
			// Do nothing
		}
		return ret;
	}

	@Override
	public String getPolicyForVersionNumber(Long policyId) {
		RangerPolicy ret = null;
		try {
			ret = getPolicy(policyId);
		} catch (Exception e) {
			// Do nothing
		}
		return ret == null ? null : ret.getName();
	}

	@Override
	public Long getServicePolicyVersion(String serviceName) {
		RangerService service = null;
		try {
			service = getServiceByName(serviceName);
		} catch (Exception exception) {
			LOG.error("Failed to get service object for service:" + serviceName);
		}
		return service != null ? service.getPolicyVersion() : null;
	}

	protected void preCreate(RangerBaseModelObject obj) throws Exception {
		obj.setId(0L);
		if(obj.getGuid() == null) {
			obj.setGuid(UUID.randomUUID().toString());
		}
		obj.setCreateTime(new Date());
		obj.setUpdateTime(obj.getCreateTime());
		obj.setVersion(1L);
	}

	protected void preCreate(RangerService service) throws Exception {
		preCreate((RangerBaseModelObject)service);

		service.setPolicyVersion(0L);
		service.setPolicyUpdateTime(service.getCreateTime());

		service.setTagVersion(0L);
		service.setTagUpdateTime(service.getCreateTime());
	}

	protected void postCreate(RangerBaseModelObject obj) throws Exception {
		if(obj instanceof RangerServiceDef) {
			updateTagServiceDefForUpdatingAccessTypes((RangerServiceDef)obj);
		}
	}

	protected void preUpdate(RangerBaseModelObject obj) throws Exception {
		if(obj.getId() == null) {
			obj.setId(0L);
		}

		if(obj.getGuid() == null) {
			obj.setGuid(UUID.randomUUID().toString());
		}

		if(obj.getCreateTime() == null) {
			obj.setCreateTime(new Date());
		}

		Long version = obj.getVersion();

		if(version == null) {
			version = 1L;
		} else {
			version = version + 1;
		}

		obj.setVersion(version);
		obj.setUpdateTime(new Date());
	}

	protected void postUpdate(RangerBaseModelObject obj) throws Exception {
		if(obj instanceof RangerServiceDef) {
			RangerServiceDef serviceDef = (RangerServiceDef)obj;

			updateTagServiceDefForUpdatingAccessTypes(serviceDef);
			updateServicesForServiceDefUpdate(serviceDef);
		}
	}

	protected void preDelete(RangerBaseModelObject obj) throws Exception {
		// TODO:
	}

	protected void postDelete(RangerBaseModelObject obj) throws Exception {
		if(obj instanceof RangerServiceDef) {
			updateTagServiceDefForDeletingAccessTypes(((RangerServiceDef) obj).getName());
		}
	}

	protected long getMaxId(List<? extends RangerBaseModelObject> objs) {
		long ret = -1;

		if (objs != null) {
			for (RangerBaseModelObject obj : objs) {
				if (obj.getId() > ret) {
					ret = obj.getId();
				}
			}
		}
		return ret;
	}

	// when a service-def is updated, the updated service-def should be made available to plugins
	//   this is achieved by incrementing policyVersion of all its services
	protected void updateServicesForServiceDefUpdate(RangerServiceDef serviceDef) throws Exception {
		boolean isTagServiceDef = StringUtils.equals(serviceDef.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME);

		SearchFilter filter = new SearchFilter();

		filter.setParam(SearchFilter.SERVICE_TYPE, serviceDef.getName());

		List<RangerService> services = getServices(filter);

		if(CollectionUtils.isNotEmpty(services)) {
			for(RangerService service : services) {
				service.setPolicyVersion(getNextVersion(service.getPolicyVersion()));
				service.setPolicyUpdateTime(serviceDef.getUpdateTime());

				updateService(service);

				if(isTagServiceDef) {
					filter = new SearchFilter();

					filter.setParam(SearchFilter.TAG_SERVICE_NAME, service.getName());

					List<RangerService> referrringServices = getServices(filter);

					if(CollectionUtils.isNotEmpty(referrringServices)) {
						for(RangerService referringService : referrringServices) {
							referringService.setPolicyVersion(getNextVersion(referringService.getPolicyVersion()));
							referringService.setPolicyUpdateTime(serviceDef.getUpdateTime());

							updateService(referringService);
						}
					}
				}
			}
		}
	}

	private RangerServiceDef.RangerAccessTypeDef findAccessTypeDef(long itemId, List<RangerServiceDef.RangerAccessTypeDef> accessTypeDefs) {
		RangerServiceDef.RangerAccessTypeDef ret = null;

		for(RangerServiceDef.RangerAccessTypeDef accessTypeDef : accessTypeDefs) {
			if(itemId == accessTypeDef.getItemId()) {
				ret = accessTypeDef;
				break;
			}
		}
		return ret;
	}

	private void updateTagServiceDefForUpdatingAccessTypes(RangerServiceDef serviceDef) throws Exception {
		if (StringUtils.equals(serviceDef.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
			return;
		}

		if(EmbeddedServiceDefsUtil.instance().getTagServiceDefId() == -1) {
			LOG.info("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes(" + serviceDef.getName() + "): tag service-def does not exist");
		}

		String serviceDefName = serviceDef.getName();

		RangerServiceDef tagServiceDef = null;
		try {
			tagServiceDef = this.getServiceDef(EmbeddedServiceDefsUtil.instance().getTagServiceDefId());
		} catch (Exception e) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes" + serviceDef.getName() + "): could not find TAG ServiceDef.. ", e);
			throw e;
		}

		if(tagServiceDef == null) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes(" + serviceDef.getName() + "): could not find TAG ServiceDef.. ");

			return;
		}

		List<RangerServiceDef.RangerAccessTypeDef> toAdd    = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();
		List<RangerServiceDef.RangerAccessTypeDef> toUpdate = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();
		List<RangerServiceDef.RangerAccessTypeDef> toDelete = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();

		List<RangerServiceDef.RangerAccessTypeDef> svcDefAccessTypes = serviceDef.getAccessTypes();
		List<RangerServiceDef.RangerAccessTypeDef> tagDefAccessTypes = tagServiceDef.getAccessTypes();

		long itemIdOffset = serviceDef.getId() * (MAX_ACCESS_TYPES_IN_SERVICE_DEF + 1);

		for (RangerServiceDef.RangerAccessTypeDef svcAccessType : svcDefAccessTypes) {
			long tagAccessTypeItemId = svcAccessType.getItemId() + itemIdOffset;

			RangerServiceDef.RangerAccessTypeDef tagAccessType = findAccessTypeDef(tagAccessTypeItemId, tagDefAccessTypes);

			if(tagAccessType == null) {
				tagAccessType = new RangerServiceDef.RangerAccessTypeDef();

				tagAccessType.setItemId(tagAccessTypeItemId);
				tagAccessType.setName(serviceDefName + ":" + svcAccessType.getName());
				tagAccessType.setLabel(svcAccessType.getLabel());
				tagAccessType.setRbKeyLabel(svcAccessType.getRbKeyLabel());

				tagAccessType.setImpliedGrants(new HashSet<String>());
				if(CollectionUtils.isNotEmpty(svcAccessType.getImpliedGrants())) {
					for(String svcImpliedGrant : svcAccessType.getImpliedGrants()) {
						tagAccessType.getImpliedGrants().add(serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR + svcImpliedGrant);
					}
				}

				toAdd.add(tagAccessType);
			}
		}

		for (RangerServiceDef.RangerAccessTypeDef tagAccessType : tagDefAccessTypes) {
			if (tagAccessType.getName().startsWith(serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR)) {
				long svcAccessTypeItemId = tagAccessType.getItemId() - itemIdOffset;

				RangerServiceDef.RangerAccessTypeDef svcAccessType = findAccessTypeDef(svcAccessTypeItemId, svcDefAccessTypes);

				if(svcAccessType == null) { // accessType has been deleted in service
					toDelete.add(tagAccessType);
					continue;
				}

				boolean isUpdated = false;

				if(! Objects.equals(tagAccessType.getName().substring(serviceDefName.length() + 1), svcAccessType.getName())) {
					isUpdated = true;
				} else if(! Objects.equals(tagAccessType.getLabel(), svcAccessType.getLabel())) {
					isUpdated = true;
				} else if(! Objects.equals(tagAccessType.getRbKeyLabel(), svcAccessType.getRbKeyLabel())) {
					isUpdated = true;
				} else {
					Collection<String> tagImpliedGrants = tagAccessType.getImpliedGrants();
					Collection<String> svcImpliedGrants = svcAccessType.getImpliedGrants();

					int tagImpliedGrantsLen = tagImpliedGrants == null ? 0 : tagImpliedGrants.size();
					int svcImpliedGrantsLen = svcImpliedGrants == null ? 0 : svcImpliedGrants.size();

					if(tagImpliedGrantsLen != svcImpliedGrantsLen) {
						isUpdated = true;
					} else if(tagImpliedGrantsLen > 0) {
						for(String svcImpliedGrant : svcImpliedGrants) {
							if(! tagImpliedGrants.contains(serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR + svcImpliedGrant)) {
								isUpdated = true;
								break;
							}
						}
					}
				}

				if(isUpdated) {
					tagAccessType.setName(serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR + svcAccessType.getName());
					tagAccessType.setLabel(svcAccessType.getLabel());
					tagAccessType.setRbKeyLabel(svcAccessType.getRbKeyLabel());

					tagAccessType.setImpliedGrants(new HashSet<String>());
					if(CollectionUtils.isNotEmpty(svcAccessType.getImpliedGrants())) {
						for(String svcImpliedGrant : svcAccessType.getImpliedGrants()) {
							tagAccessType.getImpliedGrants().add(serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR + svcImpliedGrant);
						}
					}

					toUpdate.add(tagAccessType);
				}
			}
		}

		if(CollectionUtils.isNotEmpty(toAdd) || CollectionUtils.isNotEmpty(toUpdate) || CollectionUtils.isNotEmpty(toDelete)) {
			tagDefAccessTypes.addAll(toAdd);
			tagDefAccessTypes.removeAll(toDelete);

			try {
				updateServiceDef(tagServiceDef);
				LOG.info("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes -- updated TAG service def with " + serviceDefName + " access types");
			} catch (Exception e) {
				LOG.error("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes -- Failed to update TAG ServiceDef.. ", e);
				throw e;
			}
		}
	}

	private void updateTagServiceDefForDeletingAccessTypes(String serviceDefName) throws Exception {
		if (serviceDefName.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
			return;
		}

		RangerServiceDef tagServiceDef;
		try {
			tagServiceDef = this.getServiceDef(EmbeddedServiceDefsUtil.instance().getTagServiceDefId());
		} catch (Exception e) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes(" + serviceDefName + "): could not find TAG ServiceDef.. ", e);
			throw e;
		}

		if(tagServiceDef == null) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes(" + serviceDefName + "): could not find TAG ServiceDef.. ");

			return;
		}

		List<RangerServiceDef.RangerAccessTypeDef> accessTypes = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();

		for (RangerServiceDef.RangerAccessTypeDef accessType : tagServiceDef.getAccessTypes()) {
			if (accessType.getName().startsWith(serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR)) {
				accessTypes.add(accessType);
			}
		}

		tagServiceDef.getAccessTypes().removeAll(accessTypes);
		try {
			updateServiceDef(tagServiceDef);
			LOG.info("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes -- updated TAG service def with " + serviceDefName + " access types");
		} catch (Exception e) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes -- Failed to update TAG ServiceDef.. ", e);
			throw e;
		}
	}

	protected long getNextVersion(Long currentVersion) {
		return currentVersion == null ? 1L : currentVersion + 1;
	}
}
