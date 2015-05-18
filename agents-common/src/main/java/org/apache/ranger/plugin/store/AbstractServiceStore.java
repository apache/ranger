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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public abstract class AbstractServiceStore implements ServiceStore {
	private static final Log LOG = LogFactory.getLog(AbstractServiceStore.class);


	private static final int MAX_ACCESS_TYPES_IN_SERVICE_DEF = 1000;

	@Override
	public void updateTagServiceDefForAccessTypes() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.updateTagServiceDefForAccessTypes()");
		}
		List<RangerServiceDef> allServiceDefs = getServiceDefs(new SearchFilter());
		for (RangerServiceDef serviceDef : allServiceDefs) {
			if (StringUtils.isEmpty(serviceDef.getName()) || serviceDef.getName().equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
				continue;
			}
			updateTagServiceDefForUpdatingAccessTypes(serviceDef);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.updateTagServiceDefForAccessTypes()");
		}
		return;
	}

	@Override
	public void deleteServiceDef(Long id, Boolean forceDelete) throws Exception {
		deleteServiceDef(id);   // Ignore forceDelete flag
	}

	@Override
	public RangerServiceDefPaginatedList getPaginatedServiceDefs(SearchFilter filter) throws Exception {
		List<RangerServiceDef> resultList = getServiceDefs(filter);

		RangerServiceDefPaginatedList ret = new RangerServiceDefPaginatedList();

		ret.setResultSize(resultList.size());
		ret.setPageSize(resultList.size());
		ret.setSortBy(filter.getSortBy());
		ret.setSortType(filter.getSortType());
		ret.setStartIndex(0);
		ret.setTotalCount(resultList.size());

		ret.setServiceDefs(resultList);

		return ret;
	}

	@Override
	public RangerServicePaginatedList getPaginatedServices(SearchFilter filter) throws Exception {
		List<RangerService> resultList = getServices(filter);

		RangerServicePaginatedList ret = new RangerServicePaginatedList();

		ret.setResultSize(resultList.size());
		ret.setPageSize(resultList.size());
		ret.setSortBy(filter.getSortBy());
		ret.setSortType(filter.getSortType());
		ret.setStartIndex(0);
		ret.setTotalCount(resultList.size());

		ret.setServices(resultList);

		return ret;
	}

	@Override
	public 	RangerPolicyPaginatedList getPaginatedPolicies(SearchFilter filter) throws Exception {
		List<RangerPolicy> resultList = getPolicies(filter);

		RangerPolicyPaginatedList ret = new RangerPolicyPaginatedList();

		ret.setResultSize(resultList.size());
		ret.setPageSize(resultList.size());
		ret.setSortBy(filter.getSortBy());
		ret.setSortType(filter.getSortType());
		ret.setStartIndex(0);
		ret.setTotalCount(resultList.size());

		ret.setPolicies(resultList);

		return ret;
	}

	@Override
	public RangerPolicyPaginatedList getPaginatedServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
		List<RangerPolicy> resultList = getServicePolicies(serviceId, filter);

		RangerPolicyPaginatedList ret = new RangerPolicyPaginatedList();

		ret.setResultSize(resultList.size());
		ret.setPageSize(resultList.size());
		ret.setSortBy(filter.getSortBy());
		ret.setSortType(filter.getSortType());
		ret.setStartIndex(0);
		ret.setTotalCount(resultList.size());

		ret.setPolicies(resultList);

		return ret;
	}

	@Override
	public 	RangerPolicyPaginatedList getPaginatedServicePolicies(String serviceName, SearchFilter filter) throws Exception {
		List<RangerPolicy> resultList = getServicePolicies(serviceName, filter);

		RangerPolicyPaginatedList ret = new RangerPolicyPaginatedList();

		ret.setResultSize(resultList.size());
		ret.setPageSize(resultList.size());
		ret.setSortBy(filter.getSortBy());
		ret.setSortType(filter.getSortType());
		ret.setStartIndex(0);
		ret.setTotalCount(resultList.size());

		ret.setPolicies(resultList);

		return ret;

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

	protected void preCreate(RangerBaseModelObject obj) throws Exception {
		obj.setId(new Long(0));
		obj.setGuid(UUID.randomUUID().toString());
		obj.setCreateTime(new Date());
		obj.setUpdateTime(obj.getCreateTime());
		obj.setVersion(new Long(1));
	}

	protected void preCreate(RangerService service) throws Exception {
		preCreate((RangerBaseModelObject)service);

		service.setPolicyVersion(new Long(0));
		service.setPolicyUpdateTime(service.getCreateTime());
	}

	protected void postCreate(RangerBaseModelObject obj) throws Exception {
		if(obj instanceof RangerServiceDef) {
			updateTagServiceDefForAddingAccessTypes((RangerServiceDef)obj);
		}
	}

	protected void preUpdate(RangerBaseModelObject obj) throws Exception {
		if(obj.getId() == null) {
			obj.setId(new Long(0));
		}

		if(obj.getGuid() == null) {
			obj.setGuid(UUID.randomUUID().toString());
		}

		if(obj.getCreateTime() == null) {
			obj.setCreateTime(new Date());
		}

		Long version = obj.getVersion();

		if(version == null) {
			version = new Long(1);
		} else {
			version = new Long(version.longValue() + 1);
		}

		obj.setVersion(version);
		obj.setUpdateTime(new Date());
	}

	protected void postUpdate(RangerBaseModelObject obj) throws Exception {
		if(obj instanceof RangerServiceDef) {
			updateTagServiceDefForUpdatingAccessTypes((RangerServiceDef) obj);
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

	private void updateTagServiceDefForAddingAccessTypes(RangerServiceDef serviceDef) throws Exception {
		if (serviceDef.getName().equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
			return;
		}

		RangerServiceDef tagServiceDef = null;
		try {
			tagServiceDef = this.getServiceDef(EmbeddedServiceDefsUtil.instance().getTagServiceDefId());
		} catch (Exception e) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForAddingAccessTypes -- Could not find TAG ServiceDef.. ", e);
			throw e;
		}
		List<RangerServiceDef.RangerAccessTypeDef> accessTypes = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();

		for (RangerServiceDef.RangerAccessTypeDef accessType : serviceDef.getAccessTypes()) {
			RangerServiceDef.RangerAccessTypeDef newAccessType = new RangerServiceDef.RangerAccessTypeDef(accessType);

			newAccessType.setItemId(serviceDef.getId()*(MAX_ACCESS_TYPES_IN_SERVICE_DEF + 1) + accessType.getItemId());
			newAccessType.setName(serviceDef.getName() + ":" + accessType.getName());
			accessTypes.add(newAccessType);
		}

		tagServiceDef.getAccessTypes().addAll(accessTypes);
		try {
			updateServiceDef(tagServiceDef);
			LOG.info("AbstractServiceStore.updateTagServiceDefForAddingAccessTypes -- updated TAG service def with " + serviceDef.getName() + " access types");
		} catch (Exception e) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForAddingAccessTypes -- Failed to update TAG ServiceDef.. ", e);
			throw e;
		}
	}

	private void updateTagServiceDefForUpdatingAccessTypes(RangerServiceDef serviceDef) throws Exception {
		if (serviceDef.getName().equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
			return;
		}

		String serviceDefName = serviceDef.getName();

		RangerServiceDef tagServiceDef = null;
		try {
			tagServiceDef = this.getServiceDef(EmbeddedServiceDefsUtil.instance().getTagServiceDefId());
		} catch (Exception e) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes -- Could not find TAG ServiceDef.. ", e);
			throw e;
		}

		List<RangerServiceDef.RangerAccessTypeDef> tagSvcDefAccessTypes = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();

		for (RangerServiceDef.RangerAccessTypeDef accessType : tagServiceDef.getAccessTypes()) {
			if (accessType.getName().startsWith(serviceDefName + ":")) {
				RangerServiceDef.RangerAccessTypeDef tagSvcDefAccessType = new RangerServiceDef.RangerAccessTypeDef(accessType);
				tagSvcDefAccessTypes.add(tagSvcDefAccessType);
			}
		}

		List<RangerServiceDef.RangerAccessTypeDef> svcDefAccessTypes = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();

		for (RangerServiceDef.RangerAccessTypeDef accessType : serviceDef.getAccessTypes()) {
			RangerServiceDef.RangerAccessTypeDef svcDefAccessType = new RangerServiceDef.RangerAccessTypeDef(accessType);
			svcDefAccessType.setItemId(serviceDef.getId()*(MAX_ACCESS_TYPES_IN_SERVICE_DEF + 1) + accessType.getItemId());
			svcDefAccessType.setName(serviceDefName + ":" + accessType.getName());
			svcDefAccessTypes.add(svcDefAccessType);
		}

		tagServiceDef.getAccessTypes().removeAll(tagSvcDefAccessTypes);
		tagServiceDef.getAccessTypes().addAll(svcDefAccessTypes);

		try {
			updateServiceDef(tagServiceDef);
			LOG.info("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes -- updated TAG service def with " + serviceDefName + " access types");
		} catch (Exception e) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes -- Failed to update TAG ServiceDef.. ", e);
			throw e;
		}

	}

	private void updateTagServiceDefForDeletingAccessTypes(String serviceDefName) throws Exception {
		if (serviceDefName.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
			return;
		}

		RangerServiceDef tagServiceDef = null;
		try {
			tagServiceDef = this.getServiceDef(EmbeddedServiceDefsUtil.instance().getTagServiceDefId());
		} catch (Exception e) {
			LOG.error("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes -- Could not find TAG ServiceDef.. ", e);
			throw e;
		}
		List<RangerServiceDef.RangerAccessTypeDef> accessTypes = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();

		for (RangerServiceDef.RangerAccessTypeDef accessType : tagServiceDef.getAccessTypes()) {
			if (accessType.getName().startsWith(serviceDefName + ":")) {
				RangerServiceDef.RangerAccessTypeDef newAccessType = new RangerServiceDef.RangerAccessTypeDef(accessType);
				accessTypes.add(newAccessType);
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
}
