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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.*;

import java.util.*;

public abstract class AbstractTagStore implements TagStore {
	private static final Log LOG = LogFactory.getLog(AbstractTagStore.class);


	protected ServiceStore svcStore;

	@Override
	public void init() throws Exception {
		// Empty
	}

	@Override
	final public void setServiceStore(ServiceStore svcStore) {
		this.svcStore = svcStore;
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

	protected void postCreate(RangerBaseModelObject obj) throws Exception {
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
			version =  version + 1;
		}

		obj.setVersion(version);
		obj.setUpdateTime(new Date());
	}

	protected void postUpdate(RangerBaseModelObject obj) throws Exception {
	}

	protected void preDelete(RangerBaseModelObject obj) throws Exception {
	}

	protected void postDelete(RangerBaseModelObject obj) throws Exception {
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

	@Override
	public void deleteAllTagObjectsForService(String serviceName) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> AbstractTagStore.deleteAllTagObjectsForService(serviceName=" + serviceName + ")");
		}

		List<RangerServiceResource> serviceResources = getServiceResourcesByService(serviceName);

		if (serviceResources != null) {

			for (RangerServiceResource serviceResource : serviceResources) {
				Long resourceId = serviceResource.getId();

				List<RangerTagResourceMap> tagResourceMapsForService = getTagResourceMapsForResourceId(resourceId);

				for (RangerTagResourceMap tagResourceMap : tagResourceMapsForService) {
					deleteTagResourceMap(tagResourceMap.getId());
				}
			}

			for (RangerServiceResource serviceResource : serviceResources) {
				deleteServiceResource(serviceResource.getId());
			}

		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== AbstractTagStore.deleteAllTagObjectsForService(serviceName=" + serviceName + ")");
		}

	}

}


