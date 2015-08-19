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
import org.apache.ranger.plugin.model.*;

import java.util.*;

public class TagValidator {
	private TagStore tagStore;

	public TagValidator() {}

	public void setTagStore(TagStore tagStore) {
		this.tagStore = tagStore;
	}

	public void preCreateTag(final RangerTag tag) throws Exception {
		if (StringUtils.isBlank(tag.getName())) {
			throw new Exception("Tag has no name");
		}
	}

	public void preUpdateTagById(final Long id, final RangerTag tag) throws Exception {
		if (StringUtils.isBlank(tag.getName())) {
			throw new Exception("Tag has no name");
		}

		if (id == null) {
			throw new Exception("Invalid/null id");
		}

		RangerTag exist = tagStore.getTagById(id);

		if (exist == null) {
			throw new Exception("Attempt to update nonexistant tag, id=" + id);
		}
		tag.setId(exist.getId());
	}

	public void preUpdateTagByExternalId(String externalId, final RangerTag tag) throws Exception {
		if (StringUtils.isBlank(tag.getName())) {
			throw new Exception("Tag has no name");
		}

		List<RangerTag> exist = tagStore.getTagsByExternalId(externalId);
		if (CollectionUtils.isEmpty(exist) || CollectionUtils.size(exist) != 1) {
			throw new Exception("Attempt to update nonexistent or multiple tags, externalId=" + externalId);
		}

		RangerTag onlyTag = exist.get(0);

		tag.setId(onlyTag.getId());
		tag.setGuid(externalId);

	}

	public void preUpdateTagByName(String name, final RangerTag tag) throws Exception {
		if (StringUtils.isNotBlank(tag.getName())) {
			throw new Exception("tag has no name");
		}

		List<RangerTag> exist = tagStore.getTagsByName(name);
		if (CollectionUtils.isEmpty(exist) || CollectionUtils.size(exist) != 1) {
			throw new Exception("Attempt to update nonexistent or multiple tags, name=" + name);
		}

		RangerTag onlyTag = exist.get(0);

		tag.setId(onlyTag.getId());
		tag.setName(name);

	}

	public RangerTag preDeleteTagById(Long id) throws Exception {
		RangerTag exist;
		exist = tagStore.getTagById(id);
		if (exist == null) {
			throw new Exception("Attempt to delete nonexistent tag, id=" + id);
		}

		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsByTagId(exist.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete tag which is associated with a service-resource, id=" + id);
		}
		return exist;
	}

	public RangerTag preDeleteTagByExternalId(String externalId) throws Exception {
		List<RangerTag> exist;
		exist = tagStore.getTagsByExternalId(externalId);
		if (CollectionUtils.isEmpty(exist) || CollectionUtils.size(exist) != 1) {
			throw new Exception("Attempt to delete nonexistent or multiple tags, externalId=" + externalId);
		}

		RangerTag ret = exist.get(0);
		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsByTagId(ret.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete tag which is associated with a service-resource, externalId=" + externalId);
		}
		return ret;
	}

	public RangerTag preDeleteTagByName(String name) throws Exception {
		List<RangerTag> exist;
		exist = tagStore.getTagsByName(name);
		if (CollectionUtils.isEmpty(exist) || CollectionUtils.size(exist) != 1) {
			throw new Exception("Attempt to delete nonexistent or multiple tags, name=" + name);
		}
		RangerTag ret = exist.get(0);
		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsByTagId(ret.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete tag which is associated with a service-resource, name=" + name);
		}
		return ret;

	}

	public void preCreateServiceResource(RangerServiceResource resource) throws Exception {
		if (StringUtils.isBlank(resource.getServiceName())
				|| resource.getResourceSpec() == null
				|| CollectionUtils.size(resource.getResourceSpec()) == 0) {
			throw new Exception("No serviceName or resourceSpec in RangerServiceResource");
		}

		List<RangerServiceResource> exist;
		exist = tagStore.getServiceResourcesByServiceAndResourceSpec(resource.getServiceName(), resource.getResourceSpec());
		if (CollectionUtils.isNotEmpty(exist)) {
			throw new Exception("Attempt to create existing resource, serviceName=" + resource.getServiceName());
		}
		RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);
		resource.setResourceSignature(serializer.getSignature());
	}

	public void preUpdateServiceResourceById(Long id, RangerServiceResource resource) throws Exception {
		if (StringUtils.isBlank(resource.getServiceName())
				|| resource.getResourceSpec() == null
				|| CollectionUtils.size(resource.getResourceSpec()) == 0) {
			throw new Exception("No serviceName or resourceSpec in RangerServiceResource");
		}

		if (id == null) {
			throw new Exception("Invalid/null id");
		}

		RangerServiceResource exist = tagStore.getServiceResourceById(id);
		if (exist == null) {
			throw new Exception("Attempt to update nonexistent resource, id=" + id);
		}
		resource.setId(exist.getId());

		RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);
		resource.setResourceSignature(serializer.getSignature());

	}

	public void preUpdateServiceResourceByExternalId(String externalId, RangerServiceResource resource) throws Exception {
		if (StringUtils.isBlank(resource.getServiceName())
				|| resource.getResourceSpec() == null
				|| CollectionUtils.size(resource.getResourceSpec()) == 0) {
			throw new Exception("No serviceName or resourceSpec in RangerServiceResource");
		}

		List<RangerServiceResource> exist;
		exist = tagStore.getServiceResourcesByExternalId(externalId);
		if (CollectionUtils.isEmpty(exist) || CollectionUtils.size(exist) != 1) {
			throw new Exception("Attempt to update nonexistent or multiple resources, externalId=" + externalId);
		}

		RangerServiceResource onlyResource = exist.get(0);

		resource.setId(onlyResource.getId());
		resource.setGuid(externalId);

		RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);
		resource.setResourceSignature(serializer.getSignature());
	}

	public RangerServiceResource preDeleteServiceResourceById(Long id) throws Exception {
		RangerServiceResource exist;
		exist = tagStore.getServiceResourceById(id);
		if (exist == null) {
			throw new Exception("Attempt to delete nonexistent resource, id=" + id);
		}
		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsByResourceId(exist.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete serviceResource which is associated with a tag, id=" + id);
		}
		return exist;
	}

	public RangerServiceResource preDeleteServiceResourceByExternalId(String externalId) throws Exception {
		List<RangerServiceResource> exist;
		exist = tagStore.getServiceResourcesByExternalId(externalId);
		if (CollectionUtils.isEmpty(exist) || CollectionUtils.size(exist) != 1) {
			throw new Exception("Attempt to delete nonexistent or multiple resources, externalId=" + externalId);
		}
		RangerServiceResource ret = exist.get(0);
		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsByResourceId(ret.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete serviceResource which is associated with a tag, externalId=" + externalId);
		}
		return ret;
	}

	public RangerTagResourceMap preCreateTagResourceMap(String externalResourceId, String externalTagId) throws Exception {
		if (StringUtils.isBlank(externalResourceId) || StringUtils.isBlank(externalTagId)) {
			throw new Exception("Both externalResourceId and internalResourceId need to be non-empty");
		}

		List<RangerTagResourceMap> exist;
		exist = tagStore.getTagResourceMap(externalResourceId, externalTagId);
		if (CollectionUtils.isNotEmpty(exist)) {
			throw new Exception("Attempt to create existing association between resourceId=" + externalResourceId + " and tagId=" + externalTagId);
		}
		List<RangerServiceResource> existingServiceResources = tagStore.getServiceResourcesByExternalId(externalResourceId);
		List<RangerTag> existingTags = tagStore.getTagsByExternalId(externalTagId);

		if (CollectionUtils.isNotEmpty(existingServiceResources) && CollectionUtils.size(existingServiceResources) == 1) {
			if (CollectionUtils.isNotEmpty(existingTags) && CollectionUtils.size(existingTags) == 1) {
				RangerTagResourceMap newTagResourceMap = new RangerTagResourceMap();
				newTagResourceMap.setResourceId(existingServiceResources.get(0).getId());
				newTagResourceMap.setTagId(existingTags.get(0).getId());
				return newTagResourceMap;
			} else {
				throw new Exception("No unique tag found for externalId=" + externalTagId);
			}
		} else {
			throw new Exception("No unique resource found for externalId=" + externalResourceId);
		}
	}

	public RangerTagResourceMap preDeleteTagResourceMap(String externalResourceId, String externalTagId) throws Exception {
		List<RangerTagResourceMap> exist;
		exist = tagStore.getTagResourceMap(externalResourceId, externalTagId);
		if (CollectionUtils.isEmpty(exist) || CollectionUtils.size(exist) != 1) {
			throw new Exception("Attempt to create nonexistent association between resourceId=" + externalResourceId + " and tagId=" + externalTagId);
		}
		return exist.get(0);
	}
}
