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
import org.apache.commons.collections.MapUtils;
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
		if (StringUtils.isBlank(tag.getType())) {
			throw new Exception("Tag has no type");
		}
	}

	public void preUpdateTag(final Long id, final RangerTag tag) throws Exception {
		if (StringUtils.isBlank(tag.getType())) {
			throw new Exception("Tag has no type");
		}

		if (id == null) {
			throw new Exception("Invalid/null id");
		}

		RangerTag exist = tagStore.getTag(id);

		if (exist == null) {
			throw new Exception("Attempt to update nonexistant tag, id=" + id);
		}
		tag.setId(exist.getId());
	}

	public void preUpdateTagByGuid(String guid, final RangerTag tag) throws Exception {
		if (StringUtils.isBlank(tag.getType())) {
			throw new Exception("Tag has no type");
		}

		RangerTag existing = tagStore.getTagByGuid(guid);
		if (existing == null) {
			throw new Exception("Attempt to update nonexistent tag, guid=" + guid);
		}

		tag.setId(existing.getId());
		tag.setGuid(existing.getGuid());
	}

	public void preUpdateTagByName(String name, final RangerTag tag) throws Exception {
		if (StringUtils.isNotBlank(tag.getType())) {
			throw new Exception("tag has no type");
		}

		List<RangerTag> exist = tagStore.getTagsByType(name);
		if (CollectionUtils.isEmpty(exist) || CollectionUtils.size(exist) != 1) {
			throw new Exception("Attempt to update nonexistent or multiple tags, name=" + name);
		}

		RangerTag onlyTag = exist.get(0);

		tag.setId(onlyTag.getId());
		tag.setType(name);

	}

	public RangerTag preDeleteTag(Long id) throws Exception {
		RangerTag exist;
		exist = tagStore.getTag(id);
		if (exist == null) {
			throw new Exception("Attempt to delete nonexistent tag, id=" + id);
		}

		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsForTagId(exist.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete tag which is associated with a service-resource, id=" + id);
		}
		return exist;
	}

	public RangerTag preDeleteTagByGuid(String guid) throws Exception {
		RangerTag exiting = tagStore.getTagByGuid(guid);
		if (exiting == null) {
			throw new Exception("Attempt to delete nonexistent tag, guid=" + guid);
		}

		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsForTagId(exiting.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete tag which is associated with a service-resource, guid=" + guid);
		}
		return exiting;
	}

	public RangerTag preDeleteTagByName(String name) throws Exception {
		List<RangerTag> exist;
		exist = tagStore.getTagsByType(name);
		if (CollectionUtils.isEmpty(exist) || CollectionUtils.size(exist) != 1) {
			throw new Exception("Attempt to delete nonexistent or multiple tags, name=" + name);
		}
		RangerTag ret = exist.get(0);
		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsForTagId(ret.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete tag which is associated with a service-resource, name=" + name);
		}
		return ret;

	}

	public void preCreateServiceResource(RangerServiceResource resource) throws Exception {
		if (StringUtils.isBlank(resource.getServiceName()) || MapUtils.isEmpty(resource.getResourceElements())) {
			throw new Exception("No serviceName or resource in RangerServiceResource");
		}

		RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);

		String resourceSignature = serializer.getSignature();

		RangerServiceResource exist = tagStore.getServiceResourceByResourceSignature(resourceSignature);

		if (exist != null) {
			throw new Exception("Attempt to create existing resource, serviceName=" + resource.getServiceName());
		}

		resource.setResourceSignature(resourceSignature);
	}

	public void preUpdateServiceResource(Long id, RangerServiceResource resource) throws Exception {
		if (StringUtils.isBlank(resource.getServiceName()) || MapUtils.isEmpty(resource.getResourceElements())) {
			throw new Exception("No serviceName or resource in RangerServiceResource");
		}

		if (id == null) {
			throw new Exception("Invalid/null id");
		}

		RangerServiceResource exist = tagStore.getServiceResource(id);
		if (exist == null) {
			throw new Exception("Attempt to update nonexistent resource, id=" + id);
		}
		resource.setId(exist.getId());

		RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);
		resource.setResourceSignature(serializer.getSignature());

	}

	public void preUpdateServiceResourceByGuid(String guid, RangerServiceResource resource) throws Exception {
		if (StringUtils.isBlank(resource.getServiceName()) || MapUtils.isEmpty(resource.getResourceElements())) {
			throw new Exception("No serviceName or resource in RangerServiceResource");
		}

		RangerServiceResource existing = tagStore.getServiceResourceByGuid(guid);
		if (existing == null) {
			throw new Exception("Attempt to update nonexistent resource, guid=" + guid);
		}

		resource.setId(existing.getId());
		resource.setGuid(guid);

		RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);
		resource.setResourceSignature(serializer.getSignature());
	}

	public RangerServiceResource preDeleteServiceResource(Long id) throws Exception {
		RangerServiceResource exist;
		exist = tagStore.getServiceResource(id);
		if (exist == null) {
			throw new Exception("Attempt to delete nonexistent resource, id=" + id);
		}
		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsForResourceId(exist.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete serviceResource which is associated with a tag, id=" + id);
		}
		return exist;
	}

	public RangerServiceResource preDeleteServiceResourceByGuid(String guid) throws Exception {
		RangerServiceResource existing = tagStore.getServiceResourceByGuid(guid);
		if (existing == null) {
			throw new Exception("Attempt to delete nonexistent resource, guid=" + guid);
		}
		List<RangerTagResourceMap> associations = tagStore.getTagResourceMapsForResourceId(existing.getId());
		if (CollectionUtils.isNotEmpty(associations)) {
			throw new Exception("Attempt to delete serviceResource which is associated with a tag, guid=" + guid);
		}
		return existing;
	}

	public RangerTagResourceMap preCreateTagResourceMap(String tagGuid, String resourceGuid) throws Exception {
		if (StringUtils.isBlank(resourceGuid) || StringUtils.isBlank(tagGuid)) {
			throw new Exception("Both resourceGuid and resourceId need to be non-empty");
		}

		RangerTagResourceMap exist = tagStore.getTagResourceMapForTagAndResourceGuid(tagGuid, resourceGuid);
		if (exist != null) {
			throw new Exception("Attempt to create existing association between resourceId=" + resourceGuid + " and tagId=" + tagGuid);
		}

		RangerServiceResource existingServiceResource = tagStore.getServiceResourceByGuid(resourceGuid);

		if(existingServiceResource == null) {
			throw new Exception("No resource found for guid=" + resourceGuid);
		}

		RangerTag existingTag = tagStore.getTagByGuid(tagGuid);

		if(existingTag == null) {
			throw new Exception("No tag found for guid=" + tagGuid);
		}

		RangerTagResourceMap newTagResourceMap = new RangerTagResourceMap();
		newTagResourceMap.setResourceId(existingServiceResource.getId());
		newTagResourceMap.setTagId(existingTag.getId());

		return newTagResourceMap;
	}

	public RangerTagResourceMap preDeleteTagResourceMap(Long id) throws Exception {
		RangerTagResourceMap existing = tagStore.getTagResourceMap(id);
		if (existing == null) {
			throw new Exception("Attempt to delete nonexistent tagResourceMap(id=" + id + ")");
		}

		return existing;
	}

	public RangerTagResourceMap preDeleteTagResourceMapByGuid(String guid) throws Exception {
		RangerTagResourceMap existing = tagStore.getTagResourceMapByGuid(guid);
		if (existing == null) {
			throw new Exception("Attempt to delete nonexistent tagResourceMap(guid=" + guid + ")");
		}

		return existing;
	}

	public RangerTagResourceMap preDeleteTagResourceMap(String tagGuid, String resourceGuid) throws Exception {
		RangerTagResourceMap existing = tagStore.getTagResourceMapForTagAndResourceGuid(tagGuid, resourceGuid);
		if (existing == null) {
			throw new Exception("Attempt to delete nonexistent association between resourceId=" + resourceGuid + " and tagId=" + tagGuid);
		}

		return existing;
	}
}
