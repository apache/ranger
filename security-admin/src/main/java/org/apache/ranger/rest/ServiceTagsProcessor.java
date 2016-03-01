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

package org.apache.ranger.rest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTagResourceMap;
import org.apache.ranger.plugin.store.RangerServiceResourceSignature;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.util.ServiceTags;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServiceTagsProcessor {
	private static final Log LOG = LogFactory.getLog(ServiceTagsProcessor.class);

	private final TagStore tagStore;

	public ServiceTagsProcessor(TagStore tagStore) {
		this.tagStore = tagStore;
	}

	public void process(ServiceTags serviceTags) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceTagsProcessor.process()");
		}

		if (tagStore != null && serviceTags != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("serviceTags:	op=" + serviceTags.getOp());
				LOG.debug("serviceTags:	tagModel=" + serviceTags.getTagModel());
			}
			String op = serviceTags.getOp();

			if (StringUtils.equalsIgnoreCase(op, ServiceTags.OP_ADD_OR_UPDATE)) {
				addOrUpdate(serviceTags);
			} else if (StringUtils.equalsIgnoreCase(op, ServiceTags.OP_DELETE)) {
				delete(serviceTags);
			} else if (StringUtils.equalsIgnoreCase(op, ServiceTags.OP_REPLACE)) {
				replace(serviceTags);
			} else {
				LOG.error("Unknown op, op=" + op);
			}
		} else {
			if(tagStore == null) {
				LOG.error("tagStore is null!!");
			}

			if (serviceTags == null) {
				LOG.error("No ServiceTags to import!!");
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceTagsProcessor.process()");
		}
	}

	// Map tagdef, tag, serviceResource ids to created ids and use them in tag-resource-mapping
	private void addOrUpdate(ServiceTags serviceTags) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceTagsProcessor.createOrUpdate()");
		}

		Map<Long, RangerTagDef>          tagDefsInStore   = new HashMap<Long, RangerTagDef>();
		Map<Long, RangerTag>             tagsInStore      = new HashMap<Long, RangerTag>();
		Map<Long, RangerServiceResource> resourcesInStore = new HashMap<Long, RangerServiceResource>();

		boolean createOrUpdate = true;

		if (MapUtils.isNotEmpty(serviceTags.getTagDefinitions())) {
			RangerTagDef tagDef = null;

			try {
				for (Map.Entry<Long, RangerTagDef> entry : serviceTags.getTagDefinitions().entrySet()) {
					tagDef = entry.getValue();

					RangerTagDef existing = null;

					if(createOrUpdate) {
						if(StringUtils.isNotEmpty(tagDef.getGuid())) {
							existing = tagStore.getTagDefByGuid(tagDef.getGuid());
						}

						if(existing == null && StringUtils.isNotEmpty(tagDef.getName())) {
							existing = tagStore.getTagDefByName(tagDef.getName());
						}
					}

					RangerTagDef tagDefInStore = null;

					if(existing == null) {
						tagDefInStore = tagStore.createTagDef(tagDef);
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug("tagDef for name:" + tagDef.getName() + " exists, will not update it");
						}
						tagDefInStore = existing;
					}

					if(tagDefsInStore != null) {
						tagDefsInStore.put(entry.getKey(), tagDefInStore);
					}
				}
			} catch (Exception exception) {
				LOG.error("createTagDef failed, tagDef=" + tagDef, exception);
				throw exception;
			}
		}

		if (MapUtils.isNotEmpty(serviceTags.getTags())) {
			RangerTag tag = null;

			try {
				for (Map.Entry<Long, RangerTag> entry : serviceTags.getTags().entrySet()) {
					tag = entry.getValue();

					RangerTag existing = null;

					if(createOrUpdate) {
						if(StringUtils.isNotEmpty(tag.getGuid())) {
							existing = tagStore.getTagByGuid(tag.getGuid());
						}
					}

					RangerTag tagInStore = null;

					if(existing == null) {
						tagInStore = tagStore.createTag(tag);
					} else {
						tag.setId(existing.getId());
						tag.setGuid(existing.getGuid());

						tagInStore = tagStore.updateTag(tag);
					}

					if(tagsInStore != null) {
						tagsInStore.put(entry.getKey(), tagInStore);
					}
				}
			} catch (Exception exception) {
				LOG.error("createTag failed, tag=" + tag, exception);
				throw exception;
			}
		}

		List<RangerServiceResource> resources = serviceTags.getServiceResources();
		if (CollectionUtils.isNotEmpty(resources)) {
			RangerServiceResource resource = null;

			try {
				for (int i = 0; i < resources.size(); i++) {
					resource = resources.get(i);

					RangerServiceResource existing          = null;
					String                resourceSignature = null;
					Long                  resourceId        = resource.getId();

					if(createOrUpdate) {
						if(StringUtils.isNotEmpty(resource.getGuid())) {
							existing = tagStore.getServiceResourceByGuid(resource.getGuid());
						}

						if(existing == null) {
							if(MapUtils.isNotEmpty(resource.getResourceElements())) {
								RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);

								resourceSignature = serializer.getSignature();

								existing = tagStore.getServiceResourceByResourceSignature(resourceSignature);
							}
						}

						if(existing != null) {
							resourceSignature = existing.getResourceSignature();
						}
					}

					if(StringUtils.isEmpty(resourceSignature)) {
						RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);

						resourceSignature = serializer.getSignature();
					}

					RangerServiceResource resourceInStore = null;

					if (existing == null) {
						resource.setResourceSignature(resourceSignature);

						resourceInStore = tagStore.createServiceResource(resource);
					} else if (StringUtils.isEmpty(resource.getServiceName()) || MapUtils.isEmpty(resource.getResourceElements())) {
						resourceInStore = existing;
					} else {
						resource.setId(existing.getId());
						resource.setGuid(existing.getGuid());
						resource.setResourceSignature(resourceSignature);

						resourceInStore = tagStore.updateServiceResource(resource);
					}

					resourcesInStore.put(resourceId, resourceInStore);
				}
			} catch (Exception exception) {
				LOG.error("createServiceResource failed, resource=" + resource, exception);
				throw exception;
			}
		}

		if (MapUtils.isNotEmpty(serviceTags.getResourceToTagIds())) {
			for (Map.Entry<Long, List<Long>> entry : serviceTags.getResourceToTagIds().entrySet()) {
				Long resourceId = entry.getKey();

				RangerServiceResource resourceInStore = resourcesInStore.get(resourceId);

				if (resourceInStore == null) {
					LOG.error("Resource (id=" + resourceId + ") not found. Skipping tags update");
					continue;
				}

				// Get all tags associated with this resourceId
				List<Long> tagsToDelete = null;
				try {
					tagsToDelete = tagStore.getTagIdsForResourceId(resourceInStore.getId());
				} catch (Exception exception) {
					LOG.error("RangerTags cannot be retrieved for resource with guid=" + resourceInStore.getGuid());
					throw exception;
				}

				List<Long> tagIds = entry.getValue();
				try {
					for (Long tagId : tagIds) {
						RangerTag tagInStore = tagsInStore.get(tagId);

						if (tagInStore == null) {
							LOG.error("Tag (id=" + tagId + ") not found. Skipping addition of this tag for resource (id=" + resourceId + ")");
							continue;
						}

						RangerTagResourceMap existing = null;

						if(createOrUpdate) {
							existing = tagStore.getTagResourceMapForTagAndResourceId(tagInStore.getId(), resourceInStore.getId());
						}

						if(existing == null) {
							RangerTagResourceMap tagResourceMap = new RangerTagResourceMap();

							tagResourceMap.setTagId(tagInStore.getId());
							tagResourceMap.setResourceId(resourceInStore.getId());

							tagResourceMap = tagStore.createTagResourceMap(tagResourceMap);
						}

						if(tagsToDelete != null) {
							tagsToDelete.remove((Long)tagInStore.getId());
						}
					}
				} catch (Exception exception) {
					LOG.error("createRangerTagResourceMap failed", exception);
					throw exception;
				}

				if (CollectionUtils.isNotEmpty(tagsToDelete)) {
					Long tagId = null;

					try {
						for(int i = 0; i < tagsToDelete.size(); i++) {
							tagId = tagsToDelete.get(i);

							RangerTagResourceMap tagResourceMap = tagStore.getTagResourceMapForTagAndResourceId(tagId, resourceInStore.getId());

							if(tagResourceMap != null) {
								tagStore.deleteTagResourceMap(tagResourceMap.getId());
							}

							if (LOG.isDebugEnabled()) {
								LOG.debug("Deleted tagResourceMap(tagId=" + tagId + ", resourceId=" + resourceInStore.getId());
							}

							if (StringUtils.equals(serviceTags.getTagModel(), ServiceTags.TAGMODEL_RESOURCE_PRIVATE)) {
								tagStore.deleteTag(tagId);

								if (LOG.isDebugEnabled()) {
									LOG.debug("Deleted tag(tagId=" + tagId + ") as tagModel=" + ServiceTags.TAGMODEL_RESOURCE_PRIVATE);
								}
							}
						}
					} catch(Exception exception) {
						LOG.error("deleteTagResourceMap failed, tagId=" + tagId + ", resourceId=" + resourceInStore.getId());
						throw exception;
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceTagsProcessor.createOrUpdate()");
		}
	}

	private void delete(ServiceTags serviceTags) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceTagsProcessor.delete()");
		}

		// We dont expect any resourceId->tagId mappings in delete operation, so ignoring them if specified

		List<RangerServiceResource> serviceResources = serviceTags.getServiceResources();
		if (CollectionUtils.isNotEmpty(serviceResources)) {
			boolean isResourePrivateTag = StringUtils.equals(serviceTags.getTagModel(), ServiceTags.TAGMODEL_RESOURCE_PRIVATE) ? true : false;

			for (RangerServiceResource serviceResource : serviceResources) {
				try {
					RangerServiceResource objToDelete = tagStore.getServiceResourceByGuid(serviceResource.getGuid());

					if (objToDelete != null) {

						List<RangerTagResourceMap> tagResourceMaps = tagStore.getTagResourceMapsForResourceGuid(objToDelete.getGuid());

						if (CollectionUtils.isNotEmpty(tagResourceMaps)) {
							for (RangerTagResourceMap tagResourceMap : tagResourceMaps) {
								long tagId = tagResourceMap.getTagId();

								tagStore.deleteTagResourceMap(tagResourceMap.getId());

								if(isResourePrivateTag) {
									tagStore.deleteTag(tagId);
								}
							}
						}

						tagStore.deleteServiceResource(objToDelete.getId());
					}
				} catch (Exception exception) {
					LOG.error("deleteServiceResourceByGuid failed, guid=" + serviceResource.getGuid(), exception);
					throw exception;
				}
			}
		}

		Map<Long, RangerTag> tagsMap = serviceTags.getTags();
		if (MapUtils.isNotEmpty(tagsMap)) {
			for (Map.Entry<Long, RangerTag> entry : tagsMap.entrySet()) {
				RangerTag tag = entry.getValue();
				try {
					RangerTag objToDelete = tagStore.getTagByGuid(tag.getGuid());

					if (objToDelete != null) {
						tagStore.deleteTag(objToDelete.getId());
					}
				} catch (Exception exception) {
					LOG.error("deleteTag failed, guid=" + tag.getGuid(), exception);
					throw exception;
				}
			}
		}

		Map<Long, RangerTagDef> tagDefsMap = serviceTags.getTagDefinitions();
		if (MapUtils.isNotEmpty(tagDefsMap)) {
			for (Map.Entry<Long, RangerTagDef> entry : tagDefsMap.entrySet()) {
				RangerTagDef tagDef = entry.getValue();
				try {
					RangerTagDef objToDelete = tagStore.getTagDefByGuid(tagDef.getGuid());

					if(objToDelete != null) {
						tagStore.deleteTagDef(objToDelete.getId());
					}
				} catch (Exception exception) {
					LOG.error("deleteTagDef failed, guid=" + tagDef.getGuid(), exception);
					throw exception;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceTagsProcessor.delete()");
		}
	}

	// Delete all tagdef, tag, serviceResource and tagResourceMaps and then add all objects in provided ServiceTagsids
	private void replace(ServiceTags serviceTags) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceTagsProcessor.replace()");
		}

		// TODO:
		// This is an inefficient implementation. Replace by direct database deletes
		boolean isResourePrivateTag = StringUtils.equals(serviceTags.getTagModel(), ServiceTags.TAGMODEL_RESOURCE_PRIVATE) ? true : false;

		tagStore.deleteAllTagObjectsForService(serviceTags.getServiceName(), isResourePrivateTag);

		addOrUpdate(serviceTags);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceTagsProcessor.replace()");
		}
	}
}
