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

package org.apache.ranger.tagsync.source.atlas;

import org.apache.atlas.v1.model.notification.EntityNotificationV1;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTagDef.RangerTagAttributeDef;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntityWithTags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AtlasNotificationMapper {
	private static final Log LOG = LogFactory.getLog(AtlasNotificationMapper.class);


	private static Map<String, Long> unhandledEventTypes = new HashMap<String, Long>();

	private static void logUnhandledEntityNotification(EntityNotificationV1 entityNotification) {

		final int REPORTING_INTERVAL_FOR_UNHANDLED_ENTITYTYPE_IN_MILLIS = 5 * 60 * 1000; // 5 minutes

		boolean loggingNeeded = false;
		String entityTypeName = entityNotification != null && entityNotification.getEntity() != null ?
				entityNotification.getEntity().getTypeName() : null;

		if (entityTypeName != null) {
			Long timeInMillis = unhandledEventTypes.get(entityTypeName);
			long currentTimeInMillis = System.currentTimeMillis();
			if (timeInMillis == null ||
					(currentTimeInMillis - timeInMillis) >= REPORTING_INTERVAL_FOR_UNHANDLED_ENTITYTYPE_IN_MILLIS) {
				unhandledEventTypes.put(entityTypeName, currentTimeInMillis);
				loggingNeeded = true;
			}
		} else {
			LOG.error("EntityNotification contains NULL entity or NULL entity-type");
		}

		if (loggingNeeded) {
			LOG.warn("Ignoring entity notification of type " + entityTypeName);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Ignoring entity notification of type " + entityTypeName);
		}
	}

	@SuppressWarnings("unchecked")
	public static ServiceTags processEntityNotification(EntityNotificationV1 entityNotification) {

		ServiceTags ret = null;

		if (isNotificationHandled(entityNotification)) {
			try {
				RangerAtlasEntityWithTags entityWithTags = new RangerAtlasEntityWithTags(entityNotification);

				if (entityNotification.getOperationType() == EntityNotificationV1.OperationType.ENTITY_DELETE) {
					ret = buildServiceTagsForEntityDeleteNotification(entityWithTags);
				} else {
					ret = buildServiceTags(entityWithTags, null);
				}

			} catch (Exception exception) {
				LOG.error("createServiceTags() failed!! ", exception);
			}
		} else {
			logUnhandledEntityNotification(entityNotification);
		}
		return ret;
	}

	public static Map<String, ServiceTags> processAtlasEntities(List<RangerAtlasEntityWithTags> atlasEntities) {
		Map<String, ServiceTags> ret = null;

		try {
			ret = buildServiceTags(atlasEntities);
		} catch (Exception exception) {
			LOG.error("Failed to build serviceTags", exception);
		}

		return ret;
	}

	static private boolean isNotificationHandled(EntityNotificationV1 entityNotification) {
		boolean ret = false;

		EntityNotificationV1.OperationType opType = entityNotification.getOperationType();

		if (opType != null) {
			switch (opType) {
				case ENTITY_CREATE:
					ret = CollectionUtils.isNotEmpty(entityNotification.getAllTraits());
					break;
				case ENTITY_UPDATE:
				case ENTITY_DELETE:
				case TRAIT_ADD:
				case TRAIT_UPDATE:
				case TRAIT_DELETE: {
					ret = true;
					break;
				}
				default:
					LOG.error(opType + ": unknown notification received - not handled");
					break;
			}
			if (ret) {
				final Referenceable entity = entityNotification.getEntity();

				ret = entity != null
						&& entity.getId().getState() == Id.EntityState.ACTIVE
						&& AtlasResourceMapperUtil.isEntityTypeHandled(entity.getTypeName());
			}
		}

		return ret;
	}

	static private ServiceTags buildServiceTagsForEntityDeleteNotification(RangerAtlasEntityWithTags entityWithTags) throws Exception {
		final ServiceTags ret;

		RangerAtlasEntity entity = entityWithTags.getEntity();

		String guid = entity.getGuid();
		if (StringUtils.isNotBlank(guid)) {
			ret = new ServiceTags();
			RangerServiceResource serviceResource = new RangerServiceResource();
			serviceResource.setGuid(guid);
			ret.getServiceResources().add(serviceResource);
		} else {
			ret = buildServiceTags(entityWithTags, null);
			if (ret != null) {
				// tag-definitions should NOT be deleted as part of service-resource delete
				ret.setTagDefinitions(MapUtils.EMPTY_MAP);
				// Ranger deletes tags associated with deleted service-resource
				ret.setTags(MapUtils.EMPTY_MAP);
			}
		}

		if (ret != null) {
			ret.setOp(ServiceTags.OP_DELETE);
		}

		return ret;
	}

	static private Map<String, ServiceTags> buildServiceTags(List<RangerAtlasEntityWithTags> entitiesWithTags) throws Exception {

		Map<String, ServiceTags> ret = new HashMap<String, ServiceTags>();

		for (RangerAtlasEntityWithTags element : entitiesWithTags) {
			RangerAtlasEntity entity = element.getEntity();
			if (entity != null) {
				buildServiceTags(element, ret);
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Ignoring entity because its State is not ACTIVE: " + element);
				}
			}
		}

		// Remove duplicate tag definitions
		if(CollectionUtils.isNotEmpty(ret.values())) {
			for (ServiceTags serviceTag : ret.values()) {
				if(MapUtils.isNotEmpty(serviceTag.getTagDefinitions())) {
					Map<String, RangerTagDef> uniqueTagDefs = new HashMap<String, RangerTagDef>();

					for (RangerTagDef tagDef : serviceTag.getTagDefinitions().values()) {
						RangerTagDef existingTagDef = uniqueTagDefs.get(tagDef.getName());

						if (existingTagDef == null) {
							uniqueTagDefs.put(tagDef.getName(), tagDef);
						} else {
							if(CollectionUtils.isNotEmpty(tagDef.getAttributeDefs())) {
								for(RangerTagAttributeDef tagAttrDef : tagDef.getAttributeDefs()) {
									boolean attrDefExists = false;

									if(CollectionUtils.isNotEmpty(existingTagDef.getAttributeDefs())) {
										for(RangerTagAttributeDef existingTagAttrDef : existingTagDef.getAttributeDefs()) {
											if(StringUtils.equalsIgnoreCase(existingTagAttrDef.getName(), tagAttrDef.getName())) {
												attrDefExists = true;
												break;
											}
										}
									}

									if(! attrDefExists) {
										existingTagDef.getAttributeDefs().add(tagAttrDef);
									}
								}
							}
						}
					}

					serviceTag.getTagDefinitions().clear();
					for(RangerTagDef tagDef : uniqueTagDefs.values()) {
						serviceTag.getTagDefinitions().put(tagDef.getId(), tagDef);
					}
				}
			}
		}

		if (MapUtils.isNotEmpty(ret)) {
			for (Map.Entry<String, ServiceTags> entry : ret.entrySet()) {
				ServiceTags serviceTags = entry.getValue();
				serviceTags.setOp(ServiceTags.OP_REPLACE);
			}
		}
		return ret;
	}

	static private ServiceTags buildServiceTags(RangerAtlasEntityWithTags entityWithTags, Map<String, ServiceTags> serviceTagsMap) throws Exception {
		ServiceTags            ret             = null;
		RangerAtlasEntity entity          = entityWithTags.getEntity();
		RangerServiceResource  serviceResource = AtlasResourceMapperUtil.getRangerServiceResource(entity);

		if (serviceResource != null) {

			List<RangerTag>     tags        = getTags(entityWithTags);
			List<RangerTagDef>  tagDefs     = getTagDefs(entityWithTags);
			String              serviceName = serviceResource.getServiceName();

			ret = createOrGetServiceTags(serviceTagsMap, serviceName);

			if (serviceTagsMap == null || CollectionUtils.isNotEmpty(tags)) {

				serviceResource.setId((long) ret.getServiceResources().size());
				ret.getServiceResources().add(serviceResource);

				List<Long> tagIds = new ArrayList<Long>();

				if (CollectionUtils.isNotEmpty(tags)) {
					for (RangerTag tag : tags) {
						tag.setId((long) ret.getTags().size());
						ret.getTags().put(tag.getId(), tag);

						tagIds.add(tag.getId());
					}
				}
				ret.getResourceToTagIds().put(serviceResource.getId(), tagIds);

				if (CollectionUtils.isNotEmpty(tagDefs)) {
					for (RangerTagDef tagDef : tagDefs) {
						tagDef.setId((long) ret.getTagDefinitions().size());
						ret.getTagDefinitions().put(tagDef.getId(), tagDef);
					}
				}
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Entity " + entityWithTags + " does not have any tags associated with it when full-sync is being done.");
					LOG.debug("Will not add this entity to serviceTags, so that this entity, if exists,  will be removed from ranger");
				}
			}
		} else {
			LOG.error("Failed to build serviceResource for entity:" + entity.getGuid());
		}

		return ret;
	}

	static private ServiceTags createOrGetServiceTags(Map<String, ServiceTags> serviceTagsMap, String serviceName) {
		ServiceTags ret = serviceTagsMap == null ? null : serviceTagsMap.get(serviceName);

		if (ret == null) {
			ret = new ServiceTags();

			if (serviceTagsMap != null) {
				serviceTagsMap.put(serviceName, ret);
			}

			ret.setOp(ServiceTags.OP_ADD_OR_UPDATE);
			ret.setServiceName(serviceName);
		}

		return ret;
	}

	static private List<RangerTag> getTags(RangerAtlasEntityWithTags entityWithTags) {
		List<RangerTag> ret = new ArrayList<RangerTag>();

		if (entityWithTags != null && MapUtils.isNotEmpty(entityWithTags.getTags())) {
			Map<String, Map<String, String>> tags = entityWithTags.getTags();

			for (Map.Entry<String, Map<String, String>> tag : tags.entrySet()) {
				ret.add(new RangerTag(null, tag.getKey(), tag.getValue(), RangerTag.OWNER_SERVICERESOURCE));
			}
		}

		return ret;
	}

	static private List<RangerTagDef> getTagDefs(RangerAtlasEntityWithTags entityWithTags) {
		List<RangerTagDef> ret = new ArrayList<RangerTagDef>();

		if (entityWithTags != null && MapUtils.isNotEmpty(entityWithTags.getTags())) {
			Map<String, Map<String, String>> tags = entityWithTags.getTags();

			for (Map.Entry<String, Map<String, String>> tag : tags.entrySet()) {
				RangerTagDef tagDef = new RangerTagDef(tag.getKey(), "Atlas");
				if (MapUtils.isNotEmpty(tag.getValue())) {
					for (String attributeName : tag.getValue().keySet()) {
						tagDef.getAttributeDefs().add(new RangerTagAttributeDef(attributeName, entityWithTags.getTagAttributeType(tag.getKey(), attributeName)));
					}
				}
				ret.add(tagDef);
			}
		}

		return ret;
	}
}
