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

import org.apache.atlas.AtlasException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.type.AtlasBuiltInTypes;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.persistence.Id;
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class AtlasNotificationMapper {
	private static final Log LOG = LogFactory.getLog(AtlasNotificationMapper.class);


	private static Map<String, Long> unhandledEventTypes = new HashMap<String, Long>();

	private static final ThreadLocal<DateFormat> DATE_FORMATTER = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			SimpleDateFormat dateFormat = new SimpleDateFormat(AtlasBaseTypeDef.SERIALIZED_DATE_FORMAT_STR);

			dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

			return dateFormat;
		}
	};

	private static void logUnhandledEntityNotification(EntityNotification entityNotification) {

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
	public static ServiceTags processEntityNotification(EntityNotification entityNotification) {

		ServiceTags ret = null;

		if (isNotificationHandled(entityNotification)) {
			try {
				IReferenceableInstance entity = entityNotification.getEntity();

				AtlasEntityWithTraits entityWithTraits = new AtlasEntityWithTraits(entity, entityNotification.getAllTraits());

				if (entityNotification.getOperationType() == EntityNotification.OperationType.ENTITY_DELETE) {
					ret = buildServiceTagsForEntityDeleteNotification(entityWithTraits);
				} else {
					ret = buildServiceTags(entityWithTraits, null);
				}
			} catch (Exception exception) {
				LOG.error("createServiceTags() failed!! ", exception);
			}
		} else {
			logUnhandledEntityNotification(entityNotification);
		}
		return ret;
	}

	public static Map<String, ServiceTags> processAtlasEntities(List<AtlasEntityWithTraits> atlasEntities) {
		Map<String, ServiceTags> ret = null;

		try {
			ret = buildServiceTags(atlasEntities);
		} catch (Exception exception) {
			LOG.error("Failed to build serviceTags", exception);
		}

		return ret;
	}

	static private boolean isNotificationHandled(EntityNotification entityNotification) {
		boolean ret = false;

		EntityNotification.OperationType opType = entityNotification.getOperationType();

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
				final IReferenceableInstance entity = entityNotification.getEntity();

				ret = entity != null
						&& entity.getId().getState() == Id.EntityState.ACTIVE
						&& AtlasResourceMapperUtil.isEntityTypeHandled(entity.getTypeName());
			}
		}

		return ret;
	}

	static private ServiceTags buildServiceTagsForEntityDeleteNotification(AtlasEntityWithTraits entityWithTraits) throws Exception {
		final ServiceTags ret;

		IReferenceableInstance entity = entityWithTraits.getEntity();

		String guid = entity.getId()._getId();
		if (StringUtils.isNotBlank(guid)) {
			ret = new ServiceTags();
			RangerServiceResource serviceResource = new RangerServiceResource();
			serviceResource.setGuid(guid);
			ret.getServiceResources().add(serviceResource);
		} else {
			ret = buildServiceTags(entityWithTraits, null);
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

	static private Map<String, ServiceTags> buildServiceTags(List<AtlasEntityWithTraits> entitiesWithTraits) throws Exception {
		Map<String, ServiceTags> ret = new HashMap<String, ServiceTags>();

		for (AtlasEntityWithTraits element : entitiesWithTraits) {
			IReferenceableInstance entity = element.getEntity();
			if (entity != null && entity.getId().getState() == Id.EntityState.ACTIVE) {
				buildServiceTags(element, ret);
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Ignoring entity because its State is not ACTIVE: " + element);
				}
			}
		}

		return ret;
	}

	static private ServiceTags buildServiceTags(AtlasEntityWithTraits entityWithTraits, Map<String, ServiceTags> serviceTagsMap) throws Exception {
		ServiceTags            ret             = null;
		IReferenceableInstance entity          = entityWithTraits.getEntity();
		RangerServiceResource  serviceResource = AtlasResourceMapperUtil.getRangerServiceResource(entity);

		if (serviceResource != null) {
			List<RangerTag>    tags        = getTags(entityWithTraits);
			List<RangerTagDef> tagDefs     = getTagDefs(entityWithTraits);
			String             serviceName = serviceResource.getServiceName();

			ret = createOrGetServiceTags(serviceTagsMap, serviceName);

			if (serviceTagsMap == null || CollectionUtils.isNotEmpty(tags)) {
				serviceResource.setId((long) ret.getServiceResources().size());
				ret.getServiceResources().add(serviceResource);

				List<Long> tagIds = new ArrayList<>();

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
					LOG.debug("Entity " + entityWithTraits + " does not have any tags associated with it when full-sync is being done.");
					LOG.debug("Will not add this entity to serviceTags, so that this entity, if exists,  will be removed from ranger");
				}
			}
		} else {
			LOG.error("Failed to build serviceResource for entity:" + entity.getId()._getId());
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

	static private List<RangerTag> getTags(AtlasEntityWithTraits entityWithTraits) {
		List<RangerTag>        ret    = new ArrayList<RangerTag>();
		IReferenceableInstance entity = entityWithTraits != null ? entityWithTraits.getEntity() : null;

		if(entity != null && CollectionUtils.isNotEmpty(entity.getTraits())) {
			for (String traitName : entity.getTraits()) {
				IStruct             trait    = entity.getTrait(traitName);
				Map<String, String> tagAttrs = new HashMap<String, String>();

				try {
					Map<String, Object> attrs = trait.getValuesMap();

					if(MapUtils.isNotEmpty(attrs)) {
						for (Map.Entry<String, Object> attrEntry : attrs.entrySet()) {
							String attrName  = attrEntry.getKey();
							Object attrValue = attrEntry.getValue();

							tagAttrs.put(attrName, attrValue != null ? attrValue.toString() : null);
						}
					}

				} catch (AtlasException exception) {
					LOG.error("Could not get values for trait:" + trait.getTypeName(), exception);
				}

				ret.add(new RangerTag(null, trait.getTypeName(), tagAttrs, RangerTag.OWNER_SERVICERESOURCE));
			}
		}

		return ret;
	}

	static private List<RangerTagDef> getTagDefs(AtlasEntityWithTraits entityWithTraits) {
		List<RangerTagDef>     ret    = new ArrayList<RangerTagDef>();
		IReferenceableInstance entity = entityWithTraits != null ? entityWithTraits.getEntity() : null;

		if(entity != null && CollectionUtils.isNotEmpty(entity.getTraits())) {
			for (String traitName : entity.getTraits()) {
				IStruct       trait = entity.getTrait(traitName);
				RangerTagDef tagDef = new RangerTagDef(trait.getTypeName(), "Atlas");

				try {
					Map<String, Object> attrs = trait.getValuesMap();

					if(MapUtils.isNotEmpty(attrs)) {
						for (String attrName : attrs.keySet()) {
							tagDef.getAttributeDefs().add(new RangerTagAttributeDef(attrName, "string"));
						}
					}
				} catch (AtlasException exception) {
					LOG.error("Could not get values for trait:" + trait.getTypeName(), exception);
				}

				ret.add(tagDef);
			}
		}

		return ret;
	}

	public static Map<String, ServiceTags> processSearchResult(AtlasSearchResult result, AtlasTypeRegistry typeRegistry) {
		Map<String, ServiceTags> ret = null;

		try {
			ret = buildServiceTags(result, typeRegistry);
		} catch (Exception exception) {
			LOG.error("Failed to build serviceTags", exception);
		}

		return ret;
	}

	static private Map<String, ServiceTags> buildServiceTags(AtlasSearchResult result, AtlasTypeRegistry typeRegistry) throws Exception {
		Map<String, ServiceTags> ret = new HashMap<>();

		for (AtlasEntityHeader entity : result.getEntities()) {
			if (entity != null && entity.getStatus() == AtlasEntity.Status.ACTIVE) {
				buildServiceTags(entity, typeRegistry, ret);
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Ignoring entity because its State is not ACTIVE: " + entity);
				}
			}
		}

		// Remove duplicate tag definitions
		if(CollectionUtils.isNotEmpty(ret.values())) {
			for (ServiceTags serviceTag : ret.values()) {
				if(MapUtils.isNotEmpty(serviceTag.getTagDefinitions())) {
					Map<String, RangerTagDef> uniqueTagDefs = new HashMap<>();

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

	static private ServiceTags buildServiceTags(AtlasEntityHeader entity, AtlasTypeRegistry typeRegistry, Map<String, ServiceTags> serviceTagsMap) throws Exception {
		ServiceTags           ret             = null;
		RangerServiceResource serviceResource = AtlasResourceMapperUtil.getRangerServiceResource(entity);

		if (serviceResource != null) {
			List<RangerTag>     tags        = getTags(entity, typeRegistry);
			List<RangerTagDef>  tagDefs     = getTagDefs(entity);
			String              serviceName = serviceResource.getServiceName();

			ret = createOrGetServiceTags(serviceTagsMap, serviceName);

			if (serviceTagsMap == null || CollectionUtils.isNotEmpty(tags)) {
				serviceResource.setId((long) ret.getServiceResources().size());
				ret.getServiceResources().add(serviceResource);

				List<Long> tagIds = new ArrayList<>();

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
					LOG.debug("Entity " + entity + " does not have any tags associated with it when full-sync is being done.");
					LOG.debug("Will not add this entity to serviceTags, so that this entity, if exists,  will be removed from ranger");
				}
			}
		} else {
			LOG.error("Failed to build serviceResource for entity:" + entity.getGuid());
		}

		return ret;
	}

	static private List<RangerTag> getTags(AtlasEntityHeader entity, AtlasTypeRegistry typeRegistry) {
		List<RangerTag> ret = new ArrayList<>();

		if(entity != null && CollectionUtils.isNotEmpty(entity.getClassificationNames())) {
			List<AtlasClassification> classifications = entity.getClassifications();

			for (AtlasClassification classification : classifications) {
				ret.add(getRangerTag(classification, typeRegistry));

				List<AtlasClassification> superClassifications = getSuperClassifications(classification, typeRegistry);

				if (CollectionUtils.isNotEmpty(superClassifications)) {
					for (AtlasClassification superClassification : superClassifications) {
						ret.add(getRangerTag(superClassification, typeRegistry));
					}
				}
			}
		}

		return ret;
	}

	static private List<RangerTagDef> getTagDefs(AtlasEntityHeader entity) {
		List<RangerTagDef> ret = new ArrayList<>();

		if(entity != null && CollectionUtils.isNotEmpty(entity.getClassificationNames())) {
			List<AtlasClassification> traits = entity.getClassifications();

			for (AtlasClassification trait : traits) {
				RangerTagDef tagDef = new RangerTagDef(trait.getTypeName(), "Atlas");

				if(MapUtils.isNotEmpty(trait.getAttributes())) {
					for (String attrName : trait.getAttributes().keySet()) {
						tagDef.getAttributeDefs().add(new RangerTagAttributeDef(attrName, "string"));
					}
				}

				ret.add(tagDef);
			}
		}

		return ret;
	}

	static private List<AtlasClassification> getSuperClassifications(AtlasClassification classification, AtlasTypeRegistry typeRegistry) {
		List<AtlasClassification> ret                = null;
		AtlasClassificationType   classificationType = typeRegistry.getClassificationTypeByName(classification.getTypeName());

		if (classificationType != null && CollectionUtils.isNotEmpty(classificationType.getAllSuperTypes())) {
			ret = new ArrayList<>(classificationType.getAllSuperTypes().size());

			for (String superTypeName : classificationType.getAllSuperTypes()) {
				AtlasClassification superClassification = new AtlasClassification(superTypeName);

				if (MapUtils.isNotEmpty(classification.getAttributes())) {
					AtlasClassificationType superClassificationType = typeRegistry.getClassificationTypeByName(superTypeName);

					if (superClassificationType != null && MapUtils.isNotEmpty(superClassificationType.getAllAttributes())) {
						Map<String, Object> superClassificationAttributes = new HashMap<>();

						for (Map.Entry<String, Object> entry : classification.getAttributes().entrySet()) {
							String attrName = entry.getKey();

							if (superClassificationType.getAllAttributes().containsKey(attrName)) {
								superClassificationAttributes.put(attrName, entry.getValue());
							}
						}

						superClassification.setAttributes(superClassificationAttributes);
					}
				}

				ret.add(superClassification);
			}
		}

		return ret;
	}

	static private RangerTag getRangerTag(AtlasClassification classification, AtlasTypeRegistry typeRegistry) {
		final Map<String, String> tagAttrs;

		if(MapUtils.isNotEmpty(classification.getAttributes())) {
			tagAttrs = new HashMap<>();

			for (Map.Entry<String, Object> attrEntry : classification.getAttributes().entrySet()) {
				String attrName  = attrEntry.getKey();
				Object attrValue = attrEntry.getValue();

				// V2 Atlas APIs have date attributes as number; convert the value to earlier version format, so that
				// Ranger conditions can recognize the value correctly
				if (attrValue instanceof Number) {
					AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classification.getTypeName());
					AtlasAttribute          attribute          = (classificationType != null) ? classificationType.getAttribute(attrName) : null;

					if (attribute != null && attribute.getAttributeType() instanceof AtlasBuiltInTypes.AtlasDateType) {
						Date dateValue = new Date(((Number)attrValue).longValue());

						attrValue = DATE_FORMATTER.get().format(dateValue);
					}
				}

				tagAttrs.put(attrName, attrValue != null ? attrValue.toString() : null);
			}
		} else {
			tagAttrs = Collections.emptyMap();
		}

		return new RangerTag(null, classification.getTypeName(), tagAttrs, RangerTag.OWNER_SERVICERESOURCE);
	}
}
