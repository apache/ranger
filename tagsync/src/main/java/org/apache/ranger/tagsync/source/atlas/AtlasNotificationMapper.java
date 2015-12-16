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
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.util.ServiceTags;

import java.util.*;

public class AtlasNotificationMapper {
	private static final Log LOG = LogFactory.getLog(AtlasNotificationMapper.class);

	public static ServiceTags processEntityNotification(EntityNotification entityNotification) {

		ServiceTags ret = null;

		if (isNotificationHandled(entityNotification)) {
			try {
				IReferenceableInstance entity = entityNotification.getEntity();

				if (AtlasResourceMapperUtil.isEntityTypeHandled(entity.getTypeName())) {
					AtlasEntityWithTraits entityWithTraits = new AtlasEntityWithTraits(entityNotification.getEntity(), entityNotification.getAllTraits());
					ret = buildServiceTags(entityWithTraits, 1L, 1L, null);
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Ranger not interested in Entity Notification for entity-type " + entityNotification.getEntity().getTypeName());
					}
				}
			} catch (Exception exception) {
				LOG.error("createServiceTags() failed!! ", exception);
			}
		}
		return ret;
	}

	public static Map<String, ServiceTags> processEntitiesWithTraits(List<AtlasEntityWithTraits> atlasEntities) {
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

		switch (opType) {
			case ENTITY_CREATE: {
				LOG.debug("ENTITY_CREATE notification is not handled, as Ranger will get necessary information from any subsequent TRAIT_ADDED notification");
				break;
			}
			case ENTITY_UPDATE:
			case TRAIT_ADD:
			case TRAIT_DELETE: {
				ret = true;
				break;
			}
			default:
				LOG.error(opType + ": unknown notification received - not handled");
		}

		return ret;
	}

	static private Map<String, ServiceTags> buildServiceTags(List<AtlasEntityWithTraits> entitiesWithTraits) throws Exception {

		Map<String, ServiceTags> ret = new HashMap<String, ServiceTags>();

		long serviceResourceIndex = 1L;
		long tagIndex = 1L;

		for (AtlasEntityWithTraits element : entitiesWithTraits) {

			ServiceTags serviceTags = buildServiceTags(element, serviceResourceIndex, tagIndex, ret);

			serviceResourceIndex++;

			tagIndex += CollectionUtils.size(serviceTags.getTags());

		}

		// Remove duplicate tag definitions
		for (Map.Entry<String, ServiceTags> serviceTagsMapEntry : ret.entrySet()){

			Map<Long, RangerTagDef> allTagDefs = serviceTagsMapEntry.getValue().getTagDefinitions();

			Map<String, String> tagTypeIndex = new HashMap<String, String>();
			Map<Long, RangerTagDef> uniqueTagDefs = new HashMap<Long, RangerTagDef>();

			for (Map.Entry<Long, RangerTagDef> entry : allTagDefs.entrySet()) {
				String tagTypeName = entry.getValue().getName();

				if (tagTypeIndex.get(tagTypeName) == null) {
					tagTypeIndex.put(tagTypeName, tagTypeName);
					uniqueTagDefs.put(entry.getKey(), entry.getValue());
				}
			}
			serviceTagsMapEntry.getValue().setTagDefinitions(uniqueTagDefs);
		}

		return ret;
	}

	static private ServiceTags buildServiceTags(AtlasEntityWithTraits entityWithTraits, long index, long tagIndex, Map<String, ServiceTags> serviceTagsMap) throws Exception {

		ServiceTags ret = null;

		IReferenceableInstance entity = entityWithTraits.getEntity();

		RangerServiceResource serviceResource = AtlasResourceMapperUtil.getRangerServiceResource(entity);

		if (serviceResource != null) {

			serviceResource.setId(index);

			String serviceName = serviceResource.getServiceName();

			Map<Long, RangerTag> tags = getTags(entityWithTraits, tagIndex);

			Map<Long, RangerTagDef> tagDefs = getTagDefs(tags);

			Map<Long, List<Long>> resourceIdToTagIds = null;

			resourceIdToTagIds = new HashMap<Long, List<Long>>();
			List<Long> tagList = new ArrayList<Long>();

			if (MapUtils.isNotEmpty(tags)) {
				resourceIdToTagIds = new HashMap<Long, List<Long>>();

				for (Map.Entry<Long, RangerTag> entry : tags.entrySet()) {
					tagList.add(entry.getKey());
				}
			}

			resourceIdToTagIds.put(index, tagList);

			ret = createOrGetServiceTags(serviceTagsMap, serviceName);

			ret.getServiceResources().add(serviceResource);
			ret.getTagDefinitions().putAll(tagDefs);
			ret.getTags().putAll(tags);
			ret.getResourceToTagIds().putAll(resourceIdToTagIds);

		} else {
			LOG.error("AtlasResourceMapper not found for entity-type:" + entity.getTypeName());
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
			ret.setTagModel(ServiceTags.TAGMODEL_RESOURCE_PRIVATE);
			ret.setServiceName(serviceName);
		}

		return ret;
	}

	static private Map<Long, RangerTag> getTags(AtlasEntityWithTraits entityWithTraits, long index) {
		Map<Long, RangerTag> ret = null;

		ret = new HashMap<Long, RangerTag>();

		List<IStruct> traits = entityWithTraits.getAllTraits();

		for (IStruct trait : traits) {

			String traitName = trait.getTypeName();

			Map<String, String> tagAttrValues = new HashMap<String, String>();

			try {

				Map<String, Object> attrValues = trait.getValuesMap();

				for (Map.Entry<String, Object> attrValueEntry : attrValues.entrySet()) {
					String attrName = attrValueEntry.getKey();
					Object attrValue = attrValueEntry.getValue();
					try {
						String strValue = String.class.cast(attrValue);
						tagAttrValues.put(attrName, strValue);
					} catch (ClassCastException exception) {
						LOG.error("Cannot cast attribute-value to String, skipping... attrName=" + attrName);
					}
				}
			} catch (AtlasException exception) {
				LOG.error("Could not get values for trait:" + traitName, exception);
			}

			RangerTag tag = new RangerTag();

			tag.setType(traitName);
			tag.setAttributes(tagAttrValues);

			ret.put(index++, tag);

		}

		return ret;
	}

	static private Map<Long, RangerTagDef> getTagDefs(Map<Long, RangerTag> tags) {

		Map<Long, RangerTagDef> ret = null;

		if (MapUtils.isNotEmpty(tags)) {
			ret = new HashMap<Long, RangerTagDef>();

			for (Map.Entry<Long, RangerTag> entry : tags.entrySet()) {

				RangerTagDef tagDef = new RangerTagDef();
				tagDef.setName(entry.getValue().getType());
				tagDef.setId(entry.getKey());
				ret.put(entry.getKey(), tagDef);

			}
		}

		return ret;
	}

}
