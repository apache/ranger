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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.process.TagSyncConfig;

import java.util.*;

public class AtlasNotificationMapper {
	private static final Log LOG = LogFactory.getLog(AtlasNotificationMapper.class);

	public static final String ENTITY_TYPE_HIVE_DB = "hive_db";
	public static final String ENTITY_TYPE_HIVE_TABLE = "hive_table";
	public static final String ENTITY_TYPE_HIVE_COLUMN = "hive_column";

	public static final String RANGER_TYPE_HIVE_DB = "database";
	public static final String RANGER_TYPE_HIVE_TABLE = "table";
	public static final String RANGER_TYPE_HIVE_COLUMN = "column";

	public static final String ENTITY_ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
	public static final String ENTITY_ATTRIBUTE_QUALIFIED_NAME_FOR_HIVE_TABLE = "name";
	public static final String QUALIFIED_NAME_FORMAT_DELIMITER_STRING = "\\.";
	public static final String QUALIFIED_NAME_FORMAT_CLUSTER_DELIMITER_STRING = "@";

	private static Properties properties = null;

	public static ServiceTags processEntityNotification(EntityNotification entityNotification, Properties props) {

		ServiceTags ret = null;
		properties = props;

		try {
			IReferenceableInstance entity = entityNotification.getEntity();

			if (isEntityMappable(entity)) {
				ret = createServiceTags(entityNotification);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Ranger not interested in Entity Notification for entity-type " + entityNotification.getEntity().getTypeName());
				}
			}
		} catch (Exception exception) {
			LOG.error("createServiceTags() failed!! ", exception);
		}
		return ret;
	}

	static private boolean isEntityMappable(IReferenceableInstance entity) {
		boolean ret = false;

		String entityTypeName = entity.getTypeName();

		if (StringUtils.isNotBlank(entityTypeName)) {
			if (StringUtils.equals(entityTypeName, ENTITY_TYPE_HIVE_DB) ||
					StringUtils.equals(entityTypeName, ENTITY_TYPE_HIVE_TABLE) ||
					StringUtils.equals(entityTypeName, ENTITY_TYPE_HIVE_COLUMN)) {
				ret = true;
			}
		}
		return ret;
	}

	static private ServiceTags createServiceTags(EntityNotification entityNotification) throws Exception {

		ServiceTags ret = null;

		EntityNotification.OperationType opType = entityNotification.getOperationType();

		switch (opType) {
			case ENTITY_CREATE: {
				LOG.debug("ENTITY_CREATE notification is not handled, as Ranger will get necessary information from any subsequent TRAIT_ADDED notification");
				break;
			}
			case ENTITY_UPDATE: {
				ret = getServiceTags(entityNotification);
				if (MapUtils.isEmpty(ret.getTags())) {
					LOG.debug("No traits associated with this entity update notification. Ignoring it altogether");
					ret = null;
				}
				break;
			}
			case TRAIT_ADD:
			case TRAIT_DELETE: {
				ret = getServiceTags(entityNotification);
				break;
			}
			default:
				LOG.error(opType + ": unknown notification received - not handled");
		}

		return ret;
	}

	static private ServiceTags getServiceTags(EntityNotification entityNotification) throws Exception {
		ServiceTags ret = null;

		IReferenceableInstance entity = entityNotification.getEntity();

		List<RangerServiceResource> serviceResources = new ArrayList<RangerServiceResource>();

		RangerServiceResource serviceResource = getServiceResource(entity);
		serviceResources.add(serviceResource);

		Map<Long, RangerTag> tags = getTags(entityNotification);

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

		resourceIdToTagIds.put(1L, tagList);


		ret = new ServiceTags();

		ret.setOp(ServiceTags.OP_ADD_OR_UPDATE);
		ret.setTagModel(ServiceTags.TAGMODEL_RESOURCE_PRIVATE);
		ret.setServiceName(serviceResource.getServiceName());
		ret.setServiceResources(serviceResources);
		ret.setTagDefinitions(tagDefs);
		ret.setTags(tags);
		ret.setResourceToTagIds(resourceIdToTagIds);

		return ret;
	}


	static private RangerServiceResource getServiceResource(IReferenceableInstance entity) throws Exception {

		RangerServiceResource ret = null;

		Map<String, RangerPolicy.RangerPolicyResource> elements = null;
		String serviceName = null;


		elements = new HashMap<String, RangerPolicy.RangerPolicyResource>();

		List<String> components = getQualifiedNameComponents(entity);
		// components should contain qualifiedName, clusterName, dbName, tableName, columnName in that order

		String entityTypeName = entity.getTypeName();

		String qualifiedName = components.get(0);

		String clusterName, dbName, tableName, columnName;

		if (components.size() > 1) {
			clusterName = components.get(1);
			serviceName = getServiceName(clusterName, entityTypeName);
		}

		if (StringUtils.equals(entityTypeName, ENTITY_TYPE_HIVE_DB)) {
			if (components.size() > 2) {
				dbName = components.get(2);
				RangerPolicy.RangerPolicyResource dbPolicyResource = new RangerPolicy.RangerPolicyResource(dbName);
				elements.put(RANGER_TYPE_HIVE_DB, dbPolicyResource);

			} else {
				LOG.error("invalid qualifiedName for HIVE_DB, qualifiedName=" + qualifiedName);
			}
		} else if (StringUtils.equals(entityTypeName, ENTITY_TYPE_HIVE_TABLE)) {
			if (components.size() > 3) {
				dbName = components.get(2);
				tableName = components.get(3);
				RangerPolicy.RangerPolicyResource dbPolicyResource = new RangerPolicy.RangerPolicyResource(dbName);
				elements.put(RANGER_TYPE_HIVE_DB, dbPolicyResource);
				RangerPolicy.RangerPolicyResource tablePolicyResource = new RangerPolicy.RangerPolicyResource(tableName);
				elements.put(RANGER_TYPE_HIVE_TABLE, tablePolicyResource);
			} else {
				LOG.error("invalid qualifiedName for HIVE_TABLE, qualifiedName=" + qualifiedName);
			}
		} else if (StringUtils.equals(entityTypeName, ENTITY_TYPE_HIVE_COLUMN)) {
			if (components.size() > 4) {
				dbName = components.get(2);
				tableName = components.get(3);
				columnName = components.get(4);
				RangerPolicy.RangerPolicyResource dbPolicyResource = new RangerPolicy.RangerPolicyResource(dbName);
				elements.put(RANGER_TYPE_HIVE_DB, dbPolicyResource);
				RangerPolicy.RangerPolicyResource tablePolicyResource = new RangerPolicy.RangerPolicyResource(tableName);
				elements.put(RANGER_TYPE_HIVE_TABLE, tablePolicyResource);
				RangerPolicy.RangerPolicyResource columnPolicyResource = new RangerPolicy.RangerPolicyResource(columnName);
				elements.put(RANGER_TYPE_HIVE_COLUMN, columnPolicyResource);
			} else {
				LOG.error("invalid qualifiedName for HIVE_COLUMN, qualifiedName=" + qualifiedName);
			}

		}


		ret = new RangerServiceResource();
		ret.setGuid(entity.getId()._getId());
		ret.setId(1L);
		ret.setServiceName(serviceName);
		ret.setResourceElements(elements);

		return ret;
	}

	static private Map<Long, RangerTag> getTags(EntityNotification entityNotification) {
		Map<Long, RangerTag> ret = null;

		ret = new HashMap<Long, RangerTag>();

		long index = 1;

		List<IStruct> traits = entityNotification.getAllTraits();

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

	static private String getQualifiedNameAttributeName(String entityTypeName) {
		String ret = StringUtils.equals(entityTypeName, ENTITY_TYPE_HIVE_TABLE) ?
				ENTITY_ATTRIBUTE_QUALIFIED_NAME_FOR_HIVE_TABLE : ENTITY_ATTRIBUTE_QUALIFIED_NAME;

		return ret;
	}

	static private List<String> getQualifiedNameComponents(IReferenceableInstance entity) throws Exception {

		List<String> ret = null;

		String qualifiedNameAttributeName = getQualifiedNameAttributeName(entity.getTypeName());

		String qualifiedName = getEntityAttribute(entity, qualifiedNameAttributeName, String.class);

		ret = getQualifiedNameComponents(entity.getTypeName(), qualifiedName);

		if (LOG.isDebugEnabled()) {
			LOG.debug("----- Entity-Id:" + entity.getId()._getId());
			LOG.debug("----- Entity-Type-Name:" + entity.getTypeName());
			LOG.debug("----- 	Entity-Components -----");
			int i = 0;
			for (String value : ret) {
				LOG.debug("-----		Index:" + i++ + "	Value:" + value);
			}
		}
		return ret;
	}

	static public List<String> getQualifiedNameComponents(String entityTypeName, String qualifiedName) throws Exception {

		List<String> ret = null;

		String qualifiedNameAttributeName = getQualifiedNameAttributeName(entityTypeName);

		if (StringUtils.isBlank(qualifiedName)) {
			throw new Exception("Could not get a valid value for " + qualifiedNameAttributeName + " attribute from entity notification.");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Received .... " + qualifiedNameAttributeName + "=" + qualifiedName + " for entity type " + entityTypeName);
		}

		String components[] = qualifiedName.split(QUALIFIED_NAME_FORMAT_CLUSTER_DELIMITER_STRING);

		if (components == null || components.length != 2) {
			throw new Exception("Qualified Name does not contain cluster-name, qualifiedName=" + qualifiedName);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("name-hierarchy=" + components[0] + ", cluster-name=" + components[1]);
		}

		String nameHierarchy[] = components[0].split(QUALIFIED_NAME_FORMAT_DELIMITER_STRING);

		int hierarchyLevels = nameHierarchy.length;

		ret = new ArrayList<String>();

		ret.add(qualifiedName);
		ret.add(components[1]);

		for (int i = 0; i < hierarchyLevels; i++) {
			ret.add(nameHierarchy[i]);
		}

		return ret;
	}

	static private String getServiceName(String clusterName, String entityTypeName) {
		// Parse entityTypeName to get the Apache-component Name
		// Assumption: entityTypeName is <componentName>_<component_specific_type_name>
		// such as hive_table, hadoop_path, hbase_queue, etc.
		String apacheComponents[] = entityTypeName.split("_");
		String apacheComponent = null;
		if (apacheComponents.length > 0) {
			apacheComponent = apacheComponents[0].toLowerCase();
		}

		return TagSyncConfig.getServiceName(apacheComponent, clusterName, properties);
	}

	static private <T> T getEntityAttribute(IReferenceableInstance entity, String name, Class<T> type) {
		T ret = null;

		try {
			Map<String, Object> valueMap = entity.getValuesMap();
			ret = getAttribute(valueMap, name, type);
		} catch (AtlasException exception) {
			LOG.error("Cannot get map of values for entity: " + entity.getId()._getId(), exception);
		}

		return ret;
	}
	static private <T> T getAttribute(Map<String, Object> map, String name, Class<T> type) {
		return type.cast(map.get(name));
	}

}
