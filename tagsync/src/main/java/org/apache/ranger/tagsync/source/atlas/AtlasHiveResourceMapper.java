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

import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceResource;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class AtlasHiveResourceMapper extends AtlasResourceMapper {
	private static final Log LOG = LogFactory.getLog(AtlasHiveResourceMapper.class);

	public static final String COMPONENT_NAME = "hive";

	public static final String ENTITY_TYPE_HIVE_DB = "hive_db";
	public static final String ENTITY_TYPE_HIVE_TABLE = "hive_table";
	public static final String ENTITY_TYPE_HIVE_COLUMN = "hive_column";

	public static final String RANGER_TYPE_HIVE_DB = "database";
	public static final String RANGER_TYPE_HIVE_TABLE = "table";
	public static final String RANGER_TYPE_HIVE_COLUMN = "column";

	public static final String ENTITY_ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";

	public static final String TAGSYNC_DEFAULT_CLUSTERNAME_AND_COMPONENTNAME_SEPARATOR = "_";

	public static final String clusterDelimiter = "@";

	public static final String qualifiedNameDelimiter = "\\.";

	public static final String[] supportedEntityTypes = { ENTITY_TYPE_HIVE_DB, ENTITY_TYPE_HIVE_TABLE, ENTITY_TYPE_HIVE_COLUMN };

	public AtlasHiveResourceMapper() {
		super();
	}

	@Override
	public List<String> getSupportedEntityTypes() {
		return Arrays.asList(supportedEntityTypes);
	}

	@Override
	public RangerServiceResource buildResource(final IReferenceableInstance entity) throws Exception {

		Map<String, RangerPolicy.RangerPolicyResource> elements = new HashMap<String, RangerPolicy.RangerPolicyResource>();

		String serviceName = null;

		List<String> components = getQualifiedNameComponents(entity);
		// components should contain qualifiedName, clusterName, dbName, tableName, columnName in that order

		String entityTypeName = entity.getTypeName();

		String qualifiedName = components.get(0);

		String clusterName, dbName, tableName, columnName;

		if (components.size() > 1) {
			clusterName = components.get(1);
			serviceName = getRangerServiceName(clusterName);
		}

		if (StringUtils.equals(entityTypeName, ENTITY_TYPE_HIVE_DB)) {
			if (components.size() > 2) {
				dbName = components.get(2);
				RangerPolicy.RangerPolicyResource dbPolicyResource = new RangerPolicy.RangerPolicyResource(dbName);
				elements.put(RANGER_TYPE_HIVE_DB, dbPolicyResource);

			} else {
				LOG.error("invalid qualifiedName for HIVE_DB, qualifiedName=" + qualifiedName);
				throw new Exception("invalid qualifiedName for HIVE_DB, qualifiedName=" + qualifiedName);
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
				throw new Exception("invalid qualifiedName for HIVE_TABLE, qualifiedName=" + qualifiedName);
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
				throw new Exception("invalid qualifiedName for HIVE_COLUMN, qualifiedName=" + qualifiedName);
			}

		}

		RangerServiceResource ret = new RangerServiceResource();

		ret.setGuid(entity.getId()._getId());
		ret.setServiceName(serviceName);
		ret.setResourceElements(elements);

		return ret;
	}

	public String getRangerServiceName(String clusterName) {
		String ret = getRangerServiceName(COMPONENT_NAME, clusterName);

		if (StringUtils.isBlank(ret)) {
			ret = clusterName + TAGSYNC_DEFAULT_CLUSTERNAME_AND_COMPONENTNAME_SEPARATOR + COMPONENT_NAME;
		}
		return ret;
	}

	public final List<String> getQualifiedNameComponents(IReferenceableInstance entity) throws Exception {

		String qualifiedNameAttributeName = ENTITY_ATTRIBUTE_QUALIFIED_NAME;
		String qualifiedName = getEntityAttribute(entity, qualifiedNameAttributeName, String.class);

		if (StringUtils.isBlank(qualifiedName)) {
			throw new Exception("Could not get a valid value for " + qualifiedNameAttributeName + " attribute from entity.");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Received .... " + qualifiedNameAttributeName + "=" + qualifiedName + " for entity type " + entity.getTypeName());
		}

		List<String> ret = getQualifiedNameComponents(entity.getTypeName(), qualifiedName);

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

	public final List<String> getQualifiedNameComponents(String entityTypeName, String qualifiedName) throws Exception {

		String components[] = qualifiedName.split(clusterDelimiter);

		if (components.length != 2) {
			throw new Exception("Qualified Name does not contain cluster-name, qualifiedName=" + qualifiedName);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("name-hierarchy=" + components[0] + ", cluster-name=" + components[1]);
		}

		String nameHierarchy[] = components[0].split(qualifiedNameDelimiter);

		List<String> ret = new ArrayList<String>();

		ret.add(qualifiedName);
		ret.add(components[1]);

		ret.addAll(Arrays.asList(nameHierarchy));

		return ret;
	}
}
