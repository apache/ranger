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
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerServiceResource;

import java.util.Properties;
import java.util.List;
import java.util.Map;

public abstract class AtlasResourceMapper {
	private static final Log LOG = LogFactory.getLog(AtlasResourceMapper.class);

	protected static final String TAGSYNC_SERVICENAME_MAPPER_PROP_PREFIX = "ranger.tagsync.atlas.";

	protected static final String TAGSYNC_SERVICENAME_MAPPER_PROP_SUFFIX = ".ranger.service";

	protected static final String TAGSYNC_ATLAS_CLUSTER_IDENTIFIER = ".instance.";
	protected Properties properties;

	public AtlasResourceMapper() {
	}

	public void initialize(Properties properties) {
		this.properties = properties;
	}

	abstract public List<String> getSupportedEntityTypes();

	abstract public RangerServiceResource buildResource(final IReferenceableInstance entity) throws Exception;


	protected String getRangerServiceName(String componentName, String atlasInstanceName) {
		String propName = TAGSYNC_SERVICENAME_MAPPER_PROP_PREFIX + componentName
				+ TAGSYNC_ATLAS_CLUSTER_IDENTIFIER + atlasInstanceName
				+ TAGSYNC_SERVICENAME_MAPPER_PROP_SUFFIX;

		return properties.getProperty(propName);
	}

	static protected <T> T getEntityAttribute(IReferenceableInstance entity, String name, Class<T> type) {
		T ret = null;

		try {
			Map<String, Object> valueMap = entity.getValuesMap();
			ret = getAttribute(valueMap, name, type);
		} catch (AtlasException exception) {
			LOG.error("Cannot get map of values for entity: " + entity.getId()._getId(), exception);
		}

		return ret;
	}

	static protected <T> T getAttribute(Map<String, Object> map, String name, Class<T> type) {
		return type.cast(map.get(name));
	}
}
