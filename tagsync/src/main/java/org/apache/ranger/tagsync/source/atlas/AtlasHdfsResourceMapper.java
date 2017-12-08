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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;

public class AtlasHdfsResourceMapper extends AtlasResourceMapper {
	public static final String ENTITY_TYPE_HDFS_PATH = "hdfs_path";
	public static final String RANGER_TYPE_HDFS_PATH = "path";

	public static final String ENTITY_ATTRIBUTE_PATH           = "path";
	public static final String ENTITY_ATTRIBUTE_CLUSTER_NAME   = "clusterName";

	public static final String[] SUPPORTED_ENTITY_TYPES = { ENTITY_TYPE_HDFS_PATH };


	public AtlasHdfsResourceMapper() {
		super("hdfs", SUPPORTED_ENTITY_TYPES);
	}

	@Override
	public String getRangerServiceName(String clusterName) {
		String ret = getCustomRangerServiceName(clusterName);

		if (StringUtils.isBlank(ret)) {
			ret = clusterName + TAGSYNC_DEFAULT_CLUSTERNAME_AND_COMPONENTNAME_SEPARATOR + "hadoop";
		}

		return ret;
	}

	@Override
	public RangerServiceResource buildResource(final RangerAtlasEntity entity) throws Exception {
		String path          = (String)entity.getAttributes().get(ENTITY_ATTRIBUTE_PATH);
		String clusterName   = (String)entity.getAttributes().get(ENTITY_ATTRIBUTE_CLUSTER_NAME);
		String qualifiedName = (String)entity.getAttributes().get(AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME);

		if(StringUtils.isEmpty(path)) {
			path = getResourceNameFromQualifiedName(qualifiedName);

			if(StringUtils.isEmpty(path)) {
				throwExceptionWithMessage("path not found in attribute '" + ENTITY_ATTRIBUTE_PATH + "' or '" + ENTITY_ATTRIBUTE_QUALIFIED_NAME +  "'");
			}
		}

		if(StringUtils.isEmpty(clusterName)) {
			clusterName = getClusterNameFromQualifiedName(qualifiedName);

			if(StringUtils.isEmpty(clusterName)) {
				clusterName = defaultClusterName;
			}

			if(StringUtils.isEmpty(clusterName)) {
				throwExceptionWithMessage("attributes " + ENTITY_ATTRIBUTE_CLUSTER_NAME + ", " + ENTITY_ATTRIBUTE_QUALIFIED_NAME +  "' not found in entity");
			}
		}

		String  entityGuid  = entity.getGuid();
		String  serviceName = getRangerServiceName(clusterName);
		Boolean isExcludes  = Boolean.FALSE;
		Boolean isRecursive = Boolean.TRUE;

		Path pathObj = new Path(path);

		Map<String, RangerPolicyResource> elements = new HashMap<String, RangerPolicy.RangerPolicyResource>();
		elements.put(RANGER_TYPE_HDFS_PATH, new RangerPolicyResource(pathObj.toUri().getPath(), isExcludes, isRecursive));

		RangerServiceResource ret = new RangerServiceResource(entityGuid, serviceName, elements);

		return ret;
	}
}
