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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AtlasOzoneResourceMapper extends AtlasResourceMapper {
	private static final Logger LOG = LoggerFactory.getLogger(AtlasOzoneResourceMapper.class);

	public static final String ENTITY_TYPE_OZONE_VOLUME  = "ozone_volume";
	public static final String ENTITY_TYPE_OZONE_BUCKET  = "ozone_bucket";
	public static final String ENTITY_TYPE_OZONE_KEY 	 = "ozone_key";

	public static final String RANGER_TYPE_OZONE_VOLUME  = "volume";
	public static final String RANGER_TYPE_OZONE_BUCKET  = "bucket";
	public static final String RANGER_TYPE_OZONE_KEY 	 = "key";

	public static final String[] SUPPORTED_ENTITY_TYPES = { ENTITY_TYPE_OZONE_VOLUME, ENTITY_TYPE_OZONE_BUCKET, ENTITY_TYPE_OZONE_KEY };

	private static final String SEP_PROTOCOL               = "://";
	private static final String SEP_RELATIVE_PATH          = "/";
	private static final int    IDX_VOLUME       		   = 0;
	private static final int    IDX_BUCKET     			   = 1;
	private static final int    IDX_KEY 				   = 2;
	private static final int    IDX_CLUSTER_NAME           = 3;
	private static final int    RESOURCE_COUNT             = 4;

	// This flag results in ofs atlas qualifiedName to parse paths similar to o3fs
	public static final String PROP_LEGACY_PARSING       			= "ranger.tagsync.atlas.ozone.legacy.parsing.enabled";
	public static final String PROP_OFS_KEY_DELIMITER    			= "ranger.tagsync.atlas.ozone.ofs.key_entity.separator";
	public static final String PROP_OFS_BUCKET_DELIMITER 			= "ranger.tagsync.atlas.ozone.ofs.bucket_entity.separator";

	public static final String PROP_OFS_KEY_RECURSIVE_ENABLED  = "ranger.tagsync.atlas.ozone.ofs.key.is.recursive.enabled";
	public static final String PROP_O3FS_KEY_RECURSIVE_ENABLED = "ranger.tagsync.atlas.ozone.o3fs.key.is.recursive.enabled";

	private String ofsKeyDelimiter            = "/";
	private String ofsBucketDelimiter         = "\\.";
	private boolean legacyParsingEnabled 			= false;
	// keeping it true for ofs since it is new support from tagsync
	private boolean isRecursiveEnabledOFSKey  = true;
	// Setting to true by default. Causes behavior change for customer with existing deployments. Configurable if required otherwise
	private boolean isRecursiveEnabledO3FSKey = true;

	public AtlasOzoneResourceMapper() {
		super("ozone", SUPPORTED_ENTITY_TYPES);
	}
	@Override
	public void initialize(Properties properties) {
		super.initialize(properties);

		if (this.properties != null) {
			this.legacyParsingEnabled = Boolean.parseBoolean((String) this.properties.getOrDefault(PROP_LEGACY_PARSING, Boolean.toString(legacyParsingEnabled)));
			this.ofsKeyDelimiter      = (String) this.properties.getOrDefault(PROP_OFS_KEY_DELIMITER, this.ofsKeyDelimiter);
			this.ofsBucketDelimiter   = (String) this.properties.getOrDefault(PROP_OFS_BUCKET_DELIMITER, this.ofsBucketDelimiter);
			this.isRecursiveEnabledOFSKey = Boolean.parseBoolean((String) this.properties.getOrDefault(PROP_OFS_KEY_RECURSIVE_ENABLED, Boolean.toString(isRecursiveEnabledOFSKey)));
			this.isRecursiveEnabledO3FSKey = Boolean.parseBoolean((String) this.properties.getOrDefault(PROP_O3FS_KEY_RECURSIVE_ENABLED, Boolean.toString(isRecursiveEnabledO3FSKey)));
		}

		LOG.info("ofsKeyDelimiter={}", this.ofsKeyDelimiter);
		LOG.info("ofsBucketDelimiter={}", this.ofsBucketDelimiter);
		LOG.info("legacyParsingEnabled={}",this.legacyParsingEnabled);
	}
	@Override
	public RangerServiceResource buildResource(final RangerAtlasEntity entity) throws Exception {
		String qualifiedName = (String)entity.getAttributes().get(AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME);

		if (StringUtils.isEmpty(qualifiedName)) {
			throw new Exception("attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "' not found in entity");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("ENTITY_ATTRIBUTE_QUALIFIED_NAME = " + qualifiedName);
		}

		String   entityType  = entity.getTypeName();
		String   entityGuid  = entity.getGuid();
		String[] resources   = parseQualifiedName(qualifiedName, entityType);
		String   volName     = resources[IDX_VOLUME];
		String   bktName     = resources[IDX_BUCKET];
		String   keyName     = resources[IDX_KEY];
		String   clusterName = resources[IDX_CLUSTER_NAME];

		if (LOG.isDebugEnabled()) {
			LOG.debug("Ozone resources for entityType " + entityType + " are " + Arrays.toString(resources));
		}

		if (StringUtils.isEmpty(clusterName)) {
			throwExceptionWithMessage("cluster-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
		}

		String                            serviceName = getRangerServiceName(clusterName);
		Map<String, RangerPolicyResource> elements    = new HashMap<>();

		if (StringUtils.equals(entityType, ENTITY_TYPE_OZONE_VOLUME)) {
			if (StringUtils.isEmpty(volName)) {
				throwExceptionWithMessage("volume-name not found in attribute '" + ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
			}

			elements.put(RANGER_TYPE_OZONE_VOLUME, new RangerPolicyResource(volName));
		} else if (StringUtils.equals(entityType, ENTITY_TYPE_OZONE_BUCKET)) {
			if (StringUtils.isEmpty(volName)) {
				throwExceptionWithMessage("volume-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
			} else if (StringUtils.isEmpty(bktName)) {
				throwExceptionWithMessage("bucket-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
			}

			elements.put(RANGER_TYPE_OZONE_VOLUME, new RangerPolicyResource(volName));
			elements.put(RANGER_TYPE_OZONE_BUCKET, new RangerPolicyResource(bktName));
		} else if (StringUtils.equals(entityType, ENTITY_TYPE_OZONE_KEY)) {
			if (StringUtils.isEmpty(volName)) {
				throwExceptionWithMessage("volume-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
			} else if (StringUtils.isEmpty(bktName)) {
				throwExceptionWithMessage("bucket-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
			} else if (StringUtils.isEmpty(keyName)) {
				throwExceptionWithMessage("key-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
			}
			boolean isRecursive = isRecursiveEnabledO3FSKey;
			if (qualifiedName.startsWith("ofs://")){
				isRecursive = isRecursiveEnabledOFSKey;
			}
			elements.put(RANGER_TYPE_OZONE_VOLUME, new RangerPolicyResource(volName));
			elements.put(RANGER_TYPE_OZONE_BUCKET, new RangerPolicyResource(bktName));
			elements.put(RANGER_TYPE_OZONE_KEY, new RangerPolicyResource(keyName, false, isRecursive));
		} else {
			throwExceptionWithMessage("unrecognized entity-type: " + entityType);
		}

		if (elements.isEmpty()) {
			throwExceptionWithMessage("invalid qualifiedName for entity-type '" + entityType + "': " + qualifiedName);
		}

		RangerServiceResource ret = new RangerServiceResource(entityGuid, serviceName, elements);

		return ret;
	}

	/* qualifiedName can be of format, depending upon the entity-type:
	 * o3fs://<volume name>@cm (ozone_key)
	 * o3fs://<volume name>.<bucket name>@<clusterName> (ozone_bucket)
	 * o3fs://<bucket name>.<volume name>.<ozone service id>/<key path>@<clusterName> (ozone_key)
	 * ofs://myvolume@cl1
	 * ofs://myvolume.mybucket@cl1
	 * ofs://ozone1/myvolume/mybucket/key1@cl1
	 * ofs://ozone1/myvolume/mybucket/mykey/key1/@cl1
	 */
	private String[] parseQualifiedName(String qualifiedName, String entityType) {
		int    idxProtocolSep = qualifiedName.indexOf(SEP_PROTOCOL);
		String prefix         = idxProtocolSep != -1 ? qualifiedName.substring(0, idxProtocolSep) : "";

		if (LOG.isDebugEnabled()) {
			LOG.debug("Prefix for qualifiedName={} is {}", qualifiedName, prefix);
		}

		if (this.legacyParsingEnabled){
			return parseQualifiedNameO3FS(qualifiedName, entityType);
		} else if (prefix.equals("ofs")) {
			return parseQualifiedNameOFS(qualifiedName, entityType);
		} else {
			return parseQualifiedNameO3FS(qualifiedName, entityType);
		}
	}

	private String[] parseQualifiedNameOFS(String qualifiedName, String entityType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> parseQualifiedNameOFS(qualifiedName={}, entityType={})", qualifiedName, entityType);
		}

		String[] ret = new String[RESOURCE_COUNT];

		if(StringUtils.isNotBlank(qualifiedName)) {
			int idxClusterNameSep = qualifiedName.lastIndexOf(CLUSTER_DELIMITER);

			if (idxClusterNameSep != -1) {
				ret[IDX_CLUSTER_NAME] = qualifiedName.substring(idxClusterNameSep + CLUSTER_DELIMITER.length());

				int idxProtocolSep = qualifiedName.indexOf(SEP_PROTOCOL);

				if (idxProtocolSep != -1) {
					int idxResourceStart = idxProtocolSep + SEP_PROTOCOL.length();

					if (StringUtils.equals(entityType, ENTITY_TYPE_OZONE_VOLUME)) { // ofs://vol1@cl1
						ret[IDX_VOLUME] = qualifiedName.substring(idxResourceStart, idxClusterNameSep);
					} else if (StringUtils.equals(entityType, ENTITY_TYPE_OZONE_BUCKET)) { // ofs://vol1.buck1@cl1
						// anything before first "." is volume name, after that is bucket name. So, "." in volume name is invalid when tagging buckets
						String[] resources = qualifiedName.substring(idxResourceStart, idxClusterNameSep).split(this.ofsBucketDelimiter,2);

						ret[IDX_VOLUME] = resources.length > 0 ? resources[0] : null;
						ret[IDX_BUCKET] = resources.length > 1 ? resources[1] : null;
					} else if (StringUtils.equals(entityType, ENTITY_TYPE_OZONE_KEY)) { // ofs://svcid/vol1/buck1/d1/d2/key1@cl1
						// This is a special case wherein the delimiter is a "/" instead of a "." in the qualifiedName in ofs path
						idxResourceStart = qualifiedName.indexOf(this.ofsKeyDelimiter, idxProtocolSep + SEP_PROTOCOL.length()) + 1;

						String   resourceString = qualifiedName.substring(idxResourceStart, idxClusterNameSep);
						String[] resources      = resourceString.split(this.ofsKeyDelimiter, 3);

						ret[IDX_VOLUME] = resources.length > 0 ? resources[0] : null;
						ret[IDX_BUCKET] = resources.length > 1 ? resources[1] : null;
						ret[IDX_KEY]    = resources.length > 2 ? resources[2] : null;
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== parseQualifiedNameOFS(qualifiedName={}, entityType={}): volume={}, bucket={}, key={}, clusterName={}", qualifiedName, entityType, ret[IDX_VOLUME], ret[IDX_BUCKET], ret[IDX_KEY], ret[IDX_CLUSTER_NAME]);
		}

		return ret;
	}

	private String[] parseQualifiedNameO3FS(String qualifiedName, String entityType){
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> parseQualifiedNameO3FS(qualifiedName={}, entityType={})", qualifiedName, entityType);
		}

		String[] ret = new String[RESOURCE_COUNT];

		if(StringUtils.isNotBlank(qualifiedName)) {
			int idxClusterNameSep = qualifiedName.lastIndexOf(CLUSTER_DELIMITER);

			if (idxClusterNameSep != -1) {
				ret[IDX_CLUSTER_NAME] = qualifiedName.substring(idxClusterNameSep + CLUSTER_DELIMITER.length());

				int idxProtocolSep = qualifiedName.indexOf(SEP_PROTOCOL);

				if (idxProtocolSep != -1) {
					int idxResourceStart = idxProtocolSep + SEP_PROTOCOL.length();

					if (StringUtils.equals(entityType, ENTITY_TYPE_OZONE_VOLUME)) { // o3fs://vol1@cl1
						ret[IDX_VOLUME] = qualifiedName.substring(idxResourceStart, idxClusterNameSep);
					} else if (StringUtils.equals(entityType, ENTITY_TYPE_OZONE_BUCKET)) { // o3fs://vol1.buck1@cl1
						String[] resources = qualifiedName.substring(idxResourceStart, idxClusterNameSep).split(QUALIFIED_NAME_DELIMITER);

						ret[IDX_VOLUME] = resources.length > 0 ? resources[0] : null;
						ret[IDX_BUCKET] = resources.length > 1 ? resources[1] : null;
					} else if (StringUtils.equals(entityType, ENTITY_TYPE_OZONE_KEY)) { // o3fs://buck1.vol1.svc1/d1/d2/key1@cl1
						String[] resources = qualifiedName.substring(idxResourceStart, idxClusterNameSep).split(QUALIFIED_NAME_DELIMITER, 3);

						ret[IDX_BUCKET] = resources.length > 0 ? resources[0] : null;
						ret[IDX_VOLUME] = resources.length > 1 ? resources[1] : null;
						ret[IDX_KEY]    = resources.length > 2 ? resources[2] : null;

						if (ret[IDX_KEY] != null) { // skip svcid
							int idxKeySep = ret[IDX_KEY].indexOf(SEP_RELATIVE_PATH);

							if (idxKeySep != -1) {
								ret[IDX_KEY] = ret[IDX_KEY].substring(idxKeySep + SEP_RELATIVE_PATH.length());
							} else {
								ret[IDX_KEY] = null;
							}
						}
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== parseQualifiedNameO3FS(qualifiedName={}, entityType={}): volume={}, bucket={}, key={}, clusterName={}", qualifiedName, entityType, ret[IDX_VOLUME], ret[IDX_BUCKET], ret[IDX_KEY], ret[IDX_CLUSTER_NAME]);
		}

		return ret;
	}
}
