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

import java.util.HashMap;
import java.util.Map;

public class AtlasAdlsResourceMapper extends AtlasResourceMapper {
	public static final String ENTITY_TYPE_ADLS_GEN2_ACCOUNT   = "adls_gen2_account";
	public static final String ENTITY_TYPE_ADLS_GEN2_CONTAINER = "adls_gen2_container";
	public static final String ENTITY_TYPE_ADLS_GEN2_DIRECTORY = "adls_gen2_directory";

	public static final String RANGER_TYPE_ADLS_GEN2_ACCOUNT       = "storageaccount";
	public static final String RANGER_TYPE_ADLS_GEN2_CONTAINER     = "container";
	public static final String RANGER_TYPE_ADLS_GEN2_RELATIVE_PATH = "relativepath";

	public static final String[] SUPPORTED_ENTITY_TYPES = { ENTITY_TYPE_ADLS_GEN2_ACCOUNT, ENTITY_TYPE_ADLS_GEN2_CONTAINER, ENTITY_TYPE_ADLS_GEN2_DIRECTORY };

	private static final String SEP_PROTOCOL               = "://";
	private static final String SEP_CONTAINER              = "@";
	private static final String SEP_ACCOUNT                = ".";
	private static final String SEP_RELATIVE_PATH          = "/";
	private static final int    IDX_RESOURCE_ACCOUNT       = 0;
	private static final int    IDX_RESOURCE_CONTAINER     = 1;
	private static final int    IDX_RESOURCE_RELATIVE_PATH = 2;
	private static final int    IDX_CLUSTER_NAME           = 3;
	private static final int    RESOURCE_COUNT             = 4;


	public AtlasAdlsResourceMapper() {
		super("adls", SUPPORTED_ENTITY_TYPES);
	}

	@Override
	public RangerServiceResource buildResource(final RangerAtlasEntity entity) throws Exception {
		String qualifiedName = (String)entity.getAttributes().get(AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME);

		if (StringUtils.isEmpty(qualifiedName)) {
			throw new Exception("attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "' not found in entity");
		}

		String[] resources   = parseQualifiedName(qualifiedName);
		String   clusterName = resources[IDX_CLUSTER_NAME];
		String   accountName = resources[IDX_RESOURCE_ACCOUNT];

		if (StringUtils.isEmpty(clusterName)) {
			throwExceptionWithMessage("cluster-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
		}

		if (StringUtils.isEmpty(accountName)) {
			throwExceptionWithMessage("account-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
		}

		String entityType  = entity.getTypeName();
		String entityGuid  = entity.getGuid();
		String serviceName = getRangerServiceName(clusterName);

		Map<String, RangerPolicyResource> elements = new HashMap<String, RangerPolicyResource>();

		if (StringUtils.equals(entityType, ENTITY_TYPE_ADLS_GEN2_ACCOUNT)) {
			elements.put(RANGER_TYPE_ADLS_GEN2_ACCOUNT, new RangerPolicyResource(accountName));
		} else if (StringUtils.equals(entityType, ENTITY_TYPE_ADLS_GEN2_CONTAINER)) {
			String containerName = resources[IDX_RESOURCE_CONTAINER];

			if (StringUtils.isEmpty(containerName)) {
				throwExceptionWithMessage("container-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
			}

			elements.put(RANGER_TYPE_ADLS_GEN2_ACCOUNT, new RangerPolicyResource(accountName));
			elements.put(RANGER_TYPE_ADLS_GEN2_CONTAINER, new RangerPolicyResource(containerName));
		} else if (StringUtils.equals(entityType, ENTITY_TYPE_ADLS_GEN2_DIRECTORY)) {
			String containerName = resources[IDX_RESOURCE_CONTAINER];
			String relativePath  = resources[IDX_RESOURCE_RELATIVE_PATH];

			if (StringUtils.isEmpty(containerName)) {
				throwExceptionWithMessage("container-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
			}

			if (StringUtils.isEmpty(relativePath)) {
				throwExceptionWithMessage("relative-path not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
			}

			elements.put(RANGER_TYPE_ADLS_GEN2_ACCOUNT, new RangerPolicyResource(accountName));
			elements.put(RANGER_TYPE_ADLS_GEN2_CONTAINER, new RangerPolicyResource(containerName));
			elements.put(RANGER_TYPE_ADLS_GEN2_RELATIVE_PATH, new RangerPolicyResource(relativePath));
		} else {
			throwExceptionWithMessage("unrecognized entity-type: " + entityType);
		}

		RangerServiceResource ret = new RangerServiceResource(entityGuid, serviceName, elements);

		return ret;
	}

	/* qualifiedName can be of format, depending upon the entity-type:
	    adls_gen2_account:   abfs://<accountName>@<clusterName>
	    adls_gen2_container: abfs://<containerName>@<accountName>.dfs.core.windows.net@<clusterName>
	    adls_gen2_directory: abfs://<containerName>@<accountName>.dfs.core.windows.net/<relativePath>@<clusterName>
	 */
	private String[] parseQualifiedName(String qualifiedName) {
		String[] ret = new String[RESOURCE_COUNT];

		if(StringUtils.isNotBlank(qualifiedName)) {
			int idxClusterNameSep = qualifiedName.lastIndexOf(CLUSTER_DELIMITER);

			if (idxClusterNameSep != -1) {
				ret[IDX_CLUSTER_NAME] = qualifiedName.substring(idxClusterNameSep + CLUSTER_DELIMITER.length());
			}

			int idxProtocolStart = qualifiedName.indexOf(SEP_PROTOCOL);

			if (idxProtocolStart != -1) {
				int idxResourceStart = idxProtocolStart + SEP_PROTOCOL.length();
				int idxContainerSep  = qualifiedName.indexOf(SEP_CONTAINER, idxResourceStart);

				if (idxContainerSep != -1) {
					if (idxContainerSep == idxClusterNameSep) { // this is adls_gen2_account, so no containerName
						ret[IDX_RESOURCE_ACCOUNT] = qualifiedName.substring(idxResourceStart, idxContainerSep);
					} else {
						ret[IDX_RESOURCE_CONTAINER] = qualifiedName.substring(idxResourceStart, idxContainerSep);

						int idxAccountSep = qualifiedName.indexOf(SEP_ACCOUNT, idxContainerSep + SEP_CONTAINER.length());

						if (idxAccountSep != -1) {
							ret[IDX_RESOURCE_ACCOUNT] = qualifiedName.substring(idxContainerSep + SEP_CONTAINER.length(), idxAccountSep);

							int idxRelativePath = qualifiedName.indexOf(SEP_RELATIVE_PATH, idxAccountSep + SEP_ACCOUNT.length());

							if (idxRelativePath != -1) {
								if (idxClusterNameSep == -1) {
									ret[IDX_RESOURCE_RELATIVE_PATH] = qualifiedName.substring(idxRelativePath);
								} else {
									ret[IDX_RESOURCE_RELATIVE_PATH] = qualifiedName.substring(idxRelativePath, idxClusterNameSep);
								}
							}
						}
					}
				}
			}
		}

		return ret;
	}
}
