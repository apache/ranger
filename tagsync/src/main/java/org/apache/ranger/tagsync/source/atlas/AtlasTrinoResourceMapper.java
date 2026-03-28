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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;

import java.util.HashMap;
import java.util.Map;

public class AtlasTrinoResourceMapper extends AtlasResourceMapper {
    public static final String ENTITY_TYPE_TRINO_CATALOG  = "trino_catalog";
    public static final String ENTITY_TYPE_TRINO_SCHEMA   = "trino_schema";
    public static final String ENTITY_TYPE_TRINO_TABLE    = "trino_table";
    public static final String ENTITY_TYPE_TRINO_COLUMN   = "trino_column";
    public static final String RANGER_TYPE_TRINO_CATALOG  = "catalog";
    public static final String RANGER_TYPE_TRINO_SCHEMA   = "schema";
    public static final String RANGER_TYPE_TRINO_TABLE    = "table";
    public static final String RANGER_TYPE_TRINO_COLUMN   = "column";

    public static final String[] SUPPORTED_ENTITY_TYPES = {
            ENTITY_TYPE_TRINO_CATALOG,
            ENTITY_TYPE_TRINO_SCHEMA,
            ENTITY_TYPE_TRINO_TABLE,
            ENTITY_TYPE_TRINO_COLUMN
    };

    public AtlasTrinoResourceMapper() {
        super("trino", SUPPORTED_ENTITY_TYPES);
    }

    /*
     * qualifiedName can be of format, depending upon the entity-type:
     * trino_instance: <instanceName>
     * trino_catalog:  <catalog>@<instanceName>
     * trino_schema:   <catalog>.<schema>@<instanceName>
     * trino_table:    <catalog>.<schema>.<table>@<instanceName>
     * trino_column:   <catalog>.<schema>.<table>.<column>@<instanceName>
     */
    @Override
    public RangerServiceResource buildResource(RangerAtlasEntity entity) throws Exception {
        String qualifiedName = (String) entity.getAttributes().get(AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME);

        if (StringUtils.isEmpty(qualifiedName)) {
            throw new Exception("attribute '" + ENTITY_ATTRIBUTE_QUALIFIED_NAME + "' not found in entity");
        }

        String entityType  = entity.getTypeName();
        String entityGuid  = entity.getGuid();
        String resourceStr = getResourceNameFromQualifiedName(qualifiedName);

        if (StringUtils.isEmpty(resourceStr)) {
            throwExceptionWithMessage("resource not found in attribute '" + ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
        }

        String trinoInstance = getClusterNameFromQualifiedName(qualifiedName);
        if (StringUtils.equals(resourceStr, qualifiedName)) {
            trinoInstance = resourceStr;
        }

        if (StringUtils.isEmpty(trinoInstance)) {
            throwExceptionWithMessage("trino-instance not found in attribute '" + ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
        }
        String serviceName = getRangerServiceName(trinoInstance);

        String[] parts = resourceStr.split(QUALIFIED_NAME_DELIMITER);
        if (parts.length < 1 || parts.length > 4) {
            throwExceptionWithMessage("invalid resource format in attribute '" + ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
        }

        Map<String, RangerPolicyResource> elements = new HashMap<>();
        if (StringUtils.equals(entityType, ENTITY_TYPE_TRINO_CATALOG)) {
            if (parts.length == 1 && StringUtils.isNotEmpty(parts[0])) {
                elements.put(RANGER_TYPE_TRINO_CATALOG, new RangerPolicyResource(parts[0]));
            }
        } else if (StringUtils.equals(entityType, ENTITY_TYPE_TRINO_SCHEMA)) {
            if (parts.length == 2 && StringUtils.isNotEmpty(parts[0]) && StringUtils.isNotEmpty(parts[1])) {
                elements.put(RANGER_TYPE_TRINO_CATALOG, new RangerPolicyResource(parts[0]));
                elements.put(RANGER_TYPE_TRINO_SCHEMA, new RangerPolicyResource(parts[1]));
            }
        } else if (StringUtils.equals(entityType, ENTITY_TYPE_TRINO_TABLE)) {
            if (parts.length == 3 && StringUtils.isNotEmpty(parts[0]) && StringUtils.isNotEmpty(parts[1]) && StringUtils.isNotEmpty(parts[2])) {
                elements.put(RANGER_TYPE_TRINO_CATALOG, new RangerPolicyResource(parts[0]));
                elements.put(RANGER_TYPE_TRINO_SCHEMA, new RangerPolicyResource(parts[1]));
                elements.put(RANGER_TYPE_TRINO_TABLE, new RangerPolicyResource(parts[2]));
            }
        } else if (StringUtils.equals(entityType, ENTITY_TYPE_TRINO_COLUMN)) {
            if (parts.length == 4 && StringUtils.isNotEmpty(parts[0]) && StringUtils.isNotEmpty(parts[1]) && StringUtils.isNotEmpty(parts[2]) && StringUtils.isNotEmpty(parts[3])) {
                elements.put(RANGER_TYPE_TRINO_CATALOG, new RangerPolicyResource(parts[0]));
                elements.put(RANGER_TYPE_TRINO_SCHEMA, new RangerPolicyResource(parts[1]));
                elements.put(RANGER_TYPE_TRINO_TABLE, new RangerPolicyResource(parts[2]));
                elements.put(RANGER_TYPE_TRINO_COLUMN, new RangerPolicyResource(parts[3]));
            }
        } else {
            throwExceptionWithMessage("unrecognized entity-type: " + entityType);
        }

        if (elements.isEmpty()) {
            throwExceptionWithMessage("invalid qualifiedName for entity-type '" + entityType + "': " + qualifiedName);
        }

        return new RangerServiceResource(entityGuid, serviceName, elements);
    }
}
