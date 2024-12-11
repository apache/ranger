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

public class AtlasKafkaResourceMapper extends AtlasResourceMapper {
    public static final String ENTITY_TYPE_KAFKA_TOPIC = "kafka_topic";
    public static final String RANGER_TYPE_KAFKA_TOPIC = "topic";

    public static final String[] SUPPORTED_ENTITY_TYPES = {ENTITY_TYPE_KAFKA_TOPIC};

    public AtlasKafkaResourceMapper() {
        super("kafka", SUPPORTED_ENTITY_TYPES);
    }

    @Override
    public RangerServiceResource buildResource(final RangerAtlasEntity entity) throws Exception {
        String qualifiedName = (String) entity.getAttributes().get(AtlasResourceMapper.ENTITY_ATTRIBUTE_QUALIFIED_NAME);

        String topic = getResourceNameFromQualifiedName(qualifiedName);

        if (StringUtils.isEmpty(topic)) {
            throwExceptionWithMessage("topic not found in attribute '" + ENTITY_ATTRIBUTE_QUALIFIED_NAME + "'");
        }

        String clusterName = getClusterNameFromQualifiedName(qualifiedName);

        if (StringUtils.isEmpty(clusterName)) {
            clusterName = defaultClusterName;
        }

        if (StringUtils.isEmpty(clusterName)) {
            throwExceptionWithMessage("attribute '" + ENTITY_ATTRIBUTE_QUALIFIED_NAME + "' not found in entity");
        }

        Map<String, RangerPolicyResource> elements    = new HashMap<>();
        Boolean                           isExcludes  = Boolean.FALSE;
        Boolean                           isRecursive = Boolean.TRUE;

        elements.put(RANGER_TYPE_KAFKA_TOPIC, new RangerPolicyResource(topic, isExcludes, isRecursive));

        String entityGuid  = entity.getGuid();
        String serviceName = getRangerServiceName(clusterName);

        return new RangerServiceResource(entityGuid, serviceName, elements);
    }
}
