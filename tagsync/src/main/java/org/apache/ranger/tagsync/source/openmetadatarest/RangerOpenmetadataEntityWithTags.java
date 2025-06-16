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

package org.apache.ranger.tagsync.source.openmetadatarest;

import org.apache.commons.collections.CollectionUtils;
import org.openmetadata.client.model.TagLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RangerOpenmetadataEntityWithTags {
    private static final Logger LOG = LoggerFactory.getLogger(RangerOpenmetadataEntityWithTags.class);
	static private final String DEFAULT_TAG_ATTRIBUTE_TYPE = "string";
    private final RangerOpenmetadataEntity entity;
    private final List<TagLabel> tags;

    public RangerOpenmetadataEntityWithTags(RangerOpenmetadataEntity entity, List<TagLabel> allTagsForEntity) {
        this.entity       = entity;
        this.tags         = allTagsForEntity;
    }

    public RangerOpenmetadataEntity getEntity() {
        return entity;
    }

    public List<TagLabel> getTags() {
        return tags;
    }
    public String getTagAttributeType(String tagTypeName) {
        String ret = DEFAULT_TAG_ATTRIBUTE_TYPE;
        ret = tagTypeName.split(".")[1];
        return ret;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (entity != null && entity.getType()=="table") {
            sb.append("{entity=").append(entity).append(", ");

            sb.append("classifications={");

            if (CollectionUtils.isNotEmpty(tags)) {
                for (TagLabel tag : tags) {
                    sb.append("classificationName=").append(tag.getName());
                    sb.append(", attributes={}");
                    sb.append(", validityPeriods={}");
                    //Note: Openmetadata tags does not have attribute called validity periods or any timestamps
                }
            }
            sb.append("}");
            sb.append("}");
            sb.append("}");
        } else if (entity != null && entity.getType()=="column"){
            sb.append("{entity=").append(entity).append(", ");

            sb.append("classifications={");

            if (CollectionUtils.isNotEmpty(tags)) {
                for (TagLabel tag : tags) {
                    sb.append("classificationName=").append(tag.getName());
                    sb.append(", attributes={}");
                    sb.append(", validityPeriods={}");
                    //Note: Openmetadata tags does not have attribute called validity periods or any timestamps
                }
            }
            sb.append("}");
            sb.append("}");
            sb.append("}");
        }
        if (LOG.isDebugEnabled()){
            LOG.debug("==> Ranger Openmetadata entities with classifications string", sb);
        }
        return sb.toString();
    }

}