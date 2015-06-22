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

package org.apache.ranger.plugin.model;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.*;

/**
 * This class represents a RangerTaggedResource including the service-type (such as hdfs, hive, etc.) in which it is supported.
 * This implies that there is one-to-one mapping between service-type and the resource-type which is a valid assumption.
 * Service-type must be one of service-types supported by Ranger.
 *
 * This class also contains a list of (tag-name, JSON-string-representing-tagattribute-tagattributevalue-pairs)
 *
 */

@JsonAutoDetect(getterVisibility= JsonAutoDetect.Visibility.NONE, setterVisibility= JsonAutoDetect.Visibility.NONE, fieldVisibility= JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)

public class RangerTaggedResource extends RangerBaseModelObject {
    private static final long serialVersionUID = 1L;

    private String componentType                                                = null; // one of any supported by any component
    private String tagServiceName                                               = null;
    private Map<String, RangerPolicy.RangerPolicyResource> resourceSpec         = null;
    private List<RangerResourceTag> tags                                        = null;

    public RangerTaggedResource(String componentType, String tagServiceName, Map<String, RangerPolicy.RangerPolicyResource> resourceSpec, List<RangerResourceTag> tags) {
        super();
        setComponentType(componentType);
        setTagServiceName(tagServiceName);
        setResourceSpec(resourceSpec);
        setTags(tags);
    }

    public RangerTaggedResource() {
        this(null, null, null, null);
    }

    public String getComponentType() {
        return componentType;
    }

    public String getTagServiceName() {
        return tagServiceName;
    }

    public Map<String, RangerPolicy.RangerPolicyResource> getResourceSpec() {
        return resourceSpec;
    }

    public List<RangerResourceTag> getTags() {
        return tags;
    }

    // And corresponding set methods
    public void setComponentType(String componentType) {
        this.componentType = componentType;
    }

    public void setTagServiceName(String tagServiceName) {
        this.tagServiceName = tagServiceName;
    }

    public void setResourceSpec(Map<String, RangerPolicy.RangerPolicyResource> resourceSpec) {
        this.resourceSpec = resourceSpec == null ? new HashMap<String, RangerPolicy.RangerPolicyResource>() : resourceSpec;
    }

    public void setTags(List<RangerResourceTag> tags) {
        this.tags = tags == null ? new ArrayList<RangerResourceTag>() : tags;
    }

    @Override
    public String toString( ) {
        StringBuilder sb = new StringBuilder();

        toString(sb);

        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {

        sb.append("{ ");

        sb.append("componentType={").append(componentType).append("} ");
        sb.append("tagServiceName={").append(tagServiceName).append("} ");

        sb.append("RangerTaggedResource={");
        if(resourceSpec != null) {
            for(Map.Entry<String, RangerPolicy.RangerPolicyResource> e : resourceSpec.entrySet()) {
                sb.append(e.getKey()).append("={");
                e.getValue().toString(sb);
                sb.append("} ");
            }
        }
        sb.append("} ");

        sb.append("Tags={");
        if (tags != null) {
            for (RangerResourceTag tag : tags) {
                sb.append("{");
                tag.toString(sb);
                sb.append("} ");
            }
        }
        sb.append("} ");

        sb.append(" }");

        return sb;
    }
    /**
     * Represents a tag and its attribute-values for a resource.
     */

    @JsonAutoDetect(getterVisibility= JsonAutoDetect.Visibility.NONE, setterVisibility= JsonAutoDetect.Visibility.NONE, fieldVisibility= JsonAutoDetect.Visibility.ANY)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)

    public static class RangerResourceTag implements java.io.Serializable {

        private String                  name                = null;
        private Map<String, String>     attributeValues     = null;

        public RangerResourceTag(String name, Map<String, String> attributeValues) {
            super();
            setName(name);
            setAttributeValues(attributeValues);
        }

        public RangerResourceTag() {
            this(null, null);
        }

        public String getName() {
            return name;
        }
        public void setName(String name) { this.name = name; }

        public Map<String, String> getAttributeValues() {
            return attributeValues;
        }
        public void setAttributeValues(Map<String, String> attributeValues) { this.attributeValues = attributeValues; }

        @Override
        public String toString( ) {
            StringBuilder sb = new StringBuilder();

            toString(sb);

            return sb.toString();
        }

        public StringBuilder toString(StringBuilder sb) {

            sb.append("{ ");

            sb.append("name={").append(name).append("} ");

            sb.append("attributeValues={");
            if(attributeValues != null) {
                for(Map.Entry<String, String> e : attributeValues.entrySet()) {
                    sb.append(e.getKey()).append("={");
                    sb.append(e.getValue());
                    sb.append("} ");
                }
            }
            sb.append("} ");

            sb.append(" }");

            return sb;
        }
    }
}
