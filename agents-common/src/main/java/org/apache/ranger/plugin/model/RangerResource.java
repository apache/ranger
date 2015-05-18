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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class represents a RangerResource including the service-type (such as hdfs, hive, etc.) in which it is supported.
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

public class RangerResource extends RangerBaseModelObject {
    private static final long serialVersionUID = 1L;

    private String serviceType                      = null; // one of any supported by any component
    private Map<String, RangerPolicy.RangerPolicyResource> resourceSpec        = null; //
    private String tagServiceName                   = null;
    private List<RangerResourceTag> tagsAndValues   = null;

    public RangerResource(String serviceType, Map<String, RangerPolicy.RangerPolicyResource> resourceSpecs, String tagServiceName, List<RangerResourceTag> tagsAndValues) {
        super();
        setServiceType(serviceType);
        setResourceSpecs(resourceSpecs);
        setTagServiceName(tagServiceName);
        setTagsAndValues(tagsAndValues);
    }

    public RangerResource() {
        this(null, null, null, null);
    }

    public String getServiceType() {
        return serviceType;
    }

    public Map<String, RangerPolicy.RangerPolicyResource> getResourceSpecs() {
        return resourceSpec;
    }

    public String getTagServiceName() {
        return tagServiceName;
    }

    public List<RangerResourceTag> getTagsAndValues() {
        return tagsAndValues;
    }

    // And corresponding set methods
    public void setServiceType(String serviceType) {
        this.serviceType = serviceType == null ? new String() : serviceType;
    }

    public void setResourceSpecs(Map<String, RangerPolicy.RangerPolicyResource> fullName) {
        this.resourceSpec = resourceSpec == null ? new HashMap<String, RangerPolicy.RangerPolicyResource>() : resourceSpec;
    }

    public void setTagServiceName(String tagServiceName) {
        this.tagServiceName = tagServiceName == null ? new String() : tagServiceName;
    }

    public void setTagsAndValues(List<RangerResourceTag> tagsAndValues) {
        this.tagsAndValues = tagsAndValues == null ? new ArrayList<RangerResourceTag>() : tagsAndValues;
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

        private String name             = null;
        private Map<String, Object> attributeValues  = null;   // Will be JSON string with (name, value) pairs of tag attributes in database

        public RangerResourceTag(String name, Map<String, Object> attributeValues) {
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

        public Map<String, Object> getAttributeValues() {
            return attributeValues;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setAttributeValues(Map<String, Object> attributeValues) {
            this.attributeValues = attributeValues;
        }
    }
}
