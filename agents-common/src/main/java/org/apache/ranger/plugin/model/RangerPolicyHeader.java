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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;

import java.util.HashMap;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerPolicyHeader extends RangerBaseModelObject implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String                            service;
    private String                            name;
    private Integer                           policyType;
    private String                            zoneName;
    private Map<String, RangerPolicyResource> resources;

    public RangerPolicyHeader(RangerPolicy rangerPolicy) {
        super();

        setId(rangerPolicy.getId());
        setGuid(rangerPolicy.getGuid());
        setName(rangerPolicy.getName());
        setResources(rangerPolicy.getResources());
        setIsEnabled(rangerPolicy.getIsEnabled());
        setService(rangerPolicy.getService());
        setVersion(rangerPolicy.getVersion());
        setPolicyType(rangerPolicy.getPolicyType());
        setZoneName(rangerPolicy.getZoneName());
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPolicyType() {
        return policyType;
    }

    public void setPolicyType(Integer policyType) {
        this.policyType = policyType;
    }

    public String getZoneName() {
        return zoneName;
    }

    public void setZoneName(String zoneName) {
        this.zoneName = zoneName;
    }

    public Map<String, RangerPolicyResource> getResources() {
        return resources;
    }

    public void setResources(Map<String, RangerPolicyResource> resources) {
        if (this.resources == null) {
            this.resources = new HashMap<>();
        }

        if (this.resources == resources) {
            return;
        }

        this.resources.clear();

        if (resources != null) {
            this.resources.putAll(resources);
        }
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("id={").append(getId()).append("} ");
        sb.append("guid={").append(getGuid()).append("} ");
        sb.append("name={").append(name).append("} ");
        sb.append("resources={");
        if (resources != null) {
            for (Map.Entry<String, RangerPolicyResource> e : resources.entrySet()) {
                sb.append(e.getKey()).append("={");
                e.getValue().toString(sb);
                sb.append("} ");
            }
        }
        sb.append("} ");
        sb.append("isEnabled={").append(getIsEnabled()).append("} ");
        sb.append("service={").append(service).append("} ");
        sb.append("version={").append(name).append("} ");
        sb.append("policyType={").append(policyType).append("} ");
        sb.append("zoneName={").append(zoneName).append("} ");

        return sb;
    }
}
