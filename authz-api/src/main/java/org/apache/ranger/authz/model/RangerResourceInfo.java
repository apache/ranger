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

package org.apache.ranger.authz.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerResourceInfo {
    private String              name;
    private Set<String>         subResources;
    private Map<String, Object> attributes;

    public RangerResourceInfo() {
    }

    public RangerResourceInfo(String name, Set<String> subResources, Map<String, Object> attributes) {
        this.name         = name;
        this.subResources = subResources;
        this.attributes   = attributes;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<String> getSubResources() {
        return subResources;
    }

    public void setSubResources(Set<String> subResources) {
        this.subResources = subResources;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, subResources, attributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RangerResourceInfo that = (RangerResourceInfo) o;

        return Objects.equals(name, that.name) &&
                Objects.equals(subResources, that.subResources) &&
                Objects.equals(attributes, that.attributes);
    }

    @Override
    public String toString() {
        return "RangerResourceInfo{" +
                "name='" + name + '\'' +
                ", subResources=" + subResources +
                ", attributes=" + attributes +
                '}';
    }
}
