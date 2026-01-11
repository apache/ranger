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
public class RangerUserInfo {
    private String              name;
    private Map<String, Object> attributes;
    private Set<String>         groups;
    private Set<String>         roles;

    public RangerUserInfo() {
    }

    public RangerUserInfo(String name) {
        this(name, null, null, null);
    }

    public RangerUserInfo(String name, Map<String, Object> attributes, Set<String> groups, Set<String> roles) {
        this.name       = name;
        this.attributes = attributes;
        this.groups     = groups;
        this.roles      = roles;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public void setRoles(Set<String> roles) {
        this.roles = roles;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, attributes, groups, roles);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RangerUserInfo that = (RangerUserInfo) o;

        return Objects.equals(name, that.name) &&
                Objects.equals(attributes, that.attributes) &&
                Objects.equals(groups, that.groups) &&
                Objects.equals(roles, that.roles);
    }

    @Override
    public String toString() {
        return "RangerUserInfo{" +
                "name='" + name + '\'' +
                ", attributes=" + attributes +
                ", groups=" + groups +
                ", roles=" + roles +
                '}';
    }
}
