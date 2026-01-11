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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE, fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerGrant implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private RangerPrincipal principal;
    private List<String>    accessTypes;
    private List<Condition> conditions;

    public RangerGrant() {
        this(null, null, null);
    }

    public RangerGrant(RangerPrincipal principal, List<String> accessTypes, List<Condition> conditions) {
        setPrincipal(principal);
        setAccessTypes(accessTypes);
        setConditions(conditions);
    }

    public RangerPrincipal getPrincipal() {
        return principal;
    }

    public void setPrincipal(RangerPrincipal principal) {
        this.principal = principal;
    }

    public List<String> getAccessTypes() {
        return accessTypes;
    }

    public void setAccessTypes(List<String> accessTypes) {
        if (this.accessTypes == null) {
            this.accessTypes = new ArrayList<>();
        }

        if (this.accessTypes == accessTypes) {
            return;
        }

        this.accessTypes.clear();

        if (accessTypes != null) {
            this.accessTypes.addAll(accessTypes);
        }
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(List<Condition> conditions) {
        if (this.conditions == null) {
            this.conditions = new ArrayList<>();
        }

        if (this.conditions == conditions) {
            return;
        }

        this.conditions.clear();

        if (conditions != null) {
            this.conditions.addAll(conditions);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, accessTypes, conditions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (getClass() != obj.getClass()) {
            return false;
        }

        RangerGrant other = (RangerGrant) obj;

        return Objects.equals(principal, other.principal) &&
                Objects.equals(accessTypes, other.accessTypes) &&
                Objects.equals(conditions, other.conditions);
    }

    @Override
    public String toString() {
        return "RangerGrant{" +
                "principal='" + principal.toString() +
                ", accessTypes=" + accessTypes +
                ", conditions=" + conditions +
                '}';
    }

    @JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE, fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Condition implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String       type;
        private List<String> values;

        public Condition() {
            this(null, null);
        }

        public Condition(String type, List<String> values) {
            setType(type);
            setValues(values);
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<String> getValues() {
            return values;
        }

        public void setValues(List<String> values) {
            if (this.values == null) {
                this.values = new ArrayList<>();
            }

            if (this.values == values) {
                return;
            }

            this.values.clear();

            if (values != null) {
                this.values.addAll(values);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Condition that = (Condition) o;
            return Objects.equals(type, that.type) && Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, values);
        }

        @Override
        public String toString() {
            return "Conditions{" +
                    "type='" + type + '\'' +
                    ", values=" + values +
                    '}';
        }
    }
}
