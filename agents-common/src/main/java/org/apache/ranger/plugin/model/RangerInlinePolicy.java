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

import java.util.List;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerInlinePolicy implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    public enum Mode {
        ENFORCING,    // default. access will be granted only when allowed by both Ranger policies and this inline policy
        COMPLEMENTARY // access will be granted when allowed by either Ranger policies or this inline policy
    }

    private String      grantor; // example: "r:role1"; when non-empty, access must be granted for this grantor as well
    private Mode        mode;
    private List<Grant> grants;

    public RangerInlinePolicy() {
        this.mode = Mode.ENFORCING;
    }

    public RangerInlinePolicy(String grantor, Mode mode, List<Grant> grants) {
        this.grantor = grantor;
        this.mode    = mode;
        this.grants  = grants;
    }

    public String getGrantor() {
        return grantor;
    }

    public void setGrantor(String grantor) {
        this.grantor = grantor;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public List<Grant> getGrants() {
        return grants;
    }

    public void setGrants(List<Grant> grants) {
        this.grants = grants;
    }

    public static class Grant {
        private List<String> principals;  // example: [ "u:user1, "g:group1", "r:role1" ]; if empty, means public grant
        private List<String> resources;   // example: [ "key:vol1/bucket1/db1/tbl1/*", "key:vol1/bucket1/db1/tbl2/*" ]; if empty, means all resources
        private List<String> permissions; // example: [ "read", "write" ]; if empty, means no permission

        public Grant() {
        }

        public Grant(List<String> principals, List<String> resources, List<String> permissions) {
            this.principals  = principals;
            this.resources   = resources;
            this.permissions = permissions;
        }

        public List<String> getPrincipals() {
            return principals;
        }

        public void setPrincipals(List<String> principals) {
            this.principals = principals;
        }

        public List<String> getResources() {
            return resources;
        }

        public void setResources(List<String> resources) {
            this.resources = resources;
        }

        public List<String> getPermissions() {
            return permissions;
        }

        public void setPermissions(List<String> permissions) {
            this.permissions = permissions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Grant that = (Grant) o;

            return Objects.equals(principals, that.principals) &&
                   Objects.equals(resources, that.resources) &&
                   Objects.equals(permissions, that.permissions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(principals, resources, permissions);
        }

        @Override
        public String toString() {
            return "Grant{" +
                    "principals=" + principals +
                    ", resources=" + resources +
                    ", permissions=" + permissions +
                    '}';
        }
    }
}
