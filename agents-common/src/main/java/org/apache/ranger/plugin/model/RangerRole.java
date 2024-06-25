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

import org.apache.commons.collections.MapUtils;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class RangerRole extends RangerBaseModelObject implements java.io.Serializable {
    public static final String KEY_USER = "user";
    public static final String KEY_GROUP = "group";

    private static final long serialVersionUID = 1L;
    private String                  name;
    private String                  description;
    private Map<String, Object>     options;
    private List<RoleMember>        users;
    private List<RoleMember>        groups;
    private List<RoleMember>        roles;
    private String createdByUser;

    public RangerRole() {
        this(null, null, null, null, null, null);
    }

    public RangerRole(String name, String description, Map<String, Object> options, List<RoleMember> users, List<RoleMember> groups) {
        this(name, description, options, users, groups, null);
    }

    public RangerRole(String name, String description, Map<String, Object> options, List<RoleMember> users, List<RoleMember> groups, List<RoleMember> roles) {
        setName(name);
        setDescription(description);
        setOptions(options);
        setUsers(users);
        setGroups(groups);
        setRoles(roles);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public void setOptions(Map<String, Object> options) {
        this.options = options == null ? new HashMap<>() : options;
    }

    public List<RoleMember> getUsers() {
        return users;
    }

    public void setUsers(List<RoleMember> users) {
        this.users = users == null ? new ArrayList<>() : users;
    }

    public List<RoleMember> getGroups() {
        return groups;
    }

    public void setGroups(List<RoleMember> groups) {
        this.groups = groups == null ? new ArrayList<>() : groups;
    }

    public List<RoleMember> getRoles() {
        return roles;
    }

    public void setRoles(List<RoleMember> roles) {
        this.roles = roles == null ? new ArrayList<>() : roles;
    }

    public String getCreatedByUser() {
        return createdByUser;
    }

    public void setCreatedByUser(String createdByUser) {
        this.createdByUser = createdByUser;
    }

    @Override
    public String toString() {
        return "{name=" + name
                + ", description=" + description
                + ", options=" + getPrintableOptions(options)
                + ", users=" + users
                + ", groups=" + groups
                + ", roles=" + roles
                + ", createdByUser=" + createdByUser
                + "}";
    }

    @JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class RoleMember implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String  name;
        private boolean isAdmin;

        public RoleMember() {
            this(null, false);
        }
        public RoleMember(String name, boolean isAdmin) {
            setName(name);
            setIsAdmin(isAdmin);
        }
        public void setName(String name) {
            this.name = name;
        }
        public void setIsAdmin(boolean isAdmin) {
            this.isAdmin = isAdmin;
        }
        public String getName() { return name; }
        public boolean getIsAdmin() { return isAdmin; }

        @Override
        public String toString() {
            return "{" + name + ", " + isAdmin + "}";
        }
        @Override
        public int hashCode() {
            return Objects.hash(name, isAdmin);
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            RoleMember other = (RoleMember) obj;
            return Objects.equals(name, other.name) && isAdmin == other.isAdmin;
        }
    }

    private String getPrintableOptions(Map<String, Object> options) {
        if (MapUtils.isEmpty(options)) return "{}";
        StringBuilder ret = new StringBuilder();
        ret.append("{");
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            ret.append(entry.getKey()).append(", ").append("[").append(entry.getValue()).append("]").append(",");
        }
        ret.append("}");
        return ret.toString();
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((description == null) ? 0 : description.hashCode());
		result = prime * result + ((options == null) ? 0 : options.hashCode());
		result = prime * result + ((users == null) ? 0 : users.hashCode());
		result = prime * result + ((groups == null) ? 0 : groups.hashCode());
		result = prime * result + ((roles == null) ? 0 : roles.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RangerRole other = (RangerRole) obj;
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (other.name == null || !name.equals(other.name)) {
			return false;
		}
		if (description == null) {
			if (other.description != null)
				return false;
		} else if (!description.equals(other.description))
			return false;
		if (options == null) {
			if (other.options != null)
				return false;
		} else if (!options.equals(other.options))
			return false;
		if (users == null) {
			if (other.users != null)
				return false;
		} else if (!users.equals(other.users))
			return false;
		if (groups == null) {
			if (other.groups != null)
				return false;
		} else if (!groups.equals(other.groups))
			return false;
		if (roles == null) {
			if (other.roles != null)
				return false;
		} else if (!roles.equals(other.roles))
			return false;
		return true;
	}
}
