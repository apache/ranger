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

package org.apache.ranger.plugin.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;


@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class GrantRevokeRequest implements Serializable {
	private static final long serialVersionUID = 1L;

	private String              grantor                    = null;
	private Map<String, String> resource                   = null;
	private Set<String>         users                      = null;
	private Set<String>         groups                     = null;
	private Set<String>         accessTypes                = null;
	private Boolean             delegateAdmin              = Boolean.FALSE;
	private Boolean             enableAudit                = Boolean.TRUE;
	private Boolean             replaceExistingPermissions = Boolean.FALSE;


	public GrantRevokeRequest() {
		this(null, null, null, null, null, null, null, null);
	}

	public GrantRevokeRequest(String grantor, Map<String, String> resource, Set<String> users, Set<String> groups, Set<String> accessTypes, Boolean delegateAdmin, Boolean enableAudit, Boolean replaceExistingPermissions) {
		setGrantor(grantor);
		setResource(resource);
		setUsers(users);
		setGroups(groups);
		setAccessTypes(accessTypes);
		setDelegateAdmin(delegateAdmin);
		setAccessTypes(accessTypes);
		setEnableAudit(enableAudit);
		setReplaceExistingPermissions(replaceExistingPermissions);
	}

	/**
	 * @return the grantor
	 */
	public String getGrantor() {
		return grantor;
	}

	/**
	 * @param grantor the grantor to set
	 */
	public void setGrantor(String grantor) {
		this.grantor = grantor;
	}

	/**
	 * @return the resource
	 */
	public Map<String, String> getResource() {
		return resource;
	}

	/**
	 * @param resource the resource to set
	 */
	public void setResource(Map<String, String> resource) {
		this.resource = resource == null ? new HashMap<String, String>() : resource;
	}

	/**
	 * @return the users
	 */
	public Set<String> getUsers() {
		return users;
	}

	/**
	 * @param users the users to set
	 */
	public void setUsers(Set<String> users) {
		this.users = users == null ? new HashSet<String>() : users;
	}

	/**
	 * @return the groups
	 */
	public Set<String> getGroups() {
		return groups;
	}

	/**
	 * @param groups the groups to set
	 */
	public void setGroups(Set<String> groups) {
		this.groups = groups == null ? new HashSet<String>() : groups;
	}

	/**
	 * @return the accessTypes
	 */
	public Set<String> getAccessTypes() {
		return accessTypes;
	}

	/**
	 * @param accessTypes the accessTypes to set
	 */
	public void setAccessTypes(Set<String> accessTypes) {
		this.accessTypes = accessTypes == null ? new HashSet<String>() : groups;
	}

	/**
	 * @return the delegateAdmin
	 */
	public Boolean getDelegateAdmin() {
		return delegateAdmin;
	}

	/**
	 * @param delegateAdmin the delegateAdmin to set
	 */
	public void setDelegateAdmin(Boolean delegateAdmin) {
		this.delegateAdmin = delegateAdmin == null ? Boolean.FALSE : delegateAdmin;
	}

	/**
	 * @return the enableAudit
	 */
	public Boolean getEnableAudit() {
		return enableAudit;
	}

	/**
	 * @param enableAudit the enableAudit to set
	 */
	public void setEnableAudit(Boolean enableAudit) {
		this.enableAudit = enableAudit == null ? Boolean.TRUE : enableAudit;
	}

	/**
	 * @return the replaceExistingPermissions
	 */
	public Boolean getReplaceExistingPermissions() {
		return replaceExistingPermissions;
	}

	/**
	 * @param replaceExistingPermissions the replaceExistingPermissions to set
	 */
	public void setReplaceExistingPermissions(Boolean replaceExistingPermissions) {
		this.replaceExistingPermissions = replaceExistingPermissions == null ? Boolean.FALSE : replaceExistingPermissions;
	}


	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("GrantRevokeRequest={");

		sb.append("grantor={").append(grantor).append("} ");

		sb.append("resource={");
		if(resource != null) {
			for(Map.Entry<String, String> e : resource.entrySet()) {
				sb.append(e.getKey()).append("=").append(e.getValue()).append("; ");
			}
		}
		sb.append("} ");

		sb.append("users={");
		if(users != null) {
			for(String user : users) {
				sb.append(user).append(" ");
			}
		}
		sb.append("} ");

		sb.append("groups={");
		if(groups != null) {
			for(String group : groups) {
				sb.append(group).append(" ");
			}
		}
		sb.append("} ");

		sb.append("accessTypes={");
		if(accessTypes != null) {
			for(String accessType : accessTypes) {
				sb.append(accessType).append(" ");
			}
		}
		sb.append("} ");

		sb.append("delegateAdmin={").append(delegateAdmin).append("} ");
		sb.append("enableAudit={").append(enableAudit).append("} ");
		sb.append("replaceExistingPermissions={").append(replaceExistingPermissions).append("} ");

		sb.append("}");

		return sb;
	}
}
