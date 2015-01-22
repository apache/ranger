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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

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
public class RangerPolicy extends RangerBaseModelObject implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private String                            service        = null;
	private String                            name           = null;
	private String                            description    = null;
	private Boolean                           isEnabled      = null;
	private Boolean                           isAuditEnabled = null;
	private Map<String, RangerPolicyResource> resources      = null;
	private List<RangerPolicyItem>            policyItems    = null;


	/**
	 * @param type
	 */
	public RangerPolicy() {
		this(null, null, null, null, null, null);
	}

	/**
	 * @param type
	 * @param name
	 * @param description
	 * @param isEnabled
	 * @param configs
	 */
	public RangerPolicy(String service, String name, String description, Boolean isEnabled, Map<String, RangerPolicyResource> resources, List<RangerPolicyItem> policyItems) {
		super();

		setService(service);
		setName(name);
		setDescription(description);
		setIsEnabled(isEnabled);
		setIsAuditEnabled(null);
		setResources(resources);
		setPolicyItems(policyItems);
	}

	public void updateFrom(RangerPolicy other) {
		super.updateFrom(other);

		setService(other.getService());
		setName(other.getName());
		setDescription(other.getDescription());
		setIsEnabled(other.getIsEnabled());
		setIsAuditEnabled(other.getIsAuditEnabled());
		setResources(other.getResources());
		setPolicyItems(other.getPolicyItems());
	}

	/**
	 * @return the type
	 */
	public String getService() {
		return service;
	}

	/**
	 * @param type the type to set
	 */
	public void setService(String service) {
		this.service = service;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the isEnabled
	 */
	public Boolean getIsEnabled() {
		return isEnabled;
	}

	/**
	 * @param isEnabled the isEnabled to set
	 */
	public void setIsEnabled(Boolean isEnabled) {
		this.isEnabled = isEnabled == null ? Boolean.TRUE : isEnabled;
	}

	/**
	 * @return the isAuditEnabled
	 */
	public Boolean getIsAuditEnabled() {
		return isAuditEnabled;
	}

	/**
	 * @param isEnabled the isEnabled to set
	 */
	public void setIsAuditEnabled(Boolean isAuditEnabled) {
		this.isAuditEnabled = isAuditEnabled == null ? Boolean.TRUE : isAuditEnabled;
	}

	/**
	 * @return the resources
	 */
	public Map<String, RangerPolicyResource> getResources() {
		return resources;
	}

	/**
	 * @param configs the resources to set
	 */
	public void setResources(Map<String, RangerPolicyResource> resources) {
		if(this.resources == null) {
			this.resources = new HashMap<String, RangerPolicyResource>();
		}

		if(this.resources == resources) {
			return;
		}

		this.resources.clear();

		if(resources != null) {
			for(Map.Entry<String, RangerPolicyResource> e : resources.entrySet()) {
				this.resources.put(e.getKey(), e.getValue());
			}
		}
	}

	/**
	 * @return the policyItems
	 */
	public List<RangerPolicyItem> getPolicyItems() {
		return policyItems;
	}

	/**
	 * @param policyItems the policyItems to set
	 */
	public void setPolicyItems(List<RangerPolicyItem> policyItems) {
		if(this.policyItems == null) {
			this.policyItems = new ArrayList<RangerPolicyItem>();
		}

		if(this.policyItems == policyItems) {
			return;
		}

		this.policyItems.clear();

		if(policyItems != null) {
			for(RangerPolicyItem policyItem : policyItems) {
				this.policyItems.add(policyItem);
			}
		}
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerPolicy={");

		super.toString(sb);

		sb.append("service={").append(service).append("} ");
		sb.append("name={").append(name).append("} ");
		sb.append("description={").append(description).append("} ");
		sb.append("isEnabled={").append(isEnabled).append("} ");
		sb.append("isAuditEnabled={").append(isAuditEnabled).append("} ");

		sb.append("resources={");
		if(resources != null) {
			for(Map.Entry<String, RangerPolicyResource> e : resources.entrySet()) {
				sb.append(e.getKey()).append("={");
				e.getValue().toString(sb);
				sb.append("} ");
			}
		}
		sb.append("} ");

		sb.append("policyItems={");
		if(policyItems != null) {
			for(RangerPolicyItem policyItem : policyItems) {
				if(policyItem != null) {
					policyItem.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}

	public final static Comparator<RangerPolicy> nameComparator = new Comparator<RangerPolicy>() {
		@Override
		public int compare(RangerPolicy o1, RangerPolicy o2) {
			String name1 = (o1 == null) ? null : o1.getName();
			String name2 = (o2 == null) ? null : o2.getName();

			if(name1 == null) {
				return -1;
			} else if(name2 == null) {
				return 1;
			} else {
				return name1.compareTo(name2);
			}
		}
	};


	public static class RangerPolicyResource implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private List<String> values      = null;
		private Boolean      isExcludes = null;
		private Boolean      isRecursive = null;


		public RangerPolicyResource() {
			this((List<String>)null, null, null);
		}

		public RangerPolicyResource(String value, Boolean isExcludes, Boolean isRecursive) {
			setValue(value);
			setIsExcludes(isExcludes);
			setIsRecursive(isRecursive);
		}

		public RangerPolicyResource(List<String> values, Boolean isExcludes, Boolean isRecursive) {
			setValues(values);
			setIsExcludes(isExcludes);
			setIsRecursive(isRecursive);
		}

		/**
		 * @return the values
		 */
		public List<String> getValues() {
			return values;
		}

		/**
		 * @param values the values to set
		 */
		public void setValues(List<String> values) {
			if(this.values == null) {
				this.values = new ArrayList<String>();
			}

			if(this.values == values) {
				return;
			}

			this.values.clear();

			if(values != null) {
				for(String value : values) {
					this.values.add(value);
				}
			}
		}

		/**
		 * @param value the value to set
		 */
		public void setValue(String value) {
			if(this.values == null) {
				this.values = new ArrayList<String>();
			}

			this.values.clear();

			this.values.add(value);
		}

		/**
		 * @return the isExcludes
		 */
		public Boolean getIsExcludes() {
			return isExcludes;
		}

		/**
		 * @param isExcludes the isExcludes to set
		 */
		public void setIsExcludes(Boolean isExcludes) {
			this.isExcludes = isExcludes == null ? Boolean.FALSE : isExcludes;
		}

		/**
		 * @return the isRecursive
		 */
		public Boolean getIsRecursive() {
			return isRecursive;
		}

		/**
		 * @param isRecursive the isRecursive to set
		 */
		public void setIsRecursive(Boolean isRecursive) {
			this.isRecursive = isRecursive == null ? Boolean.FALSE : isRecursive;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerPolicyResource={");
			sb.append("values={");
			if(values != null) {
				for(String value : values) {
					sb.append(value).append(" ");
				}
			}
			sb.append("} ");
			sb.append("isExcludes={").append(isExcludes).append("} ");
			sb.append("isRecursive={").append(isRecursive).append("} ");
			sb.append("}");

			return sb;
		}
	}

	public static class RangerPolicyItem implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private List<RangerPolicyItemAccess>    accesses      = null;
		private List<String>                    users         = null;
		private List<String>                    groups        = null;
		private List<RangerPolicyItemCondition> conditions    = null;
		private Boolean                         delegateAdmin = null;

		public RangerPolicyItem() {
			this(null, null, null, null, null);
		}

		public RangerPolicyItem(List<RangerPolicyItemAccess> accessTypes, List<String> users, List<String> groups, List<RangerPolicyItemCondition> conditions, Boolean delegateAdmin) {
			setAccesses(accessTypes);
			setUsers(users);
			setGroups(groups);
			setConditions(conditions);
			setDelegateAdmin(delegateAdmin);
		}

		/**
		 * @return the accesses
		 */
		public List<RangerPolicyItemAccess> getAccesses() {
			return accesses;
		}
		/**
		 * @param accesses the accesses to set
		 */
		public void setAccesses(List<RangerPolicyItemAccess> accesses) {
			if(this.accesses == null) {
				this.accesses = new ArrayList<RangerPolicyItemAccess>();
			}

			if(this.accesses == accesses) {
				return;
			}

			if(accesses != null) {
				for(RangerPolicyItemAccess access : accesses) {
					this.accesses.add(access);
				}
			}
		}
		/**
		 * @return the users
		 */
		public List<String> getUsers() {
			return users;
		}
		/**
		 * @param users the users to set
		 */
		public void setUsers(List<String> users) {
			if(this.users == null) {
				this.users = new ArrayList<String>();
			}

			if(this.users == users) {
				return;
			}

			if(users != null) {
				for(String user : users) {
					this.users.add(user);
				}
			}
		}
		/**
		 * @return the groups
		 */
		public List<String> getGroups() {
			return groups;
		}
		/**
		 * @param groups the groups to set
		 */
		public void setGroups(List<String> groups) {
			if(this.groups == null) {
				this.groups = new ArrayList<String>();
			}

			if(this.groups == groups) {
				return;
			}

			if(groups != null) {
				for(String group : groups) {
					this.groups.add(group);
				}
			}
		}
		/**
		 * @return the conditions
		 */
		public List<RangerPolicyItemCondition> getConditions() {
			return conditions;
		}
		/**
		 * @param conditions the conditions to set
		 */
		public void setConditions(List<RangerPolicyItemCondition> conditions) {
			if(this.conditions == null) {
				this.conditions = new ArrayList<RangerPolicyItemCondition>();
			}

			if(this.conditions == conditions) {
				return;
			}

			if(conditions != null) {
				for(RangerPolicyItemCondition condition : conditions) {
					this.conditions.add(condition);
				}
			}
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

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerPolicyItem={");

			sb.append("accessTypes={");
			if(accesses != null) {
				for(RangerPolicyItemAccess access : accesses) {
					if(access != null) {
						access.toString(sb);
					}
				}
			}
			sb.append("} ");

			sb.append("users={");
			if(users != null) {
				for(String user : users) {
					if(user != null) {
						sb.append(user).append(" ");
					}
				}
			}
			sb.append("} ");

			sb.append("groups={");
			if(groups != null) {
				for(String group : groups) {
					if(group != null) {
						sb.append(group).append(" ");
					}
				}
			}
			sb.append("} ");

			sb.append("conditions={");
			if(conditions != null) {
				for(RangerPolicyItemCondition condition : conditions) {
					if(condition != null) {
						condition.toString(sb);
					}
				}
			}
			sb.append("} ");

			sb.append("delegateAdmin={").append(delegateAdmin).append("} ");
			sb.append("}");

			return sb;
		}
	}

	public static class RangerPolicyItemAccess implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String  type      = null;
		private Boolean isAllowed = null;

		public RangerPolicyItemAccess() {
			this(null, null);
		}

		public RangerPolicyItemAccess(String type) {
			this(type, null);
		}

		public RangerPolicyItemAccess(String type, Boolean isAllowed) {
			setType(type);
			setIsAllowed(isAllowed);
		}

		/**
		 * @return the type
		 */
		public String getType() {
			return type;
		}

		/**
		 * @param type the type to set
		 */
		public void setType(String type) {
			this.type = type;
		}

		/**
		 * @return the isAllowed
		 */
		public Boolean getIsAllowed() {
			return isAllowed;
		}

		/**
		 * @param isAllowed the isAllowed to set
		 */
		public void setIsAllowed(Boolean isAllowed) {
			this.isAllowed = isAllowed == null ? Boolean.TRUE : isAllowed;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerPolicyItemAccess={");
			sb.append("type={").append(type).append("} ");
			sb.append("isAllowed={").append(isAllowed).append("} ");
			sb.append("}");

			return sb;
		}
	}

	public static class RangerPolicyItemCondition implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String type = null;
		private String value = null;

		public RangerPolicyItemCondition() {
			this(null, null);
		}

		public RangerPolicyItemCondition(String type, String value) {
			setType(type);
			setValue(value);
		}

		/**
		 * @return the type
		 */
		public String getType() {
			return type;
		}

		/**
		 * @param type the type to set
		 */
		public void setType(String type) {
			this.type = type;
		}

		/**
		 * @return the value
		 */
		public String getValue() {
			return value;
		}

		/**
		 * @param value the value to set
		 */
		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerPolicyItemCondition={");
			sb.append("type={").append(type).append("} ");
			sb.append("value={").append(value).append("} ");
			sb.append("}");

			return sb;
		}
	}
}
