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

package org.apache.ranger.plugin.policyengine;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;

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

import static org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.ACCESS_ALLOWED;
import static org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.ACCESS_DENIED;

public class RangerResourceACLs {
	final private Map<String, Map<String, AccessResult>> userACLs  = new HashMap<>();
	final private Map<String, Map<String, AccessResult>> groupACLs = new HashMap<>();
	final private Map<String, Map<String, AccessResult>> roleACLs  = new HashMap<>();
	public RangerResourceACLs() {
	}

	public Map<String, Map<String, AccessResult>> getUserACLs() {
		return userACLs;
	}

	public Map<String, Map<String, AccessResult>> getGroupACLs() {
		return groupACLs;
	}

	public Map<String, Map<String, AccessResult>> getRoleACLs() { return roleACLs; }

	public void finalizeAcls() {
		Map<String, AccessResult>  publicGroupAccessInfo = groupACLs.get(RangerPolicyEngine.GROUP_PUBLIC);
		if (publicGroupAccessInfo != null) {

			for (Map.Entry<String, AccessResult> entry : publicGroupAccessInfo.entrySet()) {
				String accessType = entry.getKey();
				AccessResult accessResult = entry.getValue();
				int access = accessResult.getResult();

				if (access == ACCESS_DENIED || access == ACCESS_ALLOWED) {
					for (Map.Entry<String, Map<String, AccessResult>> mapEntry : userACLs.entrySet()) {
						Map<String, AccessResult> mapValue = mapEntry.getValue();
						AccessResult savedAccessResult = mapValue.get(accessType);
						if (savedAccessResult != null && !savedAccessResult.getIsFinal()) {
							mapValue.remove(accessType);
						}
					}

					for (Map.Entry<String, Map<String, AccessResult>> mapEntry : groupACLs.entrySet()) {
						if (!StringUtils.equals(mapEntry.getKey(), RangerPolicyEngine.GROUP_PUBLIC)) {
							Map<String, AccessResult> mapValue = mapEntry.getValue();
							AccessResult savedAccessResult = mapValue.get(accessType);
							if (savedAccessResult != null && !savedAccessResult.getIsFinal()) {
								mapValue.remove(accessType);
							}
						}
					}
				}
			}
		}
		finalizeAcls(userACLs);
		finalizeAcls(groupACLs);
		finalizeAcls(roleACLs);
	}

	public void setUserAccessInfo(String userName, String accessType, Integer access, RangerPolicy policy) {
		Map<String, AccessResult> userAccessInfo = userACLs.get(userName);

		if (userAccessInfo == null) {
			userAccessInfo = new HashMap<>();

			userACLs.put(userName, userAccessInfo);
		}

		AccessResult accessResult = userAccessInfo.get(accessType);

		if (accessResult == null) {
			accessResult = new AccessResult(access, policy);

			userAccessInfo.put(accessType, accessResult);
		} else {
			accessResult.setResult(access);
			accessResult.setPolicy(policy);
		}
	}

	public void setGroupAccessInfo(String groupName, String accessType, Integer access, RangerPolicy policy) {
		Map<String, AccessResult> groupAccessInfo = groupACLs.get(groupName);

		if (groupAccessInfo == null) {
			groupAccessInfo = new HashMap<>();

			groupACLs.put(groupName, groupAccessInfo);
		}

		AccessResult accessResult = groupAccessInfo.get(accessType);

		if (accessResult == null) {
			accessResult = new AccessResult(access, policy);

			groupAccessInfo.put(accessType, accessResult);
		} else {
			accessResult.setResult(access);
			accessResult.setPolicy(policy);
		}
	}

	public void setRoleAccessInfo(String roleName, String accessType, Integer access, RangerPolicy policy) {
		Map<String, AccessResult> roleAccessInfo = roleACLs.get(roleName);

		if (roleAccessInfo == null) {
			roleAccessInfo = new HashMap<>();

			roleACLs.put(roleName, roleAccessInfo);
		}

		AccessResult accessResult = roleAccessInfo.get(accessType);

		if (accessResult == null) {
			accessResult = new AccessResult(access, policy);

			roleAccessInfo.put(accessType, accessResult);
		} else {
			accessResult.setResult(access);
			accessResult.setPolicy(policy);
		}
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();

		sb.append("{");

		sb.append("UserACLs={");
		for (Map.Entry<String, Map<String, AccessResult>> entry : userACLs.entrySet()) {
			sb.append("user=").append(entry.getKey()).append(":");
			sb.append("permissions={");
			for (Map.Entry<String, AccessResult> permission : entry.getValue().entrySet()) {
				sb.append("{Permission=").append(permission.getKey()).append(", value=").append(permission.getValue()).append("},");
				sb.append("{RangerPolicyID=").append(permission.getValue().getPolicy().getId()).append("},");
			}
			sb.append("},");
		}
		sb.append("}");

		sb.append(", GroupACLs={");
		for (Map.Entry<String, Map<String, AccessResult>> entry : groupACLs.entrySet()) {
			sb.append("group=").append(entry.getKey()).append(":");
			sb.append("permissions={");
			for (Map.Entry<String, AccessResult> permission : entry.getValue().entrySet()) {
				sb.append("{Permission=").append(permission.getKey()).append(", value=").append(permission.getValue()).append("}, ");
				sb.append("{RangerPolicy ID=").append(permission.getValue().getPolicy().getId()).append("},");
			}
			sb.append("},");
		}
		sb.append("}");

		sb.append(", RoleACLs={");
		for (Map.Entry<String, Map<String, AccessResult>> entry : roleACLs.entrySet()) {
			sb.append("role=").append(entry.getKey()).append(":");
			sb.append("permissions={");
			for (Map.Entry<String, AccessResult> permission : entry.getValue().entrySet()) {
				sb.append("{Permission=").append(permission.getKey()).append(", value=").append(permission.getValue()).append("}, ");
				sb.append("{RangerPolicy ID=").append(permission.getValue().getPolicy().getId()).append("},");
			}
			sb.append("},");
		}
		sb.append("}");

		sb.append("}");

		return sb.toString();
	}

	private void finalizeAcls(Map<String, Map<String, AccessResult>> acls) {
		List<String> keysToRemove = new ArrayList<>();
		for (Map.Entry<String, Map<String, AccessResult>> entry : acls.entrySet()) {
			if (entry.getValue().isEmpty()) {
				keysToRemove.add(entry.getKey());
			} else {
				for (Map.Entry<String, AccessResult> permission : entry.getValue().entrySet()) {
					permission.getValue().setIsFinal(true);
				}
			}

		}
		for (String keyToRemove : keysToRemove) {
			acls.remove(keyToRemove);
		}
	}

	@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
	@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown=true)
	@XmlRootElement
	@XmlAccessorType(XmlAccessType.FIELD)
	public static class AccessResult {
		private int     result;
		private boolean isFinal;
		private RangerPolicy  policy;

		public AccessResult() {
			this(-1, null);
		}

		public AccessResult(int result, RangerPolicy policy) {
			this(result, false, policy);
		}

		public AccessResult(int result, boolean isFinal, RangerPolicy policy) {
			setIsFinal(isFinal);
			setResult(result);
			setPolicy(policy);
		}

		public int getResult() { return result; }

		public void setResult(int result) {
			if (!isFinal) {
				this.result = result;

				if (this.result == ACCESS_DENIED) {
					isFinal = true;
				}
			}
		}

		public boolean getIsFinal() { return isFinal; }

		public void setIsFinal(boolean isFinal) { this.isFinal = isFinal; }

		public RangerPolicy getPolicy() {
			return policy;
		}

		public void setPolicy(RangerPolicy policy){
			this.policy = policy;
		}

		@Override
		public boolean equals(Object other) {
			if (other == null)
				return false;
			if (other instanceof AccessResult) {
				AccessResult otherObject = (AccessResult)other;
				return result == otherObject.result && isFinal == otherObject.isFinal;
			} else
				return false;

		}
		@Override
		public String toString() {
			if (result == ACCESS_ALLOWED) {
				return "ALLOWED, final=" + isFinal;
			}
			if (result == ACCESS_DENIED) {
				return "NOT_ALLOWED, final=" + isFinal;
			}
			return "CONDITIONAL_ALLOWED, final=" + isFinal;
		}
	}
}
