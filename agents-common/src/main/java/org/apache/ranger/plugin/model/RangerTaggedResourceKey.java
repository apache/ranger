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

import java.util.HashMap;
import java.util.Map;

public class RangerTaggedResourceKey implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private String                                         serviceName  = null;
	private Map<String, RangerPolicy.RangerPolicyResource> resourceSpec = null;

	public RangerTaggedResourceKey() { this(null, null); }

	public RangerTaggedResourceKey(String serviceName, Map<String, RangerPolicy.RangerPolicyResource> resourceSpec) {
		super();

		setServiceName(serviceName);
		setResourceSpec(resourceSpec);
	}

	public String getServiceName() { return serviceName; }

	public Map<String, RangerPolicy.RangerPolicyResource> getResourceSpec() { return resourceSpec; }

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public void setResourceSpec(Map<String, RangerPolicy.RangerPolicyResource> resourceSpec) {
		this.resourceSpec = resourceSpec == null ? new HashMap<String, RangerPolicy.RangerPolicyResource>() : resourceSpec;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {

		sb.append("{ ");

		sb.append("tagServiceName={").append(serviceName).append("} ");

		sb.append("resourceSpec={");
		if(resourceSpec != null) {
			for(Map.Entry<String, RangerPolicy.RangerPolicyResource> e : resourceSpec.entrySet()) {
				sb.append(e.getKey()).append("={");
				e.getValue().toString(sb);
				sb.append("} ");
			}
		}
		sb.append("} ");

		sb.append(" }");

		return sb;
	}
}
