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

import org.apache.ranger.plugin.model.RangerServiceDef;


public class RangerAccessResult {
	private String              serviceName = null;
	private RangerServiceDef    serviceDef  = null;
	private RangerAccessRequest request     = null;

	private boolean  isAllowed = false;
	private boolean  isAudited = false;
	private long     policyId  = -1;
	private String   reason    = null;

	public RangerAccessResult(String serviceName, RangerServiceDef serviceDef, RangerAccessRequest request) {
		this(serviceName, serviceDef, request, false, false, -1, null);
	}

	public RangerAccessResult(String serviceName, RangerServiceDef serviceDef, RangerAccessRequest request, boolean isAllowed, boolean isAudited, long policyId, String reason) {
		this.serviceName = serviceName;
		this.serviceDef  = serviceDef;
		this.request     = request;
		this.isAllowed   = isAllowed;
		this.isAudited   = isAudited;
		this.policyId    = policyId;
		this.reason      = reason;
	}

	/**
	 * @return the serviceName
	 */
	public String getServiceName() {
		return serviceName;
	}

	/**
	 * @return the serviceDef
	 */
	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}

	/**
	 * @return the request
	 */
	public RangerAccessRequest getAccessRequest() {
		return request;
	}

	/**
	 * @return the isAllowed
	 */
	public boolean getIsAllowed() {
		return isAllowed;
	}

	/**
	 * @param isAllowed the isAllowed to set
	 */
	public void setIsAllowed(boolean isAllowed) {
		this.isAllowed = isAllowed;
	}

	/**
	 * @return the isAudited
	 */
	public boolean getIsAudited() {
		return isAudited;
	}

	/**
	 * @param isAudited the isAudited to set
	 */
	public void setIsAudited(boolean isAudited) {
		this.isAudited = isAudited;
	}

	/**
	 * @return the policyId
	 */
	public long getPolicyId() {
		return policyId;
	}

	/**
	 * @return the policyId
	 */
	public void setPolicyId(long policyId) {
		this.policyId = policyId;
	}

	public int getServiceType() {
		int ret = -1;

		if(serviceDef != null && serviceDef.getId() != null) {
			ret = serviceDef.getId().intValue();
		}

		return ret;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerAccessResult={");

		sb.append("isAllowed={").append(isAllowed).append("} ");
		sb.append("isAudited={").append(isAudited).append("} ");
		sb.append("policyId={").append(policyId).append("} ");
		sb.append("reason={").append(reason).append("} ");

		sb.append("}");

		return sb;
	}
}
