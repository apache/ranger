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
	private final String              serviceName;
	private final RangerServiceDef    serviceDef;
	private final RangerAccessRequest request;

	private boolean isAccessDetermined = false;
	private boolean  isAllowed = false;
	private boolean isAuditedDetermined = false;
	private boolean  isAudited = false;
	private long     policyId  = -1;
	private String   reason    = null;

	public RangerAccessResult(final String serviceName, final RangerServiceDef serviceDef, final RangerAccessRequest request) {
		this.serviceName = serviceName;
		this.serviceDef  = serviceDef;
		this.request     = request;
		this.isAccessDetermined = false;
		this.isAllowed   = false;
		this.isAuditedDetermined = false;
		this.isAudited   = false;
		this.policyId    = -1;
		this.reason      = null;
	}

	public void setAccessResultFrom(final RangerAccessResult other) {
		this.isAccessDetermined = other.getIsAccessDetermined();
		this.isAllowed   = other.getIsAllowed();
		this.policyId    = other.getPolicyId();
		this.reason      = other.getReason();
	}

	public void setAuditResultFrom(final RangerAccessResult other) {
		this.isAuditedDetermined = other.getIsAuditedDetermined();
		this.isAudited = other.getIsAudited();
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

	public boolean getIsAccessDetermined() { return isAccessDetermined; }

	public void setIsAccessDetermined(boolean value) { isAccessDetermined = value; }

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
		if(! isAllowed) {
			setIsAccessDetermined(true);
		}

		this.isAllowed = isAllowed;
	}

	/**
	 * @param reason the reason to set
	 */
	public void setReason(String reason) {
		this.reason = reason;
	}

	public boolean getIsAuditedDetermined() { return isAuditedDetermined; }

	private void setIsAuditedDetermined(boolean value) { isAuditedDetermined = value; }
	
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
		setIsAuditedDetermined(true);
		this.isAudited = isAudited;
	}

	/**
	 * @return the reason
	 */
	public String getReason() {
		return reason;
	}

	/**
	 * @return the policyId
	 */
	public long getPolicyId() {
		return policyId;
	}

	/**
	 * @param policyId the policyId to set
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

        sb.append("isAccessDetermined={").append(isAccessDetermined).append("} ");
		sb.append("isAllowed={").append(isAllowed).append("} ");
        sb.append("isAuditedDetermined={").append(isAuditedDetermined).append("} ");
        sb.append("isAudited={").append(isAudited).append("} ");
		sb.append("policyId={").append(policyId).append("} ");
		sb.append("reason={").append(reason).append("} ");

		sb.append("}");

		return sb;
	}
}
