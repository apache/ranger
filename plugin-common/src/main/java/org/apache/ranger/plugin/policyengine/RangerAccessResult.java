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


public class RangerAccessResult {
	public enum Result { ALLOWED, DENIED, PARTIALLY_DENIED };

	private RangerAccessRequest request        = null;
	private Result              result         = null;
	private boolean             isAudited      = false;
	private long                policyId       = -1;
	private String              reason         = null;


	public RangerAccessResult(RangerAccessRequest request) {
		this(request, Result.DENIED, false, -1, null);
	}

	public RangerAccessResult(RangerAccessRequest request, Result result, boolean isAudited) {
		this(request, result, isAudited, -1, null);
	}

	public RangerAccessResult(RangerAccessRequest request, Result result, boolean isAudited, long policyId, String reason) {
		this.request   = request;
		this.result    = result;
		this.isAudited = isAudited;
		this.policyId  = policyId;
		this.reason    = reason;
	}

	/**
	 * @return the request
	 */
	public RangerAccessRequest getRequest() {
		return request;
	}

	/**
	 * @return the result
	 */
	public Result getResult() {
		return result;
	}

	/**
	 * @param result the result to set
	 */
	public void setResult(Result result) {
		this.result = result;
	}

	/**
	 * @return the auditAccess
	 */
	public boolean isAudited() {
		return isAudited;
	}

	/**
	 * @param auditAccess the auditAccess to set
	 */
	public void setAudited(boolean isAudited) {
		this.isAudited = isAudited;
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

	/**
	 * @return the reason
	 */
	public String getReason() {
		return reason;
	}

	/**
	 * @param reason the reason to set
	 */
	public void setReason(String reason) {
		this.reason = reason;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerAccessResult={");

		sb.append("request={").append(request).append("} ");
		sb.append("result={").append(result).append("} ");
		sb.append("isAudited={").append(isAudited).append("} ");
		sb.append("policyId={").append(policyId).append("} ");
		sb.append("reason={").append(reason).append("} ");

		sb.append("}");

		return sb;
	}
}
