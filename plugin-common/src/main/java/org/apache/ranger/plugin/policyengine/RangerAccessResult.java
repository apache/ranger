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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerServiceDef;


public class RangerAccessResult {
	public enum Result { ALLOWED, DENIED, PARTIALLY_ALLOWED };

	private String              serviceName = null;
	private RangerServiceDef    serviceDef  = null;
	private RangerAccessRequest request     = null;

	private boolean                   isAudited         = false;
	private Map<String, ResultDetail> accessTypeResults = null;

	public RangerAccessResult(String serviceName, RangerServiceDef serviceDef, RangerAccessRequest request) {
		this(serviceName, serviceDef, request, false, null);
	}

	public RangerAccessResult(String serviceName, RangerServiceDef serviceDef, RangerAccessRequest request, boolean isAudited, Map<String, ResultDetail> accessTypeResults) {
		this.serviceName = serviceName;
		this.serviceDef  = serviceDef;
		this.request     = request;
		this.isAudited   = isAudited;

		setAccessTypeResults(accessTypeResults);
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
	 * @return the accessTypeResults
	 */
	public Map<String, ResultDetail> getAccessTypeResults() {
		return accessTypeResults;
	}

	/**
	 * @param result the result to set
	 */
	public void setAccessTypeResults(Map<String, ResultDetail> accessTypeResults) {
		this.accessTypeResults = accessTypeResults == null ? new HashMap<String, ResultDetail>() : accessTypeResults;

		// ensure that accessTypeResults has all the accessTypes in the request
		if(request != null && request.getAccessTypes() != null) {
			for(String accessType : request.getAccessTypes()) {
				if(! this.accessTypeResults.containsKey(accessType)) {
					this.accessTypeResults.put(accessType, new ResultDetail());
				}
			}
		}
	}

	/**
	 * @param accessType the accessType
	 * @return the accessTypeResult
	 */
	public ResultDetail getAccessTypeResult(String accessType) {
		return accessTypeResults == null ? null : accessTypeResults.get(accessType);
	}

	/**
	 * @param accessType the accessType
	 * @param result the result to set
	 */
	public void setAccessTypeResult(String accessType, ResultDetail result) {
		if(accessTypeResults == null) {
			accessTypeResults = new HashMap<String, ResultDetail>();
		}

		accessTypeResults.put(accessType, result);
	}

	/**
	 * @return the overall result
	 */
	public Result getResult() {
		Result ret = Result.ALLOWED;

		if(accessTypeResults != null && !accessTypeResults.isEmpty()) {
			boolean anyAllowed    = false;
			boolean anyNotAllowed = false;

			for(Map.Entry<String, ResultDetail> e : accessTypeResults.entrySet()) {
				ResultDetail result = e.getValue();

				if(result.isAllowed) {
					anyAllowed = true;
				} else {
					anyNotAllowed = true;
				}

				if(anyAllowed && anyNotAllowed) {
					break;
				}
			}
			
			if(anyAllowed && anyNotAllowed) {
				ret = Result.PARTIALLY_ALLOWED;
			} else if(anyNotAllowed) {
				ret = Result.DENIED;
			} else {
				ret = Result.ALLOWED;
			}
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

		sb.append("isAudited={").append(isAudited).append("} ");
		sb.append("accessTypeResults={");
		if(accessTypeResults != null) {
			for(Map.Entry<String, ResultDetail> e : accessTypeResults.entrySet()) {
				sb.append(e.getKey()).append("={").append(e.getValue()).append("} ");
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}

	public static class ResultDetail {
		private boolean isAllowed;
		private long    policyId;
		private String  reason;

		public ResultDetail() {
			setIsAllowed(false);
			setPolicyId(RangerPolicyEngine.UNKNOWN_POLICY);
			setReason(null);
		}

		/**
		 * @return the isAllowed
		 */
		public boolean isAllowed() {
			return isAllowed;
		}

		/**
		 * @param isAllowed the isAllowed to set
		 */
		public void setIsAllowed(boolean isAllowed) {
			this.isAllowed = isAllowed;
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
		public boolean equals(Object obj) {
			boolean ret = false;

			if(obj != null && (obj instanceof ResultDetail)) {
				ResultDetail other = (ResultDetail)obj;

				ret = (this == other);

				if(! ret) {
					ret = this.isAllowed == other.isAllowed &&
						  this.policyId == other.policyId &&
						  StringUtils.equals(this.reason, other.reason);
				}
			}

			return ret;
		}

		@Override
		public int hashCode() {
			int ret = 7;

			ret = 31 * ret + (isAllowed ? 1 : 0);
			ret = 31 * ret + (int)policyId;
			ret = 31 * ret + (reason == null ? 0 : reason.hashCode());

			return ret;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("isAllowed={").append(isAllowed).append("} ");
			sb.append("policyId={").append(policyId).append("} ");
			sb.append("reason={").append(reason).append("} ");

			return sb;
		}
	}
}
