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

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;


public class RangerAccessResult {
	public enum Result { ALLOWED, DENIED, PARTIALLY_ALLOWED };

	private Map<String, ResultDetail> accessTypeResults = null;

	public RangerAccessResult() {
		this(null);
	}

	public RangerAccessResult(Map<String, ResultDetail> accessTypeResults) {
		setAccessTypeResults(accessTypeResults);
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
	}

	/**
	 * @param accessType the accessType
	 * @return the accessTypeResult
	 */
	public ResultDetail getAccessTypeResult(String accessType) {
		if(accessTypeResults == null) {
			accessTypeResults = new HashMap<String, ResultDetail>();
		}
		
		ResultDetail ret = accessTypeResults.get(accessType);
		
		if(ret == null) {
			ret = new ResultDetail();
			
			accessTypeResults.put(accessType, ret);
		}

		return ret;
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

	public boolean isAllAllowedAndAudited() {
		boolean ret = true;

		if(accessTypeResults != null) {
			for(Map.Entry<String, ResultDetail> e : accessTypeResults.entrySet()) {
				ResultDetail result = e.getValue();
				
				ret = result.isAllowed && result.isAudited;
				
				if(! ret) {
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * @return the overall result
	 */
	public Result getResult() {
		Result ret = Result.ALLOWED;

		if(accessTypeResults != null) {
			int numAllowed = 0;

			for(Map.Entry<String, ResultDetail> e : accessTypeResults.entrySet()) {
				ResultDetail result = e.getValue();
				
				if(result.isAllowed) {
					numAllowed++;
				}
			}
			
			if(numAllowed == accessTypeResults.size()) {
				ret = Result.ALLOWED;
			} else if(numAllowed == 0) {
				ret = Result.DENIED;
			} else {
				ret = Result.PARTIALLY_ALLOWED;
			}
		}

		return ret;
	}

	@Override
	public boolean equals(Object obj) {
		boolean ret = false;

		if(obj != null && (obj instanceof RangerAccessResult)) {
			RangerAccessResult other = (RangerAccessResult)obj;

			ret = (this == other) ||
				   ObjectUtils.equals(accessTypeResults, other.accessTypeResults);
		}

		return ret;
	}

	@Override
	public int hashCode() {
		int ret = 7;

		ret = 31 * ret + (accessTypeResults == null ? 0 : accessTypeResults.hashCode()); // TODO: review

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
		private boolean isAudited;
		private long    policyId;
		private String  reason;

		public ResultDetail() {
			setIsAllowed(false);
			setIsAudited(false);
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
		 * @return the isAudited
		 */
		public boolean isAudited() {
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
						  this.isAudited == other.isAudited &&
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
			ret = 31 * ret + (isAudited ? 1 : 0);
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
			sb.append("isAudited={").append(isAudited).append("} ");
			sb.append("policyId={").append(policyId).append("} ");
			sb.append("reason={").append(reason).append("} ");

			return sb;
		}
	}
}
