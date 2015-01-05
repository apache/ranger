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

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;


public class RangerAccessResult {
	public enum Result { ALLOWED, DENIED };

	private Result  result    = null;
	private boolean isAudited = false;
	private boolean isFinal   = false;
	private long    policyId  = -1;
	private String  reason    = null;


	public RangerAccessResult() {
		this(Result.DENIED, false, false, -1, null);
	}

	public RangerAccessResult(Result result, boolean isAudited, boolean isFinal) {
		this(result, isAudited, isFinal, -1, null);
	}

	public RangerAccessResult(Result result, boolean isAudited, boolean isFinal, long policyId, String reason) {
		this.result    = result;
		this.isAudited = isAudited;
		this.isFinal   = isFinal;
		this.policyId  = policyId;
		this.reason    = reason;
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
	 * @return the isAudited
	 */
	public boolean isAudited() {
		return isAudited;
	}

	/**
	 * @param isAudited the isAudited to set
	 */
	public void setAudited(boolean isAudited) {
		this.isAudited = isAudited;
	}

	/**
	 * @return the isFinal
	 */
	public boolean isFinal() {
		return isFinal;
	}

	/**
	 * @param isFinal the isFinal to set
	 */
	public void setFinal(boolean isFinal) {
		this.isFinal = isFinal;
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

		if(obj != null && (obj instanceof RangerAccessResult)) {
			RangerAccessResult other = (RangerAccessResult)obj;

			ret = (this == other);

			if(! ret) {
				ret = this.isAudited == other.isAudited &&
					  this.policyId == other.policyId &&
					  StringUtils.equals(this.reason, other.reason) &&
					  ObjectUtils.equals(this.result, other.result);
			}
		}

		return ret;
	}

	@Override
	public int hashCode() {
		int ret = 7;

		ret = 31 * ret + (isAudited ? 1 : 0);
		ret = 31 * ret + (int)policyId;
		ret = 31 * ret + (reason == null ? 0 : reason.hashCode());
		ret = 31 * ret + (result == null ? 0 : result.hashCode());

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

		sb.append("result={").append(result).append("} ");
		sb.append("isAudited={").append(isAudited).append("} ");
		sb.append("isFinal={").append(isFinal).append("} ");
		sb.append("policyId={").append(policyId).append("} ");
		sb.append("reason={").append(reason).append("} ");

		sb.append("}");

		return sb;
	}
}
