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
	private RangerAccessRequest request;
	private boolean             isAllowed;
	private boolean             auditAccess;
	private long                policyId;
	private String              reason;


	public RangerAccessResult(RangerAccessRequest request, boolean isAllowed, boolean auditAccess) {
		this(request, isAllowed, auditAccess, -1, null);
	}

	public RangerAccessResult(RangerAccessRequest request, boolean isAllowed, boolean auditAccess, long policyId, String reason) {
		this.request     = request;
		this.isAllowed   = isAllowed;
		this.auditAccess = auditAccess;
		this.policyId    = policyId;
		this.reason      = reason;
	}

	public RangerAccessRequest getRequest() {
		return request;
	}

	public boolean isAllowed() {
		return isAllowed;
	}

	public boolean auditAccess() {
		return auditAccess;
	}

	public long getPolicyId() {
		return policyId;
	}

	public String getReason() {
		return reason;
	}
}
