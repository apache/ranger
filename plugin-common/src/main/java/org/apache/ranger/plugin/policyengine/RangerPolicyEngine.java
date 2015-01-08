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

import java.util.Collection;
import java.util.List;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;

public interface RangerPolicyEngine {
	public static final String GROUP_PUBLIC   = "public";
	public static final long   UNKNOWN_POLICY = -1;

	void setPolicies(String serviceName, RangerServiceDef serviceDef, List<RangerPolicy> policies);

	RangerAccessResult isAccessAllowed(RangerAccessRequest request);

	List<RangerAccessResult> isAccessAllowed(List<RangerAccessRequest> requests);

	void logAudit(AuthzAuditEvent auditEvent);

	void logAudit(Collection<AuthzAuditEvent> auditEvents);

	Collection<AuthzAuditEvent> getAuditEvents(RangerAccessRequest request, RangerAccessResult result);

	Collection<AuthzAuditEvent> getAuditEvents(List<RangerAccessRequest> requests, List<RangerAccessResult> results);
}
