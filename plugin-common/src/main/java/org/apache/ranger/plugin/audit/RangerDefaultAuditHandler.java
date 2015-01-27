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

package org.apache.ranger.plugin.audit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult.ResultDetail;


public class RangerDefaultAuditHandler implements RangerAuditHandler {
	private static final Log LOG = LogFactory.getLog(RangerDefaultAuditHandler.class);

	private static final String RESOURCE_SEP = "/";


	public RangerDefaultAuditHandler() {
	}

	@Override
	public void logAudit(RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultAuditHandler.logAudit(" + result + ")");
		}

		Collection<AuthzAuditEvent> events = getAuthzEvents(result);

		logAuthzAudits(events);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultAuditHandler.logAudit(" + result + ")");
		}
	}

	@Override
	public void logAudit(Collection<RangerAccessResult> results) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultAuditHandler.logAudit(" + results + ")");
		}

		Collection<AuthzAuditEvent> events = getAuthzEvents(results);

		logAuthzAudits(events);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultAuditHandler.logAudit(" + results + ")");
		}
	}


	public Collection<AuthzAuditEvent> getAuthzEvents(RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultAuditHandler.getAuthzEvents(" + result + ")");
		}

		List<AuthzAuditEvent> ret = null;

		RangerAccessRequest request = result != null ? result.getAccessRequest() : null;

		if(request != null && result != null && result.getIsAudited()) {
			RangerServiceDef serviceDef   = result.getServiceDef();
			String           resourceType = getResourceName(request.getResource(), serviceDef);
			String           resourcePath = getResourceValueAsString(request.getResource(), serviceDef);

			// TODO: optimize the number of audit logs created
			for(Map.Entry<String, ResultDetail> e : result.getAccessTypeResults().entrySet()) {
				String       accessType   = e.getKey();
				ResultDetail accessResult = e.getValue();

				AuthzAuditEvent event = createAuthzAuditEvent();

				event.setRepositoryName(result.getServiceName());
				event.setRepositoryType(result.getServiceType());
				event.setResourceType(resourceType);
				event.setResourcePath(resourcePath);
				event.setRequestData(request.getRequestData());
				event.setEventTime(request.getAccessTime());
				event.setUser(request.getUser());
				event.setAccessType(request.getAction());
				event.setAccessResult((short)(accessResult.isAllowed() ? 1 : 0));
				event.setPolicyId(result.getPolicyId());
				event.setAclEnforcer("ranger-acl"); // TODO: review
				event.setAction(accessType);
				event.setClientIP(request.getClientIPAddress());
				event.setClientType(request.getClientType());
				event.setAgentHostname(null);
				event.setAgentId(null);
				event.setEventId(null);

				if(ret == null) {
					ret = new ArrayList<AuthzAuditEvent>();
				}

				ret.add(event);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultAuditHandler.getAuthzEvents(" + result + "): " + ret);
		}

		return ret;
	}

	public Collection<AuthzAuditEvent> getAuthzEvents(Collection<RangerAccessResult> results) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultAuditHandler.getAuthzEvents(" + results + ")");
		}

		List<AuthzAuditEvent> ret = null;

		if(results != null) {
			// TODO: optimize the number of audit logs created
			for(RangerAccessResult result : results) {
				Collection<AuthzAuditEvent> events = getAuthzEvents(result);

				if(events == null) {
					continue;
				}

				if(ret == null) {
					ret = new ArrayList<AuthzAuditEvent>();
				}

				ret.addAll(events);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultAuditHandler.getAuthzEvents(" + results + "): " + ret);
		}

		return ret;
	}

	public void logAuthzAudit(AuthzAuditEvent auditEvent) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultAuditHandler.logAuthzAudit(" + auditEvent + ")");
		}

		if(auditEvent != null) {
			AuditProviderFactory.getAuditProvider().log(auditEvent);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultAuditHandler.logAuthzAudit(" + auditEvent + ")");
		}
	}

	public void logAuthzAudits(Collection<AuthzAuditEvent> auditEvents) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultAuditHandler.logAuthzAudits(" + auditEvents + ")");
		}

		if(auditEvents != null) {
			for(AuthzAuditEvent auditEvent : auditEvents) {
				logAuthzAudit(auditEvent);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultAuditHandler.logAuthzAudits(" + auditEvents + ")");
		}
	}

	public AuthzAuditEvent createAuthzAuditEvent() {
		return new AuthzAuditEvent();
	}

	public String getResourceName(RangerResource resource, RangerServiceDef serviceDef) {
		String ret = null;

		if(resource != null && serviceDef != null && serviceDef.getResources() != null) {
			List<RangerResourceDef> resourceDefs = serviceDef.getResources();

			for(int idx = resourceDefs.size() - 1; idx >= 0; idx--) {
				RangerResourceDef resourceDef = resourceDefs.get(idx);

				if(resourceDef == null || !resource.exists(resourceDef.getName())) {
					continue;
				}

				ret = resourceDef.getName();

				break;
			}
		}
		
		return ret;
	}

	public String getResourceValueAsString(RangerResource resource, RangerServiceDef serviceDef) {
		String ret = null;

		if(resource != null && serviceDef != null && serviceDef.getResources() != null) {
			StringBuilder sb = new StringBuilder();

			for(RangerResourceDef resourceDef : serviceDef.getResources()) {
				if(resourceDef == null || !resource.exists(resourceDef.getName())) {
					continue;
				}

				if(sb.length() > 0) {
					sb.append(RESOURCE_SEP);
				}

				sb.append(resource.getValue(resourceDef.getName()));
			}

			if(sb.length() > 0) {
				ret = sb.toString();
			}
		}

		return ret;
	}
}
