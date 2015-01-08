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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult.ResultDetail;
import org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.model.AuthzAuditEvent;


public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private static final String RESOURCE_SEP = "/";

	private String                      serviceName      = null;
	private RangerServiceDef            serviceDef       = null;
	private List<RangerPolicyEvaluator> policyEvaluators = null;


	public RangerPolicyEngineImpl() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl()");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl()");
		}
	}

	@Override
	public void setPolicies(String serviceName, RangerServiceDef serviceDef, List<RangerPolicy> policies) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.setPolicies(" + serviceName + ", " + serviceDef + ", " + policies + ")");
		}

		if(serviceName != null && serviceDef != null && policies != null) {
			List<RangerPolicyEvaluator> evaluators = new ArrayList<RangerPolicyEvaluator>();

			for(RangerPolicy policy : policies) {
				if(policy.getIsEnabled()) {
					RangerPolicyEvaluator evaluator = getPolicyEvaluator(policy, serviceDef);
	
					if(evaluator != null) {
						evaluators.add(evaluator);
					}
				}
			}

			this.serviceName      = serviceName;
			this.serviceDef       = serviceDef;
			this.policyEvaluators = evaluators;
		} else {
			LOG.error("RangerPolicyEngineImpl.setPolicies(): invalid arguments - null serviceDef/policies");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.setPolicies(" + serviceName + ", " + serviceDef + ", " + policies + ")");
		}
	}

	@Override
	public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + request + ")");
		}

		RangerAccessResult ret = isAccessAllowedNoAudit(request);

		logAudit(getAuditEvents(request, ret));

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + request + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerAccessResult> isAccessAllowed(List<RangerAccessRequest> requests) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + requests + ")");
		}
		
		List<RangerAccessResult> ret = new ArrayList<RangerAccessResult>();

		if(requests != null) {
			for(RangerAccessRequest request : requests) {
				RangerAccessResult result = isAccessAllowedNoAudit(request);

				ret.add(result);
			}
		}

		logAudit(getAuditEvents(requests, ret));

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + requests + "): " + ret);
		}

		return ret;
	}

	@Override
	public Collection<AuthzAuditEvent> getAuditEvents(RangerAccessRequest request, RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getAuditEvents(" + request + ", " + result + ")");
		}

		List<AuthzAuditEvent> ret = null;

		if(request != null && result != null) {
			// TODO: optimize the number of audit logs created
			for(Map.Entry<String, ResultDetail> e : result.getAccessTypeResults().entrySet()) {
				String       accessType   = e.getKey();
				ResultDetail accessResult = e.getValue();

				if(! accessResult.isAudited()) {
					continue;
				}

				AuthzAuditEvent event = new AuthzAuditEvent();

				event.setRepositoryName(serviceName);
				event.setRepositoryType(serviceDef.getId().intValue());
				event.setResourcePath(getResourceValueAsString(request.getResource()));
				event.setEventTime(request.getAccessTime());
				event.setUser(request.getUser());
				event.setAccessType(request.getAction());
				event.setAccessResult((short)(accessResult.isAllowed() ? 1 : 0));
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
			LOG.debug("<== RangerPolicyEngineImpl.getAuditEvents(" + request + ", " + result + "): " + ret);
		}

		return ret;
	}
	
	@Override
	public Collection<AuthzAuditEvent> getAuditEvents(List<RangerAccessRequest> requests, List<RangerAccessResult> results) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getAuditEvents(" + requests + ", " + results + ")");
		}

		List<AuthzAuditEvent> ret = null;

		if(requests != null && results != null) {
			int count = Math.min(requests.size(), results.size());

			// TODO: optimize the number of audit logs created
			for(int i = 0; i < count; i++) {
				Collection<AuthzAuditEvent> events = getAuditEvents(requests.get(i), results.get(i));

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
			LOG.debug("<== RangerPolicyEngineImpl.getAuditEvents(" + requests + ", " + results + "): " + ret);
		}

		return ret;
	}

	@Override
	public void logAudit(AuthzAuditEvent auditEvent) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.logAudit(" + auditEvent + ")");
		}

		if(auditEvent != null) {
			AuditProviderFactory.getAuditProvider().log(auditEvent);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.logAudit(" + auditEvent + ")");
		}
	}

	@Override
	public void logAudit(Collection<AuthzAuditEvent> auditEvents) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.logAudit(" + auditEvents + ")");
		}

		if(auditEvents != null) {
			for(AuthzAuditEvent auditEvent : auditEvents) {
				logAudit(auditEvent);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.logAudit(" + auditEvents + ")");
		}
	}


	/*
	public void init(String svcName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.init(" + svcName + ")");
		}

		ServiceManager    svcMgr = new ServiceManager();
		ServiceDefManager sdMgr  = new ServiceDefManager();

		RangerServiceDef   serviceDef = null;
		List<RangerPolicy> policies   = null;

		RangerService  service = svcMgr.getByName(svcName);

		if(service == null) {
			String msg = svcName + ": service not found";

			LOG.error(msg);

			throw new Exception(msg);
		} else {
			serviceDef = sdMgr.getByName(service.getType());

			if(serviceDef == null) {
				String msg = service.getType() + ": service-def not found";

				LOG.error(msg);

				throw new Exception(msg);
			}

			policies = svcMgr.getPolicies(service.getId());

			if(LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyEngineImpl.init(): found " + (policyEvaluators == null ? 0 : policyEvaluators.size()) + " policies in service '" + svcName + "'");
			}
		}

		setPolicies(serviceDef, policies);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.init(" + svcName + ")");
		}
	}
	*/

	public String getResourceName(RangerResource resource) {
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

	public String getResourceValueAsString(RangerResource resource) {
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

	protected RangerAccessResult isAccessAllowedNoAudit(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + ")");
		}

		RangerAccessResult ret = new RangerAccessResult();

		if(request != null) {
			if(CollectionUtils.isEmpty(request.getAccessTypes())) {
				ret.setAccessTypeResult(RangerPolicyEngine.ACCESS_ANY, new RangerAccessResult.ResultDetail());
			} else {
				for(String accessType : request.getAccessTypes()) {
					ret.setAccessTypeResult(accessType, new RangerAccessResult.ResultDetail());
				}
			}

			List<RangerPolicyEvaluator> evaluators = policyEvaluators;

			if(evaluators != null) {
				for(RangerPolicyEvaluator evaluator : evaluators) {
					evaluator.evaluate(request, ret);

					if(ret.isAllAllowedAndAudited()) {
						break;
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + "): " + ret);
		}

		return ret;
	}

	private RangerPolicyEvaluator getPolicyEvaluator(RangerPolicy policy, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getPolicyEvaluator(" + policy + "," + serviceDef + ")");
		}

		RangerPolicyEvaluator ret = null;

		ret = new RangerDefaultPolicyEvaluator(); // TODO: configurable evaluator class?

		ret.init(policy, serviceDef);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getPolicyEvaluator(" + policy + "," + serviceDef + "): " + ret);
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
		sb.append("RangerPolicyEngineImpl={");

		sb.append("policyEvaluators={");
		if(policyEvaluators != null) {
			for(RangerPolicyEvaluator policyEvaluator : policyEvaluators) {
				if(policyEvaluator != null) {
					sb.append(policyEvaluator).append(" ");
				}
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}
}
