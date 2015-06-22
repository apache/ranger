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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerTaggedResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.*;

public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private final RangerPolicyRepository policyRepository;
	private final RangerPolicyRepository tagPolicyRepository;
	
	private final List<RangerContextEnricher> allContextEnrichers;

	public RangerPolicyEngineImpl(ServicePolicies servicePolicies, RangerPolicyEngineOptions options) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl(" + servicePolicies + ", " + options + ")");
		}

		if (options == null) {
			options = new RangerPolicyEngineOptions();
		}

		policyRepository = new RangerPolicyRepository(servicePolicies, options);

		ServicePolicies.TagPolicies tagPolicies = servicePolicies.getTagPolicies();

		if (!options.disableTagPolicyEvaluation
				&& tagPolicies != null
				&& !StringUtils.isEmpty(tagPolicies.getServiceName())
				&& tagPolicies.getServiceDef() != null
				&& !CollectionUtils.isEmpty(tagPolicies.getPolicies())) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyEngineImpl : Building tag-policy-repository for tag-service " + tagPolicies.getServiceName());
			}

			tagPolicyRepository = new RangerPolicyRepository(tagPolicies, options, servicePolicies.getServiceDef());

		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyEngineImpl : No tag-policy-repository for service " + servicePolicies.getServiceName());
			}
			tagPolicyRepository = null;
		}

		List<RangerContextEnricher> tmpList;

		List<RangerContextEnricher> tagContextEnrichers = tagPolicyRepository == null ? null :tagPolicyRepository.getContextEnrichers();
		List<RangerContextEnricher> resourceContextEnrichers = policyRepository.getContextEnrichers();

		if (CollectionUtils.isEmpty(tagContextEnrichers)) {
			tmpList = resourceContextEnrichers;
		} else if (CollectionUtils.isEmpty(resourceContextEnrichers)) {
			tmpList = tagContextEnrichers;
		} else {
			tmpList = new ArrayList<RangerContextEnricher>(tagContextEnrichers);
			tmpList.addAll(resourceContextEnrichers);
		}

		this.allContextEnrichers = tmpList;

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl()");
		}
	}

	@Override
	public String getServiceName() {
		return policyRepository.getServiceName();
	}

	@Override
	public RangerServiceDef getServiceDef() {
		return policyRepository.getServiceDef();
	}

	@Override
	public long getPolicyVersion() {
		return policyRepository.getPolicyVersion();
	}

	@Override
	public RangerAccessResult createAccessResult(RangerAccessRequest request) {
		return new RangerAccessResult(this.getServiceName(), policyRepository.getServiceDef(), request);
	}

	@Override
	public void preProcess(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.preProcess(" + request + ")");
		}

		setResourceServiceDef(request);

		List<RangerContextEnricher> enrichers = allContextEnrichers;

		if(!CollectionUtils.isEmpty(enrichers)) {
			for(RangerContextEnricher enricher : enrichers) {
				enricher.enrich(request);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.preProcess(" + request + ")");
		}
	}

	@Override
	public void preProcess(Collection<RangerAccessRequest> requests) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.preProcess(" + requests + ")");
		}

		if(CollectionUtils.isNotEmpty(requests)) {
			for(RangerAccessRequest request : requests) {
				setResourceServiceDef(request);
			}

			List<RangerContextEnricher> enrichers = allContextEnrichers;

			if(CollectionUtils.isNotEmpty(enrichers)) {
				for(RangerContextEnricher enricher : enrichers) {
					for(RangerAccessRequest request : requests) {
						enricher.enrich(request);
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.preProcess(" + requests + ")");
		}
	}

	@Override
	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + request + ")");
		}

		RangerAccessResult ret = isAccessAllowedNoAudit(request);

		if (resultProcessor != null) {
			resultProcessor.processResult(ret);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + request + "): " + ret);
		}

		return ret;
	}

	@Override
	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + requests + ")");
		}

		Collection<RangerAccessResult> ret = new ArrayList<RangerAccessResult>();

		if (requests != null) {
			for (RangerAccessRequest request : requests) {

				RangerAccessResult result = isAccessAllowedNoAudit(request);

				ret.add(result);
			}
		}

		if (resultProcessor != null) {
			resultProcessor.processResults(ret);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + requests + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isAccessAllowed(RangerAccessResource resource, String user, Set<String> userGroups, String accessType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + resource + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = false;

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			ret = evaluator.isAccessAllowed(resource, user, userGroups, accessType);

			if (ret) {
				break;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + resource + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}


	@Override
	public boolean isAccessAllowed(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = false;

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			ret = evaluator.isAccessAllowed(resources, user, userGroups, accessType);

			if (ret) {
				break;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerPolicy getExactMatchPolicy(RangerAccessResource resource) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getExactMatchPolicy(" + resource + ")");
		}

		RangerPolicy ret = null;

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			if (evaluator.isSingleAndExactMatch(resource)) {
				ret = evaluator.getPolicy();

				break;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getExactMatchPolicy(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getAllowedPolicies(String user, Set<String> userGroups, String accessType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getAllowedPolicies(" + user + ", " + userGroups + ", " + accessType + ")");
		}

		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			RangerPolicy policy = evaluator.getPolicy();

			boolean isAccessAllowed = isAccessAllowed(policy.getResources(), user, userGroups, accessType);

			if (isAccessAllowed) {
				ret.add(policy);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getAllowedPolicies(" + user + ", " + userGroups + ", " + accessType + "): policyCount=" + ret.size());
		}

		return ret;
	}

	protected RangerAccessResult isAccessAllowedNoAudit(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + ")");
		}

		RangerAccessResult ret = createAccessResult(request);

		if (ret != null && request != null) {

			if (tagPolicyRepository != null && CollectionUtils.isNotEmpty(tagPolicyRepository.getPolicies())) {

				RangerAccessResult tagAccessResult = isAccessAllowedForTagPolicies(request);

				if (tagAccessResult.getIsAccessDetermined() && tagAccessResult.getIsAuditedDetermined()) {

					if (LOG.isDebugEnabled()) {
						LOG.debug("RangerPolicyEngineImpl.isAccessAllowedNoAudit() - access and audit determined by tag policy. No resource policies will be evaluated, request=" + request + ", result=" + tagAccessResult);

						LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + "): " + tagAccessResult);
					}

					return tagAccessResult;
				}

				ret.setAccessResultFrom(tagAccessResult);
				ret.setAuditResultFrom(tagAccessResult);

			}

			List<RangerPolicyEvaluator> evaluators = policyRepository.getPolicyEvaluators();

			if (evaluators != null) {

				boolean foundInCache = policyRepository.setAuditEnabledFromCache(request, ret);

				for (RangerPolicyEvaluator evaluator : evaluators) {
					evaluator.evaluate(request, ret);

					// stop once isAccessDetermined==true && isAuditedDetermined==true
					if (ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
						break;
					}
				}

				if (!foundInCache) {
					policyRepository.storeAuditEnabledInCache(request, ret);
				}

			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + "): " + ret);
		}

		return ret;
	}

	protected RangerAccessResult isAccessAllowedForTagPolicies(final RangerAccessRequest request) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowedForTagPolicies(" + request + ")");
		}

		RangerAccessResult result = createAccessResult(request);

		Map<String, Object> context = request.getContext();
		Object contextObj;

		if (context != null && (contextObj = context.get(KEY_CONTEXT_TAGS)) != null) {

			@SuppressWarnings("unchecked")
			List<RangerTaggedResource.RangerResourceTag> resourceTags = (List<RangerTaggedResource.RangerResourceTag>) contextObj;

			List<RangerPolicyEvaluator> evaluators;

			if (!CollectionUtils.isEmpty(evaluators = tagPolicyRepository.getPolicyEvaluators())) {

				boolean someTagAllowedAudit = false;

				RangerAccessResult savedAccessResult = createAccessResult(request);

				List<RangerTagAuditEvent> tagAuditEvents = new ArrayList<RangerTagAuditEvent>();

				for (RangerTaggedResource.RangerResourceTag resourceTag : resourceTags) {

					if (LOG.isDebugEnabled()) {
						LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: Evaluating policies for tag (" + resourceTag.getName() + ")");
					}

					RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(resourceTag, tagPolicyRepository.getServiceDef(), request);

					RangerAccessResult tagEvalResult = createAccessResult(tagEvalRequest);

					for (RangerPolicyEvaluator evaluator : evaluators) {

						evaluator.evaluate(tagEvalRequest, tagEvalResult);

						if (tagEvalResult.getIsAccessDetermined() && tagEvalResult.getIsAuditedDetermined()) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: concluding eval of tag (" + resourceTag.getName() + ") with authorization=" + tagEvalResult.getIsAllowed());
							}
							break;			// Break out of policy-evaluation loop for this tag
						}
					}

					if (tagEvalResult.getIsAuditedDetermined()) {
						someTagAllowedAudit = true;
						// And generate an audit event
						if (tagEvalResult.getIsAccessDetermined()) {
							RangerTagAuditEvent event = new RangerTagAuditEvent(resourceTag.getName(), tagEvalResult);
							tagAuditEvents.add(event);
						}
					}

					if (tagEvalResult.getIsAccessDetermined()) {

						savedAccessResult.setAccessResultFrom(tagEvalResult);

						if (!tagEvalResult.getIsAllowed()) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: concluding eval of tag-policies as tag (" + resourceTag.getName() + "), tag-policy-id=" + tagEvalResult.getPolicyId() + " denied access.");
							}
							break;		// Break out of tags evaluation loop altogether
						}
					}
				}

				result.setAccessResultFrom(savedAccessResult);

				if (someTagAllowedAudit) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: at least one tag-policy requires generation of audit event");
					}
					result.setIsAudited(true);

					boolean isAccessDenied = result.getIsAccessDetermined() && !result.getIsAllowed();

					RangerTagAuditEvent.processTagEvents(tagAuditEvents, isAccessDenied);
					// Set processed list into result
					// result.setAuxilaryAuditInfo(tagAuditEvents);
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies() : result=" + result);
					LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies() : auditEventList=" + tagAuditEvents);
				}
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedForTagPolicies(" + result + ")" );
		}

		return result;
	}

	private void setResourceServiceDef(RangerAccessRequest request) {
		RangerAccessResource resource = request.getResource();

		if (resource.getServiceDef() == null) {
			if (resource instanceof RangerMutableResource) {
				RangerMutableResource mutable = (RangerMutableResource) resource;
				mutable.setServiceDef(getServiceDef());
			} else {
				LOG.debug("RangerPolicyEngineImpl.setResourceServiceDef(): Cannot set ServiceDef in RangerTaggedResource.");
			}
		}
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerPolicyEngineImpl={");

		sb.append("serviceName={").append(this.getServiceName()).append("} ");
		sb.append(policyRepository);

		sb.append("}");

		return sb;
	}
}
class RangerTagResource extends RangerAccessResourceImpl {
	private static final String KEY_TAG = "tag";


	public RangerTagResource(String tagName, RangerServiceDef tagServiceDef) {
		super.setValue(KEY_TAG, tagName);
		super.setServiceDef(tagServiceDef);
	}
}

class RangerTagAccessRequest extends RangerAccessRequestImpl {
	public RangerTagAccessRequest(RangerTaggedResource.RangerResourceTag resourceTag, RangerServiceDef tagServiceDef, RangerAccessRequest request) {
		super.setResource(new RangerTagResource(resourceTag.getName(), tagServiceDef));
		super.setUser(request.getUser());
		super.setUserGroups(request.getUserGroups());
		super.setAction(request.getAction());
		super.setAccessType(request.getAccessType());
		super.setAccessTime(request.getAccessTime());
		super.setRequestData(request.getRequestData());

		Map<String, Object> requestContext = request.getContext();

		requestContext.put(RangerPolicyEngine.KEY_CONTEXT_TAG_OBJECT, resourceTag);
		requestContext.put(RangerPolicyEngine.KEY_CONTEXT_RESOURCE, request.getResource());

		super.setContext(requestContext);

		super.setClientType(request.getClientType());
		super.setClientIPAddress(request.getClientIPAddress());
		super.setSessionId(request.getSessionId());
	}
}


class RangerTagAuditEvent {
	private final String tagName;
	private final RangerAccessResult result;

	RangerTagAuditEvent(String tagName, RangerAccessResult result) {
		this.tagName = tagName;
		this.result = result;
	}
	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public void toString(StringBuilder sb) {
		sb.append("RangerTagAuditEvent={");

		sb.append("tagName={").append(this.tagName).append("} ");
		sb.append("isAccessDetermined={").append(this.result.getIsAccessDetermined()).append("}");
		sb.append("isAllowed={").append(this.result.getIsAllowed()).append("}");
		sb.append("policyId={").append(this.result.getPolicyId()).append("}");
		sb.append("reason={").append(this.result.getReason()).append("}");

		sb.append("}");

	}

	static void processTagEvents(List<RangerTagAuditEvent> tagAuditEvents, final boolean deniedAccess) {
		// Process tagAuditEvents to delete unwanted events

		if (CollectionUtils.isEmpty(tagAuditEvents)) return;

		List<RangerTagAuditEvent> unwantedEvents = new ArrayList<RangerTagAuditEvent> ();
		if (deniedAccess) {
			for (RangerTagAuditEvent auditEvent : tagAuditEvents) {
				RangerAccessResult result = auditEvent.result;
				if (result.getIsAllowed()) {
					unwantedEvents.add(auditEvent);
				}
			}
			tagAuditEvents.removeAll(unwantedEvents);
		}
	}
}
