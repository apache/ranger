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
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.*;

public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private final RangerPolicyRepository policyRepository;
	private final RangerPolicyRepository tagPolicyRepository;
	
	private List<RangerContextEnricher> allContextEnrichers;

	public RangerPolicyEngineImpl(String appId, ServicePolicies servicePolicies, RangerPolicyEngineOptions options) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl(" + appId + ", " + servicePolicies + ", " + options + ")");
		}

		if (options == null) {
			options = new RangerPolicyEngineOptions();
		}

		policyRepository = new RangerPolicyRepository(appId, servicePolicies, options);

		ServicePolicies.TagPolicies tagPolicies = servicePolicies.getTagPolicies();

		if (!options.disableTagPolicyEvaluation
				&& tagPolicies != null
				&& !StringUtils.isEmpty(tagPolicies.getServiceName())
				&& tagPolicies.getServiceDef() != null
				&& !CollectionUtils.isEmpty(tagPolicies.getPolicies())) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyEngineImpl : Building tag-policy-repository for tag-service " + tagPolicies.getServiceName());
			}

			tagPolicyRepository = new RangerPolicyRepository(appId, tagPolicies, options, servicePolicies.getServiceDef(), servicePolicies.getServiceName());

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

		// TODO: run through evaluator in tagPolicyRepository as well
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
			if (hasTagPolicies()) {
				isAccessAllowedForTagPolicies(request, ret);

				if (LOG.isDebugEnabled()) {
					if (ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
						LOG.debug("RangerPolicyEngineImpl.isAccessAllowedNoAudit() - access and audit determined by tag policy. No resource policies will be evaluated, request=" + request + ", result=" + ret);
					}
				}
			}

			if (!ret.getIsAccessDetermined() || !ret.getIsAuditedDetermined()) {
				if (hasResourcePolicies()) {
					boolean foundInCache = policyRepository.setAuditEnabledFromCache(request, ret);
					RangerPolicyEvaluator allowedEvaluator = null;

					List<RangerPolicyEvaluator> evaluators = policyRepository.getPolicyEvaluators();
					for (RangerPolicyEvaluator evaluator : evaluators) {
						evaluator.evaluate(request, ret);

						if(allowedEvaluator == null && ret.getIsAllowed()) {
							allowedEvaluator = evaluator;
						}

						// stop once isAccessDetermined==true && isAuditedDetermined==true
						if(ret.getIsAuditedDetermined()) {
							if(ret.getIsAccessDetermined() || (allowedEvaluator != null && !evaluator.hasDeny())) {
								break;			// Break out of policy-evaluation loop for this tag
							}
						}
					}

					if(!ret.getIsAccessDetermined() && allowedEvaluator != null) {
						ret.setIsAllowed(true);
						ret.setPolicyId(allowedEvaluator.getPolicy().getId());
						ret.setIsAccessDetermined(true);
					}

					if (!foundInCache) {
						policyRepository.storeAuditEnabledInCache(request, ret);
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + "): " + ret);
		}

		return ret;
	}

	protected RangerAccessResult isAccessAllowedForTagPolicies(final RangerAccessRequest request, RangerAccessResult result) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowedForTagPolicies(" + request + ")");
		}

		List<RangerPolicyEvaluator> evaluators = tagPolicyRepository.getPolicyEvaluators();

		if (CollectionUtils.isNotEmpty(evaluators)) {
			List<RangerTag> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

			if(CollectionUtils.isNotEmpty(tags)) {
				boolean                   someTagAllowedAudit = false;
				RangerAccessResult        savedAccessResult   = createAccessResult(request);
				List<RangerTagAuditEvent> tagAuditEvents      = new ArrayList<RangerTagAuditEvent>();

				for (RangerTag tag : tags) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: Evaluating policies for tag (" + tag.getType() + ")");
					}

					RangerAccessRequest   tagEvalRequest   = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
					RangerAccessResult    tagEvalResult    = createAccessResult(tagEvalRequest);
					RangerPolicyEvaluator allowedEvaluator = null;

					for (RangerPolicyEvaluator evaluator : evaluators) {
						evaluator.evaluate(tagEvalRequest, tagEvalResult);

						if(allowedEvaluator == null && tagEvalResult.getIsAllowed()) {
							allowedEvaluator = evaluator;
						}

						if(tagEvalResult.getIsAuditedDetermined()) {
							if(tagEvalResult.getIsAccessDetermined() || (allowedEvaluator != null && !evaluator.hasDeny())) {
								if (LOG.isDebugEnabled()) {
									LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: concluding eval of tag (" + tag.getType() + ") with authorization=" + tagEvalResult.getIsAllowed());
								}
								break;			// Break out of policy-evaluation loop for this tag
							}
						}
					}

					if(!tagEvalResult.getIsAccessDetermined() && allowedEvaluator != null) {
						tagEvalResult.setIsAllowed(true);
						tagEvalResult.setPolicyId(allowedEvaluator.getPolicy().getId());
						tagEvalResult.setIsAccessDetermined(true);
					}

					if (tagEvalResult.getIsAuditedDetermined()) {
						someTagAllowedAudit = true;
						// And generate an audit event
						if (tagEvalResult.getIsAccessDetermined()) {
							RangerTagAuditEvent event = new RangerTagAuditEvent(tag.getType(), tagEvalResult);
							tagAuditEvents.add(event);
						}
					}

					if (tagEvalResult.getIsAccessDetermined()) {
						savedAccessResult.setAccessResultFrom(tagEvalResult);

						if (!tagEvalResult.getIsAllowed()) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: concluding eval of tag-policies as tag (" + tag.getType() + "), tag-policy-id=" + tagEvalResult.getPolicyId() + " denied access.");
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
				LOG.debug("RangerPolicyEngineImpl.setResourceServiceDef(): Cannot set ServiceDef in RangerTagResourceMap.");
			}
		}
	}

	private boolean hasTagPolicies() {
		return tagPolicyRepository != null && CollectionUtils.isNotEmpty(tagPolicyRepository.getPolicies());
	}

	private boolean hasResourcePolicies() {
		return policyRepository != null && CollectionUtils.isNotEmpty(policyRepository.getPolicies());
	}

	@Override
	public boolean preCleanup() {

		boolean ret = true;
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.preCleanup()");
		}

		if (CollectionUtils.isNotEmpty(allContextEnrichers)) {
			for (RangerContextEnricher contextEnricher : allContextEnrichers) {
				boolean notReadyForCleanup = contextEnricher.preCleanup();
				if (!notReadyForCleanup) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("contextEnricher.preCleanup() failed for contextEnricher=" + contextEnricher.getName());
					}
					ret = false;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.preCleanup() : result=" + ret);
		}

		return ret;
	}

	@Override
	public void cleanup() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.cleanup()");
		}

		preCleanup();

		if (CollectionUtils.isNotEmpty(allContextEnrichers)) {
			for (RangerContextEnricher contextEnricher : allContextEnrichers) {
				contextEnricher.cleanup();
			}
		}

		this.allContextEnrichers = null;

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.cleanup()");
		}
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			cleanup();
		}
		finally {
			super.finalize();
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


	public RangerTagResource(String tagType, RangerServiceDef tagServiceDef) {
		super.setValue(KEY_TAG, tagType);
		super.setServiceDef(tagServiceDef);
	}
}

class RangerTagAccessRequest extends RangerAccessRequestImpl {
	public RangerTagAccessRequest(RangerTag resourceTag, RangerServiceDef tagServiceDef, RangerAccessRequest request) {
		super.setResource(new RangerTagResource(resourceTag.getType(), tagServiceDef));
		super.setUser(request.getUser());
		super.setUserGroups(request.getUserGroups());
		super.setAction(request.getAction());
		super.setAccessType(request.getAccessType());
		super.setAccessTime(request.getAccessTime());
		super.setRequestData(request.getRequestData());

		Map<String, Object> requestContext = request.getContext();

		RangerAccessRequestUtil.setCurrentTagInContext(request.getContext(), resourceTag);
		RangerAccessRequestUtil.setCurrentResourceInContext(request.getContext(), request.getResource());

		super.setContext(requestContext);

		super.setClientType(request.getClientType());
		super.setClientIPAddress(request.getClientIPAddress());
		super.setSessionId(request.getSessionId());
	}
}


class RangerTagAuditEvent {
	private final String tagType;
	private final RangerAccessResult result;

	RangerTagAuditEvent(String tagType, RangerAccessResult result) {
		this.tagType = tagType;
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

		sb.append("tagType={").append(this.tagType).append("} ");
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
