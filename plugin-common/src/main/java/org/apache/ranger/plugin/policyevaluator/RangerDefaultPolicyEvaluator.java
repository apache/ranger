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

package org.apache.ranger.plugin.policyevaluator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerResource;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;


public class RangerDefaultPolicyEvaluator extends RangerAbstractPolicyEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerDefaultPolicyEvaluator.class);

	private List<ResourceDefMatcher> matchers = null;

	@Override
	public void init(RangerPolicy policy, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.init()");
		}

		super.init(policy, serviceDef);

		this.matchers = new ArrayList<ResourceDefMatcher>();

		if(policy != null && policy.getResources() != null) {
			for(Map.Entry<String, RangerPolicyResource> e : policy.getResources().entrySet()) {
				String               resourceName   = e.getKey();
				RangerPolicyResource policyResource = e.getValue();
				RangerResourceDef    resourceDef    = getResourceDef(resourceName);

				RangerResourceMatcher matcher = createResourceMatcher(resourceDef, policyResource);

				if(matcher != null) {
					matchers.add(new ResourceDefMatcher(resourceDef, matcher));
				} else {
					// TODO: ERROR: no matcher found for resourceName
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.init()");
		}
	}

	@Override
	public void evaluate(RangerAccessRequest request, RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
		}

		RangerPolicy policy = getPolicy();

		if(policy != null && request != null && result != null) {
			if(matchResource(request.getResource())) {
				for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
					for(String accessType : request.getAccessTypes()) {
						RangerPolicyItemAccess access = getAccess(policyItem, accessType);

						if(access == null) {
							continue;
						}

						RangerAccessResult.ResultDetail accessResult = result.getAccessTypeResult(accessType);
						
						if(accessResult.isAllowed() && accessResult.isAudited()) {
							continue;
						}

						if(!accessResult.isAudited() && policy.getIsAuditEnabled()) {
							accessResult.setIsAudited(true);
						}

						if(matchUserGroup(policyItem, request.getUser(), request.getUserGroups())) {
							if(matchCustomConditions(policyItem, request)) {
								if(!accessResult.isAllowed() && access.getIsAllowed()) {
									accessResult.setIsAllowed(true);
									accessResult.setPolicyId(policy.getId());
								}
							}
						}

						if(result.isAllAllowedAndAudited()) {
							break;
						}
					}

					if(result.isAllAllowedAndAudited()) {
						break;
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
		}
	}

	protected boolean matchResource(RangerResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.matchResource(" + resource + ")");
		}

		boolean ret = false;

		if(matchers != null && !matchers.isEmpty()) {
			ret = true;

			for(ResourceDefMatcher matcher : matchers) {
				 String resourceName  = matcher.getResourceName();
				 String resourceValue = resource.getValue(resourceName);

				 if(resourceValue != null) {
					 ret = matcher.isMatch(resourceValue);
				 }

				 if(! ret) {
					 break;
				 }
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.matchResource(" + resource + "): " + ret);
		}

		return ret;
	}

	protected boolean matchUserGroup(RangerPolicyItem policyItem, String user, Collection<String> groups) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.matchUserGroup(" + policyItem + ", " + user + ", " + groups + ")");
		}

		boolean ret = false;

		if(policyItem != null) {
			if(!ret && user != null && policyItem.getUsers() != null) {
				ret = policyItem.getUsers().contains(user);
			}
	
			if(!ret && groups != null && policyItem.getGroups() != null) {
				ret = policyItem.getGroups().contains(RangerPolicyEngine.GROUP_PUBLIC) ||
						!Collections.disjoint(policyItem.getGroups(), groups);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.matchUserGroup(" + policyItem + ", " + user + ", " + groups + "): " + ret);
		}

		return ret;
	}

	protected boolean matchCustomConditions(RangerPolicyItem policyItem, RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.matchCustomConditions(" + policyItem + ", " + request + ")");
		}

		boolean ret = false;

		// TODO:
		ret = true;

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.matchCustomConditions(" + policyItem + ", " + request + "): " + ret);
		}

		return ret;
	}

	protected RangerPolicyItemAccess getAccess(RangerPolicyItem policyItem, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.getAccess(" + policyItem + ", " + accessType + ")");
		}

		RangerPolicyItemAccess ret = null;

		if(policyItem != null && accessType != null && policyItem.getAccesses() != null) {
			for(RangerPolicyItemAccess access : policyItem.getAccesses()) {
				if(StringUtils.equalsIgnoreCase(accessType, access.getType())) {
					ret = access;

					break;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.getAccess(" + policyItem + ", " + accessType + "): " + ret);
		}

		return ret;
	}

	protected RangerResourceDef getResourceDef(String resourceName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.getResourceDef(" + resourceName + ")");
		}

		RangerResourceDef ret = null;

		RangerServiceDef serviceDef = getServiceDef();

		if(serviceDef != null && resourceName != null) {
			for(RangerResourceDef resourceDef : serviceDef.getResources()) {
				if(StringUtils.equalsIgnoreCase(resourceName, resourceDef.getName())) {
					ret = resourceDef;

					break;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.getResourceDef(" + resourceName + "): " + ret);
		}

		return ret;
	}

	protected RangerResourceMatcher createResourceMatcher(RangerResourceDef resourceDef, RangerPolicyResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.createResourceMatcher(" + resourceDef + ", " + resource + ")");
		}

		RangerResourceMatcher ret = null;

		String clsName = resourceDef != null ? resourceDef.getMatcher() : null;
		String options = resourceDef != null ? resourceDef.getMatcherOptions() : null;

		if(StringUtils.isEmpty(clsName)) {
			ret = new RangerDefaultResourceMatcher();
		} else {
			try {
				@SuppressWarnings("unchecked")
				Class<RangerResourceMatcher> matcherClass = (Class<RangerResourceMatcher>)Class.forName(clsName);

				ret = matcherClass.newInstance();
			} catch(ClassNotFoundException excp) {
				// TODO: ERROR
				excp.printStackTrace();
			} catch (InstantiationException excp) {
				// TODO: ERROR
				excp.printStackTrace();
			} catch (IllegalAccessException excp) {
				// TODO: ERROR
				excp.printStackTrace();
			}
		}

		if(ret != null) {
			ret.init(resource,  options);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.createResourceMatcher(" + resourceDef + ", " + resource + "): " + ret);
		}

		return ret;
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerDefaultPolicyEvaluator={");
		
		super.toString(sb);

		sb.append("matchers={");
		if(matchers != null) {
			for(ResourceDefMatcher matcher : matchers) {
				sb.append("{");
				matcher.toString(sb);
				sb.append("} ");
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}
	
	class ResourceDefMatcher {
		RangerResourceDef     resourceDef     = null;
		RangerResourceMatcher resourceMatcher = null;

		ResourceDefMatcher(RangerResourceDef resourceDef, RangerResourceMatcher resourceMatcher) {
			this.resourceDef     = resourceDef;
			this.resourceMatcher = resourceMatcher;
		}
		
		String getResourceName() {
			return resourceDef.getName();
		}

		boolean isMatch(String value) {
			return resourceMatcher.isMatch(value);
		}

		boolean isMatch(Collection<String> values) {
			boolean ret = false;

			if(values == null || values.isEmpty()) {
				ret = resourceMatcher.isMatch(null);
			} else {
				for(String value : values) {
					ret = resourceMatcher.isMatch(value);

					if(! ret) {
						break;
					}
				}
			}

			return ret;
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("resourceDef={").append(resourceDef).append("} ");
			sb.append("resourceMatcher={").append(resourceMatcher).append("} ");

			return sb;
		}
	}
}
