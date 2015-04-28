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

package org.apache.ranger.plugin.model.validation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.store.ServiceStore;

import com.google.common.collect.Sets;

public class RangerPolicyValidator extends RangerValidator {

	private static final Log LOG = LogFactory.getLog(RangerPolicyValidator.class);

	public RangerPolicyValidator(ServiceStore store) {
		super(store);
	}

	public void validate(RangerPolicy policy, Action action, boolean isAdmin) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.validate(%s, %s, %s)", policy, action, isAdmin));
		}

		List<ValidationFailureDetails> failures = new ArrayList<ValidationFailureDetails>();
		boolean valid = isValid(policy, action, isAdmin, failures);
		String message = "";
		try {
			if (!valid) {
				message = serializeFailures(failures);
				throw new Exception(message);
			}
		} finally {
			if(LOG.isDebugEnabled()) {
				LOG.debug(String.format("<== RangerPolicyValidator.validate(%s, %s, %s): %s, reason[%s]", policy, action, isAdmin, valid, message));
			}
		}
	}

	@Override
	boolean isValid(Long id, Action action, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValid(%s, %s, %s)", id, action, failures));
		}

		boolean valid = true;
		if (action != Action.DELETE) {
			failures.add(new ValidationFailureDetailsBuilder()
				.isAnInternalError()
				.becauseOf("isValid(Long) is only supported for DELETE")
				.build());
			valid = false;
		} else if (id == null) {
			failures.add(new ValidationFailureDetailsBuilder()
				.field("id")
				.isMissing()
				.build());
			valid = false;
		} else if (getPolicy(id) == null) {
			failures.add(new ValidationFailureDetailsBuilder()
				.field("id")
				.isSemanticallyIncorrect()
				.becauseOf("no policy found for id[" + id + "]")
				.build());
			valid = false;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValid(%s, %s, %s): %s", id, action, failures, valid));
		}
		return valid;
	}

	boolean isValid(RangerPolicy policy, Action action, boolean isAdmin, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValid(%s, %s, %s, %s)", policy, action, isAdmin, failures));
		}

		if (!(action == Action.CREATE || action == Action.UPDATE)) {
			throw new IllegalArgumentException("isValid(RangerPolicy, ...) is only supported for create/update");
		}
		boolean valid = true;
		if (policy == null) {
			String message = "policy object passed in was null";
			LOG.debug(message);
			failures.add(new ValidationFailureDetailsBuilder()
				.field("policy")
				.isMissing()
				.becauseOf(message)
				.build());
			valid = false;
		} else {
			Long id = policy.getId();
			if (action == Action.UPDATE) { // id is ignored for CREATE
				if (id == null) {
					String message = "policy id was null/empty/blank"; 
					LOG.debug(message);
					failures.add(new ValidationFailureDetailsBuilder()
						.field("id")
						.isMissing()
						.becauseOf(message)
						.build());
					valid = false;
				} else if (getPolicy(id) == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("id")
						.isSemanticallyIncorrect()
						.becauseOf("no policy exists with id[" + id +"]")
						.build());
					valid = false;
				}
			}
			String policyName = policy.getName();
			String serviceName = policy.getService();
			if (StringUtils.isBlank(policyName)) {
				String message = "policy name was null/empty/blank[" + policyName + "]"; 
				LOG.debug(message);
				failures.add(new ValidationFailureDetailsBuilder()
					.field("name")
					.isMissing()
					.becauseOf(message)
					.build());
				valid = false;
			} else {
				List<RangerPolicy> policies = getPolicies(serviceName, policyName);
				if (CollectionUtils.isNotEmpty(policies)) {
					if (policies.size() > 1) {
						failures.add(new ValidationFailureDetailsBuilder()
							.isAnInternalError()
							.becauseOf("multiple policies found with the name[" + policyName + "]")
							.build());
						valid = false;
					} else if (action == Action.CREATE) { // size == 1
						failures.add(new ValidationFailureDetailsBuilder()
							.field("name")
							.isSemanticallyIncorrect()
							.becauseOf("policy already exists with name[" + policyName + "]; its id is[" + policies.iterator().next().getId() + "]")
							.build());
						valid = false;
					} else if (!policies.iterator().next().getId().equals(id)) { // size == 1 && action == UPDATE
						failures.add(new ValidationFailureDetailsBuilder()
							.field("id/name")
							.isSemanticallyIncorrect()
							.becauseOf("id/name conflict: another policy already exists with name[" + policyName + "], its id is[" + policies.iterator().next().getId() + "]")
							.build());
						valid = false;
					}
				}
			}
			RangerService service = null;
			if (StringUtils.isBlank(serviceName)) {
				failures.add(new ValidationFailureDetailsBuilder()
				.field("service")
				.isMissing()
				.becauseOf("service name was null/empty/blank")
				.build());
				valid = false;
			} else {
				service = getService(serviceName);
				if (service == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("service")
						.isSemanticallyIncorrect()
						.becauseOf("service does not exist")
						.build());
					valid = false;
				}
			}
			List<RangerPolicyItem> policyItems = policy.getPolicyItems();
			boolean isAuditEnabled = getIsAuditEnabled(policy);
			RangerServiceDef serviceDef = null;
			String serviceDefName = null;
			if (CollectionUtils.isEmpty(policyItems) && !isAuditEnabled) {
				failures.add(new ValidationFailureDetailsBuilder()
					.field("policy items")
					.isMissing()
					.becauseOf("at least one policy item must be specified if audit isn't enabled")
					.build());
				valid = false;
			} else if (service != null) {
				serviceDefName = service.getType();
				serviceDef = getServiceDef(serviceDefName);
				if (serviceDef == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("policy service def")
						.isAnInternalError()
						.becauseOf("Service def of policies service does not exist")
						.build());
					valid = false;
				} else {
					valid = isValidPolicyItems(policyItems, failures, serviceDef) && valid;
				}
			}
			valid = isValidResources(policy, failures, action, isAdmin, serviceDef, serviceName) && valid;
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValid(%s, %s, %s, %s): %s", policy, action, isAdmin, failures, valid));
		}
		return valid;
	}
	
	boolean isValidResources(RangerPolicy policy, final List<ValidationFailureDetails> failures, Action action, boolean isAdmin, final RangerServiceDef serviceDef, final String serviceName) {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValidResources(%s, %s, %s, %s, %s, %s)", policy, failures, action, isAdmin, serviceDef, serviceName));
		}
		
		boolean valid = true;
		if (serviceDef != null) { // following checks can't be done meaningfully otherwise
//			TODO - disabled till a more robust fix for Hive resources definition can be found
//			valid = isValidResourceNames(policy, failures, serviceDef);
			Map<String, RangerPolicyResource> resourceMap = policy.getResources();
			if (resourceMap != null) { // following checks can't be done meaningfully otherwise
				valid = isValidResourceValues(resourceMap, failures, serviceDef) && valid;
				valid = isValidResourceFlags(resourceMap, failures, serviceDef.getResources(), serviceDef.getName(), policy.getName(), isAdmin) && valid;
			}
		}
		if (StringUtils.isNotBlank(serviceName)) { // resource uniqueness check cannot be done meaningfully otherwise
			valid = isPolicyResourceUnique(policy, failures, action, serviceName) && valid;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValidResources(%s, %s, %s, %s, %s, %s): %s", policy, failures, action, isAdmin, serviceDef, serviceName, valid));
		}
		return valid;
	}
	
	boolean isPolicyResourceUnique(RangerPolicy policy, final List<ValidationFailureDetails> failures, Action action, final String serviceName) {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isPolicyResourceUnique(%s, %s, %s, %s)", policy, failures, action, serviceName));
		}

		boolean foundDuplicate = false;
		RangerPolicyResourceSignature signature = _factory.createPolicyResourceSignature(policy);
		List<RangerPolicy> policies = getPolicies(serviceName, null);
		if (CollectionUtils.isNotEmpty(policies)) {
			Iterator<RangerPolicy> iterator = policies.iterator();
			while (iterator.hasNext() && !foundDuplicate) {
				RangerPolicy otherPolicy = iterator.next();
				if (otherPolicy.getId().equals(policy.getId()) && action == Action.UPDATE) {
					LOG.debug("isPolicyResourceUnique: Skipping self during update!");
				} else {
					RangerPolicyResourceSignature otherSignature = _factory.createPolicyResourceSignature(otherPolicy);
					if (signature.equals(otherSignature)) {
						foundDuplicate = true;
						failures.add(new ValidationFailureDetailsBuilder()
							.field("resources")
							.isSemanticallyIncorrect()
							.becauseOf("found another policy[" + otherPolicy.getName() + "] with matching resources[" + otherPolicy.getResources() + "]!")
							.build());
					}
				}
			}
		}

		boolean valid = !foundDuplicate;
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isPolicyResourceUnique(%s, %s, %s, %s): %s", policy, failures, action, serviceName, valid));
		}
		return valid;
	}

	boolean isValidResourceNames(final RangerPolicy policy, final List<ValidationFailureDetails> failures, final RangerServiceDef serviceDef) {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValidResourceNames(%s, %s, %s)", policy, failures, serviceDef));
		}

		boolean valid = true;
		Set<String> mandatoryResources = getMandatoryResourceNames(serviceDef);
		Set<String> policyResources = getPolicyResources(policy);
		Set<String> missingResources = Sets.difference(mandatoryResources, policyResources);
		if (!missingResources.isEmpty()) {
			failures.add(new ValidationFailureDetailsBuilder()
				.field("resources")
				.subField(missingResources.iterator().next()) // we return any one parameter!
				.isMissing()
				.becauseOf("required resources[" + missingResources + "] are missing")
				.build());
			valid = false;
		}
		Set<String> allResource = getAllResourceNames(serviceDef);
		Set<String> unknownResources = Sets.difference(policyResources, allResource);
		if (!unknownResources.isEmpty()) {
			failures.add(new ValidationFailureDetailsBuilder()
				.field("resources")
				.subField(unknownResources.iterator().next()) // we return any one parameter!
				.isSemanticallyIncorrect()
				.becauseOf("resource[" + unknownResources + "] is not valid for service-def[" + serviceDef.getName() + "]")
				.build());
			valid = false;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValidResourceNames(%s, %s, %s): %s", policy, failures, serviceDef, valid));
		}
		return valid;
	}
	
	boolean isValidResourceFlags(final Map<String, RangerPolicyResource> inputPolicyResources, final List<ValidationFailureDetails> failures,
			final List<RangerResourceDef> resourceDefs, final String serviceDefName, final String policyName, boolean isAdmin) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValidResourceFlags(%s, %s, %s, %s, %s, %s)", inputPolicyResources, failures, resourceDefs, serviceDefName, policyName, isAdmin));
		}

		boolean valid = true;
		if (resourceDefs == null) {
			LOG.debug("isValidResourceFlags: service Def is null");
		} else {
			Map<String, RangerPolicyResource> policyResources = getPolicyResourceWithLowerCaseKeys(inputPolicyResources);
			for (RangerResourceDef resourceDef : resourceDefs) {
				if (resourceDef == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("resource-def")
						.isAnInternalError()
						.becauseOf("a resource-def on resource def collection of service-def[" + serviceDefName + "] was null")
						.build());
					valid = false;
				} else if (StringUtils.isBlank(resourceDef.getName())) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("resource-def-name")
						.isAnInternalError()
						.becauseOf("name of a resource-def on resource def collection of service-def[" + serviceDefName + "] was null")
						.build());
					valid = false;
				} else {
					String resourceName = resourceDef.getName().toLowerCase();
					RangerPolicyResource policyResource = policyResources.get(resourceName);
					if (policyResource == null) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("a policy-resource object for resource[" + resourceName + "] on policy [" + policyName + "] was null");
						}
					} else {
						boolean excludesSupported = Boolean.TRUE.equals(resourceDef.getExcludesSupported()); // could be null
						boolean policyResourceIsExcludes = Boolean.TRUE.equals(policyResource.getIsExcludes()); // could be null
						if (policyResourceIsExcludes && !excludesSupported) {
							failures.add(new ValidationFailureDetailsBuilder()
								.field("isExcludes")
								.subField(resourceName)
								.isSemanticallyIncorrect()
								.becauseOf("isExcludes specified as [" + policyResourceIsExcludes + "] for resource [" + resourceName + "] which doesn't support isExcludes")
								.build());
							valid = false;
						}
						if (policyResourceIsExcludes && !isAdmin) {
							failures.add(new ValidationFailureDetailsBuilder()
								.field("isExcludes")
								.subField("isAdmin")
								.isSemanticallyIncorrect()
								.becauseOf("isExcludes specified as [" + policyResourceIsExcludes + "] for resource [" + resourceName + "].  Insufficient permissions to create excludes policy.")
								.build());
							valid = false;
						}
						boolean recursiveSupported = Boolean.TRUE.equals(resourceDef.getRecursiveSupported());
						boolean policyIsRecursive = Boolean.TRUE.equals(policyResource.getIsRecursive());
						if (policyIsRecursive && !recursiveSupported) {
							failures.add(new ValidationFailureDetailsBuilder()
								.field("isRecursive")
								.subField(resourceName)
								.isSemanticallyIncorrect()
								.becauseOf("isRecursive specified as [" + policyIsRecursive + "] for resource [" + resourceName + "] which doesn't support isRecursive")
								.build());
							valid = false;
						}
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValidResourceFlags(%s, %s, %s, %s, %s, %s): %s", inputPolicyResources, failures, resourceDefs, serviceDefName, policyName, isAdmin, valid));
		}
		return valid;
	}

	boolean isValidResourceValues(Map<String, RangerPolicyResource> resourceMap, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValidResourceValues(%s, %s, %s)", resourceMap, failures, serviceDef));
		}

		boolean valid = true;
		Map<String, String> validationRegExMap = getValidationRegExes(serviceDef);
		for (Map.Entry<String, RangerPolicyResource> entry : resourceMap.entrySet()) {
			String name = entry.getKey();
			RangerPolicyResource policyResource = entry.getValue();
			if (validationRegExMap.containsKey(name) && policyResource != null && CollectionUtils.isNotEmpty(policyResource.getValues())) {
				String regEx = validationRegExMap.get(name);
				for (String aValue : policyResource.getValues()) {
					if (StringUtils.isBlank(aValue)) {
						LOG.debug("resource value was blank");
					} else if (!aValue.matches(regEx)) {
						failures.add(new ValidationFailureDetailsBuilder()
							.field("resource-values")
							.subField(name)
							.isSemanticallyIncorrect()
							.becauseOf("resources value[" + aValue + "] does not match validation regex[" + regEx + "] defined on service-def[" + serviceDef.getName() + "]")
							.build());
						valid = false;
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValidResourceValues(%s, %s, %s): %s", resourceMap, failures, serviceDef, valid));
		}
		return valid;
	}

	boolean isValidPolicyItems(List<RangerPolicyItem> policyItems, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValid(%s, %s, %s)", policyItems, failures, serviceDef));
		}

		boolean valid = true;
		if (CollectionUtils.isEmpty(policyItems)) {
			LOG.debug("policy items collection was null/empty");
		} else {
			for (RangerPolicyItem policyItem : policyItems) {
				if (policyItem == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("policy item")
						.isMissing()
						.becauseOf("policy items object was null")
						.build());
					valid = false;
				} else {
					// we want to go through all elements even though one may be bad so all failures are captured
					valid = isValidPolicyItem(policyItem, failures, serviceDef) && valid;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValid(%s, %s, %s): %s", policyItems, failures, serviceDef, valid));
		}
		return valid;
	}

	boolean isValidPolicyItem(RangerPolicyItem policyItem, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValid(%s, %s, %s)", policyItem, failures, serviceDef));
		}
		
		boolean valid = true;
		if (policyItem == null) {
			LOG.debug("policy item was null!");
		} else {
			// access items collection can't be empty (unless delegated admin is true) and should be otherwise valid
			if (CollectionUtils.isEmpty(policyItem.getAccesses())) {
				if (!Boolean.TRUE.equals(policyItem.getDelegateAdmin())) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("policy item accesses")
						.isMissing()
						.becauseOf("policy items accesses collection was null")
						.build());
					valid = false;
				} else {
					LOG.debug("policy item collection was null but delegated admin is true. Ok");
				}
			} else {
				valid = isValidItemAccesses(policyItem.getAccesses(), failures, serviceDef) && valid;
			}
			// both users and user-groups collections can't be empty
			if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
				failures.add(new ValidationFailureDetailsBuilder()
					.field("policy item users/user-groups")
					.isMissing()
					.becauseOf("both users and user-groups collections on the policy item were null/empty")
					.build());
				valid = false;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValid(%s, %s, %s): %s", policyItem, failures, serviceDef, valid));
		}
		return valid;
	}

	boolean isValidItemAccesses(List<RangerPolicyItemAccess> accesses, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValid(%s, %s, %s)", accesses, failures, serviceDef));
		}
		
		boolean valid = true;
		if (CollectionUtils.isEmpty(accesses)) {
			LOG.debug("policy item accesses collection was null/empty!");
		} else {
			Set<String> accessTypes = getAccessTypes(serviceDef);
			for (RangerPolicyItemAccess access : accesses) {
				if (access == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("policy item access")
						.isMissing()
						.becauseOf("policy items access object was null")
						.build());
					valid = false;
				} else {
					// we want to go through all elements even though one may be bad so all failures are captured
					valid = isValidPolicyItemAccess(access, failures, accessTypes) && valid;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValid(%s, %s): %s", accesses, failures, serviceDef, valid));
		}
		return valid;
	}

	boolean isValidPolicyItemAccess(RangerPolicyItemAccess access, List<ValidationFailureDetails> failures, Set<String> accessTypes) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValidPolicyItemAccess(%s, %s, %s)", access, failures, accessTypes));
		}
		
		boolean valid = true;
		if (CollectionUtils.isEmpty(accessTypes)) { // caller should firewall this argument!
			LOG.debug("isValidPolicyItemAccess: accessTypes was null!");
		} else if (access == null) {
			LOG.debug("isValidPolicyItemAccess: policy item access was null!");
		} else {
			String accessType = access.getType();
			if (StringUtils.isBlank(accessType)) {
				failures.add(new ValidationFailureDetailsBuilder()
					.field("policy item access type")
					.isMissing()
					.becauseOf("policy items access type's name was null/empty/blank")
					.build());
				valid = false;
			} else if (!accessTypes.contains(accessType.toLowerCase())) {
				String message = String.format("access type[%s] not among valid types for service[%s]", accessType, accessTypes);
				LOG.debug(message);
				failures.add(new ValidationFailureDetailsBuilder()
					.field("policy item access type")
					.isSemanticallyIncorrect()
					.becauseOf(message)
					.build());
				valid = false;
			}
			Boolean isAllowed = access.getIsAllowed();
			// it can be null (which is treated as allowed) but not false
			if (isAllowed != null && isAllowed == false) {
				String message = "access type is set to deny.  Currently deny access types are not supported.";
				LOG.debug(message);
				failures.add(new ValidationFailureDetailsBuilder()
					.field("policy item access type allowed")
					.isSemanticallyIncorrect()
					.becauseOf(message)
					.build());
				valid = false;
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValidPolicyItemAccess(%s, %s, %s): %s", access, failures, accessTypes, valid));
		}
		return valid;
	}
}
