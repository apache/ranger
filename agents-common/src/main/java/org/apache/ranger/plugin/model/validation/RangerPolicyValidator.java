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

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.store.ServiceStore;

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
			failures.add(new RangerPolicyValidationErrorBuilder()
				.isAnInternalError()
				.becauseOf("method signature isValid(Long) is only supported for DELETE")
				.errorCode(ErrorCode.InternalError_InvalidMethodInvocation)
				.build());
			valid = false;
		} else if (id == null) {
			failures.add(new RangerPolicyValidationErrorBuilder()
				.becauseOf("policy id was null/missing")
				.field("id")
				.isMissing()
				.errorCode(ErrorCode.Missing_PolicyId_Delete)
				.build());
			valid = false;
		} else if (getPolicy(id) == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No policy found for id[" + id + "]! ok!");
			}
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
			failures.add(new RangerPolicyValidationErrorBuilder()
				.field("policy")
				.isMissing()
				.becauseOf(message)
				.errorCode(ErrorCode.Missing_PolicyObject)
				.build());
			valid = false;
		} else {
			Long id = policy.getId();
			if (action == Action.UPDATE) { // id is ignored for CREATE
				if (id == null) {
					String message = "policy id was null/empty/blank"; 
					LOG.debug(message);
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("id")
						.isMissing()
						.becauseOf(message)
						.errorCode(ErrorCode.Missing_PolicyId_Update)
						.build());
					valid = false;
				} else if (getPolicy(id) == null) {
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("id")
						.isSemanticallyIncorrect()
						.becauseOf("Invalid policy id provided for update: no policy found for id[" + id + "]")
						.errorCode(ErrorCode.Invalid_PolicyId)
						.build());
					valid = false;
				}
			}
			String policyName = policy.getName();
			String serviceName = policy.getService();
			if (StringUtils.isBlank(policyName)) {
				String message = "policy name was null/empty/blank[" + policyName + "]";
				LOG.debug(message);
				failures.add(new RangerPolicyValidationErrorBuilder()
					.field("name")
					.isMissing()
					.becauseOf(message)
					.errorCode(ErrorCode.Missing_PolicyName)
					.build());
				valid = false;
			} else {
				List<RangerPolicy> policies = getPolicies(serviceName, policyName);
				if (CollectionUtils.isNotEmpty(policies)) {
					if (policies.size() > 1) {
						failures.add(new RangerPolicyValidationErrorBuilder()
							.field("name")
							.isAnInternalError()
							.becauseOf("multiple policies found with the name[" + policyName + "]")
							.errorCode(ErrorCode.InternalError_Data_MultiplePoliciesSameName)
							.build());
						valid = false;
					} else if (action == Action.CREATE) { // size == 1
						failures.add(new RangerPolicyValidationErrorBuilder()
							.field("policy name")
							.isSemanticallyIncorrect()
							.becauseOf("A policy already exists with name[" + policyName + "] for service[" + serviceName + "]; its id is[" + policies.iterator().next().getId() + "]")
							.errorCode(ErrorCode.Duplicate_PolicyName_Create)
							.build());
						valid = false;
					} else if (!policies.iterator().next().getId().equals(id)) { // size == 1 && action == UPDATE
						failures.add(new RangerPolicyValidationErrorBuilder()
							.field("id/name")
							.isSemanticallyIncorrect()
							.errorCode(ErrorCode.Duplicate_PolicyName_Update)
							.becauseOf("id/name conflict: another policy already exists with name[" + policyName + "], its id is[" + policies.iterator().next().getId() + "]")
							.build());
						valid = false;
					}
				}
			}
			RangerService service = null;
			boolean serviceNameValid = false;
			if (StringUtils.isBlank(serviceName)) {
				failures.add(new RangerPolicyValidationErrorBuilder()
					.field("service name")
					.isMissing()
					.errorCode(ErrorCode.Missing_ServiceName)
					.becauseOf("service name was null/empty/blank")
					.build());
				valid = false;
			} else {
				service = getService(serviceName);
				if (service == null) {
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("service name")
						.isSemanticallyIncorrect()
						.becauseOf("no service found with name[" + serviceName + "]")
						.errorCode(ErrorCode.Invalid_ServiceName)
						.build());
					valid = false;
				} else {
					serviceNameValid = true;
				}
			}
			List<RangerPolicyItem> policyItems = policy.getPolicyItems();
			boolean isAuditEnabled = getIsAuditEnabled(policy);
			RangerServiceDef serviceDef = null;
			String serviceDefName = null;
			if (CollectionUtils.isEmpty(policyItems) && !isAuditEnabled) {
				failures.add(new RangerPolicyValidationErrorBuilder()
					.field("policy items")
					.isMissing()
					.becauseOf("at least one policy item must be specified if audit isn't enabled")
					.errorCode(ErrorCode.Missing_PolicyItems)
					.build());
				valid = false;
			} else if (service != null) {
				serviceDefName = service.getType();
				serviceDef = getServiceDef(serviceDefName);
				if (serviceDef == null) {
					String message = String.format("Service def[%s] of policy's service[%s] does not exist!", serviceDefName, serviceName);
					LOG.debug(message);
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("policy service def")
						.isAnInternalError()
						.becauseOf(message)
						.errorCode(ErrorCode.InternalError_Data_MissingServiceDef)
						.build());
					valid = false;
				} else {
					valid = isValidPolicyItems(policyItems, failures, serviceDef) && valid;
				}
			}
			if (serviceNameValid) { // resource checks can't be done meaningfully otherwise
				valid = isValidResources(policy, failures, action, isAdmin, serviceDef) && valid;
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValid(%s, %s, %s, %s): %s", policy, action, isAdmin, failures, valid));
		}
		return valid;
	}
	
	boolean isValidResources(RangerPolicy policy, final List<ValidationFailureDetails> failures, Action action, 
			boolean isAdmin, final RangerServiceDef serviceDef) {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValidResources(%s, %s, %s, %s, %s)", policy, failures, action, isAdmin, serviceDef));
		}
		
		boolean valid = true;
		Map<String, RangerPolicyResource> resourceMap = policy.getResources();
		if (resourceMap != null) { // following checks can't be done meaningfully otherwise
			valid = isPolicyResourceUnique(policy, failures, action) && valid;
			if (serviceDef != null) { // following checks can't be done meaningfully otherwise
				valid = isValidResourceNames(policy, failures, serviceDef) && valid;
				valid = isValidResourceValues(resourceMap, failures, serviceDef) && valid;
				valid = isValidResourceFlags(resourceMap, failures, serviceDef.getResources(), serviceDef.getName(), policy.getName(), isAdmin) && valid;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValidResources(%s, %s, %s, %s, %s): %s", policy, failures, action, isAdmin, serviceDef, valid));
		}
		return valid;
	}
	
	boolean isPolicyResourceUnique(RangerPolicy policy, final List<ValidationFailureDetails> failures, Action action) {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isPolicyResourceUnique(%s, %s, %s)", policy, failures, action));
		}

		boolean valid = true;
		if (!Boolean.TRUE.equals(policy.getIsEnabled())) {
			LOG.debug("Policy is disabled. Skipping resource uniqueness validation.");
		} else {
			RangerPolicyResourceSignature policySignature = _factory.createPolicyResourceSignature(policy);
			String signature = policySignature.getSignature();
			List<RangerPolicy> policies = getPoliciesForResourceSignature(policy.getService(), signature);
			if (CollectionUtils.isNotEmpty(policies)) {
				RangerPolicy matchedPolicy = policies.iterator().next();
				// there shouldn't be a matching policy for create.  During update only match should be to itself
				if (action == Action.CREATE || (action == Action.UPDATE && (policies.size() > 1 || !matchedPolicy.getId().equals(policy.getId())))) {
					String message = String.format("another policy[%s] with matching resources[%s] exists for service[%s]!",
							matchedPolicy.getName(), matchedPolicy.getResources(), policy.getService());
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("resources")
						.isSemanticallyIncorrect()
						.becauseOf(message)
						.errorCode(ErrorCode.Duplicate_PolicyResource)
						.build());
					valid = false;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isPolicyResourceUnique(%s, %s, %s): %s", policy, failures, action, valid));
		}
		return valid;
	}
	
	boolean isValidResourceNames(final RangerPolicy policy, final List<ValidationFailureDetails> failures, final RangerServiceDef serviceDef) {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerPolicyValidator.isValidResourceNames(%s, %s, %s)", policy, failures, serviceDef));
		}

		boolean valid = true;
		Set<String> policyResources = getPolicyResources(policy);

		RangerServiceDefHelper defHelper = new RangerServiceDefHelper(serviceDef);
		Set<List<RangerResourceDef>> hierarchies = defHelper.getResourceHierarchies(); // this can be empty but not null!
		if (hierarchies.isEmpty()) {
			LOG.warn("RangerPolicyValidator.isValidResourceNames: serviceDef does not have any resource hierarchies, possibly due to a old/migrated service def!  Skipping this check!");
		} else {
			/*
			 * A policy is for a single hierarchy however, it doesn't specify which one.  So we have to guess which hierarchy(s) it possibly be for.  First, see if the policy could be for 
			 * any of the known hierarchies?  A candidate hierarchy is one whose resource levels are a superset of those in the policy.
			 * Why?  What we want to catch at this stage is policies that straddles multiple hierarchies, e.g. db, udf and column for a hive policy.
			 * This has the side effect of catch spurious levels specified on the policy, e.g. having a level "blah" on a hive policy.  
			 */
			Set<List<RangerResourceDef>> candidateHierarchies = filterHierarchies_hierarchyHasAllPolicyResources(policyResources, hierarchies, defHelper);
			if (candidateHierarchies.isEmpty()) {
				// let's build a helpful message for user
				String message = String.format("policy resources %s are not compatible with any resource hierarchy for service def[%s]! Valid hierarchies are: %s",
						policyResources.toString(), serviceDef.getName(), toStringHierarchies_all(hierarchies, defHelper));
				failures.add(new RangerPolicyValidationErrorBuilder()
					.field("policy resources")
					.subField("incompatible")
					.isSemanticallyIncorrect()
					.becauseOf(message)
					.errorCode(ErrorCode.Invalid_PolicyResource_NoCompatibleHierarchy)
					.build());
				valid = false;
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("isValidResourceNames: Found [" + candidateHierarchies.size() + "] compatible hierarchies: " + toStringHierarchies_all(candidateHierarchies, defHelper));
				}
				/*
				 * Among the candidate hierarchies there should be at least one for which policy specifies all of the mandatory resources.  Note that there could be multiple
				 * hierarchies that meet that criteria, e.g. a hive policy that specified only DB.  It is not clear if it belongs to DB->UDF or DB->TBL->COL hierarchy.
				 * However, if both UDF and TBL were required then we can detect that policy does not specify mandatory levels for any of the candidate hierarchies.
				 */
				Set<List<RangerResourceDef>> validHierarchies = filterHierarchies_mandatoryResourcesSpecifiedInPolicy(policyResources, candidateHierarchies, defHelper);
				if (validHierarchies.isEmpty()) {
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("policy resources")
						.subField("missing mandatory")
						.isSemanticallyIncorrect()
						.errorCode(ErrorCode.Invalid_PolicyResource_MissingMandatory)
						.becauseOf("policy is missing required resources. Mandatory resources of potential hierarchies are: " + toStringHierarchies_mandatory(candidateHierarchies, defHelper))
						.build());
					valid = false;
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("isValidResourceNames: Found hierarchies with all mandatory fields specified: " + toStringHierarchies_mandatory(validHierarchies, defHelper));
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValidResourceNames(%s, %s, %s): %s", policy, failures, serviceDef, valid));
		}
		return valid;
	}
	
	/**
	 * String representation of mandatory resources of all the hierarchies suitable of showing to user.  Mandatory resources within a hierarchy are not ordered per the hierarchy. 
	 * @param hierarchies
	 * @param defHelper
	 * @return
	 */
	String toStringHierarchies_mandatory(Set<List<RangerResourceDef>> hierarchies, RangerServiceDefHelper defHelper) {

		// helper function skipping sanity checks of getting null arguments passed
		StringBuilder builder = new StringBuilder();
		for (List<RangerResourceDef> aHierarchy : hierarchies) {
			builder.append(defHelper.getMandatoryResourceNames(aHierarchy));
			builder.append(" ");
		}
		return builder.toString();
	}
	
	/**
	 * String representation of all resources of all hierarchies.  Resources within a hierarchy are ordered per the hierarchy.
	 * @param hierarchies
	 * @param defHelper
	 * @return
	 */
	String toStringHierarchies_all(Set<List<RangerResourceDef>> hierarchies, RangerServiceDefHelper defHelper) {

		// helper function skipping sanity checks of getting null arguments passed
		StringBuilder builder = new StringBuilder();
		for (List<RangerResourceDef> aHierarchy : hierarchies) {
			builder.append(defHelper.getAllResourceNamesOrdered(aHierarchy));
			builder.append(" ");
		}
		return builder.toString();
	}
	/**
	 * Returns the subset of all hierarchies that are a superset of the policy's resources. 
	 * @param policyResources
	 * @param hierarchies
	 * @return
	 */
	Set<List<RangerResourceDef>> filterHierarchies_hierarchyHasAllPolicyResources(Set<String> policyResources, Set<List<RangerResourceDef>> hierarchies, RangerServiceDefHelper defHelper) {
		
		// helper function skipping sanity checks of getting null arguments passed
		Set<List<RangerResourceDef>> result = new HashSet<List<RangerResourceDef>>(hierarchies.size());
		for (List<RangerResourceDef> aHierarchy : hierarchies) {
			Set<String> hierarchyResources = defHelper.getAllResourceNames(aHierarchy);
			if (hierarchyResources.containsAll(policyResources)) {
				result.add(aHierarchy);
			}
		}
		return result;
	}
	
	/**
	 * Returns the subset of hierarchies all of whose mandatory resources were found in policy's resource set.  candidate hierarchies are expected to have passed 
	 * <code>filterHierarchies_hierarchyHasAllPolicyResources</code> check first.
	 * @param policyResources
	 * @param hierarchies
	 * @param defHelper
	 * @return
	 */
	Set<List<RangerResourceDef>> filterHierarchies_mandatoryResourcesSpecifiedInPolicy(Set<String> policyResources, Set<List<RangerResourceDef>> hierarchies, RangerServiceDefHelper defHelper) {
		
		// helper function skipping sanity checks of getting null arguments passed
		Set<List<RangerResourceDef>> result = new HashSet<List<RangerResourceDef>>(hierarchies.size());
		for (List<RangerResourceDef> aHierarchy : hierarchies) {
			Set<String> mandatoryResources = defHelper.getMandatoryResourceNames(aHierarchy);
			if (policyResources.containsAll(mandatoryResources)) {
				result.add(aHierarchy);
			}
		}
		return result;
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
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("resource-def")
						.isAnInternalError()
						.errorCode(ErrorCode.InternalError_Data_NullResourceDef)
						.becauseOf("a resource-def on resource def collection of service-def[" + serviceDefName + "] was null")
						.build());
					valid = false;
				} else if (StringUtils.isBlank(resourceDef.getName())) {
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("resource-def-name")
						.isAnInternalError()
						.errorCode(ErrorCode.InternalError_Data_NullResourceDefName)
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
							failures.add(new RangerPolicyValidationErrorBuilder()
								.field("isExcludes")
								.subField(resourceName)
								.isSemanticallyIncorrect()
								.errorCode(ErrorCode.Invalid_Excludes_NotSupported)
								.becauseOf("isExcludes specified as [" + policyResourceIsExcludes + "] for resource [" + resourceName + "] which doesn't support isExcludes")
								.build());
							valid = false;
						}
						if (policyResourceIsExcludes && !isAdmin) {
							failures.add(new RangerPolicyValidationErrorBuilder()
								.field("isExcludes")
								.subField("isAdmin")
								.isSemanticallyIncorrect()
								.becauseOf("isExcludes specified as [" + policyResourceIsExcludes + "] for resource [" + resourceName + "].  Insufficient permissions to create excludes policy.")
								.errorCode(ErrorCode.Invalid_Excludes_RequiresAdmin)
								.build());
							valid = false;
						}
						boolean recursiveSupported = Boolean.TRUE.equals(resourceDef.getRecursiveSupported());
						boolean policyIsRecursive = Boolean.TRUE.equals(policyResource.getIsRecursive());
						if (policyIsRecursive && !recursiveSupported) {
							failures.add(new RangerPolicyValidationErrorBuilder()
								.field("isRecursive")
								.subField(resourceName)
								.isSemanticallyIncorrect()
								.becauseOf("isRecursive specified as [" + policyIsRecursive + "] for resource [" + resourceName + "] which doesn't support isRecursive")
								.errorCode(ErrorCode.Invalid_Recursive_NotSupported)
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
						String message = String.format("Value[%s] of resource[%s] does not conform to the validation regex[%s] defined on the service-def[%s]", aValue, name, regEx, serviceDef.getName());
						LOG.debug(message);
						failures.add(new RangerPolicyValidationErrorBuilder()
							.field("resource-values")
							.subField(name)
							.isSemanticallyIncorrect()
							.becauseOf(message)
							.errorCode(ErrorCode.Invalid_ResourceValue_RegEx)
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
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("policy item")
						.isMissing()
						.becauseOf("policy items object was null")
						.errorCode(ErrorCode.InternalError_Data_NullPolicyItem)
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
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("policy item accesses")
						.isMissing()
						.becauseOf("policy items accesses collection was null")
						.errorCode(ErrorCode.Missing_PolicyItemAccesses)
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
				failures.add(new RangerPolicyValidationErrorBuilder()
					.field("policy item users/user-groups")
					.isMissing()
					.becauseOf("both users and user-groups collections on the policy item were null/empty")
					.errorCode(ErrorCode.Missing_PolicyItemUserGroup)
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
					failures.add(new RangerPolicyValidationErrorBuilder()
						.field("policy item access")
						.isMissing()
						.becauseOf("policy items access object was null")
						.errorCode(ErrorCode.InternalError_Data_NullPolicyItemAccess)
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
				failures.add(new RangerPolicyValidationErrorBuilder()
					.field("policy item access type")
					.isMissing()
					.becauseOf("policy items access type's name was null/empty/blank")
					.errorCode(ErrorCode.Missing_PolicyItemAccessType)
					.build());
				valid = false;
			} else if (!accessTypes.contains(accessType.toLowerCase())) {
				String message = String.format("access type[%s] not among valid types for service[%s]", accessType, accessTypes);
				LOG.debug(message);
				failures.add(new RangerPolicyValidationErrorBuilder()
					.field("policy item access type")
					.isSemanticallyIncorrect()
					.becauseOf(message)
					.errorCode(ErrorCode.Invalid_PolicyItemAccessType)
					.build());
				valid = false;
			}
			Boolean isAllowed = access.getIsAllowed();
			// it can be null (which is treated as allowed) but not false
			if (isAllowed != null && isAllowed == false) {
				String message = "access type is set to deny.  Currently deny access types are not supported.";
				LOG.debug(message);
				failures.add(new RangerPolicyValidationErrorBuilder()
					.field("policy item access type allowed")
					.isSemanticallyIncorrect()
					.becauseOf(message)
					.errorCode(ErrorCode.Invalid_PolicyItemAccessType_Deny)
					.build());
				valid = false;
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerPolicyValidator.isValidPolicyItemAccess(%s, %s, %s): %s", access, failures, accessTypes, valid));
		}
		return valid;
	}

	static class RangerPolicyValidationErrorBuilder extends ValidationFailureDetailsBuilder {

		@Override
		ValidationFailureDetails build() {
			return new RangerPolicyValidationFailure(_errorCode, _fieldName, _subFieldName, _missing, _semanticError, _internalError, _reason);
		}
	}

	static class RangerPolicyValidationFailure extends  ValidationFailureDetails {

		public RangerPolicyValidationFailure(int errorCode, String fieldName, String subFieldName, boolean missing, boolean semanticError, boolean internalError, String reason) {
			super(errorCode, fieldName, subFieldName, missing, semanticError, internalError, reason);
		}

		// TODO remove and move to baseclass when all 3 move to new message framework
		@Override
		public String toString() {
			LOG.debug("RangerPolicyValidationFailure.toString");
			String result = null;
			if (_ErrorCode2MessageTemplate.containsKey(_errorCode)) {
				Integer templateId = _ErrorCode2MessageTemplate.get(_errorCode);
				if (templateId != null && _TemplateId2Template.containsKey(templateId)) {
					String messageTemplate = _TemplateId2Template.get(templateId);
					if (StringUtils.isNotBlank(messageTemplate)) {
						// in the worst case result should be at least same as the messageTemplate which we know is not blank
						result = substituteVariables(messageTemplate);
					} else {
						LOG.warn("Internal error: Message template string for template [" + templateId + "] was empty!");
					}
				} else {
					LOG.warn("Internal error: template id for error code [" + templateId + "] was null or template id to message map did not comtain the templateid");
				}
			} else {
				LOG.warn("Internal error: error code [" + _errorCode + "] not found in errorcode to message template map");
			}
			if (result == null) {
				result = super.toString();
			}
			return "Policy validation failure: " + result;
		}
	}

	static class ErrorCode {
		public static final int InternalError_InvalidMethodInvocation 					= 1001;
		public static final int Missing_PolicyId_Delete 								= 1002;
		public static final int Missing_PolicyObject 									= 1003;
		public static final int Missing_PolicyId_Update 								= 1004;
		public static final int Invalid_PolicyId 										= 1005;
		public static final int Missing_PolicyName 										= 1006;
		public static final int InternalError_Data_MultiplePoliciesSameName 			= 1007;
		public static final int Duplicate_PolicyName_Create 							= 1008;
		public static final int Duplicate_PolicyName_Update 							= 1009;
		public static final int Missing_ServiceName 									= 1010;
		public static final int Invalid_ServiceName 									= 1011;
		public static final int Missing_PolicyItems 									= 1012;
		public static final int InternalError_Data_MissingServiceDef					= 1013;
		public static final int Duplicate_PolicyResource 								= 1014;
		public static final int Invalid_PolicyResource_NoCompatibleHierarchy 			= 1015;
		public static final int Invalid_PolicyResource_MissingMandatory					= 1016;
		public static final int InternalError_Data_NullResourceDef						= 1017;
		public static final int InternalError_Data_NullResourceDefName					= 1018;
		public static final int Invalid_Excludes_NotSupported							= 1019;
		public static final int Invalid_Excludes_RequiresAdmin							= 1020;
		public static final int Invalid_Recursive_NotSupported							= 1021;
		public static final int Invalid_ResourceValue_RegEx								= 1022;
		public static final int InternalError_Data_NullPolicyItem 						= 1023;
		public static final int Missing_PolicyItemAccesses 								= 1024;
		public static final int Missing_PolicyItemUserGroup 							= 1025;
		public static final int InternalError_Data_NullPolicyItemAccess					= 1026;
		public static final int Missing_PolicyItemAccessType							= 1027;
		public static final int Invalid_PolicyItemAccessType							= 1028;
		public static final int Invalid_PolicyItemAccessType_Deny						= 1029;
	}
	static class MessageId {
		public static final int InternalError 					= 1;
		public static final int MissingField 					= 2;
		public static final int InternalError_BadData 			= 3;
		public static final int DuplicateValue 					= 4;
		public static final int InvalidField 					= 5;
	}

	static Object[][] MessageTemplateData = new Object[][] {
			{ MessageId.InternalError,								"Internal error: {reason}."},
			{ MessageId.InternalError_BadData,						"Internal error: bad data encountered [{field}]: {reason}"},
			{ MessageId.MissingField,								"Missing Required field [{field}]: {reason}"},
			{ MessageId.InvalidField,								"Invalid value specified for field [{field}]: {reason}"},
			{ MessageId.DuplicateValue,								"Duplicate value for [{field}]: {reason}"},
	};
	static final Map<Integer, String> _TemplateId2Template = createMap(MessageTemplateData);

	static int[][] ErrorCode2MessageTemplateData = new int[][] {
			{ ErrorCode.InternalError_InvalidMethodInvocation,					MessageId.InternalError},
			{ ErrorCode.Missing_PolicyId_Delete,								MessageId.MissingField},
			{ ErrorCode.Missing_PolicyObject,									MessageId.InternalError},
			{ ErrorCode.Missing_PolicyId_Update,								MessageId.MissingField},
			{ ErrorCode.Invalid_PolicyId,										MessageId.InvalidField},
			{ ErrorCode.Missing_PolicyName,										MessageId.MissingField},
			{ ErrorCode.InternalError_Data_MultiplePoliciesSameName,			MessageId.InternalError_BadData},
			{ ErrorCode.Duplicate_PolicyName_Create,							MessageId.DuplicateValue},
			{ ErrorCode.Duplicate_PolicyName_Update,							MessageId.DuplicateValue},
			{ ErrorCode.Missing_ServiceName,									MessageId.MissingField},
			{ ErrorCode.Invalid_ServiceName,									MessageId.InvalidField},
			{ ErrorCode.Missing_PolicyItems,									MessageId.MissingField},
			{ ErrorCode.InternalError_Data_MissingServiceDef,                   MessageId.InternalError_BadData},
			{ ErrorCode.Duplicate_PolicyResource,								MessageId.DuplicateValue},
			{ ErrorCode.Invalid_PolicyResource_NoCompatibleHierarchy,			MessageId.InvalidField},
			{ ErrorCode.Invalid_PolicyResource_MissingMandatory,				MessageId.MissingField},
			{ ErrorCode.InternalError_Data_NullResourceDef,						MessageId.InternalError_BadData},
			{ ErrorCode.InternalError_Data_NullResourceDefName,					MessageId.InternalError_BadData},
			{ ErrorCode.Invalid_Excludes_NotSupported,							MessageId.InvalidField},
			{ ErrorCode.Invalid_Excludes_RequiresAdmin,							MessageId.InvalidField},
			{ ErrorCode.Invalid_Recursive_NotSupported,							MessageId.InvalidField},
			{ ErrorCode.Invalid_ResourceValue_RegEx,							MessageId.InvalidField},
			{ ErrorCode.InternalError_Data_NullPolicyItem,						MessageId.InternalError_BadData},
			{ ErrorCode.Missing_PolicyItemAccesses,								MessageId.MissingField},
			{ ErrorCode.Missing_PolicyItemUserGroup,							MessageId.MissingField},
			{ ErrorCode.InternalError_Data_NullPolicyItemAccess,				MessageId.InternalError_BadData},
			{ ErrorCode.Missing_PolicyItemAccessType,							MessageId.MissingField},
			{ ErrorCode.Invalid_PolicyItemAccessType,							MessageId.InvalidField},
			{ ErrorCode.Invalid_PolicyItemAccessType_Deny,						MessageId.InvalidField},

	};
	static final Map<Integer, Integer> _ErrorCode2MessageTemplate = createMap(ErrorCode2MessageTemplateData);

}
