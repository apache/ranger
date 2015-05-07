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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.store.ServiceStore;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class RangerServiceDefValidator extends RangerValidator {

	private static final Log LOG = LogFactory.getLog(RangerServiceDefValidator.class);

	public RangerServiceDefValidator(ServiceStore store) {
		super(store);
	}

	public void validate(final RangerServiceDef serviceDef, final Action action) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.validate(%s, %s)", serviceDef, action));
		}

		List<ValidationFailureDetails> failures = new ArrayList<ValidationFailureDetails>();
		boolean valid = isValid(serviceDef, action, failures);
		String message = "";
		try {
			if (!valid) {
				message = serializeFailures(failures);
				throw new Exception(message);
			}
		} finally {
			if(LOG.isDebugEnabled()) {
				LOG.debug(String.format("<== RangerServiceDefValidator.validate(%s, %s): %s, reason[%s]", serviceDef, action, valid, message));
			}
		}
	}

	boolean isValid(final Long id, final Action action, final List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceDefValidator.isValid(" + id + ")");
		}

		boolean valid = true;
		if (action != Action.DELETE) {
			failures.add(new ValidationFailureDetailsBuilder()
				.isAnInternalError()
				.becauseOf("unsupported action[" + action + "]; isValid(Long) is only supported for DELETE")
				.build());
			valid = false;
		} else if (id == null) {
			failures.add(new ValidationFailureDetailsBuilder()
				.field("id")
				.isMissing()
				.build());
			valid = false;
		} else if (getServiceDef(id) == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No service found for id[" + id + "]! ok!");
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceDefValidator.isValid(" + id + "): " + valid);
		}
		return valid;
	}
	
	boolean isValid(final RangerServiceDef serviceDef, final Action action, final List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceDefValidator.isValid(" + serviceDef + ")");
		}
		
		if (!(action == Action.CREATE || action == Action.UPDATE)) {
			throw new IllegalArgumentException("isValid(RangerServiceDef, ...) is only supported for CREATE/UPDATE");
		}
		boolean valid = true;
		if (serviceDef == null) {
			String message = "service def object passed in was null";
			LOG.debug(message);
			failures.add(new ValidationFailureDetailsBuilder()
				.field("service def")
				.isMissing()
				.becauseOf(message)
				.build());
			valid = false;
		} else {
			Long id = serviceDef.getId();
			valid = isValidServiceDefId(id, action, failures) && valid;
			valid = isValidServiceDefName(serviceDef.getName(), id, action, failures) && valid;
			valid = isValidAccessTypes(serviceDef.getAccessTypes(), failures) && valid;
			if (isValidResources(serviceDef, failures)) {
				// Semantic check of resource graph can only be done if resources are "syntactically" valid
				valid = isValidResourceGraph(serviceDef, failures) && valid;
			} else {
				valid = false;
			}
			List<RangerEnumDef> enumDefs = serviceDef.getEnums();
			if (isValidEnums(enumDefs, failures)) {
				// config def validation requires valid enums
				valid = isValidConfigs(serviceDef.getConfigs(), enumDefs, failures) && valid;
			} else {
				valid = false;
			}
			valid = isValidPolicyConditions(serviceDef.getPolicyConditions(), failures) && valid;
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceDefValidator.isValid(" + serviceDef + "): " + valid);
		}
		return valid;
	}

	boolean isValidServiceDefId(Long id, final Action action, final List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidServiceDefId(%s, %s, %s)", id, action, failures));
		}
		boolean valid = true;

		if (action == Action.UPDATE) { // id is ignored for CREATE
			if (id == null) {
				String message = "service def id was null/empty/blank"; 
				LOG.debug(message);
				failures.add(new ValidationFailureDetailsBuilder()
					.field("id")
					.isMissing()
					.becauseOf(message)
					.build());
				valid = false;
			} else if (getServiceDef(id) == null) {
				failures.add(new ValidationFailureDetailsBuilder()
					.field("id")
					.isSemanticallyIncorrect()
					.becauseOf("no service def exists with id[" + id +"]")
					.build());
				valid = false;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidServiceDefId(%s, %s, %s): %s", id, action, failures, valid));
		}
		return valid;
	}
	
	boolean isValidServiceDefName(String name, Long id, final Action action, final List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidServiceDefName(%s, %s, %s, %s)", name, id, action, failures));
		}
		boolean valid = true;

		if (StringUtils.isBlank(name)) {
			String message = "service def name[" + name + "] was null/empty/blank"; 
			LOG.debug(message);
			failures.add(new ValidationFailureDetailsBuilder()
				.field("name")
				.isMissing()
				.becauseOf(message)
				.build());
			valid = false;
		} else {
			RangerServiceDef otherServiceDef = getServiceDef(name);
			if (otherServiceDef != null && action == Action.CREATE) {
				failures.add(new ValidationFailureDetailsBuilder()
					.field("name")
					.isSemanticallyIncorrect()
					.becauseOf("service def with the name[" + name + "] already exists")
					.build());
				valid = false;
			} else if (otherServiceDef != null && !Objects.equals(id, otherServiceDef.getId())) {
				failures.add(new ValidationFailureDetailsBuilder()
					.field("id/name")
					.isSemanticallyIncorrect()
					.becauseOf("id/name conflict: another service def already exists with name[" + name + "], its id is [" + otherServiceDef.getId() + "]")
					.build());
				valid = false;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidServiceDefName(%s, %s, %s, %s): %s", name, id, action, failures, valid));
		}
		return valid;
	}
	
	boolean isValidAccessTypes(final List<RangerAccessTypeDef> accessTypeDefs, final List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidAccessTypes(%s, %s)", accessTypeDefs, failures));
		}
		
		boolean valid = true;
		if (CollectionUtils.isEmpty(accessTypeDefs)) {
			failures.add(new ValidationFailureDetailsBuilder()
				.field("access types")
				.isMissing()
				.becauseOf("access types collection was null/empty")
				.build());
			valid = false;
		} else {
			List<RangerAccessTypeDef> defsWithImpliedGrants = new ArrayList<RangerAccessTypeDef>();
			Set<String> accessNames = new HashSet<String>();
			Set<Long> ids = new HashSet<Long>();
			for (RangerAccessTypeDef def : accessTypeDefs) {
				String name = def.getName();
				valid = isUnique(name, accessNames, "access type name", "access types", failures) && valid;
				valid = isUnique(def.getId(), ids, "access type id", "access types", failures) && valid;
				if (CollectionUtils.isNotEmpty(def.getImpliedGrants())) {
					defsWithImpliedGrants.add(def);
				}
			}
			// validate implied grants
			for (RangerAccessTypeDef def : defsWithImpliedGrants) {
				Collection<String> impliedGrants = getImpliedGrants(def);
				Set<String> unknownAccessTypes = Sets.difference(Sets.newHashSet(impliedGrants), accessNames);
				if (!unknownAccessTypes.isEmpty()) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("implied grants")
						.subField(unknownAccessTypes.iterator().next())  // we return just on item here.  Message has all unknow items
						.isSemanticallyIncorrect()
						.becauseOf("implied grant[" + impliedGrants + "] contains an unknown access types[" + unknownAccessTypes + "]")
						.build());
					valid = false;
				}
				// implied grant should not imply itself! 
				String name = def.getName(); // note: this name could be null/blank/empty!
				if (impliedGrants.contains(name)) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("implied grants")
						.subField(name)
						.isSemanticallyIncorrect()
						.becauseOf("implied grants list [" + impliedGrants + "] for access type[" + name + "] contains itself")
						.build());
					valid = false;
				}
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidAccessTypes(%s, %s): %s", accessTypeDefs, failures, valid));
		}
		return valid;
	}

	boolean isValidPolicyConditions(List<RangerPolicyConditionDef> policyConditions, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidPolicyConditions(%s, %s)", policyConditions, failures));
		}
		boolean valid = true;

		if (CollectionUtils.isEmpty(policyConditions)) {
			LOG.debug("Configs collection was null/empty! ok");
		} else {
			Set<Long> ids = new HashSet<Long>();
			Set<String> names = new HashSet<String>();
			for (RangerPolicyConditionDef conditionDef : policyConditions) {
				valid = isUnique(conditionDef.getId(), ids, "policy condition def id", "policy condition defs", failures) && valid;
				String name = conditionDef.getName();
				valid = isUnique(name, names, "policy condition def name", "policy condition defs", failures) && valid;
				if (StringUtils.isBlank(conditionDef.getEvaluator())) {
					String reason = String.format("evaluator on policy condition definition[%s] was null/empty!", name);
					LOG.debug(reason);
					failures.add(new ValidationFailureDetailsBuilder()
						.field("policy condition def evaluator")
						.subField(name)
						.isMissing()
						.becauseOf(reason)
						.build());
					valid = false;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidPolicyConditions(%s, %s): %s", policyConditions, failures, valid));
		}
		return valid;
	}

	boolean isValidConfigs(List<RangerServiceConfigDef> configs, List<RangerEnumDef> enumDefs, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidConfigs(%s, %s, %s)", configs, enumDefs, failures));
		}
		boolean valid = true;

		if (CollectionUtils.isEmpty(configs)) {
			LOG.debug("Configs collection was null/empty! ok");
		} else {
			Set<Long> ids = new HashSet<Long>(configs.size());
			Set<String> names = new HashSet<String>(configs.size());
			for (RangerServiceConfigDef aConfig : configs) {
				valid = isUnique(aConfig.getId(), ids, "config def id", "config defs", failures) && valid;
				String configName = aConfig.getName();
				valid = isUnique(configName, names, "config def name", "config defs", failures) && valid;
				String type = aConfig.getType();
				valid = isValidConfigType(type, configName, failures) && valid;
				if ("enum".equals(type)) {
					valid = isValidConfigOfEnumType(aConfig, enumDefs, failures) && valid;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidConfigs(%s, %s, %s): %s", configs, enumDefs, failures, valid));
		}
		return valid;
	}
	
	boolean isValidConfigOfEnumType(RangerServiceConfigDef configDef, List<RangerEnumDef> enumDefs, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidConfigOfEnumType(%s, %s, %s)", configDef, enumDefs, failures));
		}
		boolean valid = true;

		if (!"enum".equals(configDef.getType())) {
			LOG.debug("ConfigDef wasn't of enum type!");
		} else {
			Map<String, RangerEnumDef> enumDefsMap = getEnumDefMap(enumDefs);
			Set<String> enumTypes = enumDefsMap.keySet();
			String subType = configDef.getSubType();
			String configName = configDef.getName();
			
			if (!enumTypes.contains(subType)) {
				String reason = String.format("subtype[%s] of service def config[%s] was not among defined enums[%s]", subType, configName, enumTypes);
				failures.add(new ValidationFailureDetailsBuilder()
					.field("config def subtype")
					.subField(configName)
					.isSemanticallyIncorrect()
					.becauseOf(reason)
					.build());
				valid = false;
			} else {
				// default value check is possible only if sub-type is correctly configured
				String defaultValue = configDef.getDefaultValue();
				if (StringUtils.isNotBlank(defaultValue)) {
					RangerEnumDef enumDef = enumDefsMap.get(subType);
					Set<String> enumValues = getEnumValues(enumDef);
					if (!enumValues.contains(defaultValue)) {
						String reason = String.format("default value[%s] of service def config[%s] was not among the valid values[%s] of enums[%s]", defaultValue, configName, enumValues, subType);
						failures.add(new ValidationFailureDetailsBuilder()
							.field("config def default value")
							.subField(configName)
							.isSemanticallyIncorrect()
							.becauseOf(reason)
							.build());
						valid = false;
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidConfigOfEnumType(%s, %s, %s): %s", configDef, enumDefs, failures, valid));
		}
		return valid;
	}
	
	boolean isValidConfigType(String type, String configName, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidConfigType(%s, %s, %s)", type, configName, failures));
		}
		boolean valid = true;

		Set<String> validTypes = ImmutableSet.of("bool", "enum", "int", "string", "password", "path");
		if (StringUtils.isBlank(type)) {
			String reason = String.format("type of service def config[%s] was null/empty", configName);
			failures.add(new ValidationFailureDetailsBuilder()
				.field("config def type")
				.subField(configName)
				.isMissing()
				.becauseOf(reason)
				.build());
			valid = false;
		} else if (!validTypes.contains(type)) {
			String reason = String.format("type[%s] of service def config[%s] is not among valid types: %s", type, configName, validTypes);
			failures.add(new ValidationFailureDetailsBuilder()
				.field("config def type")
				.subField(configName)
				.isSemanticallyIncorrect()
				.becauseOf(reason)
				.build());
			valid = false;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidConfigType(%s, %s, %s): %s", type, configName, failures, valid));
		}
		return valid;
	}

	boolean isValidResources(RangerServiceDef serviceDef, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidResources(%s, %s)", serviceDef, failures));
		}
		boolean valid = true;

		List<RangerResourceDef> resources = serviceDef.getResources();
		if (CollectionUtils.isEmpty(resources)) {
			String reason = "service def resources collection was null/empty";
			failures.add(new ValidationFailureDetailsBuilder()
					.field("resources")
					.isMissing()
					.becauseOf(reason)
					.build());
			valid = false;
		} else {
			Set<String> names = new HashSet<String>(resources.size());
			Set<Long> ids = new HashSet<Long>(resources.size());
			for (RangerResourceDef resource : resources) {
				/*
				 * While id is the natural key, name is a surrogate key.  At several places code expects resource name to be unique within a service.
				 */
				valid = isUnique(resource.getName(), names, "resource name", "resources", failures) && valid;
				valid = isUnique(resource.getId(), ids, "resource id", "resources", failures) && valid;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidResources(%s, %s): %s", serviceDef, failures, valid));
		}
		return valid;
	}
	
	boolean isValidResourceGraph(RangerServiceDef serviceDef, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidResourceGraph(%s, %s)", serviceDef, failures));
		}
		boolean valid = true;
		// We don't want this helper to get into the cache or to use what is in the cache!!
		RangerServiceDefHelper defHelper = _factory.createServiceDefHelper(serviceDef, false);
		if (!defHelper.isResourceGraphValid()) {
			failures.add(new ValidationFailureDetailsBuilder()
				.field("resource graph")
				.isSemanticallyIncorrect()
				.becauseOf("Resource graph implied by various resources, e.g. parent value is invalid.  Valid graph must forest (union of disjoint trees).")
				.build());
			valid = false;
		}
		// resource level should be unique within a hierarchy
		Set<List<RangerResourceDef>> hierarchies = defHelper.getResourceHierarchies();
		for (List<RangerResourceDef> aHierarchy : hierarchies) {
			Set<Integer> levels = new HashSet<Integer>(aHierarchy.size());
			for (RangerResourceDef resourceDef : aHierarchy) {
				valid = isUnique(resourceDef.getLevel(), levels, "resource level", "resources", failures) && valid;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidResourceGraph(%s, %s): %s", serviceDef, failures, valid));
		}
		return valid;
	}
	
	boolean isValidEnums(List<RangerEnumDef> enumDefs, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidEnums(%s, %s)", enumDefs, failures));
		}
		
		boolean valid = true;
		if (CollectionUtils.isEmpty(enumDefs)) {
			LOG.debug("enum def collection passed in was null/empty. Ok.");
		} else {
			Set<String> names = new HashSet<String>();
			Set<Long> ids = new HashSet<Long>();
			for (RangerEnumDef enumDef : enumDefs) {
				if (enumDef == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("enum def")
						.isMissing()
						.becauseOf("An enum def in enums collection is null")
						.build());
					valid = false;
				} else {
					// enum-names and ids must non-blank and be unique to a service definition
					String enumName = enumDef.getName(); 
					valid = isUnique(enumName, names, "enum def name", "enum defs", failures) && valid;
					valid = isUnique(enumDef.getId(), ids, "enum def id", "enum defs", failures) && valid;		
					// enum must contain at least one valid value and those values should be non-blank and distinct
					if (CollectionUtils.isEmpty(enumDef.getElements())) {
						failures.add(new ValidationFailureDetailsBuilder()
							.field("enum values")
							.subField(enumName)
							.isMissing()
							.becauseOf("enum [" + enumName + "] does not have any elements")
							.build());
						valid = false;
					} else {
						valid = isValidEnumElements(enumDef.getElements(), failures, enumName) && valid;
						// default index should be valid
						int defaultIndex = getEnumDefaultIndex(enumDef);
						if (defaultIndex < 0 || defaultIndex >= enumDef.getElements().size()) { // max index is one less than the size of the elements list
							failures.add(new ValidationFailureDetailsBuilder()
								.field("enum default index")
								.subField(enumName)
								.isSemanticallyIncorrect()
								.becauseOf("default index[" + defaultIndex + "] for enum [" + enumName + "] is invalid")
								.build());
							valid = false;
						}
					}
				}
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidEnums(%s, %s): %s", enumDefs, failures, valid));
		}
		return valid;
	}

	boolean isValidEnumElements(List<RangerEnumElementDef> enumElementsDefs, List<ValidationFailureDetails> failures, String enumName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidEnumElements(%s, %s)", enumElementsDefs, failures));
		}
		
		boolean valid = true;
		if (CollectionUtils.isEmpty(enumElementsDefs)) {
			LOG.debug("Enum elements list passed in was null/empty!");
		} else {
			// enum element names should be valid and distinct
			Set<String> elementNames = new HashSet<String>();
			Set<Long> ids = new HashSet<Long>();
			for (RangerEnumElementDef elementDef : enumElementsDefs) {
				if (elementDef == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("enum element")
						.subField(enumName)
						.isMissing()
						.becauseOf("An enum element in enum element collection of enum [" + enumName + "] is null")
						.build());
					valid = false;
				} else {
					valid = isUnique(elementDef.getName(), enumName, elementNames, "enum element name", "enum elements", failures) && valid;
					valid = isUnique(elementDef.getId(), enumName, ids, "enum element id", "enum elements", failures) && valid;
				}
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidEnumElements(%s, %s): %s", enumElementsDefs, failures, valid));
		}
		return valid;
	}
}
