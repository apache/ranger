package org.apache.ranger.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.store.ServiceStore;

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
			failures.add(new ValidationFailureDetailsBuilder()
				.field("id")
				.isSemanticallyIncorrect()
				.becauseOf("no service def found for id[" + id + "]")
				.build());
			valid = false;
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
			// validate the service def name
			String name = serviceDef.getName();
			boolean nameSpecified = StringUtils.isNotBlank(name);
			if (!nameSpecified) {
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
				} else if (otherServiceDef != null && otherServiceDef.getId() !=null && otherServiceDef.getId() != id) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("id/name")
						.isSemanticallyIncorrect()
						.becauseOf("id/name conflict: another service def already exists with name[" + name + "], its id is [" + otherServiceDef.getId() + "]")
						.build());
					valid = false;
				}
			}
			if (CollectionUtils.isEmpty(serviceDef.getAccessTypes())) {
				failures.add(new ValidationFailureDetailsBuilder()
					.field("access types")
					.isMissing()
					.becauseOf("access types collection was null/empty")
					.build());
				valid = false;
			} else {
				valid = isValidAccessTypes(serviceDef.getAccessTypes(), failures) && valid;
			}
			if (CollectionUtils.isEmpty(serviceDef.getEnums())) {
				LOG.debug("No enums specified on the service def.  Ok!");
			} else {
				valid = isValidEnums(serviceDef.getEnums(), failures) && valid;
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceDefValidator.isValid(" + serviceDef + "): " + valid);
		}
		return valid;
	}

	boolean isValidAccessTypes(final List<RangerAccessTypeDef> accessTypeDefs, final List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidAccessTypes(%s, %s)", accessTypeDefs, failures));
		}
		
		boolean valid = true;
		if (CollectionUtils.isEmpty(accessTypeDefs)) {
			LOG.debug("access type def collection is empty/null");
		} else {
			List<RangerAccessTypeDef> defsWithImpliedGrants = new ArrayList<RangerAccessTypeDef>();
			Set<String> accessNames = new HashSet<String>();
			for (RangerAccessTypeDef def : accessTypeDefs) {
				String name = def.getName();
				// name can't be null/empty/blank
				if (StringUtils.isBlank(name)) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("access type name")
						.isMissing()
						.becauseOf("access type name[" + name + "] is null/empty")
						.build());
					valid = false;
				} else if (accessNames.contains(name.toLowerCase())) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("access type name")
						.subField(name)
						.isSemanticallyIncorrect()
						.becauseOf("duplicate access type names in access types collection: [" + name + "]")
						.build());
					valid = false;
				} else {
					accessNames.add(name.toLowerCase()); // we have a new unique access type
				}
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

	boolean isValidEnums(List<RangerEnumDef> enumDefs, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidEnums(%s, %s)", enumDefs, failures));
		}
		
		boolean valid = true;
		if (CollectionUtils.isEmpty(enumDefs)) {
			LOG.debug("enum def collection passed in was null/empty");
		} else {
			Set<String> enumNames = new HashSet<String>();
			for (RangerEnumDef enumDef : enumDefs) {
				if (enumDef == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("enum def")
						.isMissing()
						.becauseOf("An enum def in enums collection is null")
						.build());
					valid = false;
				} else {
					// enum-names must non-blank and be unique to a service definition
					String enumName = enumDef.getName();
					if (StringUtils.isBlank(enumName)) {
						failures.add(new ValidationFailureDetailsBuilder()
							.field("enum def name")
							.isMissing()
							.becauseOf("enum name [" + enumName + "] is null/empty")
							.build());
						valid = false;
					} else if (enumNames.contains(enumName.toLowerCase())) {
						failures.add(new ValidationFailureDetailsBuilder()
							.field("enum def name")
							.subField(enumName)
							.isSemanticallyIncorrect()
							.becauseOf("dumplicate enum name [" + enumName + "] found")
							.build());
						valid = false;
					} else {
						enumNames.add(enumName.toLowerCase());
					}
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
			LOG.debug(String.format("==> RangerServiceDefValidator.isValidEnums(%s, %s)", enumElementsDefs, failures));
		}
		
		boolean valid = true;
		if (CollectionUtils.isEmpty(enumElementsDefs)) {
			LOG.debug("Enum elements list passed in was null/empty!");
		} else {
			// enum element names should be valid and distinct
			Set<String> elementNames = new HashSet<String>();
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
					String elementName = elementDef.getName();
					if (StringUtils.isBlank(elementName)) {
						failures.add(new ValidationFailureDetailsBuilder()
							.field("enum element name")
							.subField(enumName)
							.isMissing()
							.becauseOf("Name of an element of enum [" + enumName + "] is null/empty[" + elementName + "]")
							.build());
						valid = false;
					} else if (elementNames.contains(elementName.toLowerCase())) {
						failures.add(new ValidationFailureDetailsBuilder()
							.field("enum element name")
							.subField(enumName)
							.isSemanticallyIncorrect()
							.becauseOf("dumplicate enum element name [" + elementName + "] found for enum[" + enumName + "]")
							.build());
						valid = false;
					} else {
						elementNames.add(elementName.toLowerCase());
					}
				}
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerServiceDefValidator.isValidEnums(%s, %s): %s", enumElementsDefs, failures, valid));
		}
		return valid;
	}
}
