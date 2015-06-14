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
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.ServiceStore;

import com.google.common.collect.Sets;

public class RangerServiceValidator extends RangerValidator {

	private static final Log LOG = LogFactory.getLog(RangerServiceValidator.class);

	public RangerServiceValidator(ServiceStore store) {
		super(store);
	}

	public void validate(RangerService service, Action action) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerServiceValidator.validate(%s, %s)", service, action));
		}

		List<ValidationFailureDetails> failures = new ArrayList<ValidationFailureDetails>();
		boolean valid = isValid(service, action, failures);
		String message = "";
		try {
			if (!valid) {
				message = serializeFailures(failures);
				throw new Exception(message);
			}
		} finally {
			if(LOG.isDebugEnabled()) {
				LOG.debug(String.format("<== RangerServiceValidator.validate(%s, %s): %s, reason[%s]", service, action, valid, message));
			}
		}
	}
	
	boolean isValid(Long id, Action action, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceValidator.isValid(" + id + ")");
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
		} else if (getService(id) == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No service found for id[" + id + "]! ok!");
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceValidator.isValid(" + id + "): " + valid);
		}
		return valid;
	}
	
	boolean isValid(RangerService service, Action action, List<ValidationFailureDetails> failures) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceValidator.isValid(" + service + ")");
		}
		if (!(action == Action.CREATE || action == Action.UPDATE)) {
			throw new IllegalArgumentException("isValid(RangerService, ...) is only supported for CREATE/UPDATE");
		}
		
		boolean valid = true;
		if (service == null) {
			String message = "service object passed in was null";
			LOG.debug(message);
			failures.add(new ValidationFailureDetailsBuilder()
				.field("service")
				.isMissing()
				.becauseOf(message)
				.build());
			valid = false;
		} else {
			Long id = service.getId();
			if (action == Action.UPDATE) { // id is ignored for CREATE
				if (id == null) {
					String message = "service id was null/empty/blank";
					LOG.debug(message);
					failures.add(new ValidationFailureDetailsBuilder()
							.field("id")
							.isMissing()
							.becauseOf(message)
							.build());
					valid = false;
				} else if (getService(id) == null) {
					failures.add(new ValidationFailureDetailsBuilder()
							.field("id")
							.isSemanticallyIncorrect()
							.becauseOf("no service exists with id[" + id + "]")
							.build());
					valid = false;
				}
			}
			String name = service.getName();
			boolean nameSpecified = StringUtils.isNotBlank(name);
			RangerServiceDef serviceDef = null;
			if (!nameSpecified) {
				String message = "service name[" + name + "] was null/empty/blank";
				LOG.debug(message);
				failures.add(new ValidationFailureDetailsBuilder()
						.field("name")
						.isMissing()
						.becauseOf(message)
						.build());
				valid = false;
			} else {
				RangerService otherService = getService(name);
				if (otherService != null && action == Action.CREATE) {
					failures.add(new ValidationFailureDetailsBuilder()
							.field("name")
							.isSemanticallyIncorrect()
							.becauseOf("service with the name[" + name + "] already exists")
							.build());
					valid = false;
				} else if (otherService != null && otherService.getId() !=null && !otherService.getId().equals(id)) {
					failures.add(new ValidationFailureDetailsBuilder()
							.field("id/name")
							.isSemanticallyIncorrect()
							.becauseOf("id/name conflict: another service already exists with name[" + name + "], its id is [" + otherService.getId() + "]")
							.build());
					valid = false;
				}
			}
			String type = service.getType();
			boolean typeSpecified = StringUtils.isNotBlank(type);
			if (!typeSpecified) {
				failures.add(new ValidationFailureDetailsBuilder()
						.field("type")
						.isMissing()
						.becauseOf("service def [" + type + "] was null/empty/blank")
						.build());
				valid = false;
			} else {
				serviceDef = getServiceDef(type);
				if (serviceDef == null) {
					failures.add(new ValidationFailureDetailsBuilder()
							.field("type")
							.isSemanticallyIncorrect()
							.becauseOf("service def named[" + type + "] not found")
							.build());
					valid = false;
				}
			}
			if (nameSpecified && serviceDef != null) {
				// check if required parameters were specified
				Set<String> reqiredParameters = getRequiredParameters(serviceDef);
				Set<String> inputParameters = getServiceConfigParameters(service);
				Set<String> missingParameters = Sets.difference(reqiredParameters, inputParameters);
				if (!missingParameters.isEmpty()) {
					failures.add(new ValidationFailureDetailsBuilder()
							.field("configuration")
							.subField(missingParameters.iterator().next()) // we return any one parameter!
							.isMissing()
							.becauseOf("required configuration parameter is missing; missing parameters: " + missingParameters)
							.build());
					valid = false;
				}
			}

			String tagServiceName = service.getTagService();

			if (StringUtils.isNotBlank(tagServiceName) && StringUtils.equals(type, EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
				failures.add(new ValidationFailureDetailsBuilder()
						.field("tag_service")
						.isSemanticallyIncorrect()
						.becauseOf("tag service cannot be part of any other service")
						.build());
				valid = false;
			}

			boolean needToEnsureServiceType = false;

			if (action == Action.UPDATE) {
				RangerService otherService = getService(name);
				String otherTagServiceName = otherService == null ? null : otherService.getTagService();

				if (StringUtils.isNotBlank(tagServiceName)) {
					if (!StringUtils.equals(tagServiceName, otherTagServiceName)) {
						needToEnsureServiceType = true;
					}
				}
			} else {    // action == Action.CREATE
				if (StringUtils.isNotBlank(tagServiceName)) {
					needToEnsureServiceType = true;
				}
			}

			if (needToEnsureServiceType) {
				RangerService maybeTagService = getService(tagServiceName);
				if (maybeTagService == null || !StringUtils.equals(maybeTagService.getType(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME)) {
					failures.add(new ValidationFailureDetailsBuilder()
							.field("tag_service")
							.isSemanticallyIncorrect()
							.becauseOf("tag service name does not refer to existing tag service:" + tagServiceName)
							.build());
					valid = false;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceValidator.isValid(" + service + "): " + valid);
		}
		return valid;
	}
}
