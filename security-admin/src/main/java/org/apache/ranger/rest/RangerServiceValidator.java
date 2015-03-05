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

package org.apache.ranger.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.ServiceStore;

import com.google.common.collect.Sets;

public class RangerServiceValidator extends RangerValidator {

	private static final Log LOG = LogFactory.getLog(RangerServiceValidator.class);

	public RangerServiceValidator(ServiceStore store) {
		super(store);
	}

	public void validate(RangerService service, Action action) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerValidator.validate(" + service + ")");
		}

		List<ValidationFailureDetails> failures = new ArrayList<ValidationFailureDetails>();
		if (isValid(service, action, failures)) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerValidator.validate(" + service + "): valid");
			}
		} else {
			String message = serializeFailures(failures);
			LOG.debug("<== RangerValidator.validate(" + service + "): invalid, reason[" + message + "]");
			throw new Exception(message);
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
				.becauseOf("isValid(Long) is only supported for DELETE")
				.build());
			valid = false;
		} else if (id == null) {
			failures.add(new ValidationFailureDetailsBuilder()
				.field("id")
				.isMissing()
				.build());
			valid = false;
		} else if (getService(id) == null) {
			failures.add(new ValidationFailureDetailsBuilder()
				.field("id")
				.isSemanticallyIncorrect()
				.becauseOf("no service found for id[" + id + "]")
				.build());
			valid = false;
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
						.becauseOf("no service exists with id[" + id +"]")
						.build());
					valid = false;
				}
			}
			String name = service.getName();
			boolean nameSpecified = StringUtils.isNotBlank(name);
			RangerServiceDef serviceDef = null;
			if (!nameSpecified) {
				String message = "service name was null/empty/blank[" + name + "]"; 
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
						.becauseOf("service already exists with name[" + name + "]")
						.build());
					valid = false;
				} else if (otherService != null && otherService.getId() !=null && otherService.getId() != id) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("id/name")
						.isSemanticallyIncorrect()
						.becauseOf("id/name conflict: service already exists with name[" + name + "], its id is [" + otherService.getId() + "]")
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
					.becauseOf("service def was null/empty/blank")
					.build());
				valid = false;
			} else {
				serviceDef = getServiceDef(type);
				if (serviceDef == null) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("type")
						.isSemanticallyIncorrect()
						.becauseOf("service def not found for type[" + type + "]")
						.build());
					valid = false;
				}
			}
			if (nameSpecified && serviceDef != null) {
				Set<String> reqiredParameters = getRequiredParameters(serviceDef);
				Set<String> inputParameters = getServiceConfigParameters(service);
				Set<String> missingParameters = Sets.difference(reqiredParameters, inputParameters);
				if (!missingParameters.isEmpty()) {
					failures.add(new ValidationFailureDetailsBuilder()
						.field("configuration")
						.subField(missingParameters.iterator().next()) // we return any one parameter!
						.isMissing()
						.becauseOf("required configuration parameter is missing")
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
