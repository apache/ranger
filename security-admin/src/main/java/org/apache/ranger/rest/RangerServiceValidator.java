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

	public RangerServiceValidator(ServiceStore store, Action action) {
		super(store, action);
	}

	public void validate(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerValidator.validate(" + service + ")");
		}

		if (isValid(service)) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerValidator.validate(" + service + "): valid");
			}
		} else {
			String message = getFailureMessage();
			LOG.debug("<== RangerValidator.validate(" + service + "): invalid, reason[" + message + "]");
			throw new Exception(message);
		}
	}
	
	public void validate(long id) throws Exception {
		if (isValid(id)) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerValidator.validate(" + id + "): valid");
			}
		} else {
			String message = getFailureMessage();
			LOG.debug("<== RangerValidator.validate(" + id + "): invalid, reason[" + message + "]");
			throw new Exception(message);
		}
	}
	
	boolean isValid(Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceValidator.isValid(" + id + ")");
		}

		if (_action != Action.DELETE) {
			addFailure(new ValidationFailureDetailsBuilder()
				.isAnInternalError()
				.becauseOf("isValid(Long) is only supported for DELETE")
				.build());
		} else if (id == null) {
			addFailure(new ValidationFailureDetailsBuilder()
				.field("id")
				.isMissing()
				.build());
		} else {
			boolean found = false;
			try {
				if (_store.getService(id) != null) {
					found = true;
				}
			} catch (Exception e) {
				LOG.debug("Encountred exception while retrieving service from service store!", e);
			}
			if (!found) {
				addFailure(new ValidationFailureDetailsBuilder()
					.field("id")
					.isSemanticallyIncorrect()
					.becauseOf("no service found for id[" + id + "]")
					.build());
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceValidator.isValid(" + id + "): " + _valid);
		}
		return _valid;
	}
	
	boolean isValid(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceValidator.isValid(" + service + ")");
		}
		
		if (service == null) {
			String message = "service object passed in was null";
			LOG.debug(message);
			addFailure(new ValidationFailureDetailsBuilder()
				.field("service")
				.isMissing()
				.becauseOf(message)
				.build());
		} else {
			String name = service.getName();
			String type = service.getType();
			boolean nameSpecified = StringUtils.isNotBlank(name);
			boolean typeSpecified = StringUtils.isNotBlank(type);
			RangerService existingService = null;
			RangerServiceDef serviceDef = null;
			if (!nameSpecified) {
				String message = "service name was null/empty/blank"; 
				LOG.debug(message);
				addFailure(new ValidationFailureDetailsBuilder()
					.field("name")
					.isMissing()
					.becauseOf(message)
					.build());
			} else {
				existingService = getService(name);
				if (existingService != null && _action == Action.CREATE) {
					addFailure(new ValidationFailureDetailsBuilder()
						.field("name")
						.isSemanticallyIncorrect()
						.becauseOf("service with the same name already exists")
						.build());
				} else if (existingService == null && _action == Action.UPDATE) {
					addFailure(new ValidationFailureDetailsBuilder()
						.field("name")
						.isSemanticallyIncorrect()
						.becauseOf("service with the same name doesn't exist")
						.build());
				}
			}
			if (!typeSpecified) {
				addFailure(new ValidationFailureDetailsBuilder()
					.field("type")
					.isMissing()
					.becauseOf("service def was null/empty/blank")
					.build());
			} else {
				serviceDef = getServiceDef(type);
				if (serviceDef == null) {
					addFailure(new ValidationFailureDetailsBuilder()
						.field("type")
						.isSemanticallyIncorrect()
						.becauseOf("service def not found")
						.build());
				}
			}
			if (nameSpecified && serviceDef != null) {
				Set<String> reqiredParameters = getRequiredParameters(serviceDef);
				Set<String> inputParameters = getServiceConfigParameters(service);
				Set<String> missingParameters = Sets.difference(reqiredParameters, inputParameters);
				if (!missingParameters.isEmpty()) {
					addFailure(new ValidationFailureDetailsBuilder()
						.field("configuration")
						.subField(missingParameters.iterator().next()) // we return any one parameter!
						.isMissing()
						.becauseOf("required configuration parameter is missing")
						.build());
				}
			}
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceValidator.isValid(" + service + "): " + _valid);
		}
		return _valid;
	}
}
