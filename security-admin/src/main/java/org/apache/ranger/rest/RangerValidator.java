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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.store.ServiceStore;

public abstract class RangerValidator {
	
	private static final Log LOG = LogFactory.getLog(RangerValidator.class);

	ServiceStore _store;
	boolean _valid = true;
	List<ValidationFailureDetails> _failures;
	Action _action;

	public enum Action {
		CREATE, UPDATE, DELETE;
	};
	
	protected RangerValidator(ServiceStore store, Action action) {
		if (store == null) {
			throw new IllegalArgumentException("ServiceValidator(): store is null!");
		}
		_store = store;
		if (action == null) {
			throw new IllegalArgumentException("ServiceValidator(): action is null!");
		}
		_action = action;
	}

	protected List<ValidationFailureDetails> getFailures() {
		if (_valid) {
			LOG.warn("getFailureDetails: called while _valid == true");
		}
		return _failures;
	}
	
	String getFailureMessage() {
		if (_valid) {
			LOG.warn("getFailureDetails: called while validator is true!");
		}
		if (_failures == null) {
			LOG.warn("getFailureDetails: called while list of failures is null!");
			return null;
		}
		StringBuilder builder = new StringBuilder();
		for (ValidationFailureDetails aFailure : _failures) {
			builder.append(aFailure.toString());
			builder.append(";");
		}
		return builder.toString();
	}

	void addFailure(ValidationFailureDetails aFailure) {
		if (_failures == null) {
			_failures = new ArrayList<ValidationFailureDetails>();
		}
		_failures.add(aFailure);
		_valid = false;
	}
	
	Set<String> getServiceConfigParameters(RangerService service) {
		if (service == null || service.getConfigs() == null) {
			return new HashSet<String>();
		} else {
			return service.getConfigs().keySet();
		}
	}

	Set<String> getRequiredParameters(RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceValidator.getRequiredParameters(" + serviceDef + ")");
		}

		Set<String> result;
		if (serviceDef == null) {
			result = Collections.emptySet();
		} else {
			List<RangerServiceConfigDef> configs = serviceDef.getConfigs();
			if (CollectionUtils.isEmpty(configs)) {
				result = Collections.emptySet();
			} else {
				result = new HashSet<String>(configs.size()); // at worse all of the config items are required!
				for (RangerServiceConfigDef configDef : configs) {
					if (configDef.getMandatory()) {
						result.add(configDef.getName());
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceValidator.getRequiredParameters(" + serviceDef + "): " + result);
		}
		return result;
	}

	RangerServiceDef getServiceDef(String type) {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceValidator.getServiceDef(" + type + ")");
		}
		RangerServiceDef result = null;
		try {
			result = _store.getServiceDefByName(type);
		} catch (Exception e) {
			LOG.debug("Encountred exception while retrieving service definition from service store!", e);
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceValidator.getServiceDef(" + type + "): " + result);
		}
		return result;
	}

	RangerService getService(String name) {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServiceValidator.getService(" + name + ")");
		}
		RangerService result = null;
		try {
			result = _store.getServiceByName(name);
		} catch (Exception e) {
			LOG.debug("Encountred exception while retrieving service from service store!", e);
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServiceValidator.getService(" + name + "): " + result);
		}
		return result;
	}
}
