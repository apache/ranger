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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.rest.RangerValidator.Action;
import org.junit.Before;
import org.junit.Test;

public class TestRangerServiceValidator {
	
	final Action[] cud = new Action[] { Action.CREATE, Action.UPDATE, Action.DELETE };
	final Action[] cu = new Action[] { Action.CREATE, Action.UPDATE };
	final Action[] ud = new Action[] { Action.UPDATE, Action.DELETE };

	@Before
	public void before() {
		_store = mock(ServiceStore.class);
		_action = Action.CREATE; // by default we set action to create
		_validator = new RangerServiceValidator(_store, _action);
	}

	@Test
	public void testIsValid_failures() throws Exception {
		RangerService service = mock(RangerService.class);
		List<ValidationFailureDetails> failures;
		// create/update/delete: null, empty of blank name renders a service invalid
		for (Action action : cud) {
			_validator = new RangerServiceValidator(_store, action);
			when(service.getName()).thenReturn(null);
			assertFalse(_validator.isValid(service));
			// let's verify the sort of error information that is returned (for one of these)
			failures = _validator.getFailures();
			// assert that among the failure reason is one about field name being missing.
			boolean found = false;
			for (ValidationFailureDetails f : failures) {
				if ("name".equals(f.getFieldName()) && 
						f._internalError == false && 
						f._missing == true &&
						f._semanticError == false) {
					found = true;
				}
			}
			assertTrue("Matching failure located", found);
			// let's assert behavior for other flavors of this condition, too.
			when(service.getName()).thenReturn("");
			assertFalse(_validator.isValid(service));
			when(service.getName()).thenReturn("  "); // spaces
			assertFalse(_validator.isValid(service));
		}
		
		// Create/update: null, empty or blank type is also invalid
		for (Action action : cu) {
			_validator = new RangerServiceValidator(_store, action);
			when(service.getName()).thenReturn("aName");
			when(service.getType()).thenReturn(null);
			assertFalse(_validator.isValid(service));
			when(service.getType()).thenReturn("");
			assertFalse(_validator.isValid(service));
			when(service.getType()).thenReturn("	"); // a tab
			assertFalse(_validator.isValid(service));
			// let's verify the error information returned (for the last scenario)
			failures = _validator.getFailures();
			boolean found = false;
			for (ValidationFailureDetails f : failures) {
				if ("type".equals(f._fieldName) && 
						f._missing == true && 
						f._semanticError == false) {
					found = true;
				}
			}
			assertTrue("Matching failure located", found);
		}

		// Create/update: if non-empty, the type should also exist!
		for (Action action : cu) {
			_validator = new RangerServiceValidator(_store, action);
			when(service.getName()).thenReturn("aName");
			when(service.getType()).thenReturn("aType");
			when(_store.getServiceDefByName("aType")).thenReturn(null);
			assertFalse(_validator.isValid(service));
			// let's verify the error information returned (for the last scenario)
			failures = _validator.getFailures();
			boolean found = false;
			for (ValidationFailureDetails f : failures) {
				if ("type".equals(f._fieldName) && 
						f._missing == false && 
						f._semanticError == true) {
					found = true;
				}
			}
			assertTrue("Matching failure located", found);
		}
		
		// Create: if service already exists then that such a service should be considered invalid by create
		when(service.getName()).thenReturn("aName");
		when(service.getType()).thenReturn("aType");
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		when(_store.getServiceDefByName("aType")).thenReturn(serviceDef);
		// test both when service exists and when it doesn't -- the result is opposite for the two cases
		RangerService existingService = mock(RangerService.class);
		when(_store.getServiceByName("aName")).thenReturn(existingService);

		_validator = new RangerServiceValidator(_store, Action.CREATE);
		assertFalse(_validator.isValid(service));
		
		// check the error returned: it is a semantic error about service's name
		failures = _validator.getFailures();
		boolean found = false;
		for (ValidationFailureDetails f : failures) {
			if ("name".equals(f._fieldName) && 
					f._missing == false && 
					f._semanticError == true) {
				found = true;
			}
		}
		assertTrue("Matching failure located", found);
		
		// Update: Exact inverse is true, i.e. service must exist!
		when(_store.getServiceByName("anotherName")).thenReturn(null);
		when(service.getName()).thenReturn("anotherName");
		
		_validator = new RangerServiceValidator(_store, Action.UPDATE);
		assertFalse(_validator.isValid(service));
		// check the error returned: it is a semantic error about service's name
		failures = _validator.getFailures();
		found = false;
		for (ValidationFailureDetails f : failures) {
			if ("name".equals(f._fieldName) && 
					f._missing == false && 
					f._semanticError == true) {
				found = true;
			}
		}
		assertTrue("Matching failure located", found);
	}
	
	@Test
	public void test_isValid_missingRequiredParameter() throws Exception {
		// Create/Update: simulate a condition where required parameters are missing
		Object[][] input = new Object[][] {
				{ "param1", true },
				{ "param2", true },
				{ "param3", false },
				{ "param4", false },
		};
		List<RangerServiceConfigDef> configDefs = _utils.createServiceConditionDefs(input);
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		when(serviceDef.getConfigs()).thenReturn(configDefs);
		// wire this service def into store
		when(_store.getServiceDefByName("aType")).thenReturn(serviceDef);
		// create a service with some require parameters missing
		RangerService service = mock(RangerService.class);
		when(service.getType()).thenReturn("aType");
		when(service.getName()).thenReturn("aName");
		// required parameters param2 is missing
		String[] params = new String[] { "param1", "param3", "param4", "param5" };
		Map<String, String> paramMap = _utils.createMap(params);
		when(service.getConfigs()).thenReturn(paramMap);
		// service does not exist in the store
		when(_store.getServiceByName("aService")).thenReturn(null);
		for (Action action : cu) {
			// it should be invalid
			_validator = new RangerServiceValidator(_store, action);
			assertFalse(_validator.isValid(service));
			// check the error message
			List<ValidationFailureDetails> failures = _validator.getFailures();
			boolean found = false;
			for (ValidationFailureDetails f : failures) {
				if ("configuration".equals(f.getFieldName()) &&
						"param2".equals(f._subFieldName) &&
						f._missing == true &&
						f._internalError == false && 
						f._semanticError == false) {
					found = true;
				}
			}
			assertTrue(found);
		}
	}

	@Test
	public void test_isValid_happyPath() throws Exception {
		// create a service def with some required parameters 
		Object[][] serviceDefInput = new Object[][] {
				{ "param1", true },
				{ "param2", true },
				{ "param3", false },
				{ "param4", false },
				{ "param5", true },
		};
		List<RangerServiceConfigDef> configDefs = _utils.createServiceConditionDefs(serviceDefInput);
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		when(serviceDef.getConfigs()).thenReturn(configDefs);
		// create a service with some parameters on it
		RangerService service = mock(RangerService.class);
		when(service.getName()).thenReturn("aName");
		when(service.getType()).thenReturn("aType");
		// contains an extra parameter (param6) and one optional is missing(param4)
		String[] configs = new String[] { "param1", "param2", "param3", "param5", "param6" };
		Map<String, String> configMap = _utils.createMap(configs);  
		when(service.getConfigs()).thenReturn(configMap);
		// wire then into the store
		try {
			// service does not exists
			when(_store.getServiceByName("aName")).thenReturn(null);
			// service def exists
			when(_store.getServiceDefByName("aType")).thenReturn(serviceDef);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected error encountered while mocking!");
		}
		_validator = new RangerServiceValidator(_store, Action.CREATE);
		assertTrue(_validator.isValid(service));
		// for update to work the only additional requirement is that service should exist
		RangerService existingService = mock(RangerService.class);
		when(_store.getServiceByName("aName")).thenReturn(existingService);
		_validator = new RangerServiceValidator(_store, Action.UPDATE);
		assertTrue(_validator.isValid(service));
	}

	ValidationFailureDetails getFailure(List<ValidationFailureDetails> failures) {
		if (failures == null || failures.size() == 0) {
			return null;
		} else {
			return failures.iterator().next();
		}
	}
	private ServiceStore _store;
	private RangerServiceValidator _validator;
	private Action _action;
	private ValidationTestUtils _utils = new ValidationTestUtils();
}
