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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.rest.RangerValidator.Action;
import org.junit.Before;
import org.junit.Test;

public class TestServiceValidator {

	static class TestRangerValidator extends RangerValidator {

		public TestRangerValidator(ServiceStore store, Action action) {
			super(store, action);
		}
		
		boolean isValid(String behavior) {
			if (behavior.equals("valid")) {
				_valid = true;
			} else {
				_valid = false;
				if (behavior.equals("reason")) {
					_failures = new ArrayList<ValidationFailureDetails>();
					_failures.add(new ValidationFailureDetails("", "", false, false, false, ""));
				}
			}
			return _valid;
		}
	}
	
	@Before
	public void before() {
		_store = mock(ServiceStore.class);
		_validator = new TestRangerValidator(_store, Action.CREATE);
	}

	@Test
	public void test_ctor_firewalling() {
		try {
			// service store can't be null during construction  
			new TestRangerValidator(null, Action.CREATE);
			fail("Should have thrown exception!");
		} catch (IllegalArgumentException e) {
			// expected exception
		}
		try {
			// action can't be null
			new TestRangerValidator(_store, null);
			fail("Should have thrown exception!");
		} catch (IllegalArgumentException e) {
			// expected exception
		}
	}

	public void test_getFailures_firewalling() {
		// it is illegal to query validator for reason without first having it check something!
		try {
			_validator.getFailures();
			fail("Should have thrown exception!");
		} catch (IllegalStateException e) {
			// expected exception.
		}
		
		try {
			// we know this call will fail
			_validator.isValid("invalid");
			_validator.getFailures();
		} catch (Throwable t) {
			t.printStackTrace();
			fail("Unexpected exception!");
		}
	}
	
	@Test
	public void test_getServiceConfigParameters() {
		// reasonable protection against null values
		Set<String> parameters = _validator.getServiceConfigParameters(null);
		assertNotNull(parameters);
		assertTrue(parameters.isEmpty());
		
		RangerService service = mock(RangerService.class);
		when(service.getConfigs()).thenReturn(null);
		parameters = _validator.getServiceConfigParameters(service);
		assertNotNull(parameters);
		assertTrue(parameters.isEmpty());
		
		when(service.getConfigs()).thenReturn(new HashMap<String, String>());
		parameters = _validator.getServiceConfigParameters(service);
		assertNotNull(parameters);
		assertTrue(parameters.isEmpty());

		String[] keys = new String[] { "a", "b", "c" };
		Map<String, String> map = _utils.createMap(keys);
		when(service.getConfigs()).thenReturn(map);
		parameters = _validator.getServiceConfigParameters(service);
		for (String key: keys) {
			assertTrue("key", parameters.contains(key));
		}
	}
	
	@Test
	public void test_getRequiredParameters() {
		// reasonable protection against null things
		Set<String> parameters = _validator.getRequiredParameters(null);
		assertNotNull(parameters);
		assertTrue(parameters.isEmpty());

		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		when(serviceDef.getConfigs()).thenReturn(null);
		parameters = _validator.getRequiredParameters(null);
		assertNotNull(parameters);
		assertTrue(parameters.isEmpty());

		List<RangerServiceConfigDef> configs = new ArrayList<RangerServiceDef.RangerServiceConfigDef>();
		when(serviceDef.getConfigs()).thenReturn(configs);
		parameters = _validator.getRequiredParameters(null);
		assertNotNull(parameters);
		assertTrue(parameters.isEmpty());
		
		Object[][] input = new Object[][] {
				{ "param1", false },
				{ "param2", true },
				{ "param3", true },
				{ "param4", false },
		};
		configs = _utils.createServiceConditionDefs(input);
		when(serviceDef.getConfigs()).thenReturn(configs);
		parameters = _validator.getRequiredParameters(serviceDef);
		assertTrue("result does not contain: param2", parameters.contains("param2"));
		assertTrue("result does not contain: param3", parameters.contains("param3"));
	}
	
	@Test
	public void test_getServiceDef() {
		try {
			// if service store returns null or throws an exception then service is deemed invalid
			when(_store.getServiceDefByName("return null")).thenReturn(null);
			when(_store.getServiceDefByName("throw")).thenThrow(new Exception());
			RangerServiceDef serviceDef = mock(RangerServiceDef.class);
			when(_store.getServiceDefByName("good-service")).thenReturn(serviceDef);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected exception during mocking!");
		}
		
		assertNull(_validator.getServiceDef("return null"));
		assertNull(_validator.getServiceDef("throw"));
		assertFalse(_validator.getServiceDef("good-service") == null);
	}

	@Test
	public void test_getService() {
		try {
			// if service store returns null or throws an exception then service is deemed invalid
			when(_store.getServiceByName("return null")).thenReturn(null);
			when(_store.getServiceByName("throw")).thenThrow(new Exception());
			RangerService service = mock(RangerService.class);
			when(_store.getServiceByName("good-service")).thenReturn(service);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Unexpected exception during mocking!");
		}
		
		assertNull(_validator.getService("return null"));
		assertNull(_validator.getService("throw"));
		assertFalse(_validator.getService("good-service") == null);
	}

	private TestRangerValidator _validator;
	private ServiceStore _store;
	private ValidationTestUtils _utils = new ValidationTestUtils();
}
