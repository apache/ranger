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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.ws.rs.WebApplicationException;

import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.rest.RangerValidator.Action;
import org.junit.Before;
import org.junit.Test;

public class TestServiceRESTForValidation {

	@Before
	public void setUp() throws Exception {
		_serviceRest = new ServiceREST();
		// inject out store in it
		_store = mock(ServiceDBStore.class);
		_serviceRest.svcStore = _store;
		// and our validator factory
		_action = Action.CREATE;
		_factory = mock(RangerValidatorFactory.class);
		_validator = mock(RangerServiceValidator.class);
		when(_factory.getServiceValidator(_store, _action)).thenReturn(_validator);
		_serviceRest.validatorFactory = _factory;
		// and other things that are needed for service rest to work correctly
		_restErrorUtil = mock(RESTErrorUtil.class);
		WebApplicationException webApplicationException = new WebApplicationException();
		when(_restErrorUtil.createRESTException(anyInt(), anyString(), anyBoolean())).thenReturn(webApplicationException);
		_serviceRest.restErrorUtil = _restErrorUtil;
		// other object of use in multiple tests
		_service = mock(RangerService.class);
		_exception = new Exception();
	}

	@Test
	public final void testCreateService_happyPath() throws Exception {
		// creation should succeed if neither validator nor dbstore throw exception
		when(_store.createService(_service)).thenReturn(null); // return value isn't important
		// by default validator mock would not throw exception!
		try {
			_serviceRest.createService(_service);
			// validator must be excercised
			verify(_validator).validate(_service);
			// db store would also have been excercised but that is not the focus of this test!!
		} catch (Throwable t) {
			t.printStackTrace();
			fail("Unexpected exception thrown!");
		}
	}

	@Test
	public final void testCreateService_failureStore() throws Exception {
		// creation should fail if either validator or dbstore throw exception
		// first have only the dbstore throw and exception
		when(_store.createService(_service)).thenThrow(_exception);
		// by default validator mock would not throw exception!
		try {
			_serviceRest.createService(_service);
			fail("Should have thrown an exception!");
		} catch (WebApplicationException t) {
			// expected exception - confirm that validator was excercised
			verify(_validator).validate(_service);
		} catch (Throwable t) {
			fail("Unexpected exception thrown!");
		}
	}

	@Test
	public final void testCreateService_failureValidator() throws Exception {
		// creation should fail if either validator or dbstore throw exception
		// Now we only have the 
		doThrow(_exception).when(_validator).validate(_service);
		// by default validator mock would not throw exception!
		try {
			_serviceRest.createService(_service);
			fail("Should have thrown an exception!");
		} catch (WebApplicationException t) {
			/*
			 * Expected exception - but we still need to validate two things:
			 * - That validator was exercised; accidentally call to validator should not get bypassed.
			 * - That dbstore was NOT exercised; we expect validator failure to short circuit that
			 */
			verify(_validator).validate(_service);
			// And that db store was never called!  We don't expect call to go to it if validator throws exception.
			verify(_store, never()).createService(_service);
		} catch (Throwable t) {
			fail("Unexpected exception thrown!");
		}
	}

	RangerValidatorFactory _factory;
	RangerServiceValidator _validator;
	ServiceDBStore _store;
	Action _action;
	ServiceREST _serviceRest;
	RangerService _service;
	Exception _exception;
	RESTErrorUtil _restErrorUtil;
}
