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
import static org.mockito.Mockito.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerPolicyValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

public class TestServiceRESTForValidation {

	private static final Log LOG = LogFactory.getLog(TestServiceRESTForValidation.class);

	@Before
	public void setUp() throws Exception {
		_serviceRest = new ServiceREST();
		// inject out store in it
		_store = mock(ServiceDBStore.class);
		_serviceRest.svcStore = _store;
		_bizUtils = mock(RangerBizUtil.class);
		_serviceRest.bizUtil = _bizUtils;
		
		// and our validator factory
		_factory = mock(RangerValidatorFactory.class);
		_serviceValidator = mock(RangerServiceValidator.class);
		when(_factory.getServiceValidator(_store)).thenReturn(_serviceValidator);
		_policyValidator = mock(RangerPolicyValidator.class);
		when(_factory.getPolicyValidator(_store)).thenReturn(_policyValidator);
		_serviceRest.validatorFactory = _factory;
		_serviceDefValidator = mock(RangerServiceDefValidator.class);
		when(_factory.getServiceDefValidator(_store)).thenReturn(_serviceDefValidator);

		// and other things that are needed for service rest to work correctly
		_restErrorUtil = mock(RESTErrorUtil.class);
		WebApplicationException webApplicationException = new WebApplicationException();
		when(_restErrorUtil.createRESTException(anyInt(), anyString(), anyBoolean())).thenReturn(webApplicationException);
		_serviceRest.restErrorUtil = _restErrorUtil;
		
		_guidUtil = mock(GUIDUtil.class);
		when(_guidUtil.genGUID()).thenReturn("a-guid");
		_serviceRest.guidUtil = _guidUtil;
		
		// other object of use in multiple tests
		_service = mock(RangerService.class);
		_policy = mock(RangerPolicy.class);
		_exception = new Exception();
	}

	Action[] cu = new Action[] { Action.CREATE, Action.UPDATE };
	
	@Test
	public final void testService_happyPath() throws Exception {
		/*
		 * Creation should succeed if neither validator nor dbstore throw exception.
		 * - by default mocks return null for unspecified methods, so no additional mocking needed.
		 * - We just assert that validator is called with right set of arguments.
		 * - db store would also have been excercised but that is not the focus of this test, so we don't assert about it!!
		 */
		try {
			_serviceRest.createService(_service);
			verify(_serviceValidator).validate(_service, Action.CREATE);
			// 
			_serviceRest.updateService(_service);
			verify(_serviceValidator).validate(_service, Action.UPDATE);
	
			_serviceRest.deleteService(3L);
			verify(_serviceValidator).validate(3L, Action.DELETE);
		} catch (Throwable t) {
			t.printStackTrace();
			fail("Unexpected exception thrown!");
		}
	}
	
	@Test
	public final void testService_storeFailure() throws Exception {
		/*
		 * API operation should fail if either validator or dbstore throw exception.  For this test we have first just the dbstore throw an exception
		 * - we assert that exception is thrown and that validate is called.
		 */
		// 
		when(_store.createService(_service)).thenThrow(_exception);
		try {
			_serviceRest.createService(_service);
			fail("Should have thrown an exception!");
		} catch (WebApplicationException t) {
			// expected exception - confirm that validator was excercised and that after that call fall through to the store
			verify(_serviceValidator).validate(_service, Action.CREATE);
			verify(_store).createService(_service);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception thrown!");
		}

		when(_store.updateService(_service)).thenThrow(_exception);
		try {
			_serviceRest.updateService(_service);
			fail("Should have thrown an exception!");
		} catch (WebApplicationException t) {
			// expected exception - confirm that validator was excercised
			verify(_serviceValidator).validate(_service, Action.UPDATE);
			verify(_store).updateService(_service);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception thrown!");
		}
		
		doThrow(_exception).when(_store).deleteService(4L);
		try {
			_serviceRest.deleteService(4L);
			fail("Should have thrown an exception!");
		} catch (WebApplicationException t) {
			// expected exception - confirm that validator was excercised
			verify(_serviceValidator).validate(4L, Action.DELETE);
			verify(_store).deleteService(4L);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception thrown!");
		}
	}

	@Test
	public final void testService_validatorFailure() throws Exception {
		/*
		 * If validator throws an exception then API itself should throw an exception.  We need to validate two things:
		 * - That validator was exercised; accidentally call to validator should not get bypassed.
		 * - That dbstore was NOT exercised; we expect validator failure to short circuit that
		 */
		doThrow(_exception).when(_serviceValidator).validate(_service, Action.CREATE);
		try {
			_serviceRest.createService(_service);
			fail("Should have thrown an exception!");
		} catch (WebApplicationException t) {
			// Expected exception
			verify(_serviceValidator).validate(_service, Action.CREATE);
			verify(_store, never()).createService(_service);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception thrown!");
		}

		doThrow(_exception).when(_serviceValidator).validate(_service, Action.UPDATE);
		try {
			_serviceRest.updateService(_service);
			fail("Should have thrown an exception!");
		} catch (WebApplicationException t) {
			// Expected exception
			verify(_serviceValidator).validate(_service, Action.UPDATE);
			verify(_store, never()).updateService(_service);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception thrown!");
		}

		doThrow(_exception).when(_serviceValidator).validate(5L, Action.DELETE);
		try {
			_serviceRest.deleteService(5L);
			fail("Should have thrown an exception!");
		} catch (WebApplicationException t) {
			// Expected exception
			verify(_serviceValidator).validate(5L, Action.DELETE);
			verify(_store, never()).deleteService(5L);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception thrown!");
		}
	}

	@Test
	final public void testPolicy_happyPath() {
		setupBizUtils();
		
		try {
			_serviceRest.updatePolicy(_policy);
			verify(_policyValidator).validate(_policy, Action.UPDATE, true);

			_serviceRest.createPolicy(_policy);
			verify(_policyValidator).validate(_policy, Action.CREATE, true);
		} catch (Exception e) {
			LOG.debug(e);
			fail("unexpected exception");
		}
	}
	
	@Test
	final public void testPolicy_happyPath_deletion() {
		setupBizUtils();
		
		try {
			long id = 3;
			ServiceREST spy = setupForDelete(id);
			spy.deletePolicy(id);
			verify(_policyValidator).validate(id, Action.DELETE);
		} catch (Exception e) {
			LOG.debug(e);
			fail("unexpected exception");
		}
	}
	
	@Test
	final public void testPolicy_validatorFailure() throws Exception {

		// let's have bizutil return true everytime
		setupBizUtils();
		
		doThrow(_exception).when(_policyValidator).validate(_policy, Action.CREATE, true);
		try {
			_serviceRest.createPolicy(_policy);
			fail("Should have thrown exception!");
		} catch (WebApplicationException t) {
			verify(_policyValidator).validate(_policy, Action.CREATE, true);
			verify(_store, never()).createPolicy(_policy);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}

		doThrow(_exception).when(_policyValidator).validate(_policy, Action.UPDATE, true);
		try {
			_serviceRest.updatePolicy(_policy);
			fail("Should have thrown exception!");
		} catch (WebApplicationException t) {
			verify(_policyValidator).validate(_policy, Action.UPDATE, true);
			verify(_store, never()).updatePolicy(_policy);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}

		doThrow(_exception).when(_policyValidator).validate(4L, Action.DELETE);
		try {
			_serviceRest.deletePolicy(4L);
			fail("Should have thrown exception!");
		} catch (WebApplicationException t) {
			verify(_policyValidator).validate(4L, Action.DELETE);
			verify(_store, never()).deletePolicy(4L);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}
	}
	
	@Test
	final public void testPolicy_storeFailure() throws Exception {

		// let's have bizutils return true for now
		setupBizUtils();
		
		doThrow(_exception).when(_store).createPolicy(_policy);
		try {
			_serviceRest.createPolicy(_policy);
			fail("Should have thrown exception!");
		} catch (WebApplicationException e) {
			verify(_policyValidator).validate(_policy, Action.CREATE, true);
			verify(_store).createPolicy(_policy);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}
		
		doThrow(_exception).when(_store).updatePolicy(_policy);
		try {
			_serviceRest.updatePolicy(_policy);
			fail("Should have thrown exception!");
		} catch (WebApplicationException e) {
			verify(_policyValidator).validate(_policy, Action.UPDATE, true);
			verify(_store).updatePolicy(_policy);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}
	}

	@Test
	final public void testPolicy_storeFailure_forDelete() throws Exception {

		// let's have bizutils return true for now
		setupBizUtils();
		
		Long id = 5L;
		ServiceREST spy = setupForDelete(id);
		doThrow(_exception).when(_store).deletePolicy(id);
		try {
			spy.deletePolicy(id);
			fail("Should have thrown exception!");
		} catch (WebApplicationException e) {
			verify(_policyValidator).validate(id, Action.DELETE);
			verify(_store).deletePolicy(id);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}
	}

	@Test
	public final void testServiceDef_happyPath() throws Exception {
		/*
		 * Creation should succeed if neither validator nor dbstore throw exception.
		 * - by default mocks return null for unspecified methods, so no additional mocking needed.
		 * - We just assert that validator is called with right set of arguments.
		 * - db store would also have been excercised but that is not the focus of this test, so we don't assert about it!!
		 */
		try {
			_serviceRest.createServiceDef(_serviceDef);
			verify(_serviceDefValidator).validate(_serviceDef, Action.CREATE);
			// 
			_serviceRest.updateServiceDef(_serviceDef);
			verify(_serviceDefValidator).validate(_serviceDef, Action.UPDATE);

			HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
			_serviceRest.deleteServiceDef(3L, request);
			verify(_serviceDefValidator).validate(3L, Action.DELETE);
		} catch (Throwable t) {
			t.printStackTrace();
			fail("Unexpected exception thrown!");
		}
	}
	
	@Test
	public void testServiveDef_validatorFailure() throws Exception {
		
		doThrow(_exception).when(_serviceDefValidator).validate(_serviceDef, Action.CREATE);
		try {
			_serviceRest.createServiceDef(_serviceDef);
			fail("Should have thrown exception!");
		} catch (WebApplicationException t) {
			verify(_serviceDefValidator).validate(_serviceDef, Action.CREATE);
			verify(_store, never()).createServiceDef(_serviceDef);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}

		doThrow(_exception).when(_serviceDefValidator).validate(_serviceDef, Action.UPDATE);
		try {
			_serviceRest.updateServiceDef(_serviceDef);
			fail("Should have thrown exception!");
		} catch (WebApplicationException t) {
			verify(_serviceDefValidator).validate(_serviceDef, Action.UPDATE);
			verify(_store, never()).updateServiceDef(_serviceDef);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}

		doThrow(_exception).when(_serviceDefValidator).validate(4L, Action.DELETE);
		try {
			HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
			_serviceRest.deleteServiceDef(4L, request);
			fail("Should have thrown exception!");
		} catch (WebApplicationException t) {
			verify(_serviceDefValidator).validate(4L, Action.DELETE);
			verify(_store, never()).deleteServiceDef(4L);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}
	}
	
	@Test
	public void testServiceDef_storeFailure() throws Exception {
		doThrow(_exception).when(_store).createServiceDef(_serviceDef);
		try {
			_serviceRest.createServiceDef(_serviceDef);
			fail("Should have thrown exception!");
		} catch (WebApplicationException e) {
			verify(_serviceDefValidator).validate(_serviceDef, Action.CREATE);
			verify(_store).createServiceDef(_serviceDef);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}
		
		doThrow(_exception).when(_store).updateServiceDef(_serviceDef);
		try {
			_serviceRest.updateServiceDef(_serviceDef);
			fail("Should have thrown exception!");
		} catch (WebApplicationException e) {
			verify(_serviceDefValidator).validate(_serviceDef, Action.UPDATE);
			verify(_store).updateServiceDef(_serviceDef);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}
		
		doThrow(_exception).when(_store).deleteServiceDef(5L, false);
		try {
			HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
			_serviceRest.deleteServiceDef(5L, request);
			fail("Should have thrown exception!");
		} catch (WebApplicationException e) {
			verify(_serviceDefValidator).validate(5L, Action.DELETE);
			verify(_store).deleteServiceDef(5L, false);
		} catch (Throwable t) {
			LOG.debug(t);
			fail("Unexpected exception!");
		}
	}

	void setupBizUtils() {
		when(_bizUtils.isAdmin()).thenReturn(true);
	}
	
	@SuppressWarnings("unchecked")
	ServiceREST setupForDelete(long id) throws Exception {
		// deletion now asserts admin privileges.  Ensure that it will find the policy from the store
		when(_store.getPolicy(id)).thenReturn(_policy);
		// now we have to ensure that real admin check never gets called -- we are not interested in its working
		ServiceREST spy = spy(_serviceRest);
		doNothing().when(spy).ensureAdminAccess(anyString(), anyMap());
		return spy;
	}
	
	private RangerValidatorFactory _factory;
	private RangerServiceValidator _serviceValidator;
	private RangerPolicyValidator _policyValidator;
	private RangerServiceDefValidator _serviceDefValidator;

	private ServiceDBStore _store;
	private ServiceREST _serviceRest;
	private Exception _exception;
	private RESTErrorUtil _restErrorUtil;
	private RangerBizUtil _bizUtils;

	private RangerService _service;
	private RangerPolicy _policy;
	private RangerServiceDef _serviceDef;
	private GUIDUtil _guidUtil;
}
