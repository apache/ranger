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

package org.apache.ranger.plugin.policyevaluator;


import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.conditionevaluator.RangerConditionEvaluator;
import org.apache.ranger.plugin.conditionevaluator.RangerIpMatcher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RangerDefaultPolicyEvaluatorTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test_newConditionEvauator() {
		RangerDefaultPolicyEvaluator evaluator = new RangerDefaultPolicyEvaluator();
		RangerConditionEvaluator ipMatcher = evaluator.newConditionEvauator("org.apache.ranger.plugin.conditionevaluator.RangerIpMatcher");
		assertTrue(ipMatcher.getClass().equals(RangerIpMatcher.class));
		
		// bogus value will lead to null object from coming back
		ipMatcher = evaluator.newConditionEvauator("org.apache.ranger.plugin.conditionevaluator.RangerIpMatcha");
		assertNull(ipMatcher);
		ipMatcher = evaluator.newConditionEvauator("RangerIpMatcher");
		assertNull(ipMatcher);
	}
	
	@Test
	public void test_getEvaluatorName() {

		// null policy passing has reasonable response
		RangerDefaultPolicyEvaluator evaluator = new RangerDefaultPolicyEvaluator();
		String className = evaluator.getEvaluatorName(null, "aCondition");
		assertNull(className);
		// null policy condition def collection should behave sensibly
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		when(serviceDef.getPolicyConditions()).thenReturn(null);
		className = evaluator.getEvaluatorName(serviceDef, "aCondition");
		assertNull(className);
		
		// so should an service def with empty list of policy conditions.
		when(serviceDef.getPolicyConditions()).thenReturn(new ArrayList<RangerServiceDef.RangerPolicyConditionDef>());
		className = evaluator.getEvaluatorName(serviceDef, "aCondition");
		assertNull(className);
		
		// if service has a condition then sensible answer should come back
		Map<String, String> pairs = new HashMap<String, String>();
		pairs.put("type1", "com.company.SomeEvaluator");
		pairs.put("type2", "com.company.AnotherEvaluator");
		serviceDef = getMockServiceDef(pairs);
		className = evaluator.getEvaluatorName(serviceDef, "type1");
		assertEquals("com.company.SomeEvaluator", className);
		className = evaluator.getEvaluatorName(serviceDef, "type2");
		assertEquals("com.company.AnotherEvaluator", className);
		className = evaluator.getEvaluatorName(serviceDef, "type3");
		assertNull(className);
	}
	
	@Test
	public void test_initializeConditionEvaluators_firewalling() {
		RangerServiceDef serviceDef = null;
		RangerPolicy policy = null;
		// passing null values should yield sensible response
		RangerDefaultPolicyEvaluator policyEvaluator = new RangerDefaultPolicyEvaluator();
		Map<String, RangerConditionEvaluator> result = policyEvaluator.initializeConditionEvaluators(policy, serviceDef);
		assertNotNull(result);
		assertTrue(result.isEmpty());

		// or if is the policy item collection in the policy is null
		policy = mock(RangerPolicy.class);
		when(policy.getPolicyItems()).thenReturn(null);
		result = policyEvaluator.initializeConditionEvaluators(policy, serviceDef);
		assertNotNull(result);
		assertTrue(result.isEmpty());
		
		// or if the policy item collection is empty
		List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicy.RangerPolicyItem>();
		when(policy.getPolicyItems()).thenReturn(policyItems);
		result = policyEvaluator.initializeConditionEvaluators(policy, serviceDef);
		assertNotNull(result);
		assertTrue(result.isEmpty());
		
		// or when the policy conditions collection is null
		RangerPolicyItem aPolicyItem = mock(RangerPolicyItem.class);
		when(aPolicyItem.getConditions()).thenReturn(null);
		policyItems.add(aPolicyItem);
		when(policy.getPolicyItems()).thenReturn(policyItems);
		result = policyEvaluator.initializeConditionEvaluators(policy, serviceDef);
		assertNotNull(result);
		assertTrue(result.isEmpty());

		// or when the policy conditions collection is not null but empty
		List<RangerPolicyItemCondition> itemConditions = new ArrayList<RangerPolicy.RangerPolicyItemCondition>();
		when(aPolicyItem.getConditions()).thenReturn(itemConditions);
		// remove left over from prior test
		policyItems.clear(); policyItems.add(aPolicyItem);
		when(policy.getPolicyItems()).thenReturn(policyItems);
		result = policyEvaluator.initializeConditionEvaluators(policy, serviceDef);
		assertNotNull(result);
		assertTrue(result.isEmpty());
		
		// or when any combination of fields of item conditions are null
		RangerPolicyItemCondition anItemCondition = mock(RangerPolicyItemCondition.class);
		when(anItemCondition.getType()).thenReturn(null);
		when(anItemCondition.getValues()).thenReturn(null);
		itemConditions.add(anItemCondition);
		when(aPolicyItem.getConditions()).thenReturn(itemConditions);
		policyItems.clear(); policyItems.add(aPolicyItem);
		when(policy.getPolicyItems()).thenReturn(policyItems);
		result = policyEvaluator.initializeConditionEvaluators(policy, serviceDef);
		assertNotNull(result);
		assertTrue(result.isEmpty());
	}
	
	@Test
	public void test_initializeConditionEvaluators_happyPath() {
		/*
		 * A policy could contain several policy items and each policy item could contain non-overlapping sets of conditions in them.
		 * Resulting map should contain a union of conditions in it and each pointing to correct evaluator object.
		 */
		// first create a service with right condition-name and evaluator names
		Map<String, String> conditionEvaluatorMap = new HashMap<String, String>();
		conditionEvaluatorMap.put("c1", "org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluatorTest$Evaluator1");
		conditionEvaluatorMap.put("c2", "org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluatorTest$Evaluator2");
		conditionEvaluatorMap.put("c3", "org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluatorTest$Evaluator3");
		conditionEvaluatorMap.put("c4", "org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluatorTest$Evaluator4");
		RangerServiceDef serviceDef = getMockServiceDef(conditionEvaluatorMap);
		// create policy items each with overlapping but dissimilar sets of conditions in them.
		RangerPolicyItem anItem = getMockPolicyItem(new String[] {"c1", "c2"});
		RangerPolicyItem anotherItem = getMockPolicyItem(new String[] {"c2", "c3"});
		RangerPolicy policy = mock(RangerPolicy.class);
		when(policy.getPolicyItems()).thenReturn(Arrays.asList(anItem, anotherItem));
		RangerDefaultPolicyEvaluator policyEvaluator = new RangerDefaultPolicyEvaluator();
		Map<String, RangerConditionEvaluator> result = policyEvaluator.initializeConditionEvaluators(policy, serviceDef);
		
		assertNotNull(result);
		assertEquals(3, result.size());
		assertTrue(result.containsKey("c1"));
		assertTrue(result.containsKey("c2"));
		assertTrue(result.containsKey("c3"));

		RangerConditionEvaluator anEvaluator = result.get("c1");
		assertTrue(anEvaluator.getClass().equals(Evaluator1.class));
		anEvaluator = result.get("c2");
		assertTrue(anEvaluator.getClass().equals(Evaluator2.class));
		anEvaluator = result.get("c3");
		assertTrue(anEvaluator.getClass().equals(Evaluator3.class));
	}

	/**
	 * Test classs: that exists only for testing purposes
	 * @author alal
	 *
	 */
	static class AlwaysPass implements RangerConditionEvaluator {

		@Override
		public void init(RangerPolicyItemCondition condition) {
			// empty body!
		}
		@Override
		public boolean isMatched(String value) {
			return true;
		}
		
	}
	
	static class AlwaysFail implements RangerConditionEvaluator {

		@Override
		public void init(RangerPolicyItemCondition condition) {
			// empty body
		}

		@Override
		public boolean isMatched(String value) {
			return false;
		}
		
	}
	
	static class Evaluator1 extends AlwaysPass {}
	static class Evaluator2 extends AlwaysPass {}
	static class Evaluator3 extends AlwaysFail {}
	static class Evaluator4 extends AlwaysFail {}
	
	/**
	 * A request may contain a value for several conditions.  A policy could contain evaluators for more conditions than that are in the request.
	 * check should fail if any condition check fails for the conditions that are contained in the request
	 */
	@Test
	public void test_matchCustomConditions_happyPath() {
		
		// let's first create a request with 3 different conditons
		Map<String, String> requestValues = new HashMap<String, String>();
		requestValues.put("c1", "value1");
		requestValues.put("c2", "value2");
		// let's create the condition evaluator map for each of these conditions and some more.
		RangerAccessRequest request = mock(RangerAccessRequest.class);
		when(request.getConditions()).thenReturn(requestValues);
		Map<String, RangerConditionEvaluator> evaluators = new HashMap<String, RangerConditionEvaluator>();
		evaluators.put("c1", new Evaluator1());
		evaluators.put("c2", new Evaluator2());
		evaluators.put("c3", new Evaluator3()); // conditions 3 and 4 would always fail!
		evaluators.put("c4", new Evaluator4());
		// stuff the evaluator with this map
		RangerDefaultPolicyEvaluator policyEvaluator = new RangerDefaultPolicyEvaluator();
		boolean result = policyEvaluator.matchCustomConditions(request, evaluators);
		assertTrue(result);
		
		// now check for failure
		requestValues.clear();
		requestValues.put("c1", "value1");
		requestValues.put("c3", "value3");
		when(request.getConditions()).thenReturn(requestValues);
		result = policyEvaluator.matchCustomConditions(request, evaluators);
		assertFalse(result);
	}
	
	RangerPolicyItem getMockPolicyItem(String[] strings) {
		RangerPolicyItem policyItem = mock(RangerPolicyItem.class);
		if (strings == null) {
			when(policyItem.getConditions()).thenReturn(null);
		} else if (strings.length == 0) {
			when(policyItem.getConditions()).thenReturn(new ArrayList<RangerPolicy.RangerPolicyItemCondition>());
		} else {
			List<RangerPolicyItemCondition> conditions = new ArrayList<RangerPolicy.RangerPolicyItemCondition>(strings.length);
			for (String name : strings) {
				RangerPolicyItemCondition aCondition = mock(RangerPolicyItemCondition.class);
				when(aCondition.getType()).thenReturn(name);
				when(aCondition.getValues()).thenReturn(null); // values aren't used/needed so set it to a predictable value
				conditions.add(aCondition);
			}
			when(policyItem.getConditions()).thenReturn(conditions);
		}
		return policyItem;
	}

	RangerServiceDef getMockServiceDef(Map<String, String> pairs) {
		// create a service def
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		if (pairs == null) {
			return serviceDef;
		}
		List<RangerPolicyConditionDef> conditions = new ArrayList<RangerServiceDef.RangerPolicyConditionDef>();
		// null policy condition def collection should behave sensibly
		for (Map.Entry<String, String> anEntry : pairs.entrySet()) {
			RangerPolicyConditionDef aCondition = mock(RangerPolicyConditionDef.class);
			when(aCondition.getName()).thenReturn(anEntry.getKey());
			when(aCondition.getEvaluator()).thenReturn(anEntry.getValue());
			conditions.add(aCondition);
		}
		when(serviceDef.getPolicyConditions()).thenReturn(conditions);
		return serviceDef;
	}
	
}
