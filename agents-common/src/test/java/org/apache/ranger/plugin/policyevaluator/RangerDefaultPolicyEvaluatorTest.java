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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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

import com.google.common.collect.Lists;

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
	public void test_getConditionEvaluator() {

		// null service def and/or policy has reasonable response
		RangerDefaultPolicyEvaluator evaluator = new RangerDefaultPolicyEvaluator();
		RangerPolicyConditionDef conditionDef = evaluator.getConditionDef(null, null);
		assertNull(conditionDef);

		conditionDef = evaluator.getConditionDef(null, "aCondition");
		assertNull(conditionDef);
		
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		conditionDef = evaluator.getConditionDef(null, null);
		assertNull(conditionDef);

		// null policy condition def collection should behave sensibly
		when(serviceDef.getPolicyConditions()).thenReturn(null);
		conditionDef = evaluator.getConditionDef(serviceDef, "aCondition");
		assertNull(conditionDef);
		
		// so should an service def with empty list of policy conditions.
		when(serviceDef.getPolicyConditions()).thenReturn(new ArrayList<RangerServiceDef.RangerPolicyConditionDef>());
		conditionDef = evaluator.getConditionDef(serviceDef, "aCondition");
		assertNull(conditionDef);
		
		// if service has a condition then sensible answer should come back
		RangerPolicyConditionDef aConditionDef = getMockPolicyConditionDef("type1", "com.company.SomeEvaluator", null);
		RangerPolicyConditionDef anotherConditionDef = getMockPolicyConditionDef("type2", "com.company.AnotherEvaluator", "key1");
		List<RangerPolicyConditionDef> conditionDefs = Lists.newArrayList(aConditionDef, anotherConditionDef);
		
		serviceDef = getMockServiceDef(conditionDefs);
		conditionDef = evaluator.getConditionDef(serviceDef, "type1");
		assertEquals(aConditionDef, conditionDef);
		conditionDef = evaluator.getConditionDef(serviceDef, "type2");
		assertEquals(anotherConditionDef, conditionDef);
		conditionDef = evaluator.getConditionDef(serviceDef, "type3");
		assertNull(conditionDef);
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
		Map<String, String[]> conditionEvaluatorMap = new HashMap<String, String[]>();
		conditionEvaluatorMap.put("c1", new String[] { "org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluatorTest$Evaluator1", null });
		conditionEvaluatorMap.put("c2", new String[] { "org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluatorTest$Evaluator2", null });
		conditionEvaluatorMap.put("c3", new String[] { "org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluatorTest$Evaluator3", null });
		conditionEvaluatorMap.put("c4", new String[] { "org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluatorTest$Evaluator4", null });
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
		public void init(RangerPolicyConditionDef conditionDef, RangerPolicyItemCondition condition) {
			// empty body!
		}
		@Override
		public boolean isMatched(RangerAccessRequest request) {
			return true;
		}
		
	}
	
	static class AlwaysFail implements RangerConditionEvaluator {

		@Override
		public void init(RangerPolicyConditionDef conditionDef, RangerPolicyItemCondition condition) {
			// empty body
		}

		@Override
		public boolean isMatched(RangerAccessRequest request) {
			return false;
		}
		
	}
	
	static class Evaluator1 extends AlwaysPass {}
	static class Evaluator2 extends AlwaysPass {}
	static class Evaluator3 extends AlwaysFail {}
	static class Evaluator4 extends AlwaysFail {}
	
	/**
	 * A request may contain a value for several conditions.  A policy item could contain evaluators for more/different conditions than that are in the request.
	 * check should fail if condition evaluator for any policy fails for a conditions contained in the request.  If request does not have data for a condition
	 * then it should succeed.  Data in request for condition that are not part of the policy item shouldn't affect the result.
	 */
	@Test
	public void test_matchCustomConditions_happyPath1() {

		// let's create the condition evaluator map for 4 conditions.
		Map<String, RangerConditionEvaluator> evaluators = createEvaluatorMap();
		
		// let's first create a request with 2 different conditions
		RangerAccessRequest request = createAccessRequestWithConditions(new String[] {"c1", "c2"});

		// Create a policy item -- which also has same exact same number of policy conditions defined on it (2)
		RangerPolicyItem policyItem = createPolicyItemForConditions(new String[] {"c1", "c2"} );
		
		// check for success
		RangerDefaultPolicyEvaluator policyEvaluator = new RangerDefaultPolicyEvaluator();
		boolean result = policyEvaluator.matchCustomConditions(policyItem, request, evaluators);
		assertTrue(result);

		// missing conditions on request are ok, too -- they always succeed
		// policy item has conditions c1 and c2 where as context will only have c1. 
		request = createAccessRequestWithConditions(new String[] { "c1" } );
		result = policyEvaluator.matchCustomConditions(policyItem, request, evaluators);
		assertTrue(result);
		
		// Extra conditions on request are ok, too -- they always succeed
		// policy item has conditions c1 and c2 where as context has values for conditions c3 and c4 on it and we know their evaluators always fail! 
		request = createAccessRequestWithConditions(new String[] {"c3", "c4"});
		result = policyEvaluator.matchCustomConditions(policyItem, request, evaluators);
		assertTrue(result);
	}
	
	@Test
	public void test_matchCustomConditions_happyPath2() {
		// let's create the condition evaluator map for 4 conditions and some more.
		Map<String, RangerConditionEvaluator> evaluators = createEvaluatorMap();
		
		// create policy item with a condition that we know will always fail
		RangerPolicyItem policyItem = createPolicyItemForConditions(new String[] { "c1", "c3" } );
		
		// let's first create a request with 2 different conditions
		RangerAccessRequest request = createAccessRequestWithConditions(new String[]{"c1", "c3"});

		RangerDefaultPolicyEvaluator policyEvaluator = new RangerDefaultPolicyEvaluator();
		boolean result = policyEvaluator.matchCustomConditions(policyItem, request, evaluators);
		assertFalse(result);
	}
	
	Map<String, RangerConditionEvaluator> createEvaluatorMap() {
		Map<String, RangerConditionEvaluator> map = new HashMap<String, RangerConditionEvaluator>();
		map.put("c1", new Evaluator1());
		map.put("c2", new Evaluator2());
		map.put("c3", new Evaluator3()); // conditions 3 and 4 would always fail!
		map.put("c4", new Evaluator4());

		return map;
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

	RangerServiceDef getMockServiceDef(List<RangerPolicyConditionDef> conditionDefs) {
		// create a service def
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		when(serviceDef.getPolicyConditions()).thenReturn(conditionDefs);
		return serviceDef;
	}
	
	RangerServiceDef getMockServiceDef(Map<String, String[]> pairs) {
		// create a service def
		RangerServiceDef serviceDef = mock(RangerServiceDef.class);
		if (pairs == null) {
			return serviceDef;
		}
		List<RangerPolicyConditionDef> conditions = getMockPolicyConditionDefs(pairs);
		when(serviceDef.getPolicyConditions()).thenReturn(conditions);
		return serviceDef;
	}
	
	// takes in a map of condition name to a an two element array where 1st element is evaluator-class-name and second is evaluator-options if any
	List<RangerPolicyConditionDef> getMockPolicyConditionDefs(Map<String, String[]> pairs) {
		List<RangerPolicyConditionDef> conditions = new ArrayList<RangerServiceDef.RangerPolicyConditionDef>();
		// null policy condition def collection should behave sensibly
		for (Map.Entry<String, String[]> anEntry : pairs.entrySet()) {
			RangerPolicyConditionDef aCondition = mock(RangerPolicyConditionDef.class);
			when(aCondition.getName()).thenReturn(anEntry.getKey());
			when(aCondition.getEvaluator()).thenReturn(anEntry.getValue()[0]);
			when(aCondition.getEvaluatorOptions()).thenReturn(anEntry.getValue()[1]);
			conditions.add(aCondition);
		}
		return conditions;
	}
	
	RangerPolicyConditionDef getMockPolicyConditionDef(String name, String evaluatorClassName, String evaluatorOption) {
		// null policy condition def collection should behave sensibly
		RangerPolicyConditionDef aCondition = mock(RangerPolicyConditionDef.class);
		when(aCondition.getName()).thenReturn(name);
		when(aCondition.getEvaluator()).thenReturn(evaluatorClassName);
		when(aCondition.getEvaluatorOptions()).thenReturn(evaluatorOption);
		return aCondition;
	}
	
	RangerPolicyItem createPolicyItemForConditions(String[] conditions) {

		List<RangerPolicyItemCondition> itemConditions = new ArrayList<RangerPolicy.RangerPolicyItemCondition>(conditions.length);
		for (String conditionName : conditions) {
			RangerPolicyItemCondition condition = mock(RangerPolicyItemCondition.class);
			when(condition.getType()).thenReturn(conditionName);
			itemConditions.add(condition);
		}

		RangerPolicyItem policyItem = mock(RangerPolicyItem.class);
		when(policyItem.getConditions()).thenReturn(itemConditions);
		
		return policyItem;
	}
	
	RangerAccessRequest createAccessRequestWithConditions(String[] conditionNames) {
		// let's first create a request with 2 different conditions
		Map<String, Object> context = new HashMap<String, Object>(conditionNames.length);
		for (String conditionName: conditionNames) {
			// value is not important for our test
			context.put(conditionName, conditionName + "-value");
		}
		RangerAccessRequest request = mock(RangerAccessRequest.class);
		when(request.getContext()).thenReturn(context);
		
		return request;
	}
}
