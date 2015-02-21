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

package org.apache.ranger.plugin.conditionevaluator;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.junit.Test;

public class RangerSimpleMatcherTest {

	final String _conditionOption = "key1";
	@Test
	public void testIsMatched_happyPath() {
		// this documents some unexpected behavior of the ip matcher
		RangerSimpleMatcher ipMatcher = createMatcher(new String[]{"US", "C*"} );
		assertTrue(ipMatcher.isMatched(createRequest("US")));
		assertTrue(ipMatcher.isMatched(createRequest("CA")));
		assertTrue(ipMatcher.isMatched(createRequest("C---")));
		assertFalse(ipMatcher.isMatched(createRequest(" US ")));
		assertFalse(ipMatcher.isMatched(createRequest("Us")));
		assertFalse(ipMatcher.isMatched(createRequest("ca")));
	}
	
	@Test
	public void test_firewallings() {
		
		// create a request for some policyValue, say, country and use it to match against matcher initialized with all sorts of bad data
		RangerAccessRequest request = createRequest("AB");

		RangerSimpleMatcher matcher = new RangerSimpleMatcher();
		// Matcher initialized with null policy should behave sensibly!  It matches everything!
		matcher.init(null, null);
		assertTrue(matcher.isMatched(request));
		
		RangerPolicyItemCondition policyItemCondition = mock(RangerPolicyItemCondition.class);
		matcher.init(null, policyItemCondition);
		assertTrue(matcher.isMatched(request));
		
		RangerPolicyConditionDef conditionDef = mock(RangerPolicyConditionDef.class);
		matcher.init(conditionDef, null);
		assertTrue(matcher.isMatched(request));
		
		// so should a policy item condition with initialized with null list of values 
		when(policyItemCondition.getValues()).thenReturn(null);
		matcher.init(conditionDef, policyItemCondition);
		assertTrue(matcher.isMatched(request));

		// not null item condition with empty condition list
		List<String> values = new ArrayList<String>();
		when(policyItemCondition.getValues()).thenReturn(values);
		matcher.init(conditionDef, policyItemCondition);
		assertTrue(matcher.isMatched(request));

		// values as sensible items in it, however, the conditionDef has null evaluator option, so that too suppresses any check
		values.add("AB");
		when(policyItemCondition.getValues()).thenReturn(values);
		when(conditionDef.getEvaluatorOptions()).thenReturn(null);
		matcher.init(conditionDef, policyItemCondition);
		assertTrue(matcher.isMatched(request));

		// If evaluator option on the condition def is non-null then it starts to evaluate for real
		when(conditionDef.getEvaluatorOptions()).thenReturn(_conditionOption);
		matcher.init(conditionDef, policyItemCondition);
		assertTrue(matcher.isMatched(request));
	}
	
	RangerSimpleMatcher createMatcher(String[] ipArray) {
		RangerSimpleMatcher matcher = new RangerSimpleMatcher();

		if (ipArray == null) {
			matcher.init(null, null);
		} else {
			RangerPolicyItemCondition condition = mock(RangerPolicyItemCondition.class);
			List<String> addresses = Arrays.asList(ipArray);
			when(condition.getValues()).thenReturn(addresses);
			
			RangerPolicyConditionDef conditionDef = mock(RangerPolicyConditionDef.class);
			when(conditionDef.getEvaluatorOptions()).thenReturn(_conditionOption);
			matcher.init(conditionDef, condition);
		}
		
		return matcher;
	}
	
	RangerAccessRequest createRequest(String value) {
		Map<String, Object> context = new HashMap<String, Object>();
		context.put(_conditionOption, value);
		RangerAccessRequest request = mock(RangerAccessRequest.class);
		when(request.getContext()).thenReturn(context);
		return request;
	}
}
