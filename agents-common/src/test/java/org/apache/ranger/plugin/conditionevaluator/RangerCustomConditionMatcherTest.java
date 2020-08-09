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


import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangerCustomConditionMatcherTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRangerAnyOfExpectedTagsPresentConditionEvaluator() {
		List<String> policyConditionTags = Arrays.asList("PCI", "PII");
		RangerAnyOfExpectedTagsPresentConditionEvaluator tagsAnyPresentConditionEvaluator = createRangerAnyOfExpectedTagsPresentConditionEvaluator(policyConditionTags);

		// When any tag in the resourceTags matches policyConditionTags it should return TRUE
		List<String> resourceTags = Arrays.asList("PCI", "PHI");
		Assert.assertTrue(tagsAnyPresentConditionEvaluator.isMatched(createRequest(resourceTags)));
		resourceTags = Arrays.asList("PHI", "PII" ,"HIPPA");
		Assert.assertTrue(tagsAnyPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When no Tag Matches between resourceTags and PolicyConditionTags it should return FALSE
		resourceTags = Arrays.asList("HIPPA", "PHI");
		Assert.assertFalse(tagsAnyPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When policyConditionTags and resourceTags contains empty set it should return TRUE as empty set matches.
		policyConditionTags = Arrays.asList("");
		resourceTags = Arrays.asList("");
		tagsAnyPresentConditionEvaluator = createRangerAnyOfExpectedTagsPresentConditionEvaluator(policyConditionTags);
		Assert.assertTrue(tagsAnyPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When policyConditionTags is not empty and resourceTags empty it should return FALSE as there is no any match.
		policyConditionTags = Arrays.asList("PCI", "PII");
		resourceTags = Arrays.asList("");
		tagsAnyPresentConditionEvaluator = createRangerAnyOfExpectedTagsPresentConditionEvaluator(policyConditionTags);
		Assert.assertFalse(tagsAnyPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When policyConditionTags is empty and resourceTags in not empty it should return FALSE as there is no any match.
		policyConditionTags = Arrays.asList("");
		resourceTags = Arrays.asList("PCI", "PII");
		tagsAnyPresentConditionEvaluator = createRangerAnyOfExpectedTagsPresentConditionEvaluator(policyConditionTags);
		Assert.assertFalse(tagsAnyPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When policyConditionTags is not empty and resourceTags is NULL it should return FALSE as there is no any match.
		policyConditionTags = Arrays.asList("PCI", "PII");
		resourceTags = null;
		tagsAnyPresentConditionEvaluator = createRangerAnyOfExpectedTagsPresentConditionEvaluator(policyConditionTags);
		Assert.assertFalse(tagsAnyPresentConditionEvaluator.isMatched(createRequest(resourceTags)));
	}


	@Test
	public void testRangerTagsNotPresentConditionEvaluator() {

		List<String> policyConditionTags = Arrays.asList("PCI", "PII");
		RangerNoneOfExpectedTagsPresentConditionEvaluator tagsNotPresentConditionEvaluator = createRangerTagsNotPresentConditionEvaluator(policyConditionTags);

		// When no Tag Matches between resourceTags and PolicyConditionTags it should return TRUE
		List<String> resourceTags = Arrays.asList("HIPPA", "PHI");
		Assert.assertTrue(tagsNotPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When any Tag Matches between resourceTags and PolicyConditionTags it should return FALSE
		resourceTags = Arrays.asList("HIPPA", "PII", "");
		Assert.assertFalse(tagsNotPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When policyConditionTags and resourceTags both are empty is should return FALSE as both matches.
		policyConditionTags = Arrays.asList("");
		resourceTags = Arrays.asList("");
		tagsNotPresentConditionEvaluator = createRangerTagsNotPresentConditionEvaluator(policyConditionTags);
		Assert.assertFalse(tagsNotPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When policyConditionTags is not empty and resourceTags empty it should return TRUE as there is no tag match between these two sets.
		policyConditionTags = Arrays.asList("PCI", "PII");
		resourceTags = Arrays.asList("");
		tagsNotPresentConditionEvaluator = createRangerTagsNotPresentConditionEvaluator(policyConditionTags);
		Assert.assertTrue(tagsNotPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When policyConditionTags is  empty and resourceTags in not empty it should return TRUE as there is no tag match between these two sets.
		policyConditionTags = Arrays.asList("");
		resourceTags = Arrays.asList("PCI", "PII");
		tagsNotPresentConditionEvaluator = createRangerTagsNotPresentConditionEvaluator(policyConditionTags);
		Assert.assertTrue(tagsNotPresentConditionEvaluator.isMatched(createRequest(resourceTags)));

		// When policyConditionTags is not empty and resourceTags is NULL it should return TRUE as there is no tag match between these two sets.
		policyConditionTags = Arrays.asList("PCI", "PII");
		resourceTags = Arrays.asList("");
		tagsNotPresentConditionEvaluator = createRangerTagsNotPresentConditionEvaluator(policyConditionTags);
		Assert.assertTrue(tagsNotPresentConditionEvaluator.isMatched(createRequest(resourceTags)));
	}

	RangerAnyOfExpectedTagsPresentConditionEvaluator createRangerAnyOfExpectedTagsPresentConditionEvaluator(List<String> policyConditionTags) {
		RangerAnyOfExpectedTagsPresentConditionEvaluator matcher = new RangerAnyOfExpectedTagsPresentConditionEvaluator();

		if (policyConditionTags == null) {
			matcher.setConditionDef(null);
			matcher.setPolicyItemCondition(null);
		} else {
			RangerPolicyItemCondition condition = mock(RangerPolicyItemCondition.class);
			when(condition.getValues()).thenReturn(policyConditionTags);
			matcher.setConditionDef(null);
			matcher.setPolicyItemCondition(condition);
		}

		matcher.init();

		return matcher;
	}

	RangerNoneOfExpectedTagsPresentConditionEvaluator createRangerTagsNotPresentConditionEvaluator(List<String> policyConditionTags) {
		RangerNoneOfExpectedTagsPresentConditionEvaluator matcher = new RangerNoneOfExpectedTagsPresentConditionEvaluator();

		if (policyConditionTags == null) {
			matcher.setConditionDef(null);
			matcher.setPolicyItemCondition(null);
		} else {
			RangerPolicyItemCondition condition = mock(RangerPolicyItemCondition.class);
			when(condition.getValues()).thenReturn(policyConditionTags);
			matcher.setConditionDef(null);
			matcher.setPolicyItemCondition(condition);
		}

		matcher.init();

		return matcher;
	}

	RangerAccessRequest createRequest(List<String> resourceTags) {
		RangerAccessResource                  resource          = mock(RangerAccessResource.class);
		when(resource.getAsString()).thenReturn("Dummy resource");

		RangerAccessRequest                   request           = new RangerAccessRequestImpl(resource,"dummy","test", null, null);
		Set<RangerTagForEval>                 rangerTagForEvals = new HashSet<>();
		RangerPolicyResourceMatcher.MatchType matchType         = RangerPolicyResourceMatcher.MatchType.NONE;

		if (resourceTags != null) {
			for (String resourceTag : resourceTags) {
				RangerTag tag = new RangerTag(UUID.randomUUID().toString(), resourceTag, null, null, null, null);
				rangerTagForEvals.add(new RangerTagForEval(tag, matchType));
			}
			request.getContext().put("TAGS", rangerTagForEvals);
		}  else {
			request.getContext().put("TAGS", null);
		}

		return request;
	}
}
