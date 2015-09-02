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

package org.apache.ranger.plugin.policyengine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyengine.TestPolicyDb.PolicyDbTestCase.TestData;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TestPolicyDb {
	static Gson gsonBuilder  = null;


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
									   .setPrettyPrinting()
									   .create();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testPolicyDb_hdfs() {
		String[] hdfsTestResourceFiles = { "/policyengine/test_policydb_hdfs.json" };

		runTestsFromResourceFiles(hdfsTestResourceFiles);
	}

	private void runTestsFromResourceFiles(String[] resourceNames) {
		for(String resourceName : resourceNames) {
			InputStream       inStream = this.getClass().getResourceAsStream(resourceName);
			InputStreamReader reader   = new InputStreamReader(inStream);

			runTests(reader, resourceName);
		}
	}

	private void runTests(InputStreamReader reader, String testName) {
		PolicyDbTestCase testCase = gsonBuilder.fromJson(reader, PolicyDbTestCase.class);

		assertTrue("invalid input: " + testName, testCase != null && testCase.servicePolicies != null && testCase.tests != null && testCase.servicePolicies.getPolicies() != null);


		RangerPolicyEngineOptions policyEngineOptions = new RangerPolicyEngineOptions();

		policyEngineOptions.evaluatorType           = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
		policyEngineOptions.cacheAuditResults       = false;
		policyEngineOptions.disableContextEnrichers = true;
		policyEngineOptions.disableCustomConditions = true;

		RangerPolicyEngine policyEngine = new RangerPolicyEngineImpl("test-policydb", testCase.servicePolicies, policyEngineOptions);

		for(TestData test : testCase.tests) {
			boolean expected = test.result;

			if(test.allowedPolicies != null) {
				List<RangerPolicy> allowedPolicies = policyEngine.getAllowedPolicies(test.user, test.userGroups, test.accessType);

				assertEquals("allowed-policy count mismatch!", test.allowedPolicies.size(), allowedPolicies.size());
				
				Set<Long> allowedPolicyIds = new HashSet<Long>();
				for(RangerPolicy allowedPolicy : allowedPolicies) {
					allowedPolicyIds.add(allowedPolicy.getId());
				}
				assertEquals("allowed-policy list mismatch!", test.allowedPolicies, allowedPolicyIds);
			} else {
				boolean result = policyEngine.isAccessAllowed(test.resources, test.user, test.userGroups, test.accessType);

				assertEquals("isAccessAllowed mismatched! - " + test.name, expected, result);
			}
		}
	}

	static class PolicyDbTestCase {
		public ServicePolicies servicePolicies;
		public List<TestData>  tests;
		
		class TestData {
			public String                            name;
			public Map<String, RangerPolicyResource> resources;
			public String                            user;
			public Set<String>                       userGroups;
			public String                            accessType;
			public boolean                           result;
			public Set<Long>                         allowedPolicies;
		}
	}
}
