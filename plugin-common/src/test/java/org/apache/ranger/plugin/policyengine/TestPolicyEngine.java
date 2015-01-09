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

import static org.junit.Assert.*;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.TestPolicyEngine.PolicyEngineTestCase.TestData;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;


public class TestPolicyEngine {
	static RangerPolicyEngineImpl policyEngine = null;
	static Gson                   gsonBuilder  = null;


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		policyEngine = new RangerPolicyEngineImpl();
		gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
									   .setPrettyPrinting()
									   .registerTypeAdapter(RangerAccessRequest.class, new RangerAccessRequestDeserializer())
									   .registerTypeAdapter(RangerResource.class,  new RangerResourceDeserializer())
									   .create();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testPolicyEngine_hive() {
		String            filename = "/policyengine/test_policyengine_hive.json";
		InputStream       inStream = this.getClass().getResourceAsStream(filename);
		InputStreamReader reader   = new InputStreamReader(inStream);

		runTests(reader, filename);
	}

	@Test
	public void testPolicyEngine_hbase() {
		String            filename = "/policyengine/test_policyengine_hbase.json";
		InputStream       inStream = this.getClass().getResourceAsStream(filename);
		InputStreamReader reader   = new InputStreamReader(inStream);

		runTests(reader, filename);
	}

	public void runTests(InputStreamReader reader, String testName) {
		try {
			PolicyEngineTestCase testCase = gsonBuilder.fromJson(reader, PolicyEngineTestCase.class);

			assertTrue("invalid input: " + testName, testCase != null && testCase.serviceDef != null && testCase.policies != null && testCase.tests != null);

			policyEngine.setPolicies(testCase.serviceName, testCase.serviceDef, testCase.policies);

			for(TestData test : testCase.tests) {
				RangerAccessResult expected = test.result;
				RangerAccessResult result   = policyEngine.isAccessAllowed(test.request);

				assertEquals(test.name, expected, result);
			}
		} catch(Throwable excp) {
			excp.printStackTrace();
		}
		
	}

	static class PolicyEngineTestCase {
		public String             serviceName;
		public RangerServiceDef   serviceDef;
		public List<RangerPolicy> policies;
		public List<TestData>     tests;
		
		class TestData {
			public String              name;
			public RangerAccessRequest request;
			public RangerAccessResult  result;
		}
	}
	
	static class RangerAccessRequestDeserializer implements JsonDeserializer<RangerAccessRequest> {
		@Override
		public RangerAccessRequest deserialize(JsonElement jsonObj, Type type,
				JsonDeserializationContext context) throws JsonParseException {
			return gsonBuilder.fromJson(jsonObj, RangerAccessRequestImpl.class);
		}
	}
	
	static class RangerResourceDeserializer implements JsonDeserializer<RangerResource> {
		@Override
		public RangerResource deserialize(JsonElement jsonObj, Type type,
				JsonDeserializationContext context) throws JsonParseException {
			return gsonBuilder.fromJson(jsonObj, RangerResourceImpl.class);
		}
	}
}

