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

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.TestPolicyEngine.PolicyEngineTestCase.TestData;
import org.apache.ranger.plugin.util.RangerRequestedResources;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;


public class TestPolicyEngine {
	static RangerPolicyEngine policyEngine = null;
	static Gson               gsonBuilder  = null;


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
				.setPrettyPrinting()
				.registerTypeAdapter(RangerAccessRequest.class, new RangerAccessRequestDeserializer())
				.registerTypeAdapter(RangerAccessResource.class, new RangerResourceDeserializer())
				.create();

		// For setting up auditProvider
		Properties auditProperties = new Properties();

		String AUDIT_PROPERTIES_FILE = "xasecure-audit.properties";

		File propFile = new File(AUDIT_PROPERTIES_FILE);

		if (propFile.exists()) {
			System.out.println("Loading Audit properties file" + AUDIT_PROPERTIES_FILE);

			auditProperties.load(new FileInputStream(propFile));
		} else {
			System.out.println("Audit properties file missing: " + AUDIT_PROPERTIES_FILE);

			auditProperties.setProperty("xasecure.audit.jpa.javax.persistence.jdbc.url", "jdbc:mysql://node-1:3306/xasecure_audit");
			auditProperties.setProperty("xasecure.audit.jpa.javax.persistence.jdbc.user", "xalogger");
			auditProperties.setProperty("xasecure.audit.jpa.javax.persistence.jdbc.password", "xalogger");
			auditProperties.setProperty("xasecure.audit.jpa.javax.persistence.jdbc.driver", "com.mysql.jdbc.Driver");

			auditProperties.setProperty("xasecure.audit.is.enabled", "false"); // Set this to true to enable audit logging
			auditProperties.setProperty("xasecure.audit.log4j.is.enabled", "false");
			auditProperties.setProperty("xasecure.audit.log4j.is.async", "false");
			auditProperties.setProperty("xasecure.audit.log4j.async.max.queue.size", "100000");
			auditProperties.setProperty("xasecure.audit.log4j.async.max.flush.interval.ms", "30000");
			auditProperties.setProperty("xasecure.audit.db.is.enabled", "true");
			auditProperties.setProperty("xasecure.audit.db.is.async", "false");
			auditProperties.setProperty("xasecure.audit.db.async.max.queue.size", "100000");
			auditProperties.setProperty("xasecure.audit.db.async.max.flush.interval.ms", "30000");
			auditProperties.setProperty("xasecure.audit.db.batch.size", "100");
		}

		AuditProviderFactory.getInstance().init(auditProperties, "hdfs"); // second parameter does not matter for v2

		AuditHandler provider = AuditProviderFactory.getAuditProvider();

		System.out.println("provider=" + provider.toString());

/*
		// For setting up TestTagProvider

		Path filePath = new Path("file:///tmp/ranger-admin-test-site.xml");
		Configuration config = new Configuration();

		FileSystem fs = filePath.getFileSystem(config);

		FSDataOutputStream outStream = fs.create(filePath, true);

		OutputStreamWriter writer = new OutputStreamWriter(outStream);

		writer.write("<configuration>\n" +
				"        <property>\n" +
				"                <name>ranger.plugin.tag.policy.rest.url</name>\n" +
				"                <value>http://node-1.example.com:6080</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.externalurl</name>\n" +
				"                <value>http://node-1.example.com:6080</value>\n" +
				"        </property>\n" +
				"</configuration>\n");

		writer.close();

		RangerConfiguration rangerConfig = RangerConfiguration.getInstance();
		rangerConfig.addResource(filePath);
*/
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testPolicyEngine_hdfs() {
		String[] hdfsTestResourceFiles = { "/policyengine/test_policyengine_hdfs.json" };

		runTestsFromResourceFiles(hdfsTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hdfsForTag() {
		String[] hdfsTestResourceFiles = { "/policyengine/test_policyengine_tag_hdfs.json" };

		runTestsFromResourceFiles(hdfsTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hive() {
		String[] hiveTestResourceFiles = { "/policyengine/test_policyengine_hive.json" };

		runTestsFromResourceFiles(hiveTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hiveForTag() {
		String[] hiveTestResourceFiles = { "/policyengine/test_policyengine_tag_hive.json" };

		runTestsFromResourceFiles(hiveTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hbase() {
		String[] hbaseTestResourceFiles = { "/policyengine/test_policyengine_hbase.json" };

		runTestsFromResourceFiles(hbaseTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_conditions() {
		String[] conditionsTestResourceFiles = { "/policyengine/test_policyengine_conditions.json" };

		runTestsFromResourceFiles(conditionsTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hive_mutex_conditions() {
		String[] conditionsTestResourceFiles = { "/policyengine/test_policyengine_hive_mutex_conditions.json" };

		runTestsFromResourceFiles(conditionsTestResourceFiles);
	}

	private void runTestsFromResourceFiles(String[] resourceNames) {
		for(String resourceName : resourceNames) {
			InputStream       inStream = this.getClass().getResourceAsStream(resourceName);
			InputStreamReader reader   = new InputStreamReader(inStream);

			runTests(reader, resourceName);
		}
	}

	private void runTests(InputStreamReader reader, String testName) {
		PolicyEngineTestCase testCase = gsonBuilder.fromJson(reader, PolicyEngineTestCase.class);

		assertTrue("invalid input: " + testName, testCase != null && testCase.serviceDef != null && testCase.policies != null && testCase.tests != null);

		ServicePolicies servicePolicies = new ServicePolicies();
		servicePolicies.setServiceName(testCase.serviceName);;
		servicePolicies.setServiceDef(testCase.serviceDef);
		servicePolicies.setPolicies(testCase.policies);

		if (null != testCase.tagPolicyInfo) {
			ServicePolicies.TagPolicies tagPolicies = new ServicePolicies.TagPolicies();
			tagPolicies.setServiceName(testCase.tagPolicyInfo.serviceName);
			tagPolicies.setServiceDef(testCase.tagPolicyInfo.serviceDef);
			tagPolicies.setPolicies(testCase.tagPolicyInfo.tagPolicies);

			servicePolicies.setTagPolicies(tagPolicies);
		}

		String componentName = testCase.serviceDef.getName();

		RangerPolicyEngineOptions policyEngineOptions = new RangerPolicyEngineOptions();

		// Uncomment next line for testing tag-policy evaluation
		policyEngineOptions.disableTagPolicyEvaluation = false;

		policyEngine = new RangerPolicyEngineImpl(servicePolicies, policyEngineOptions);

		RangerAccessRequest request = null;

		for(TestData test : testCase.tests) {

			if (test.request.getContext().containsKey(RangerPolicyEngine.KEY_CONTEXT_TAGS) ||
					test.request.getContext().containsKey(RangerRequestedResources.KEY_CONTEXT_REQUESTED_RESOURCES)) {
				// Create a new AccessRequest
				RangerAccessRequestImpl newRequest =
						new RangerAccessRequestImpl(test.request.getResource(), test.request.getAccessType(),
								test.request.getUser(), test.request.getUserGroups());

				newRequest.setClientType(test.request.getClientType());
				newRequest.setAccessTime(test.request.getAccessTime());
				newRequest.setAction(test.request.getAction());
				newRequest.setClientIPAddress(test.request.getClientIPAddress());
				newRequest.setRequestData(test.request.getRequestData());
				newRequest.setSessionId(test.request.getSessionId());

				Map<String, Object> context = test.request.getContext();
				String tagsJsonString = (String) context.get(RangerPolicyEngine.KEY_CONTEXT_TAGS);
				context.remove(RangerPolicyEngine.KEY_CONTEXT_TAGS);

				if(!StringUtils.isEmpty(tagsJsonString)) {
					try {
						Type listType = new TypeToken<List<RangerTag>>() {
						}.getType();
						List<RangerTag> tagList = gsonBuilder.fromJson(tagsJsonString, listType);

						context.put(RangerPolicyEngine.KEY_CONTEXT_TAGS, tagList);
					} catch (Exception e) {
						System.err.println("TestPolicyEngine.runTests(): error parsing TAGS JSON string in file " + testName + ", tagsJsonString=" +
								tagsJsonString + ", exception=" + e);
					}
				} else if (test.request.getContext().containsKey(RangerRequestedResources.KEY_CONTEXT_REQUESTED_RESOURCES)) {
					String resourcesJsonString = (String) context.get(RangerRequestedResources.KEY_CONTEXT_REQUESTED_RESOURCES);
					context.remove(RangerRequestedResources.KEY_CONTEXT_REQUESTED_RESOURCES);
					if (!StringUtils.isEmpty(resourcesJsonString)) {
						try {
							/*
							Reader stringReader = new StringReader(resourcesJsonString);
							RangerRequestedResources resources = gsonBuilder.fromJson(stringReader, RangerRequestedResources.class);
							*/

							Type myType = new TypeToken<RangerRequestedResources>() {
							}.getType();
							RangerRequestedResources resources = gsonBuilder.fromJson(resourcesJsonString, myType);

							context.put(RangerRequestedResources.KEY_CONTEXT_REQUESTED_RESOURCES, resources);
						} catch (Exception e) {
							System.err.println("TestPolicyEngine.runTests(): error parsing REQUESTED_RESOURCES string in file " + testName + ", resourcesJsonString=" +
									resourcesJsonString + ", exception=" + e);
						}
					}
				}
				newRequest.setContext(context);

				// accessResource.ServiceDef is set here, so that we can skip call to policyEngine.preProcess() which
				// sets the serviceDef in the resource AND calls enrichers. We dont want enrichers to be called when
				// context already contains tags -- This may change when we want enrichers to enrich request in the
				// presence of tags!!!

				// Safe cast
				RangerAccessResourceImpl accessResource = (RangerAccessResourceImpl) test.request.getResource();
				accessResource.setServiceDef(testCase.serviceDef);

				request = newRequest;

			} else
			if (test.request.getContext().containsKey(RangerRequestedResources.KEY_CONTEXT_REQUESTED_RESOURCES)) {
			}
			else {
				request = test.request;
				policyEngine.preProcess(request);
			}

			RangerAccessResult expected = test.result;
			RangerAccessResultProcessor auditHandler = new RangerDefaultAuditHandler();

			RangerAccessResult result   = policyEngine.isAccessAllowed(request, auditHandler);

			assertNotNull("result was null! - " + test.name, result);
			assertEquals("isAllowed mismatched! - " + test.name, expected.getIsAllowed(), result.getIsAllowed());
			assertEquals("isAudited mismatched! - " + test.name, expected.getIsAudited(), result.getIsAudited());
			assertEquals("policyId mismatched! - " + test.name, expected.getPolicyId(), result.getPolicyId());
		}
	}

	static class PolicyEngineTestCase {
		public String             serviceName;
		public RangerServiceDef   serviceDef;
		public List<RangerPolicy> policies;
		public TagPolicyInfo	tagPolicyInfo;
		public List<TestData>     tests;
		
		class TestData {
			public String              name;
			public RangerAccessRequest request;
			public RangerAccessResult  result;
		}

		class TagPolicyInfo {
			public String	serviceName;
			public RangerServiceDef serviceDef;
			public List<RangerPolicy> tagPolicies;
		}
	}
	
	static class RangerAccessRequestDeserializer implements JsonDeserializer<RangerAccessRequest> {
		@Override
		public RangerAccessRequest deserialize(JsonElement jsonObj, Type type,
				JsonDeserializationContext context) throws JsonParseException {
			RangerAccessRequestImpl ret = gsonBuilder.fromJson(jsonObj, RangerAccessRequestImpl.class);

			ret.setAccessType(ret.getAccessType()); // to force computation of isAccessTypeAny and isAccessTypeDelegatedAdmin

			return ret;
		}
	}
	
	static class RangerResourceDeserializer implements JsonDeserializer<RangerAccessResource> {
		@Override
		public RangerAccessResource deserialize(JsonElement jsonObj, Type type,
				JsonDeserializationContext context) throws JsonParseException {
			return gsonBuilder.fromJson(jsonObj, RangerAccessResourceImpl.class);
		}
	}
}

