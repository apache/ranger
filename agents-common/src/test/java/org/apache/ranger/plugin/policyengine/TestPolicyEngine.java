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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.model.validation.RangerValidityScheduleValidator;
import org.apache.ranger.plugin.model.validation.ValidationFailureDetails;
import org.apache.ranger.plugin.policyengine.TestPolicyEngine.PolicyEngineTestCase.TestData;
import org.apache.ranger.plugin.policyevaluator.RangerValidityScheduleEvaluator;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerRequestedResources;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class TestPolicyEngine {
	static Gson gsonBuilder;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSSZ")
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
			auditProperties.setProperty("xasecure.audit.db.is.enabled", "false");
			auditProperties.setProperty("xasecure.audit.db.is.async", "false");
			auditProperties.setProperty("xasecure.audit.db.async.max.queue.size", "100000");
			auditProperties.setProperty("xasecure.audit.db.async.max.flush.interval.ms", "30000");
			auditProperties.setProperty("xasecure.audit.db.batch.size", "100");
		}

		AuditProviderFactory factory = new AuditProviderFactory();
		factory.init(auditProperties, "hdfs"); // second parameter does not matter for v2

		AuditHandler provider = factory.getAuditProvider();

		System.out.println("provider=" + provider.toString());

		File file = File.createTempFile("ranger-admin-test-site", ".xml");
		file.deleteOnExit();

		FileOutputStream outStream = new FileOutputStream(file);
		OutputStreamWriter writer = new OutputStreamWriter(outStream);

		/*
		// For setting up TestTagProvider

		writer.write("<configuration>\n" +
				"        <property>\n" +
				"                <name>ranger.plugin.tag.policy.rest.url</name>\n" +
				"                <value>http://os-def:6080</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.externalurl</name>\n" +
				"                <value>http://os-def:6080</value>\n" +
				"        </property>\n" +
				"</configuration>\n");
				*/

		writer.write("<configuration>\n" +
				/*
				// For setting up TestTagProvider
				"        <property>\n" +
				"                <name>ranger.plugin.tag.policy.rest.url</name>\n" +
				"                <value>http://os-def:6080</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.externalurl</name>\n" +
				"                <value>http://os-def:6080</value>\n" +
				"        </property>\n" +
				*/
				// For setting up x-forwarded-for for Hive
				"        <property>\n" +
				"                <name>ranger.plugin.hive.use.x-forwarded-for.ipaddress</name>\n" +
				"                <value>true</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.plugin.hive.trusted.proxy.ipaddresses</name>\n" +
				"                <value>255.255.255.255; 128.101.101.101;128.101.101.99</value>\n" +
				"        </property>\n" +
				"        <property>\n" +
				"                <name>ranger.plugin.tag.attr.additional.date.formats</name>\n" +
				"                <value>abcd||xyz||yyyy/MM/dd'T'HH:mm:ss.SSS'Z'</value>\n" +
				"        </property>\n" +
                "</configuration>\n");
		writer.close();

		RangerConfiguration config = RangerConfiguration.getInstance();
		config.addResource(new org.apache.hadoop.fs.Path(file.toURI()));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testPolicyEngine_hdfs_resourcespec() {
		String[] hdfsTestResourceFiles = { "/policyengine/test_policyengine_hdfs_resourcespec.json" };

		runTestsFromResourceFiles(hdfsTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hdfs() {
		String[] hdfsTestResourceFiles = { "/policyengine/test_policyengine_hdfs.json" };

		runTestsFromResourceFiles(hdfsTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hdfs_allaudit() {
		String[] hdfsTestResourceFiles = { "/policyengine/test_policyengine_hdfs_allaudit.json" };

		runTestsFromResourceFiles(hdfsTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hdfs_noaudit() {
		String[] hdfsTestResourceFiles = { "/policyengine/test_policyengine_hdfs_noaudit.json" };

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
	public void testPolicyEngine_hbase_with_multiple_matching_policies() {
		String[] hbaseTestResourceFiles = { "/policyengine/test_policyengine_hbase_multiple_matching_policies.json" };

		runTestsFromResourceFiles(hbaseTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hbase_namespace() {
		String[] hbaseTestResourceFiles = { "/policyengine/test_policyengine_hbase_namespace.json" };

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

	@Test
	public void testPolicyEngine_resourceAccessInfo() {
		String[] conditionsTestResourceFiles = { "/policyengine/test_policyengine_resource_access_info.json" };

		runTestsFromResourceFiles(conditionsTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_geo() {
		String[] conditionsTestResourceFiles = { "/policyengine/test_policyengine_geo.json" };

		runTestsFromResourceFiles(conditionsTestResourceFiles);
	}

	@Test
	public void testPolicyEngine_hiveForTag_filebased() {
		String[] conditionsTestResourceFiles = { "/policyengine/test_policyengine_tag_hive_filebased.json" };

		runTestsFromResourceFiles(conditionsTestResourceFiles);
	}

    @Test
    public void testPolicyEngine_hiveForShowDatabases() {
        String[] conditionsTestResourceFiles = { "/policyengine/test_policyengine_tag_hive_for_show_databases.json" };

        runTestsFromResourceFiles(conditionsTestResourceFiles);
    }

	@Test
	public void testPolicyEngine_descendant_tags() {
		String[] resourceFiles = {"/policyengine/test_policyengine_descendant_tags.json"};

		runTestsFromResourceFiles(resourceFiles);
	}

	@Test
	public void testPolicyEngine_hiveMasking() {
		String[] resourceFiles = {"/policyengine/test_policyengine_hive_mask_filter.json"};

		runTestsFromResourceFiles(resourceFiles);
	}

	@Test
	public void testPolicyEngine_hiveTagMasking() {
		String[] resourceFiles = {"/policyengine/test_policyengine_tag_hive_mask.json"};

		runTestsFromResourceFiles(resourceFiles);
	}

	@Test
	public void testPolicyEngine_owner() {
		String[] resourceFiles = {"/policyengine/test_policyengine_owner.json"};

		runTestsFromResourceFiles(resourceFiles);
	}
	@Test
	public void testPolicyEngine_temporary() {
		String[] resourceFiles = {"/policyengine/test_policyengine_temporary.json"};

		TimeZone defaultTZ = TimeZone.getDefault();
		TimeZone.setDefault(TimeZone.getTimeZone("PST"));

		runTestsFromResourceFiles(resourceFiles);

		TimeZone.setDefault(defaultTZ);
	}

	@Test
	public void testPolicyEngine_atlas() {
		String[] resourceFiles = { "/policyengine/test_policyengine_atlas.json" };

		runTestsFromResourceFiles(resourceFiles);
	}

	private void runTestsFromResourceFiles(String[] resourceNames) {
		for(String resourceName : resourceNames) {
			InputStream inStream = this.getClass().getResourceAsStream(resourceName);
			InputStreamReader reader   = new InputStreamReader(inStream);

			runTests(reader, resourceName);
		}
	}

	private void runTests(InputStreamReader reader, String testName) {
		PolicyEngineTestCase testCase = gsonBuilder.fromJson(reader, PolicyEngineTestCase.class);

		assertTrue("invalid input: " + testName, testCase != null && testCase.serviceDef != null && testCase.policies != null && testCase.tests != null);

		ServicePolicies servicePolicies = new ServicePolicies();
		servicePolicies.setServiceName(testCase.serviceName);
		servicePolicies.setServiceDef(testCase.serviceDef);
		servicePolicies.setPolicies(testCase.policies);

		if (StringUtils.isNotBlank(testCase.auditMode)) {
			servicePolicies.setAuditMode(testCase.auditMode);
		}

		if (null != testCase.tagPolicyInfo) {
			ServicePolicies.TagPolicies tagPolicies = new ServicePolicies.TagPolicies();
			tagPolicies.setServiceName(testCase.tagPolicyInfo.serviceName);
			tagPolicies.setServiceDef(testCase.tagPolicyInfo.serviceDef);
			tagPolicies.setPolicies(testCase.tagPolicyInfo.tagPolicies);

			if (StringUtils.isNotBlank(testCase.auditMode)) {
				tagPolicies.setAuditMode(testCase.auditMode);
			}
			servicePolicies.setTagPolicies(tagPolicies);
		}

		RangerPolicyEngineOptions policyEngineOptions = new RangerPolicyEngineOptions();

		policyEngineOptions.disableTagPolicyEvaluation = false;
		policyEngineOptions.disableAccessEvaluationWithPolicyACLSummary = false;

		boolean useForwardedIPAddress = RangerConfiguration.getInstance().getBoolean("ranger.plugin.hive.use.x-forwarded-for.ipaddress", false);
		String trustedProxyAddressString = RangerConfiguration.getInstance().get("ranger.plugin.hive.trusted.proxy.ipaddresses");
		String[] trustedProxyAddresses = StringUtils.split(trustedProxyAddressString, ';');
		if (trustedProxyAddresses != null) {
			for (int i = 0; i < trustedProxyAddresses.length; i++) {
				trustedProxyAddresses[i] = trustedProxyAddresses[i].trim();
			}
		}
		RangerPolicyEngine policyEngine = new RangerPolicyEngineImpl(testName, servicePolicies, policyEngineOptions);

		policyEngine.setUseForwardedIPAddress(useForwardedIPAddress);
		policyEngine.setTrustedProxyAddresses(trustedProxyAddresses);

		policyEngineOptions.disableAccessEvaluationWithPolicyACLSummary = true;
		RangerPolicyEngine policyEngineForResourceAccessInfo = new RangerPolicyEngineImpl(testName, servicePolicies, policyEngineOptions);

		policyEngineForResourceAccessInfo.setUseForwardedIPAddress(useForwardedIPAddress);
		policyEngineForResourceAccessInfo.setTrustedProxyAddresses(trustedProxyAddresses);

		long requestCount = 0L;

		RangerAccessRequest request = null;

		for(TestData test : testCase.tests) {
			request = test.request;
			if ((requestCount++ % 10) == 1) {
				policyEngine.reorderPolicyEvaluators();
			}
			if (request.getContext().containsKey(RangerAccessRequestUtil.KEY_CONTEXT_TAGS) ||
					request.getContext().containsKey(RangerAccessRequestUtil.KEY_CONTEXT_REQUESTED_RESOURCES)) {
				// Create a new AccessRequest
				RangerAccessRequestImpl newRequest =
						new RangerAccessRequestImpl(request.getResource(), request.getAccessType(),
								request.getUser(), request.getUserGroups());

				newRequest.setClientType(request.getClientType());
				newRequest.setAccessTime(request.getAccessTime());
				newRequest.setAction(request.getAction());
				newRequest.setRemoteIPAddress(request.getRemoteIPAddress());
				newRequest.setForwardedAddresses(request.getForwardedAddresses());
				newRequest.setRequestData(request.getRequestData());
				newRequest.setSessionId(request.getSessionId());

				Map<String, Object> context = request.getContext();
				String tagsJsonString = (String) context.get(RangerAccessRequestUtil.KEY_CONTEXT_TAGS);
				context.remove(RangerAccessRequestUtil.KEY_CONTEXT_TAGS);

				if(!StringUtils.isEmpty(tagsJsonString)) {
					try {
						Type setType = new TypeToken<Set<RangerTagForEval>>() {
						}.getType();
						Set<RangerTagForEval> tags = gsonBuilder.fromJson(tagsJsonString, setType);

						context.put(RangerAccessRequestUtil.KEY_CONTEXT_TAGS, tags);
					} catch (Exception e) {
						System.err.println("TestPolicyEngine.runTests(): error parsing TAGS JSON string in file " + testName + ", tagsJsonString=" +
								tagsJsonString + ", exception=" + e);
					}
				} else if (request.getContext().containsKey(RangerAccessRequestUtil.KEY_CONTEXT_REQUESTED_RESOURCES)) {
					String resourcesJsonString = (String) context.get(RangerAccessRequestUtil.KEY_CONTEXT_REQUESTED_RESOURCES);
					context.remove(RangerAccessRequestUtil.KEY_CONTEXT_REQUESTED_RESOURCES);
					if (!StringUtils.isEmpty(resourcesJsonString)) {
						try {
							/*
							Reader stringReader = new StringReader(resourcesJsonString);
							RangerRequestedResources resources = gsonBuilder.fromJson(stringReader, RangerRequestedResources.class);
							*/

							Type myType = new TypeToken<RangerRequestedResources>() {
							}.getType();
							RangerRequestedResources resources = gsonBuilder.fromJson(resourcesJsonString, myType);

							context.put(RangerAccessRequestUtil.KEY_CONTEXT_REQUESTED_RESOURCES, resources);
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
				RangerAccessResourceImpl accessResource = (RangerAccessResourceImpl) request.getResource();
				accessResource.setServiceDef(testCase.serviceDef);

				request = newRequest;

			} else
			if (!request.getContext().containsKey(RangerAccessRequestUtil.KEY_CONTEXT_REQUESTED_RESOURCES)) {
				policyEngine.preProcess(request);
			}

			RangerAccessResultProcessor auditHandler = new RangerDefaultAuditHandler();

			if(test.result != null) {
				RangerAccessResult expected = test.result;
				RangerAccessResult result   = policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, auditHandler);

				assertNotNull("result was null! - " + test.name, result);
				assertEquals("isAllowed mismatched! - " + test.name, expected.getIsAllowed(), result.getIsAllowed());
				assertEquals("isAudited mismatched! - " + test.name, expected.getIsAudited(), result.getIsAudited());
				assertEquals("policyId mismatched! - " + test.name, expected.getPolicyId(), result.getPolicyId());
			}

			if(test.dataMaskResult != null) {
				RangerAccessResult expected = test.dataMaskResult;
				RangerAccessResult result   = policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_DATAMASK, auditHandler);

				assertNotNull("result was null! - " + test.name, result);
				assertEquals("maskType mismatched! - " + test.name, expected.getMaskType(), result.getMaskType());
				assertEquals("maskCondition mismatched! - " + test.name, expected.getMaskCondition(), result.getMaskCondition());
				assertEquals("maskedValue mismatched! - " + test.name, expected.getMaskedValue(), result.getMaskedValue());
				assertEquals("policyId mismatched! - " + test.name, expected.getPolicyId(), result.getPolicyId());
			}

			if(test.rowFilterResult != null) {
				RangerAccessResult expected = test.rowFilterResult;
				RangerAccessResult result   = policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ROWFILTER, auditHandler);

				assertNotNull("result was null! - " + test.name, result);
				assertEquals("filterExpr mismatched! - " + test.name, expected.getFilterExpr(), result.getFilterExpr());
				assertEquals("policyId mismatched! - " + test.name, expected.getPolicyId(), result.getPolicyId());
			}

			if(test.resourceAccessInfo != null) {

				RangerResourceAccessInfo expected = new RangerResourceAccessInfo(test.resourceAccessInfo);
				RangerResourceAccessInfo result   = policyEngineForResourceAccessInfo.getResourceAccessInfo(test.request);

				assertNotNull("result was null! - " + test.name, result);
				assertEquals("allowedUsers mismatched! - " + test.name, expected.getAllowedUsers(), result.getAllowedUsers());
				assertEquals("allowedGroups mismatched! - " + test.name, expected.getAllowedGroups(), result.getAllowedGroups());
				assertEquals("deniedUsers mismatched! - " + test.name, expected.getDeniedUsers(), result.getDeniedUsers());
				assertEquals("deniedGroups mismatched! - " + test.name, expected.getDeniedGroups(), result.getDeniedGroups());
			}
		}
	}

	static class PolicyEngineTestCase {
		public String             serviceName;
		public RangerServiceDef   serviceDef;
		public List<RangerPolicy> policies;
		public TagPolicyInfo	  tagPolicyInfo;
		public String             auditMode;
		public List<TestData>     tests;
		
		class TestData {
			public String              name;
			public RangerAccessRequest request;
			public RangerAccessResult  result;
			public RangerAccessResult  dataMaskResult;
			public RangerAccessResult rowFilterResult;
			public RangerResourceAccessInfo resourceAccessInfo;
		}

		class TagPolicyInfo {
			public String	serviceName;
			public RangerServiceDef serviceDef;
			public List<RangerPolicy> tagPolicies;
		}
	}

    static class ValiditySchedulerTestResult {
        boolean isValid;
        int validationFailureCount;
        boolean isApplicable;
    }

    static class ValiditySchedulerTestCase {
        String name;
        List<RangerValiditySchedule> validitySchedules;
        Date accessTime;
        ValiditySchedulerTestResult result;
    }

    @Test
    public void testValiditySchedulerInvalid() {
        String resourceName = "/policyengine/validityscheduler/test-validity-schedules-invalid.json";

        runValiditySchedulerTests(resourceName);
    }

    @Test
    public void testValiditySchedulerValid() {
        String resourceName = "/policyengine/validityscheduler/test-validity-schedules-valid.json";

        runValiditySchedulerTests(resourceName);
    }

    @Test
    public void testValiditySchedulerApplicable() {
        String resourceName = "/policyengine/validityscheduler/test-validity-schedules-valid-and-applicable.json";

        runValiditySchedulerTests(resourceName);
    }

    private void runValiditySchedulerTests(String resourceName) {
        TimeZone defaultTZ = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("PST"));

        List<ValiditySchedulerTestCase> testCases = null;

        InputStream inStream = this.getClass().getResourceAsStream(resourceName);
        InputStreamReader reader   = new InputStreamReader(inStream);
        try {
            Type listType = new TypeToken<List<ValiditySchedulerTestCase>>() {}.getType();
            testCases = gsonBuilder.fromJson(reader, listType);
        } catch (Exception e) {
            assertFalse("Exception in reading validity-scheduler test cases.", true);
        }

        assertNotNull("TestCases are null!", testCases);


        if (CollectionUtils.isNotEmpty(testCases)) {
            for (ValiditySchedulerTestCase testCase : testCases) {
                boolean isValid = true;
                List<ValidationFailureDetails> validationFailures = new ArrayList<>();
                boolean isApplicable = false;

                List<RangerValiditySchedule> validatedSchedules = new ArrayList<>();

                for (RangerValiditySchedule validitySchedule : testCase.validitySchedules) {
                    RangerValidityScheduleValidator validator = new RangerValidityScheduleValidator(validitySchedule);
                    RangerValiditySchedule validatedSchedule = validator.validate(validationFailures);
                    isValid = isValid && validatedSchedule != null;
                    if (isValid) {
                        validatedSchedules.add(validatedSchedule);
                    }
                }
                if (isValid) {
                    for (RangerValiditySchedule validSchedule : validatedSchedules) {
                        isApplicable = new RangerValidityScheduleEvaluator(validSchedule).isApplicable(testCase.accessTime.getTime());
                        if (isApplicable) {
                            break;
                        }
                    }
                }

                assertTrue(testCase.name, isValid == testCase.result.isValid);
                assertTrue(testCase.name, isApplicable == testCase.result.isApplicable);
                assertTrue(testCase.name + ", [" + validationFailures +"]", validationFailures.size() == testCase.result.validationFailureCount);
            }
        }
        TimeZone.setDefault(defaultTZ);
    }

    static class RangerAccessRequestDeserializer implements JsonDeserializer<RangerAccessRequest> {
		@Override
		public RangerAccessRequest deserialize(JsonElement jsonObj, Type type,
				JsonDeserializationContext context) throws JsonParseException {
			RangerAccessRequestImpl ret = gsonBuilder.fromJson(jsonObj, RangerAccessRequestImpl.class);

			ret.setAccessType(ret.getAccessType()); // to force computation of isAccessTypeAny and isAccessTypeDelegatedAdmin
			if (ret.getAccessTime() == null) {
				ret.setAccessTime(new Date());
			}

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

