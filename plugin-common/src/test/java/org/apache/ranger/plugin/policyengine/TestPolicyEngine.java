package org.apache.ranger.plugin.policyengine;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.List;


import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.TestPolicyEngine.PolicyEngineTests.TestData;
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
	static RangerPolicyEngine policyEngine = null;
	static Gson               gsonBuilder  = null;


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
	public void testPolicyEngine_01() {
		String            filename = "/policyengine/test_policyengine_01.json";
		InputStream       inStream = this.getClass().getResourceAsStream(filename);
		InputStreamReader reader   = new InputStreamReader(inStream);

		runTests(reader, filename);
	}

	public void runTests(InputStreamReader reader, String testName) {
		try {
			PolicyEngineTests tests = gsonBuilder.fromJson(reader, PolicyEngineTests.class);

			assertTrue("invalid input: " + testName, tests != null && tests.serviceDef != null && tests.policies != null && tests.tests != null);

			policyEngine.setPolicies(tests.serviceDef, tests.policies);
			
			for(TestData td : tests.tests) {
				RangerAccessResult expected = td.result;
				RangerAccessResult result   = policyEngine.isAccessAllowed(td.request);

				assertEquals(result.getResult(), expected.getResult());
				assertEquals(result.isAudited(), expected.isAudited());
				assertEquals(result.getPolicyId(), expected.getPolicyId());
			}
		} catch(Throwable excp) {
			excp.printStackTrace();
		}
		
	}

	static class PolicyEngineTests {
		public RangerServiceDef      serviceDef;
		public List<RangerPolicy>    policies;
		public List<TestData>        tests;
		
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

