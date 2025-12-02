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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestInlinePolicyEvaluator {
    Gson gsonBuilder;

    @Before
    public void init() {
        gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSSZ")
                .setPrettyPrinting()
                .registerTypeAdapter(RangerAccessRequest.class, new RangerAccessRequestDeserializer())
                .registerTypeAdapter(RangerAccessResource.class, new RangerResourceDeserializer())
                .create();
    }

    @After
    public void cleanup() {
    }

    @Test
    public void testOzoneInlinePolicies() throws IOException {
        runTests("/policyevaluator/test_inline_policies_ozone.json");
    }

    private void runTests(String resourceName) throws IOException {
        try (InputStream inStream = this.getClass().getResourceAsStream(resourceName)) {
            assertNotNull("failed to find resource '" + resourceName + "'", inStream);

            InputStreamReader reader = new InputStreamReader(inStream);

            runTestCase(gsonBuilder.fromJson(reader, PolicyEngineTestCase.class));
        }
    }

    private void runTestCase(PolicyEngineTestCase testCase) {
        ServicePolicies  servicePolicies = testCase.servicePolicies;
        RangerServiceDef serviceDef      = servicePolicies.getServiceDef();

        ServiceDefUtil.normalize(serviceDef);

        RangerPolicyEngineOptions   policyEngineOptions = new RangerPolicyEngineOptions();
        RangerPluginContext         pluginContext       = new RangerPluginContext(new RangerPluginConfig(serviceDef.getName(), servicePolicies.getServiceName(), null, "cl1", "on-prem", policyEngineOptions));
        RangerPolicyEngine          policyEngine        = new RangerPolicyEngineImpl(servicePolicies, pluginContext, null);
        RangerAccessResultProcessor auditHandler        = new RangerDefaultAuditHandler(pluginContext.getConfig());

        for (TestData test : testCase.tests) {
            RangerAccessResult  expected = test.result;
            RangerAccessRequest request  = test.request;

            RangerAccessResult result = policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, auditHandler);

            assertNotNull("result was null! - " + test.name, result);
            assertEquals("isAllowed mismatched! - " + test.name, expected.getIsAllowed(), result.getIsAllowed());
            assertEquals("isAudited mismatched! - " + test.name, expected.getIsAudited(), result.getIsAudited());
            assertEquals("policyId mismatched! - " + test.name, expected.getPolicyId(), result.getPolicyId());
        }
    }

    static class PolicyEngineTestCase {
        ServicePolicies       servicePolicies;
        public List<TestData> tests;
    }

    static class TestData {
        public String              name;
        public RangerAccessRequest request;
        public RangerAccessResult  result;
    }

    class RangerAccessRequestDeserializer implements JsonDeserializer<RangerAccessRequest> {
        @Override
        public RangerAccessRequest deserialize(JsonElement jsonObj, Type type, JsonDeserializationContext context) throws JsonParseException {
            RangerAccessRequestImpl ret = gsonBuilder.fromJson(jsonObj, RangerAccessRequestImpl.class);

            ret.setAccessType(ret.getAccessType()); // to force computation of isAccessTypeAny and isAccessTypeDelegatedAdmin

            if (ret.getAccessTime() == null) {
                ret.setAccessTime(new Date());
            }

            return ret;
        }
    }

    class RangerResourceDeserializer implements JsonDeserializer<RangerAccessResource> {
        @Override
        public RangerAccessResource deserialize(JsonElement jsonObj, Type type, JsonDeserializationContext context) throws JsonParseException {
            return gsonBuilder.fromJson(jsonObj, RangerAccessResourceImpl.class);
        }
    }
}