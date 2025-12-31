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
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs.DataMaskResult;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs.RowFilterResult;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPolicyACLs {
    private static Gson gsonBuilder;

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
                .setPrettyPrinting()
                .registerTypeAdapter(RangerAccessResource.class, new RangerResourceDeserializer())
                .create();
    }

    @AfterAll
    public static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    public void setUp() throws Exception {
    }

    @AfterEach
    public void tearDown() throws Exception {
    }

    @Test
    public void testResourceMatcher_default() throws Exception {
        String[] tests = {"/policyengine/test_aclprovider_default.json"};

        runTestsFromResourceFiles(tests);
    }

    @Test
    public void testResourceACLs_dataMask() throws Exception {
        String[] tests = {"/policyengine/test_aclprovider_mask_filter.json"};

        runTestsFromResourceFiles(tests);
    }

    @Test
    public void testResourceACLs_hdfs() throws Exception {
        String[] tests = {"/policyengine/test_aclprovider_hdfs.json"};

        runTestsFromResourceFiles(tests);
    }

    @Test
    public void testResourceACLs_resource_hierarchy_tags() throws Exception {
        String[] tests = {"/policyengine/test_aclprovider_resource_hierarchy_tags.json"};

        runTestsFromResourceFiles(tests);
    }

    private void runTestsFromResourceFiles(String[] resourceNames) throws Exception {
        for (String resourceName : resourceNames) {
            InputStream       inStream = this.getClass().getResourceAsStream(resourceName);
            InputStreamReader reader   = new InputStreamReader(inStream);

            runTests(reader, resourceName);
        }
    }

    private void runTests(InputStreamReader reader, String testName) {
        PolicyACLsTests testCases = gsonBuilder.fromJson(reader, PolicyACLsTests.class);

        assertTrue(testCases != null && testCases.testCases != null, "invalid input: " + testName);

        for (PolicyACLsTests.TestCase testCase : testCases.testCases) {
            String                    serviceType         = testCase.servicePolicies.getServiceDef().getName();
            RangerPolicyEngineOptions policyEngineOptions = new RangerPolicyEngineOptions();
            RangerPluginContext       pluginContext       = new RangerPluginContext(new RangerPluginConfig(serviceType, null, "test-policy-acls", "cl1", "on-prem", policyEngineOptions));
            RangerPolicyEngine        policyEngine        = new RangerPolicyEngineImpl(testCase.servicePolicies, pluginContext, null);

            testCase.tests.parallelStream().filter(Objects::nonNull).forEach(oneTest -> {
                RangerAccessRequestImpl request = new RangerAccessRequestImpl(oneTest.resource, RangerPolicyEngine.ANY_ACCESS, null, null, null);

                request.setResourceMatchingScope(oneTest.resourceMatchingScope);

                RangerResourceACLs acls = policyEngine.getResourceACLs(request);

                assertEquals(oneTest.userPermissions, acls.getUserACLs(), oneTest.name + ": userACLs mismatch");
                assertEquals(oneTest.groupPermissions, acls.getGroupACLs(), oneTest.name + ": groupACLs mismatch");
                assertEquals(oneTest.rolePermissions, acls.getRoleACLs(), oneTest.name + ": roleACLs mismatch");
                assertEquals(oneTest.rowFilters, acls.getRowFilters(), oneTest.name + ": rowFilters mismatch");
                assertEquals(oneTest.dataMasks, acls.getDataMasks(), oneTest.name + ": dataMasks mismatch");
            });
        }
    }

    static class PolicyACLsTests {
        List<TestCase> testCases;

        static class TestCase {
            String          name;
            ServicePolicies servicePolicies;
            List<OneTest>   tests;

            static class OneTest {
                String                                                    name;
                RangerAccessResource                                      resource;
                ResourceMatchingScope                                     resourceMatchingScope;
                Map<String, Map<String, RangerResourceACLs.AccessResult>> userPermissions  = new HashMap<>();
                Map<String, Map<String, RangerResourceACLs.AccessResult>> groupPermissions = new HashMap<>();
                Map<String, Map<String, RangerResourceACLs.AccessResult>> rolePermissions  = new HashMap<>();
                List<RowFilterResult>                                     rowFilters       = new ArrayList<>();
                List<DataMaskResult>                                      dataMasks        = new ArrayList<>();
            }
        }
    }

    static class RangerResourceDeserializer implements JsonDeserializer<RangerAccessResource> {
        @Override
        public RangerAccessResource deserialize(JsonElement jsonObj, Type type, JsonDeserializationContext context) throws JsonParseException {
            return gsonBuilder.fromJson(jsonObj, RangerAccessResourceImpl.class);
        }
    }
}
