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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPolicyACLs {
    private static Gson gsonBuilder;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
                .setPrettyPrinting()
                .registerTypeAdapter(RangerAccessResource.class, new RangerResourceDeserializer())
                .create();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
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

        assertTrue("invalid input: " + testName, testCases != null && testCases.testCases != null);

        for (PolicyACLsTests.TestCase testCase : testCases.testCases) {
            String                    serviceType         = testCase.servicePolicies.getServiceDef().getName();
            RangerPolicyEngineOptions policyEngineOptions = new RangerPolicyEngineOptions();
            RangerPluginContext       pluginContext       = new RangerPluginContext(new RangerPluginConfig(serviceType, null, "test-policy-acls", "cl1", "on-prem", policyEngineOptions));
            RangerPolicyEngine        policyEngine        = new RangerPolicyEngineImpl(testCase.servicePolicies, pluginContext, null);

            testCase.tests.parallelStream().filter(Objects::nonNull).forEach(oneTest -> {
                RangerAccessRequestImpl request = new RangerAccessRequestImpl(oneTest.resource, RangerPolicyEngine.ANY_ACCESS, null, null, null);

                request.setResourceMatchingScope(oneTest.resourceMatchingScope);

                RangerResourceACLs acls = policyEngine.getResourceACLs(request);

                assertEquals(oneTest.name + ": userACLs mismatch", oneTest.userPermissions, acls.getUserACLs());
                assertEquals(oneTest.name + ": groupACLs mismatch", oneTest.groupPermissions, acls.getGroupACLs());
                assertEquals(oneTest.name + ": roleACLs mismatch", oneTest.rolePermissions, acls.getRoleACLs());
                assertEquals(oneTest.name + ": rowFilters mismatch", oneTest.rowFilters, acls.getRowFilters());
                assertEquals(oneTest.name + ": dataMasks mismatch", oneTest.dataMasks, acls.getDataMasks());
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
                Map<String, Map<String, RangerResourceACLs.AccessResult>> userPermissions;
                Map<String, Map<String, RangerResourceACLs.AccessResult>> groupPermissions;
                Map<String, Map<String, RangerResourceACLs.AccessResult>> rolePermissions;
                List<RowFilterResult>                                     rowFilters;
                List<DataMaskResult>                                      dataMasks;
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
