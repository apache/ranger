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

package org.apache.ranger.plugin.service;

import com.google.gson.*;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.*;
import org.apache.ranger.plugin.util.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.*;

import static org.junit.Assert.*;

public class TestRangerBasePlugin {
    static Gson                      gsonBuilder;
    static RangerPolicyEngineOptions peOptions;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSSZ")
                                       .setPrettyPrinting()
                                       .registerTypeAdapter(RangerAccessRequest.class, new RangerAccessRequestDeserializer())
                                       .registerTypeAdapter(RangerAccessResource.class, new RangerResourceDeserializer())
                                       .create();

        peOptions = new RangerPolicyEngineOptions();

        peOptions.disablePolicyRefresher    = true;
        peOptions.disableTagRetriever       = true;
        peOptions.disableUserStoreRetriever = true;
    }


    @Test
    public void testBasePluginHive() {
        runTestsFromResourceFile("/plugin/test_base_plugin_hive.json");
    }

    private void runTestsFromResourceFile(String resourceFile) {
        InputStream       inStream = this.getClass().getResourceAsStream(resourceFile);
        InputStreamReader reader   = new InputStreamReader(inStream);

        runTests(reader, resourceFile);
    }

    private void runTests(Reader reader, String testName) {
        RangerBasePluginTestCase testCase = readTestCase(reader);

        assertNotNull("invalid input: " + testName, testCase);
        assertNotNull("invalid input: " + testName, testCase.policies);
        assertNotNull("invalid input: " + testName, testCase.tags);
        assertNotNull("invalid input: " + testName, testCase.roles);
        assertNotNull("invalid input: " + testName, testCase.userStore);
        assertNotNull("invalid input: " + testName, testCase.tests);

        RangerPluginConfig pluginConfig = new RangerPluginConfig(testCase.policies.getServiceDef().getName(), testCase.policies.getServiceName(), "hive", "cl1", "on-prem", peOptions);
        RangerBasePlugin   plugin       = new RangerBasePlugin(pluginConfig, testCase.policies, testCase.tags, testCase.roles, testCase.userStore);

        for (TestData test : testCase.tests) {
            RangerAccessRequest request = test.request;

            if (test.result != null) {
                RangerAccessResult result = plugin.isAccessAllowed(request);

                assertNotNull("result was null! - " + test.name, result);
                assertEquals("isAllowed mismatched! - " + test.name, test.result.getIsAllowed(), result.getIsAllowed());
                assertEquals("isAccessDetermined mismatched! - " + test.name, test.result.getIsAccessDetermined(), result.getIsAccessDetermined());
                assertEquals("isAllowed mismatched! - " + test.name, test.result.getPolicyId(), result.getPolicyId());
                assertEquals("isAudited mismatched! - " + test.name, test.result.getIsAudited(), result.getIsAudited());
                assertEquals("isAuditedDetermined mismatched! - " + test.name, test.result.getIsAuditedDetermined(), result.getIsAuditedDetermined());
            }

            if (test.acls != null) {
                RangerAccessRequest req  = new RangerAccessRequestImpl(request.getResource(), RangerPolicyEngine.ANY_ACCESS, null, null, null);
                RangerResourceACLs  acls = plugin.getResourceACLs(req);

                assertEquals(test.name, test.acls, acls);
            }
        }
    }

    private RangerBasePluginTestCase readTestCase(Reader reader) {
        RangerBasePluginTestCase testCase = gsonBuilder.fromJson(reader, RangerBasePluginTestCase.class);

        if (StringUtils.isNotBlank(testCase.policiesFilename)) {
            InputStream inStream = this.getClass().getResourceAsStream(testCase.policiesFilename);

            testCase.policies = gsonBuilder.fromJson(new InputStreamReader(inStream), ServicePolicies.class);
        }

        if (StringUtils.isNotBlank(testCase.tagsFilename)) {
            InputStream inStream = this.getClass().getResourceAsStream(testCase.tagsFilename);

            testCase.tags = gsonBuilder.fromJson(new InputStreamReader(inStream), ServiceTags.class);
        }

        if (StringUtils.isNotBlank(testCase.rolesFilename)) {
            InputStream inStream = this.getClass().getResourceAsStream(testCase.rolesFilename);

            testCase.roles = gsonBuilder.fromJson(new InputStreamReader(inStream), RangerRoles.class);
        }

        if (StringUtils.isNotBlank(testCase.userStoreFilename)) {
            InputStream inStream = this.getClass().getResourceAsStream(testCase.userStoreFilename);

            testCase.userStore = gsonBuilder.fromJson(new InputStreamReader(inStream), RangerUserStore.class);
        }

        if (testCase.policies != null && testCase.policies.getServiceDef() != null) {
            testCase.policies.getServiceDef().setMarkerAccessTypes(ServiceDefUtil.getMarkerAccessTypes(testCase.policies.getServiceDef().getAccessTypes()));
        }

        return testCase;
    }

    static class RangerBasePluginTestCase {
        public ServicePolicies policies;
        public ServiceTags     tags;
        public RangerRoles     roles;
        public RangerUserStore userStore;
        public String          policiesFilename;
        public String          tagsFilename;
        public String          rolesFilename;
        public String          userStoreFilename;
        public List<TestData>  tests;
    }

    static class TestData {
        public String              name;
        public RangerAccessRequest request;
        public RangerAccessResult  result;
        public RangerResourceACLs  acls;
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
            Map<String, Object> reqContext  = ret.getContext();
            Object accessTypes = reqContext.get(RangerAccessRequestUtil.KEY_CONTEXT_ALL_ACCESSTYPES);
            if (accessTypes != null) {
                Collection<String> accessTypesCollection = (Collection<String>) accessTypes;
                Set<String> requestedAccesses = new TreeSet<>(accessTypesCollection);
                ret.getContext().put(RangerAccessRequestUtil.KEY_CONTEXT_ALL_ACCESSTYPES, requestedAccesses);
            }

            Object accessTypeGroups = reqContext.get(RangerAccessRequestUtil.KEY_CONTEXT_ALL_ACCESSTYPE_GROUPS);
            if (accessTypeGroups != null) {
                Set<Set<String>> setOfAccessTypeGroups = new HashSet<>();

                List<Object> listOfAccessTypeGroups = (List<Object>) accessTypeGroups;
                for (Object accessTypeGroup : listOfAccessTypeGroups) {
                    List<String> accesses = (List<String>) accessTypeGroup;
                    Set<String> setOfAccesses = new TreeSet<>(accesses);
                    setOfAccessTypeGroups.add(setOfAccesses);
                }

                reqContext.put(RangerAccessRequestUtil.KEY_CONTEXT_ALL_ACCESSTYPE_GROUPS, setOfAccessTypeGroups);
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
