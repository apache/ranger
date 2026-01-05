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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestRangerBasePlugin {
    static Gson                      gsonBuilder;
    static RangerPolicyEngineOptions peOptions;

    @BeforeAll
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
        peOptions.disableGdsInfoRetriever   = true;
    }

    @Test
    public void testBasePluginHive() throws Exception {
        runTestsFromResourceFile("/plugin/test_base_plugin_hive.json");
    }

    private void runTestsFromResourceFile(String resourceFile) throws Exception {
        InputStream       inStream = this.getClass().getResourceAsStream(resourceFile);
        InputStreamReader reader   = new InputStreamReader(inStream);

        runTests(reader, resourceFile);
    }

    private void runTests(Reader reader, String testName) throws Exception {
        RangerBasePluginTestCase testCase = readTestCase(reader);

        assertNotNull(testCase, "invalid input: " + testName);
        assertNotNull(testCase.policies, "invalid input: " + testName);
        assertNotNull(testCase.tags, "invalid input: " + testName);
        assertNotNull(testCase.roles, "invalid input: " + testName);
        assertNotNull(testCase.userStore, "invalid input: " + testName);
        assertNotNull(testCase.gdsInfo, "invalid input: " + testName);
        assertNotNull(testCase.tests, "invalid input: " + testName);

        RangerPluginConfig pluginConfig = new RangerPluginConfig(testCase.policies.getServiceDef().getName(), testCase.policies.getServiceName(), "hive", "cl1", "on-prem", peOptions);
        RangerBasePlugin   plugin       = new RangerBasePlugin(pluginConfig, testCase.policies, testCase.tags, testCase.roles, testCase.userStore, testCase.gdsInfo);

        for (TestData test : testCase.tests) {
            RangerAccessRequest request = test.request;

            if (test.result != null) {
                RangerAccessResult result = plugin.isAccessAllowed(request);

                assertNotNull(result, "result was null! - " + test.name);
                assertEquals(test.result.getIsAllowed(), result.getIsAllowed(), "isAllowed mismatched! - " + test.name);
                assertEquals(test.result.getIsAccessDetermined(), result.getIsAccessDetermined(), "isAccessDetermined mismatched! - " + test.name);
                assertEquals(test.result.getPolicyId(), result.getPolicyId(), "isAllowed mismatched! - " + test.name);
                assertEquals(test.result.getIsAudited(), result.getIsAudited(), "isAudited mismatched! - " + test.name);
                assertEquals(test.result.getIsAuditedDetermined(), result.getIsAuditedDetermined(), "isAuditedDetermined mismatched! - " + test.name);

                result = plugin.evalDataMaskPolicies(request, new RangerDefaultAuditHandler());

                if (test.result.getMaskType() != null) {
                    assertNotNull(result, "result was null! - " + test.name);
                    assertEquals(test.result.getMaskType(), result.getMaskType(), "maskType mismatched! - " + test.name);
                    assertEquals(test.result.getMaskedValue(), result.getMaskedValue(), "maskedValue mismatched! - " + test.name);
                    assertEquals(test.result.getMaskCondition(), result.getMaskCondition(), "maskCondition mismatched! - " + test.name);
                } else {
                    assertEquals(test.result.getMaskType(), result != null ? result.getMaskType() : null, "maskType mismatched! - " + test.name);
                }

                result = plugin.evalRowFilterPolicies(request, new RangerDefaultAuditHandler());

                if (test.result.getFilterExpr() != null) {
                    assertNotNull(result, "result was null! - " + test.name);
                    assertEquals(test.result.getFilterExpr(), result.getFilterExpr(), "filterExpr mismatched! - " + test.name);
                } else {
                    assertEquals(test.result.getFilterExpr(), result != null ? result.getFilterExpr() : null, "filterExpr mismatched! - " + test.name);
                }
            }

            if (test.acls != null) {
                RangerAccessRequest req  = new RangerAccessRequestImpl(request.getResource(), RangerPolicyEngine.ANY_ACCESS, null, null, null);
                RangerResourceACLs  acls = plugin.getResourceACLs(req);

                assertEquals(test.acls, acls, test.name);
            }
        }
    }

    private RangerBasePluginTestCase readTestCase(Reader reader) throws Exception {
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

        if (StringUtils.isNotBlank(testCase.gdsInfoFilename)) {
            InputStream inStream = this.getClass().getResourceAsStream(testCase.gdsInfoFilename);

            testCase.gdsInfo = gsonBuilder.fromJson(new InputStreamReader(inStream), ServiceGdsInfo.class);

            if (testCase.gdsInfo != null && testCase.gdsInfo.getGdsServiceDef() == null) {
                RangerServiceDef gdsServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_GDS_NAME);

                testCase.gdsInfo.setGdsServiceDef(gdsServiceDef);
            }
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
        public ServiceGdsInfo  gdsInfo;
        public String          policiesFilename;
        public String          tagsFilename;
        public String          rolesFilename;
        public String          userStoreFilename;
        public String          gdsInfoFilename;
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
            Object              accessTypes = reqContext.get(RangerAccessRequestUtil.KEY_CONTEXT_ALL_ACCESSTYPES);
            if (accessTypes != null) {
                Collection<String> accessTypesCollection = (Collection<String>) accessTypes;
                Set<String>        requestedAccesses     = new TreeSet<>(accessTypesCollection);
                ret.getContext().put(RangerAccessRequestUtil.KEY_CONTEXT_ALL_ACCESSTYPES, requestedAccesses);
            }

            Object accessTypeGroups = reqContext.get(RangerAccessRequestUtil.KEY_CONTEXT_ALL_ACCESSTYPE_GROUPS);
            if (accessTypeGroups != null) {
                Set<Set<String>> setOfAccessTypeGroups = new HashSet<>();

                List<Object> listOfAccessTypeGroups = (List<Object>) accessTypeGroups;
                for (Object accessTypeGroup : listOfAccessTypeGroups) {
                    List<String> accesses      = (List<String>) accessTypeGroup;
                    Set<String>  setOfAccesses = new TreeSet<>(accesses);
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
