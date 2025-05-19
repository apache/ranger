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
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.file.FileSystems;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class RangerBasePluginTest {
    private static final String RANGER_SERVICE_TYPE                = "hbase";
    private static final String RANGER_APP_ID                      = "hbase";
    private static final String RANGER_DEFAULT_SERVICE_NAME        = "cm_hbase";
    private static final String TEST_JSON                          = "/policyengine/test_base_plugin_hbase.json";
    private static final String RANGER_DEFAULT_SECURITY_CONF       = "/target/test-classes/policyengine/ranger-hbase-security.xml";
    private static final String RANGER_DEFAULT_AUDIT_CONF          = "/target/test-classes/policyengine/ranger-trino-audit.xml";
    private static final String RANGER_DEFAULT_POLICY_MGR_SSL_CONF = "/target/test-classes/policyengine/ranger-policymgr-ssl.xml";
    private static final String MESSAGE                            = "The failed count being zero suggests one of two possibilities: " +
            "1. The PolicyRefresher might not be starting correctly. " +
            "2. There might be a race condition in our code, preventing the policy engine modifications from being reflected in RangerBasePlugin.";

    private static Gson             gsonBuilder;
    private static RangerBasePlugin rangerBasePlugin;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
                .setPrettyPrinting()
                .registerTypeAdapter(RangerAccessRequest.class, new RangerAccessRequestDeserializer())
                .registerTypeAdapter(RangerAccessResource.class, new RangerResourceDeserializer())
                .create();

        RangerPolicyEngineOptions peOptions    = new RangerPolicyEngineOptions();
        RangerPluginConfig        pluginConfig = new RangerPluginConfig(RANGER_SERVICE_TYPE, RANGER_DEFAULT_SERVICE_NAME, RANGER_APP_ID, "cl1", "on-perm", peOptions);
        String                    basedir      = new File(".").getCanonicalPath();

        pluginConfig.addResourceIfReadable(FileSystems.getDefault().getPath(basedir, RANGER_DEFAULT_AUDIT_CONF).toString());
        pluginConfig.addResourceIfReadable(FileSystems.getDefault().getPath(basedir, RANGER_DEFAULT_SECURITY_CONF).toString());
        pluginConfig.addResourceIfReadable(FileSystems.getDefault().getPath(basedir, RANGER_DEFAULT_POLICY_MGR_SSL_CONF).toString());
        pluginConfig.getProperties().put("ranger.plugin.hbase.supports.in.place.policy.updates", "true");
        pluginConfig.getProperties().put("ranger.plugin.hbase.supports.policy.deltas", "true");

        rangerBasePlugin = new RangerBasePlugin(pluginConfig);

        rangerBasePlugin.init();
    }

    @Test
    @SuppressWarnings("PMD")
    public void testCanSetUserOperations() throws Exception {
        runTestsFromResourceFile();
    }

    private void runTestsFromResourceFile() throws Exception {
        InputStream       inStream = this.getClass().getResourceAsStream(TEST_JSON);
        InputStreamReader reader   = new InputStreamReader(inStream);

        runTests(reader);
    }

    private void runTests(Reader reader) throws Exception {
        RangerBasePluginTestCase testCase = readTestCase(reader);

        assertNotNull("testCase was null",  testCase);
        assertNotNull("testCase.policies was null",  testCase.policies);
        assertNotNull("testCase.tags was null", testCase.tags);
        assertNotNull("testCase.roles was null", testCase.roles);
        assertNotNull("testCase.userStore was null", testCase.userStore);
        assertNotNull("testCase.gdsInfo was null", testCase.gdsInfo);
        assertNotNull("testCase.tests was null", testCase.tests);

        int failedCount = 0;

        for (int count = 0; count < 10000; count++) {
            for (TestData test : testCase.tests) {
                RangerAccessRequest request = test.request;

                try {
                    if (test.result != null) {
                        RangerAccessResult result = rangerBasePlugin.isAccessAllowed(request);

                        assertNotNull("result was null! - " + test.name, result);
                        assertEquals("isAllowed mismatched! - " + test.name, test.result.getIsAllowed(), result.getIsAllowed());
                        assertEquals("isAccessDetermined mismatched! - " + test.name, test.result.getIsAccessDetermined(), result.getIsAccessDetermined());
                        assertEquals("isAllowed mismatched! - " + test.name, test.result.getPolicyId(), result.getPolicyId());
                    }
                } catch (Error e) {
                    // The PolicyRefresher modifies the policy, so it's expected that tests for the modified policy would fail.
                    if (test.result.getPolicyId() == 821) {
                        failedCount++;
                    } else {
                        throw e;
                    }
                }
            }

            if (failedCount >= 30) {
                break;
            }

            Thread.sleep(20);
        }

        if (failedCount == 0) {
            fail(MESSAGE);
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
        public RangerAccessRequest deserialize(JsonElement jsonObj, Type type, JsonDeserializationContext context) throws JsonParseException {
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
                Set<Set<String>> setOfAccessTypeGroups  = new HashSet<>();
                List<Object>     listOfAccessTypeGroups = (List<Object>) accessTypeGroups;

                for (Object accessTypeGroup : listOfAccessTypeGroups) {
                    List<String> accesses     = (List<String>) accessTypeGroup;
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
        public RangerAccessResource deserialize(JsonElement jsonObj, Type type, JsonDeserializationContext context) throws JsonParseException {
            return gsonBuilder.fromJson(jsonObj, RangerAccessResourceImpl.class);
        }
    }
}
