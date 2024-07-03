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

package org.apache.ranger.plugin.policyengine.gds;

import com.google.gson.*;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.*;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies.SecurityZoneInfo;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGdsPolicyEngine {
    static Gson gsonBuilder;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSSZ")
                .setPrettyPrinting()
                .registerTypeAdapter(RangerAccessRequest.class, new RangerAccessRequestDeserializer())
                .registerTypeAdapter(RangerAccessResource.class, new RangerResourceDeserializer())
                .create();
    }

    @Test
    public void testGdsPolicyEngineHive() throws Exception {
        runTestsFromResourceFile("/policyengine/gds/test_gds_policy_engine_hive.json");
    }

    private void runTestsFromResourceFile(String resourceFile) throws Exception {
        InputStream       inStream = this.getClass().getResourceAsStream(resourceFile);
        InputStreamReader reader   = new InputStreamReader(inStream);

        runTests(reader, resourceFile);
    }

    private void runTests(Reader reader, String testName) {
        GdsPolicyEngineTestCase testCase = gsonBuilder.fromJson(reader, GdsPolicyEngineTestCase.class);

        if (StringUtils.isNotBlank(testCase.gdsInfoFilename)) {
            InputStream inStream = this.getClass().getResourceAsStream(testCase.gdsInfoFilename);

            testCase.gdsInfo = gsonBuilder.fromJson(new InputStreamReader(inStream), ServiceGdsInfo.class);
        }

        assertTrue("invalid input: " + testName, testCase != null && testCase.gdsInfo != null && testCase.tests != null);

        testCase.serviceDef.setMarkerAccessTypes(ServiceDefUtil.getMarkerAccessTypes(testCase.serviceDef.getAccessTypes()));

        RangerPluginContext       pluginContext = new RangerPluginContext(new RangerPluginConfig(testCase.serviceDef.getName(), null, "hive", "cl1", "on-prem", null));
        RangerSecurityZoneMatcher zoneMatcher   = new RangerSecurityZoneMatcher(testCase.securityZones, testCase.serviceDef, pluginContext);
        GdsPolicyEngine           policyEngine  = new GdsPolicyEngine(testCase.gdsInfo, new RangerServiceDefHelper(testCase.serviceDef, false), pluginContext);

        for (TestData test : testCase.tests) {
            if (test.request != null) {
                Set<String> zoneNames = zoneMatcher.getZonesForResourceAndChildren(test.request.getResource());

                RangerAccessRequestUtil.setResourceZoneNamesInContext(test.request, zoneNames);

                if (test.result != null) {
                    GdsAccessResult result = policyEngine.evaluate(test.request);

                    assertEquals(test.name, test.result, result);
                }

                if (test.acls != null) {
                    RangerResourceACLs acls = policyEngine.getResourceACLs(test.request);

                    assertEquals(test.name, test.acls, acls);
                }
            } else if (test.sharedWith != null) {
                Set<String> users  = test.sharedWith.get("users");
                Set<String> groups = test.sharedWith.get("groups");
                Set<String> roles  = test.sharedWith.get("roles");

                if (test.datasets != null) {
                    Set<Long> datasets = policyEngine.getDatasetsSharedWith(users, groups, roles);

                    assertEquals(test.name, test.datasets, datasets);
                }

                if (test.projects != null) {
                    Set<Long> projects = policyEngine.getProjectsSharedWith(users, groups, roles);

                    assertEquals(test.name, test.projects, projects);
                }
            } else if (test.resourceIds != null) {
                Set<Long> resourceIds = new HashSet<>();

                if (test.datasetId != null) {
                    Iterator<GdsSharedResourceEvaluator> iter = policyEngine.getDatasetResources(test.datasetId);

                    while (iter.hasNext()) {
                        resourceIds.add(iter.next().getId());
                    }
                } else if (test.projectId != null) {
                    Iterator<GdsSharedResourceEvaluator> iter = policyEngine.getProjectResources(test.projectId);

                    while (iter.hasNext()) {
                        resourceIds.add(iter.next().getId());
                    }
                } else if (test.dataShareId != null) {
                    Iterator<GdsSharedResourceEvaluator> iter = policyEngine.getDataShareResources(test.dataShareId);

                    while (iter.hasNext()) {
                        resourceIds.add(iter.next().getId());
                    }
                } else if (test.projectIds != null || test.datasetIds != null || test.dataShareIds != null) {
                    Iterator<GdsSharedResourceEvaluator> iter = policyEngine.getResources(test.projectIds, test.datasetIds, test.dataShareIds);

                    while (iter.hasNext()) {
                        resourceIds.add(iter.next().getId());
                    }
                }

                assertEquals(test.name, test.resourceIds, resourceIds);
            }
        }
    }

    static class GdsPolicyEngineTestCase {
        public RangerServiceDef              serviceDef;
        public Map<String, SecurityZoneInfo> securityZones;
        public ServiceGdsInfo                gdsInfo;
        public String                        gdsInfoFilename;
        public List<TestData>                tests;
    }

    static class TestData {
        public String                   name;
        public RangerAccessRequest      request;
        public GdsAccessResult          result;
        public RangerResourceACLs       acls;
        public Map<String, Set<String>> sharedWith; // principals
        public Set<Long>                datasets;
        public Set<Long>                projects;
        public Long                     datasetId;
        public Long                     projectId;
        public Long                     dataShareId;
        public List<Long>               datasetIds;
        public List<Long>               projectIds;
        public List<Long>               dataShareIds;
        public Set<Long>                resourceIds;
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
