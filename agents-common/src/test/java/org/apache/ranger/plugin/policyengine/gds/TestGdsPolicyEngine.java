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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyengine.RangerSecurityZoneMatcher;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies.SecurityZoneInfo;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
    public void testGdsPolicyHiveAccess() {
        runTestsFromResourceFile("/policyengine/gds/test_gds_policy_hive_access.json");
    }

    @Test
    public void testGdsPolicyHiveDataMask() {
        runTestsFromResourceFile("/policyengine/gds/test_gds_policy_hive_data_mask.json");
    }

    @Test
    public void testGdsPolicyHiveRowFilter() {
        runTestsFromResourceFile("/policyengine/gds/test_gds_policy_hive_row_filter.json");
    }

    private void runTestsFromResourceFile(String resourceFile) {
        InputStream       inStream = this.getClass().getResourceAsStream(resourceFile);
        InputStreamReader reader   = new InputStreamReader(inStream);

        runTests(reader, resourceFile);
    }

    private void runTests(Reader reader, String testName) {
        GdsPolicyEngineTestCase testCase = gsonBuilder.fromJson(reader, GdsPolicyEngineTestCase.class);

        if (testCase.serviceDef == null) {
            try {
                testCase.serviceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(testCase.serviceType);
            } catch (Exception excp) {
                throw new RuntimeException("failed to load " + testCase.serviceType + " service-def", excp);
            }
        }

        if (testCase.gdsInfo == null) {
            try (InputStream inStream = this.getClass().getResourceAsStream(testCase.gdsInfoFilename)) {
                testCase.gdsInfo = inStream != null ? gsonBuilder.fromJson(new InputStreamReader(inStream), ServiceGdsInfo.class) : null;

                if (testCase.gdsInfo != null && testCase.gdsInfo.getGdsServiceDef() == null) {
                    try {
                        RangerServiceDef gdsServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_GDS_NAME);

                        testCase.gdsInfo.setGdsServiceDef(gdsServiceDef);
                    } catch (Exception excp) {
                        throw new RuntimeException("failed to load " + EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_GDS_NAME + " service-def", excp);
                    }
                }
            } catch (IOException excp) {
                throw new RuntimeException("failed to load gdsInfoFile " + testCase.gdsInfoFilename, excp);
            }
        }

        assertTrue("invalid input: " + testName, testCase.gdsInfo != null && testCase.tests != null);

        ServiceDefUtil.normalize(testCase.serviceDef);
        testCase.serviceDef.setMarkerAccessTypes(ServiceDefUtil.getMarkerAccessTypes(testCase.serviceDef.getAccessTypes()));

        RangerPluginContext       pluginContext = new RangerPluginContext(new RangerPluginConfig(testCase.serviceDef.getName(), null, "hive", "cl1", "on-prem", null));
        RangerSecurityZoneMatcher zoneMatcher   = new RangerSecurityZoneMatcher(testCase.securityZones, testCase.serviceDef, pluginContext);
        GdsPolicyEngine           policyEngine  = new GdsPolicyEngine(testCase.gdsInfo, new RangerServiceDefHelper(testCase.serviceDef, false), pluginContext);

        for (TestData test : testCase.tests) {
            if (test.request != null) {
                // Safe cast
                ((RangerAccessResourceImpl) test.request.getResource()).setServiceDef(testCase.serviceDef);

                Set<String> zoneNames = zoneMatcher.getZonesForResourceAndChildren(test.request.getResource());

                RangerAccessRequestUtil.setResourceZoneNamesInContext(test.request, zoneNames);

                if (test.acls != null) {
                    RangerResourceACLs acls = policyEngine.getResourceACLs(test.request);

                    assertEquals(test.name, test.acls, acls);
                } else {
                    GdsAccessResult result = policyEngine.evaluate(test.request);

                    assertEquals(test.name, test.result, result);
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
                Iterator<GdsSharedResourceEvaluator> iter;

                if (test.datasetId != null) {
                    iter = policyEngine.getDatasetResources(test.datasetId);
                } else if (test.projectId != null) {
                    iter = policyEngine.getProjectResources(test.projectId);
                } else if (test.dataShareId != null) {
                    iter = policyEngine.getDataShareResources(test.dataShareId);
                } else if (test.projectIds != null || test.datasetIds != null || test.dataShareIds != null) {
                    iter = policyEngine.getResources(test.projectIds, test.datasetIds, test.dataShareIds);
                } else {
                    iter = Collections.emptyIterator();
                }

                Set<Long> resourceIds = new HashSet<>();

                iter.forEachRemaining(e -> resourceIds.add(e.getId()));

                assertEquals(test.name, test.resourceIds, resourceIds);
            }
        }
    }

    static class GdsPolicyEngineTestCase {
        public String                        serviceType;
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
