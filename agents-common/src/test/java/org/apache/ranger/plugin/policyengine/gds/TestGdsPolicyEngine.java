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

        assertTrue("invalid input: " + testName, testCase != null && testCase.gdsInfo != null && testCase.tests != null);

        testCase.serviceDef.setMarkerAccessTypes(ServiceDefUtil.getMarkerAccessTypes(testCase.serviceDef.getAccessTypes()));

        RangerPluginContext       pluginContext = new RangerPluginContext(new RangerPluginConfig(testCase.serviceDef.getName(), null, "hive", "cl1", "on-prem", null));
        RangerSecurityZoneMatcher zoneMatcher   = new RangerSecurityZoneMatcher(testCase.securityZones, testCase.serviceDef, pluginContext);
        GdsPolicyEngine           policyEngine  = new GdsPolicyEngine(testCase.gdsInfo, new RangerServiceDefHelper(testCase.serviceDef, false), pluginContext);

        for (TestData test : testCase.tests) {
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
        }
    }

    static class GdsPolicyEngineTestCase {
        public RangerServiceDef              serviceDef;
        public Map<String, SecurityZoneInfo> securityZones;
        public ServiceGdsInfo                gdsInfo;
        public List<TestData>                tests;
    }

    static class TestData {
        public String              name;
        public RangerAccessRequest request;
        public GdsAccessResult     result;
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
            Object              accessTypes = reqContext.get("ACCESSTYPES");
            if (accessTypes != null) {
                Collection<String> accessTypesCollection = (Collection<String>) accessTypes;
                Set<String>        requestedAccesses     = new HashSet<>(accessTypesCollection);
                ret.getContext().put("ACCESSTYPES", requestedAccesses);
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
