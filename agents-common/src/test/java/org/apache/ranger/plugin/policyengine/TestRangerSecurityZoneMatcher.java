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


import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.util.ServicePolicies.SecurityZoneInfo;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class TestRangerSecurityZoneMatcher {
    final RangerPluginContext           pluginContext = new RangerPluginContext(new RangerPluginConfig("hive", null, "hive", "cl1", "on-prem", null));
    final Map<String, SecurityZoneInfo> securityZones = createSecurityZones();
    final RangerServiceDef              serviceDef    = createServiceDef();

    @Test
    public void testZoneMatcher() {
        RangerSecurityZoneMatcher zoneMatcher = new RangerSecurityZoneMatcher(securityZones, serviceDef, pluginContext);

        RangerAccessResource res;
        Set<String>          zones;

        res   = createResource("database", "db1", "table", "tbl1");
        zones = zoneMatcher.getZonesForResourceAndChildren(res);
        assertEquals(createSet("z1"), zones);

        res   = createResource("database", "db2", "table", "tbl1");
        zones = zoneMatcher.getZonesForResourceAndChildren(res);
        assertEquals(createSet("z2"), zones);

        res   = createResource("database", "db3", "table", "test_1");
        zones = zoneMatcher.getZonesForResourceAndChildren(res);
        assertEquals(createSet("z3"), zones);

        res   = createResource("database", "db3", "table", "test_2");
        zones = zoneMatcher.getZonesForResourceAndChildren(res);
        assertEquals(createSet("z3"), zones);

        res   = createResource("database", "db3", "table", "orders");
        zones = zoneMatcher.getZonesForResourceAndChildren(res);
        assertNull(zones);

        res   = createResource("database", "db3", "table", "user_1");
        zones = zoneMatcher.getZonesForResourceAndChildren(res);
        assertEquals(createSet("z4"), zones);

        res   = createResource("database", "db3", "table", "user_2");
        zones = zoneMatcher.getZonesForResourceAndChildren(res);
        assertEquals(createSet("z4"), zones);

        res   = createResource("database", "db3");
        zones = zoneMatcher.getZonesForResourceAndChildren(res);
        assertEquals(createSet("", "z3", "z4"), zones);
    }

    private Map<String, SecurityZoneInfo> createSecurityZones() {
        HashMap<String, List<String>> db1     = new HashMap<String, List<String>>() {{ put("database", Arrays.asList("db1")); }};
        HashMap<String, List<String>> db2     = new HashMap<String, List<String>>() {{ put("database", Arrays.asList("db2")); }};
        HashMap<String, List<String>> db3Test = new HashMap<String, List<String>>() {{ put("database", Arrays.asList("db3")); put("table", Arrays.asList("test_*")); }};
        HashMap<String, List<String>> db4User = new HashMap<String, List<String>>() {{ put("database", Arrays.asList("db3")); put("table", Arrays.asList("user_*")); }};

        SecurityZoneInfo z1 = new SecurityZoneInfo();
        SecurityZoneInfo z2 = new SecurityZoneInfo();
        SecurityZoneInfo z3 = new SecurityZoneInfo();
        SecurityZoneInfo z4 = new SecurityZoneInfo();

        z1.setZoneName("z1");
        z1.setResources(Arrays.asList(db1));

        z2.setZoneName("z2");
        z2.setResources(Arrays.asList(db2));

        z3.setZoneName("z3");
        z3.setResources(Arrays.asList(db3Test));

        z4.setZoneName("z4");
        z4.setResources(Arrays.asList(db4User));

        Map<String, SecurityZoneInfo> ret = new HashMap<>();

        ret.put(z1.getZoneName(), z1);
        ret.put(z2.getZoneName(), z2);
        ret.put(z3.getZoneName(), z3);
        ret.put(z4.getZoneName(), z4);

        return ret;
    }

    private RangerServiceDef createServiceDef() {
        RangerServiceDef ret = new RangerServiceDef();

        ret.setName("hive");
        ret.setResources(createResourceDefs());

        return ret;
    }

    private List<RangerResourceDef> createResourceDefs() {
        List<RangerResourceDef> ret = new ArrayList<>();

        ret.add(createResourceDef("database", null));
        ret.add(createResourceDef("table",    "database"));
        ret.add(createResourceDef("column",   "table"));

        return ret;
    }

    private RangerResourceDef createResourceDef(String name, String parent) {
        RangerResourceDef ret = new RangerResourceDef();

        ret.setName(name);
        ret.setType("string");
        ret.setParent(parent);

        return ret;
    }

    private RangerAccessResource createResource(String...args) {
        RangerAccessResourceImpl ret = new RangerAccessResourceImpl();

        for (int i = 1; i < args.length; i += 2) {
            ret.setValue(args[i - 1], args[i]);
        }

        return ret;
    }

    private Set<String> createSet(String...args) {
        Set<String> ret = new HashSet<>();

        for (String arg : args) {
            ret.add(arg);
        }

        return ret;
    }
}
