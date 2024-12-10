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

package org.apache.ranger.plugin.model.validation;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper.Delegate;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRangerServiceDefHelper {
    private RangerServiceDef       serviceDef;
    private RangerServiceDefHelper helper;

    @Before
    public void before() {
        serviceDef = mock(RangerServiceDef.class);

        when(serviceDef.getName()).thenReturn("a-service-def");

        // wipe the cache clean
        RangerServiceDefHelper.cache.clear();
    }

    @Test
    public void test_getResourceHierarchies() {
        /*
         * Create a service-def with following resource graph
         *
         *   Database -> UDF
         *       |
         *       v
         *      Table -> Column
         *         |
         *         v
         *        Table-Attribute
         *
         *  It contains following hierarchies
         *  - [ Database UDF]
         *  - [ Database Table Column ]
         *  - [ Database Table Table-Attribute ]
         */
        RangerResourceDef database       = createResourceDef("Database", "");
        RangerResourceDef udf            = createResourceDef("UDF", "Database");
        RangerResourceDef table          = createResourceDef("Table", "Database");
        RangerResourceDef column         = createResourceDef("Column", "Table", true);
        RangerResourceDef tableAttribute = createResourceDef("Table-Attribute", "Table", true);

        // order of resources in list sould not matter
        List<RangerResourceDef> resourceDefs = Lists.newArrayList(column, database, table, tableAttribute, udf);

        // stuff this into a service-def
        when(serviceDef.getResources()).thenReturn(resourceDefs);

        // now assert the behavior
        helper = new RangerServiceDefHelper(serviceDef);

        assertTrue(helper.isResourceGraphValid());

        Set<List<RangerResourceDef>> hierarchies = helper.getResourceHierarchies(RangerPolicy.POLICY_TYPE_ACCESS);
        List<RangerResourceDef>      hierarchy   = Lists.newArrayList(database, udf);

        // there should be
        assertTrue(hierarchies.contains(hierarchy));

        hierarchy = Lists.newArrayList(database, table, column);
        assertTrue(hierarchies.contains(hierarchy));

        hierarchy = Lists.newArrayList(database, table, tableAttribute);
        assertTrue(hierarchies.contains(hierarchy));
    }

    @Test
    public final void test_isResourceGraphValid_detectCycle() {
        /*
         * Create a service-def with cycles in resource graph
         *  A --> B --> C
         *  ^           |
         *  |           |
         *  |---- D <---
         */
        RangerResourceDef a = createResourceDef("A", "D"); // A's parent is D, etc.
        RangerResourceDef b = createResourceDef("B", "C");
        RangerResourceDef c = createResourceDef("C", "D");
        RangerResourceDef d = createResourceDef("D", "A");

        // order of resources in list sould not matter
        List<RangerResourceDef> resourceDefs = Lists.newArrayList(a, b, c, d);

        when(serviceDef.getResources()).thenReturn(resourceDefs);

        helper = new RangerServiceDefHelper(serviceDef);

        assertFalse("Graph was valid!", helper.isResourceGraphValid());
    }

    @Test
    public final void test_isResourceGraphValid_forest() {
        /*
         * Create a service-def which is a forest
         *   Database -> Table-space
         *       |
         *       v
         *      Table -> Column
         *
         *   Namespace -> package
         *       |
         *       v
         *     function
         *
         * Check that helper corrects reports back all of the hierarchies: levels in it and their order.
         */
        RangerResourceDef       database     = createResourceDef("database", "");
        RangerResourceDef       tableSpace   = createResourceDef("table-space", "database", true);
        RangerResourceDef       table        = createResourceDef("table", "database");
        RangerResourceDef       column       = createResourceDef("column", "table", true);
        RangerResourceDef       namespace    = createResourceDef("namespace", "");
        RangerResourceDef       function     = createResourceDef("function", "namespace", true);
        RangerResourceDef       packagE      = createResourceDef("package", "namespace", true);
        List<RangerResourceDef> resourceDefs = Lists.newArrayList(database, tableSpace, table, column, namespace, function, packagE);

        when(serviceDef.getResources()).thenReturn(resourceDefs);

        helper = new RangerServiceDefHelper(serviceDef);

        assertTrue(helper.isResourceGraphValid());

        Set<List<RangerResourceDef>> hierarchies         = helper.getResourceHierarchies(RangerPolicy.POLICY_TYPE_ACCESS);
        Set<List<String>>            expectedHierarchies = new HashSet<>();

        expectedHierarchies.add(Lists.newArrayList("database", "table-space"));
        expectedHierarchies.add(Lists.newArrayList("database", "table", "column"));
        expectedHierarchies.add(Lists.newArrayList("namespace", "package"));
        expectedHierarchies.add(Lists.newArrayList("namespace", "function"));

        for (List<RangerResourceDef> aHierarchy : hierarchies) {
            List<String> resourceNames = helper.getAllResourceNamesOrdered(aHierarchy);

            assertTrue(expectedHierarchies.contains(resourceNames));

            expectedHierarchies.remove(resourceNames);
        }

        assertTrue("Missing hierarchies: " + expectedHierarchies, expectedHierarchies.isEmpty()); // make sure we got back all hierarchies
    }

    @Test
    public final void test_isResourceGraphValid_forest_singleNodeTrees() {
        /*
         * Create a service-def which is a forest with a few single node trees
         *
         *   Database
         *
         *   Server
         *
         *   Namespace -> package
         *       |
         *       v
         *     function
         *
         * Check that helper corrects reports back all of the hierarchies: levels in it and their order.
         */
        RangerResourceDef       database     = createResourceDef("database", "");
        RangerResourceDef       server       = createResourceDef("server", "");
        RangerResourceDef       namespace    = createResourceDef("namespace", "");
        RangerResourceDef       function     = createResourceDef("function", "namespace", true);
        RangerResourceDef       packagE      = createResourceDef("package", "namespace", true);
        List<RangerResourceDef> resourceDefs = Lists.newArrayList(database, server, namespace, function, packagE);

        when(serviceDef.getResources()).thenReturn(resourceDefs);

        helper = new RangerServiceDefHelper(serviceDef);

        assertTrue(helper.isResourceGraphValid());

        Set<List<RangerResourceDef>> hierarchies         = helper.getResourceHierarchies(RangerPolicy.POLICY_TYPE_ACCESS);
        Set<List<String>>            expectedHierarchies = new HashSet<>();

        expectedHierarchies.add(Lists.newArrayList("database"));
        expectedHierarchies.add(Lists.newArrayList("server"));
        expectedHierarchies.add(Lists.newArrayList("namespace", "package"));
        expectedHierarchies.add(Lists.newArrayList("namespace", "function"));

        for (List<RangerResourceDef> aHierarchy : hierarchies) {
            List<String> resourceNames = helper.getAllResourceNamesOrdered(aHierarchy);

            assertTrue(expectedHierarchies.contains(resourceNames));

            expectedHierarchies.remove(resourceNames);
        }

        assertTrue("Missing hierarchies: " + expectedHierarchies, expectedHierarchies.isEmpty()); // make sure we got back all hierarchies
    }

    @Test
    public final void test_cacheBehavior() {
        // wipe the cache clean
        RangerServiceDefHelper.cache.clear();

        // let's add one entry to the cache
        Delegate delegate    = mock(Delegate.class);
        Date     aDate       = getNow();
        String   serviceName = "a-service-def";

        when(delegate.getServiceFreshnessDate()).thenReturn(aDate);
        when(delegate.getServiceName()).thenReturn(serviceName);

        RangerServiceDefHelper.cache.put(serviceName, delegate);

        // create a service def with matching date value
        serviceDef = mock(RangerServiceDef.class);

        when(serviceDef.getName()).thenReturn(serviceName);
        when(serviceDef.getUpdateTime()).thenReturn(aDate);

        // since cache has it, we should get back the one that we have added
        helper = new RangerServiceDefHelper(serviceDef);

        assertSame("Didn't get back the same object that was put in cache", delegate, helper.delegate);

        // if we change the date then that should force helper to create a new delegate instance
        /*
         * NOTE:: current logic would replace the cache instance even if the one in the cache is newer.  This is not likely to happen but it is important to call this out
         * as in rare cases one may end up creating re creating delegate if threads have stale copies of service def.
         */
        when(serviceDef.getUpdateTime()).thenReturn(getLastMonth());

        helper = new RangerServiceDefHelper(serviceDef);

        assertNotSame("Didn't get a delegate different than what was put in the cache", delegate, helper.delegate);

        // now that a new instance was added to the cache let's ensure that it got added to the cache
        Delegate newDelegate = helper.delegate;

        helper = new RangerServiceDefHelper(serviceDef);

        assertSame("Didn't get a delegate different than what was put in the cache", newDelegate, helper.delegate);
    }

    @Test
    public void test_getResourceHierarchies_with_leaf_specification() {
        /*
         * Leaf Spec for resources:
         *      Database: non-leaf
         *      UDF: Not-specified
         *      Table: Leaf
         *      Column: Leaf
         *      Table-Attribute: Leaf
         *
         * Create a service-def with following resource graph
         *
         *   Database -> UDF
         *       |
         *       v
         *      Table -> Column
         *         |
         *         v
         *        Table-Attribute
         *
         *  It contains following hierarchies
         *  - [ Database UDF]
         *  - [ Database Table Column ]
         *  - [ Database Table ]
         *  - [ Database Table Table-Attribute ]
         */
        RangerResourceDef database        = createResourceDef("Database", "", false);
        RangerResourceDef udf             = createResourceDef("UDF", "Database");
        RangerResourceDef table           = createResourceDef("Table", "Database", true);
        RangerResourceDef column          = createResourceDef("Column", "Table", true);
        RangerResourceDef tableAttribute = createResourceDef("Table-Attribute", "Table", true);

        // order of resources in list should not matter
        List<RangerResourceDef> resourceDefs = Lists.newArrayList(column, database, table, tableAttribute, udf);

        // stuff this into a service-def
        when(serviceDef.getResources()).thenReturn(resourceDefs);

        // now assert the behavior
        helper = new RangerServiceDefHelper(serviceDef);

        assertTrue(helper.isResourceGraphValid());

        Set<List<RangerResourceDef>> hierarchies = helper.getResourceHierarchies(RangerPolicy.POLICY_TYPE_ACCESS);
        List<RangerResourceDef>      hierarchy   = Lists.newArrayList(database, udf);

        // there should be
        assertTrue(hierarchies.contains(hierarchy));

        hierarchy = Lists.newArrayList(database, table, column);
        assertTrue(hierarchies.contains(hierarchy));

        hierarchy = Lists.newArrayList(database, table, tableAttribute);
        assertTrue(hierarchies.contains(hierarchy));

        hierarchy = Lists.newArrayList(database, table);
        assertTrue(hierarchies.contains(hierarchy));

        hierarchy = Lists.newArrayList(database);
        assertFalse(hierarchies.contains(hierarchy));
    }

    @Test
    public void test_invalid_resourceHierarchies_with_leaf_specification() {
        /*
         * Leaf Spec for resources:
         *      Database: non-leaf
         *      UDF: Not-specified
         *      Table: Leaf
         *      Column: non-Leaf
         *      Table-Attribute: Leaf
         *
         * Create a service-def with following resource graph
         *
         *   Database -> UDF
         *       |
         *       v
         *      Table -> Column
         *         |
         *         v
         *        Table-Attribute
         *
         *  It should fail as the hierarchy is invalid ("Error in path: sink node:[Column] is not leaf node")
         *
         */
        RangerResourceDef       database       = createResourceDef("Database", "", false);
        RangerResourceDef       udf            = createResourceDef("UDF", "Database");
        RangerResourceDef       table          = createResourceDef("Table", "Database", true);
        RangerResourceDef       column         = createResourceDef("Column", "Table", false);
        RangerResourceDef       tableAttribute = createResourceDef("Table-Attribute", "Table", true);
        List<RangerResourceDef> resourceDefs   = Lists.newArrayList(column, database, table, tableAttribute, udf); // order of resources in list should not matter

        // stuff this into a service-def
        when(serviceDef.getResources()).thenReturn(resourceDefs);

        // now assert the behavior
        helper = new RangerServiceDefHelper(serviceDef);

        assertFalse(helper.isResourceGraphValid());
    }

    RangerResourceDef createResourceDef(String name, String parent) {
        return createResourceDef(name, parent, null);
    }

    RangerResourceDef createResourceDef(String name, String parent, Boolean isValidLeaf) {
        RangerResourceDef resourceDef = mock(RangerResourceDef.class);

        when(resourceDef.getName()).thenReturn(name);
        when(resourceDef.getParent()).thenReturn(parent);
        when(resourceDef.getIsValidLeaf()).thenReturn(isValidLeaf);

        return resourceDef;
    }

    Date getLastMonth() {
        Calendar cal = GregorianCalendar.getInstance();

        cal.add(Calendar.MONTH, 1);

        return cal.getTime();
    }

    Date getNow() {
        return GregorianCalendar.getInstance().getTime();
    }
}
