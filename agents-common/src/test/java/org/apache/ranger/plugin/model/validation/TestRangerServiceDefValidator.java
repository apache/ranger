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

import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.ServiceStore;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRangerServiceDefValidator {
    final Action[]   cu = new Action[] {Action.CREATE, Action.UPDATE};
    final Object[][] accessTypesGood = new Object[][] {
            {1L, "read", null},                                // ok, null implied grants
            {2L, "write", new String[] {}},                  // ok, empty implied grants
            {3L, "admin", new String[] {"READ", "write"}}    // ok, admin access implies read/write, access types are case-insensitive
    };
    final Object[][] enumsGood = new Object[][] {
            {1L, "authentication-type", new String[] {"simple", "kerberos"}},
            {2L, "time-unit", new String[] {"day", "hour", "minute"}},
    };
    final Object[][] accessTypesBadUnknownType = new Object[][] {
            {1L, "read", null},                                // ok, null implied grants
            {1L, "write", new String[] {}},                 // empty implied grants-ok, duplicate id
            {3L, "admin", new String[] {"ReaD", "execute"}}  // non-existent access type (execute), read is good (case should not matter)
    };
    final Object[][] accessTypesBadSelfReference = new Object[][] {
            {1L, "read", null},                              // ok, null implied grants
            {2L, "write", new String[] {}},                // ok, empty implied grants
            {3L, "admin", new String[] {"write", "admin"}}  // non-existent access type (execute)
    };
    final Object[][] enumsBadEnumNameNull = new Object[][] {
            //  { id, enum-name,             enum-values }
            {1L, "authentication-type", new String[] {"simple", "kerberos"}},
            {2L, "time-unit", new String[] {"day", "hour", "minute"}},
            {3L, null, new String[] {"foo", "bar", "tar"}}, // null enum-name
    };
    final Object[][] enumsBadEnumNameBlank = new Object[][] {
            //  { id, enum-name,             enum-values }
            {1L, "authentication-type", new String[] {"simple", "kerberos"}},
            {1L, "time-unit", new String[] {"day", "hour", "minute"}},
            {2L, "  ", new String[] {"foo", "bar", "tar"}}, // enum name is all spaces
    };
    final Object[][] enumsBadElementsEmpty = new Object[][] {
            //  { id, enum-name,             enum-values }
            {null, "authentication-type", new String[] {"simple", "kerberos"}}, // null id
            {1L, "time-unit", new String[] {"day", "hour", "minute"}},
            {2L, "anEnum", new String[] {}}, // enum elements collection is empty
    };
    final Object[][] enumsBadEnumNameDuplicateDifferentCase = new Object[][] {
            //  { id, enum-name,             enum-values }
            {1L, "authentication-type", new String[] {"simple", "kerberos"}},
            {1L, "time-unit", new String[] {"day", "hour", "minute"}},
            {1L, "Authentication-Type", new String[] {}}, // duplicate enum-name different in case
    };
    final Object[][] invalidResources = new Object[][] {
            //  { id,   level,      name }
            {null, null, null}, // everything is null
            {1L, -10, "database"}, // -ve value for level is ok
            {1L, 10, "table"}, // id is duplicate
            {2L, -10, "DataBase"}, // (in different case) but name and level are duplicate
            {3L, 30, "  "} // Name is all whitespace
    };
    final Object[][] mixedCaseResources = new Object[][] {
            //  { id,   level,      name }
            {4L, -10, "DBase"}, // -ve value for level is ok
            {5L, 10, "TABLE"}, // id is duplicate
            {6L, -10, "Column"} // (in different case) but name and level are duplicate
    };

    RangerServiceDef               serviceDef;
    List<ValidationFailureDetails> failures;
    ServiceStore                   store;
    RangerServiceDefValidator      validator;

    private final ValidationTestUtils utils = new ValidationTestUtils();

    @Before
    public void setUp() throws Exception {
        store      = mock(ServiceStore.class);
        validator  = new RangerServiceDefValidator(store);
        failures   = new ArrayList<>();
        serviceDef = mock(RangerServiceDef.class);
    }

    @Test
    public final void test_isValid_happyPath_create() {
        // setup access types with implied access and couple of enums
        List<RangerAccessTypeDef> accessTypeDefs = utils.createAccessTypeDefs(accessTypesGood);
        when(serviceDef.getAccessTypes()).thenReturn(accessTypeDefs);
        List<RangerEnumDef> enumDefs = utils.createEnumDefs(enumsGood);
        when(serviceDef.getEnums()).thenReturn(enumDefs);
    }

    @Test
    public final void testIsValid_Long_failures() throws Exception {
        // passing in wrong action type
        boolean result = validator.isValid((Long) null, Action.CREATE, failures);
        assertFalse(result);
        utils.checkFailureForInternalError(failures);
        // passing in null id is an error
        failures.clear();
        assertFalse(validator.isValid((Long) null, Action.DELETE, failures));
        utils.checkFailureForMissingValue(failures, "id");
        // It is ok for a service def with that id to not exist!
        Long id = 3L;
        when(store.getServiceDef(id)).thenReturn(null);
        failures.clear();
        assertTrue(validator.isValid(id, Action.DELETE, failures));
        assertTrue(failures.isEmpty());
        // happypath
        when(store.getServiceDef(id)).thenReturn(serviceDef);
        failures.clear();
        assertTrue(validator.isValid(id, Action.DELETE, failures));
        assertTrue(failures.isEmpty());
    }

    @Test
    public final void testIsValid_failures() {
        // null service def and bad service def name
        for (Action action : cu) {
            // passing in null service def is an error
            assertFalse(validator.isValid((RangerServiceDef) null, action, failures));
            utils.checkFailureForMissingValue(failures, "service def");
        }
    }

    @Test
    public final void test_isValidServiceDefId_failures() throws Exception {
        // id is required for update
        assertFalse(validator.isValidServiceDefId(null, Action.UPDATE, failures));
        utils.checkFailureForMissingValue(failures, "id");

        // update: service should exist for the passed in id
        long id = 7;
        when(serviceDef.getId()).thenReturn(id);
        when(store.getServiceDef(id)).thenReturn(null);
        assertFalse(validator.isValidServiceDefId(id, Action.UPDATE, failures));
        utils.checkFailureForSemanticError(failures, "id");

        when(store.getServiceDef(id)).thenThrow(new Exception());
        assertFalse(validator.isValidServiceDefId(id, Action.UPDATE, failures));
        utils.checkFailureForSemanticError(failures, "id");
    }

    @Test
    public final void test_isValidServiceDefId_happyPath() throws Exception {
        // create: null id is ok
        assertTrue(validator.isValidServiceDefId(null, Action.CREATE, failures));
        assertTrue(failures.isEmpty());

        // update: a service with same id exist
        long id = 7;
        when(serviceDef.getId()).thenReturn(id);
        RangerServiceDef serviceDefFromDb = mock(RangerServiceDef.class);
        when(serviceDefFromDb.getId()).thenReturn(id);
        when(store.getServiceDef(id)).thenReturn(serviceDefFromDb);
        assertTrue(validator.isValidServiceDefId(id, Action.UPDATE, failures));
        assertTrue(failures.isEmpty());
    }

    @Test
    public final void test_isValidName() {
        Long id = 7L; // some arbitrary value
        // name can't be null/empty
        for (Action action : cu) {
            for (String name : new String[] {null, "", "  "}) {
                when(serviceDef.getName()).thenReturn(name);
                failures.clear();
                assertFalse(validator.isValidServiceDefName(name, id, action, failures));
                utils.checkFailureForMissingValue(failures, "name");
            }
        }
    }

    @Test
    public final void test_isValidName_create() throws Exception {
        Long id = null; // id should be irrelevant for name check for create.

        // for create a service shouldn't exist with the name
        String name = "existing-service";
        when(serviceDef.getName()).thenReturn(name);
        when(store.getServiceDefByName(name)).thenReturn(null);
        assertTrue(validator.isValidServiceDefName(name, id, Action.CREATE, failures));
        assertTrue(failures.isEmpty());

        RangerServiceDef existingServiceDef = mock(RangerServiceDef.class);
        when(store.getServiceDefByName(name)).thenReturn(existingServiceDef);
        failures.clear();
        assertFalse(validator.isValidServiceDefName(name, id, Action.CREATE, failures));
        utils.checkFailureForSemanticError(failures, "name");
    }

    @Test
    public final void test_isValidName_update() throws Exception {
        // update: if service exists with the same name then it can't point to a different service
        long id = 7;
        when(serviceDef.getId()).thenReturn(id);
        String name = "aServiceDef";
        when(serviceDef.getName()).thenReturn(name);
        when(store.getServiceDefByName(name)).thenReturn(null); // no service with the new name (we are updating the name to a unique value)
        assertTrue(validator.isValidServiceDefName(name, id, Action.UPDATE, failures));
        assertTrue(failures.isEmpty());

        RangerServiceDef existingServiceDef = mock(RangerServiceDef.class);
        when(existingServiceDef.getId()).thenReturn(id);
        when(existingServiceDef.getName()).thenReturn(name);
        when(store.getServiceDefByName(name)).thenReturn(existingServiceDef);
        assertTrue(validator.isValidServiceDefName(name, id, Action.UPDATE, failures));
        assertTrue(failures.isEmpty());

        long anotherId = 49;
        when(existingServiceDef.getId()).thenReturn(anotherId);
        assertFalse(validator.isValidServiceDefName(name, id, Action.UPDATE, failures));
        utils.checkFailureForSemanticError(failures, "id/name");
    }

    @Test
    public final void test_isValidAccessTypes_happyPath() {
        long id = 7;
        when(serviceDef.getId()).thenReturn(id);
        List<RangerAccessTypeDef> input = utils.createAccessTypeDefs(accessTypesGood);
        assertTrue(validator.isValidAccessTypes(id, input, failures, Action.CREATE));
        assertTrue(failures.isEmpty());
    }

    @Test
    public final void test_isValidAccessTypes_failures() {
        long id = 7;
        when(serviceDef.getId()).thenReturn(id);
        // null or empty access type defs
        List<RangerAccessTypeDef> accessTypeDefs = null;
        failures.clear();
        assertFalse(validator.isValidAccessTypes(id, accessTypeDefs, failures, Action.CREATE));
        utils.checkFailureForMissingValue(failures, "access types");

        accessTypeDefs = new ArrayList<>();
        failures.clear();
        assertFalse(validator.isValidAccessTypes(id, accessTypeDefs, failures, Action.CREATE));
        utils.checkFailureForMissingValue(failures, "access types");

        // null/empty access types
        accessTypeDefs = utils.createAccessTypeDefs(new String[] {null, "", "\t\t"});
        failures.clear();
        assertFalse(validator.isValidAccessTypes(id, accessTypeDefs, failures, Action.CREATE));
        utils.checkFailureForMissingValue(failures, "access type name");

        // duplicate access types
        accessTypeDefs = utils.createAccessTypeDefs(new String[] {"read", "write", "execute", "read"});
        failures.clear();
        assertFalse(validator.isValidAccessTypes(id, accessTypeDefs, failures, Action.CREATE));
        utils.checkFailureForSemanticError(failures, "access type name", "read");

        // duplicate access types - case-insensitive
        accessTypeDefs = utils.createAccessTypeDefs(new String[] {"read", "write", "execute", "READ"});
        failures.clear();
        assertFalse(validator.isValidAccessTypes(id, accessTypeDefs, failures, Action.CREATE));
        utils.checkFailureForSemanticError(failures, "access type name", "READ");

        // unknown access type in implied grants list
        accessTypeDefs = utils.createAccessTypeDefs(accessTypesBadUnknownType);
        failures.clear();
        assertFalse(validator.isValidAccessTypes(id, accessTypeDefs, failures, Action.CREATE));
        utils.checkFailureForSemanticError(failures, "implied grants", "execute");
        utils.checkFailureForSemanticError(failures, "access type itemId", "1"); // id 1 is duplicated

        // access type with implied grant referring to itself
        accessTypeDefs = utils.createAccessTypeDefs(accessTypesBadSelfReference);
        failures.clear();
        assertFalse(validator.isValidAccessTypes(id, accessTypeDefs, failures, Action.CREATE));
        utils.checkFailureForSemanticError(failures, "implied grants", "admin");
    }

    @Test
    public final void test_isValidEnums_happyPath() {
        List<RangerEnumDef> input = utils.createEnumDefs(enumsGood);
        assertTrue(validator.isValidEnums(input, failures));
        assertTrue(failures.isEmpty());
    }

    @Test
    public final void test_isValidEnums_failures() {
        // null elements in enum def list are a failure
        List<RangerEnumDef> input = utils.createEnumDefs(enumsGood);
        input.add(null);
        assertFalse(validator.isValidEnums(input, failures));
        utils.checkFailureForMissingValue(failures, "enum def");

        // enum names should be valid
        input = utils.createEnumDefs(enumsBadEnumNameNull);
        failures.clear();
        assertFalse(validator.isValidEnums(input, failures));
        utils.checkFailureForMissingValue(failures, "enum def name");

        input = utils.createEnumDefs(enumsBadEnumNameBlank);
        failures.clear();
        assertFalse(validator.isValidEnums(input, failures));
        utils.checkFailureForMissingValue(failures, "enum def name");
        utils.checkFailureForSemanticError(failures, "enum def itemId", "1");

        // enum elements collection should not be null or empty
        input = utils.createEnumDefs(enumsGood);
        RangerEnumDef anEnumDef = mock(RangerEnumDef.class);
        when(anEnumDef.getName()).thenReturn("anEnum");
        when(anEnumDef.getElements()).thenReturn(null);
        input.add(anEnumDef);
        failures.clear();
        assertFalse(validator.isValidEnums(input, failures));
        utils.checkFailureForMissingValue(failures, "enum values", "anEnum");

        input = utils.createEnumDefs(enumsBadElementsEmpty);
        failures.clear();
        assertFalse(validator.isValidEnums(input, failures));
        utils.checkFailureForMissingValue(failures, "enum values", "anEnum");
        utils.checkFailureForMissingValue(failures, "enum def itemId");

        // enum names should be distinct -- exact match
        input = utils.createEnumDefs(enumsGood);
        // add an element with same name as the first element
        String name = input.iterator().next().getName();
        when(anEnumDef.getName()).thenReturn(name);
        List<RangerEnumElementDef> elementDefs = utils.createEnumElementDefs(new String[] {"val1", "val2"});
        when(anEnumDef.getElements()).thenReturn(elementDefs);
        input.add(anEnumDef);
        failures.clear();
        assertFalse(validator.isValidEnums(input, failures));
        utils.checkFailureForSemanticError(failures, "enum def name", name);

        // enum names should be distinct -- case insensitive
        input = utils.createEnumDefs(enumsBadEnumNameDuplicateDifferentCase);
        failures.clear();
        assertFalse(validator.isValidEnums(input, failures));
        utils.checkFailureForSemanticError(failures, "enum def name", "Authentication-Type");

        // enum default index should be right
        input = utils.createEnumDefs(enumsGood);
        // set the index of 1st on to be less than 0
        when(input.iterator().next().getDefaultIndex()).thenReturn(-1);
        failures.clear();
        assertFalse(validator.isValidEnums(input, failures));
        utils.checkFailureForSemanticError(failures, "enum default index", "authentication-type");
        // set the index to be more than number of elements
        when(input.iterator().next().getDefaultIndex()).thenReturn(2);
        failures.clear();
        assertFalse(validator.isValidEnums(input, failures));
        utils.checkFailureForSemanticError(failures, "enum default index", "authentication-type");
    }

    @Test
    public final void test_isValidEnumElements_happyPath() {
        List<RangerEnumElementDef> input = utils.createEnumElementDefs(new String[] {"simple", "kerberos"});
        assertTrue(validator.isValidEnumElements(input, failures, "anEnum"));
        assertTrue(failures.isEmpty());
    }

    @Test
    public final void test_isValidEnumElements_failures() {
        // enum element collection should not have nulls in it
        List<RangerEnumElementDef> input = utils.createEnumElementDefs(new String[] {"simple", "kerberos"});
        input.add(null);
        assertFalse(validator.isValidEnumElements(input, failures, "anEnum"));
        utils.checkFailureForMissingValue(failures, "enum element", "anEnum");

        // element names can't be null/empty
        input = utils.createEnumElementDefs(new String[] {"simple", "kerberos", null});
        failures.clear();
        assertFalse(validator.isValidEnumElements(input, failures, "anEnum"));
        utils.checkFailureForMissingValue(failures, "enum element name", "anEnum");

        input = utils.createEnumElementDefs(new String[] {"simple", "kerberos", "\t\t"}); // two tabs
        failures.clear();
        assertFalse(validator.isValidEnumElements(input, failures, "anEnum"));
        utils.checkFailureForMissingValue(failures, "enum element name", "anEnum");

        // element names should be distinct - case insensitive
        input = utils.createEnumElementDefs(new String[] {"simple", "kerberos", "kerberos"}); // duplicate name - exact match
        failures.clear();
        assertFalse(validator.isValidEnumElements(input, failures, "anEnum"));
        utils.checkFailureForSemanticError(failures, "enum element name", "kerberos");

        input = utils.createEnumElementDefs(new String[] {"simple", "kerberos", "kErbErOs"}); // duplicate name - different case
        failures.clear();
        assertFalse(validator.isValidEnumElements(input, failures, "anEnum"));
        utils.checkFailureForSemanticError(failures, "enum element name", "kErbErOs");
    }

    @Test
    public final void test_isValidResources() {
        // null/empty resources are an error
        when(serviceDef.getResources()).thenReturn(null);
        failures.clear();
        assertFalse(validator.isValidResources(serviceDef, failures, Action.CREATE));
        utils.checkFailureForMissingValue(failures, "resources");

        List<RangerResourceDef> resources = new ArrayList<>();
        when(serviceDef.getResources()).thenReturn(resources);
        failures.clear();
        assertFalse(validator.isValidResources(serviceDef, failures, Action.CREATE));
        utils.checkFailureForMissingValue(failures, "resources");

        resources.addAll(utils.createResourceDefsWithIds(invalidResources));
        failures.clear();
        assertFalse(validator.isValidResources(serviceDef, failures, Action.CREATE));
        utils.checkFailureForMissingValue(failures, "resource name");
        utils.checkFailureForMissingValue(failures, "resource itemId");
        utils.checkFailureForSemanticError(failures, "resource itemId", "1"); // id 1 is duplicate
        utils.checkFailureForSemanticError(failures, "resource name", "DataBase");

        resources.clear();
        resources.addAll(utils.createResourceDefsWithIds(mixedCaseResources));
        failures.clear();
        assertFalse(validator.isValidResources(serviceDef, failures, Action.CREATE));
        utils.checkFailure(failures, null, null, null, "DBase", null);
        utils.checkFailure(failures, null, null, null, "TABLE", null);
        utils.checkFailure(failures, null, null, null, "Column", null);
    }

    @Test
    public final void test_isValidResourceGraph() {
        Object[][] dataBad = new Object[][] {
                //  { name,  excludesSupported, recursiveSupported, mandatory, reg-exp, parent-level, level }
                {"db", null, null, null, null, "", 10},
                {"table", null, null, null, null, "db", 20},   // same as db's level
                {"column-family", null, null, null, null, "table", null}, // level is null!
                {"column", null, null, null, null, "column-family", 20},   // level is duplicate for [db->table->column-family-> column] hierarchy
                {"udf", null, null, null, null, "db", 10},   // udf's id conflicts with that of db in the [db->udf] hierarchy
        };

        List<RangerResourceDef> resourceDefs = utils.createResourceDefs(dataBad);
        when(serviceDef.getResources()).thenReturn(resourceDefs);
        when(serviceDef.getName()).thenReturn("service-name");
        when(serviceDef.getUpdateTime()).thenReturn(new Date());

        failures.clear();
        assertFalse(validator.isValidResourceGraph(serviceDef, failures));
        utils.checkFailureForMissingValue(failures, "resource level");
        utils.checkFailureForSemanticError(failures, "resource level", "20"); // level 20 is duplicate for 1 hierarchy
        utils.checkFailureForSemanticError(failures, "resource level", "10"); // level 10 is duplicate for another hierarchy

        dataBad     = new Object[][] {
                //  { name,  excludesSupported, recursiveSupported, mandatory, reg-exp, parent-level, level }
                {"db", null, null, null, null, "", 10},
                {"table", null, null, null, null, "db", 20},
                {"column-family", null, null, null, null, "table", 15}, // level is smaller than table!
                {"column", null, null, null, null, "column-family", 30},
                {"udf", null, null, null, null, "db", 15},
        };
        resourceDefs = utils.createResourceDefs(dataBad);
        when(serviceDef.getResources()).thenReturn(resourceDefs);
        when(serviceDef.getName()).thenReturn("service-name");
        when(serviceDef.getUpdateTime()).thenReturn(new Date());

        failures.clear();
        assertFalse(validator.isValidResourceGraph(serviceDef, failures));
        utils.checkFailureForSemanticError(failures, "resource level", "15"); // level 20 is duplicate for 1 hierarchy

        Object[][] dataGood = new Object[][] {
                //  { name,  excludesSupported, recursiveSupported, mandatory, reg-exp, parent-level, level }
                {"db", null, null, null, null, "", -10}, // -ve level is ok
                {"table", null, null, null, null, "db", 0},   // 0 level is ok
                {"column", null, null, null, null, "table", 10},  // level is null!
                {"udf", null, null, null, null, "db", 0},   // should not conflict as it belong to a different hierarchy
        };
        resourceDefs = utils.createResourceDefs(dataGood);
        when(serviceDef.getResources()).thenReturn(resourceDefs);
        failures.clear();
        assertTrue(validator.isValidResourceGraph(serviceDef, failures));
        assertTrue(failures.isEmpty());

        Object[][] dataCycles = new Object[][] {
                //  { name,  excludesSupported, recursiveSupported, mandatory, reg-exp, parent-level, level }
                {"db", null, null, null, null, "column", -10}, // -ve level is ok
                {"table", null, null, null, null, "db", 0},   // 0 level is ok
                {"column", null, null, null, null, "table", 10},  // level is null!
                {"udf", null, null, null, null, "db", -5},   // should not conflict as it belong to a different hierarchy
        };

        resourceDefs = utils.createResourceDefs(dataCycles);
        when(serviceDef.getResources()).thenReturn(resourceDefs);
        failures.clear();
        assertFalse("Graph was valid!", validator.isValidResourceGraph(serviceDef, failures));
        assertFalse(failures.isEmpty());
        utils.checkFailureForSemanticError(failures, "resource graph");

        dataBad     = new Object[][] {
                //  { name,  excludesSupported, recursiveSupported, mandatory, reg-exp, parent-level, level }
                {"db", null, null, null, null, "", -10}, // -ve level is ok
                {"table", null, null, true, null, "db", 0},   // 0 level is ok; mandatory true here, but not at parent level?
                {"column", null, null, null, null, "table", 10},  // level is null!
                {"udf", null, null, null, null, "db", 0},   // should not conflict as it belong to a different hierarchy
        };
        resourceDefs = utils.createResourceDefs(dataBad);
        when(serviceDef.getResources()).thenReturn(resourceDefs);
        failures.clear();
        assertFalse(validator.isValidResourceGraph(serviceDef, failures));
        assertFalse(failures.isEmpty());

        dataGood    = new Object[][] {
                //  { name,  excludesSupported, recursiveSupported, mandatory, reg-exp, parent-level, level }
                {"db", null, null, true, null, "", -10}, // -ve level is ok
                {"table", null, null, null, null, "db", 0},   // 0 level is ok; mandatory true here, but not at parent level?
                {"column", null, null, null, null, "table", 10},  // level is null!
                {"udf", null, null, true, null, "db", 0},   // should not conflict as it belong to a different hierarchy
        };
        resourceDefs = utils.createResourceDefs(dataGood);
        when(serviceDef.getResources()).thenReturn(resourceDefs);
        failures.clear();
        assertTrue(validator.isValidResourceGraph(serviceDef, failures));
        assertTrue(failures.isEmpty());
    }

    @Test
    public final void test_isValidResources_happyPath() {
        Object[][] data = new Object[][] {
                //  { id,   level,      name }
                {1L, -10, "database"}, // -ve value for level
                {3L, 0, "table"}, // it is ok for id and level to skip values
                {5L, 10, "column"}, // it is ok for level to skip values
        };
        List<RangerResourceDef> resources = utils.createResourceDefsWithIds(data);
        when(serviceDef.getResources()).thenReturn(resources);
        assertTrue(validator.isValidResources(serviceDef, failures, Action.CREATE));
        assertTrue(failures.isEmpty());
    }

    @Test
    public final void test_isValidConfigs_failures() {
        assertTrue(validator.isValidConfigs(null, null, failures));
        assertTrue(failures.isEmpty());

        Object[][] configDefDataBad = new Object[][] {
                //  { id,  name, type, subtype, default-value }
                {null, null, ""}, // id and name both null, type is empty
                {1L, "security", "blah"}, // bad type for service def
                {1L, "port", "int"}, // duplicate id
                {2L, "security", "string"}, // duplicate name
                {3L, "timeout", "enum", "units", null}, // , sub-type (units) is not among known enum types
                {4L, "auth", "enum", "authentication-type", "dimple"}, // default value is not among known values for the enum (sub-type)
        };

        List<RangerServiceConfigDef> configs  = utils.createServiceDefConfigs(configDefDataBad);
        List<RangerEnumDef>          enumDefs = utils.createEnumDefs(enumsGood);
        assertFalse(validator.isValidConfigs(configs, enumDefs, failures));
        utils.checkFailureForMissingValue(failures, "config def name");
        utils.checkFailureForMissingValue(failures, "config def itemId");
        utils.checkFailureForMissingValue(failures, "config def type");
        utils.checkFailureForSemanticError(failures, "config def name", "security"); // there were two configs with same name as security
        utils.checkFailureForSemanticError(failures, "config def itemId", "1"); // a config with duplicate had id of 1
        utils.checkFailureForSemanticError(failures, "config def type", "security"); // type for config security was invalid
        utils.checkFailureForSemanticError(failures, "config def subtype", "timeout"); // type for config security was invalid
        utils.checkFailureForSemanticError(failures, "config def default value", "auth"); // type for config security was invalid
    }

    @Test
    public final void test_isValidPolicyConditions() {
        long id = 7;
        when(serviceDef.getId()).thenReturn(id);
        // null/empty policy conditions are ok
        assertTrue(validator.isValidPolicyConditions(id, null, failures, Action.CREATE));
        assertTrue(failures.isEmpty());
        List<RangerPolicyConditionDef> conditionDefs = new ArrayList<>();
        assertTrue(validator.isValidPolicyConditions(id, conditionDefs, failures, Action.CREATE));
        assertTrue(failures.isEmpty());

        Object[][] policyConditionData = {
                //  { id, name, evaluator }
                {null, null, null}, // everything null!
                {1L, "condition-1", null}, // missing evaluator
                {1L, "condition-2", ""}, // duplicate id, missing evaluator
                {2L, "condition-1", "com.evaluator"}, // duplicate name
        };

        conditionDefs.addAll(utils.createPolicyConditionDefs(policyConditionData));
        failures.clear();
        assertFalse(validator.isValidPolicyConditions(id, conditionDefs, failures, Action.CREATE));
        utils.checkFailureForMissingValue(failures, "policy condition def itemId");
        utils.checkFailureForMissingValue(failures, "policy condition def name");
        utils.checkFailureForMissingValue(failures, "policy condition def evaluator");
        utils.checkFailureForSemanticError(failures, "policy condition def itemId", "1");
        utils.checkFailureForSemanticError(failures, "policy condition def name", "condition-1");
        utils.checkFailureForMissingValue(failures, "policy condition def evaluator", "condition-2");
        utils.checkFailureForMissingValue(failures, "policy condition def evaluator", "condition-1");
    }
}
