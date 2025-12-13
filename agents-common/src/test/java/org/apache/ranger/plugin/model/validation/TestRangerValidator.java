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

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRangerValidator {
    private       RangerValidatorForTest         validator;
    private       ServiceStore                   store;
    private final ValidationTestUtils            utils = new ValidationTestUtils();
    private       List<ValidationFailureDetails> failures;

    @BeforeEach
    public void before() {
        store     = mock(ServiceStore.class);
        validator = new RangerValidatorForTest(store);
        failures  = new ArrayList<>();
    }

    @Test
    public void test_ctor_firewalling() {
        try {
            // service store can't be null during construction
            new RangerValidatorForTest(null);
            Assertions.fail("Should have thrown exception!");
        } catch (IllegalArgumentException e) {
            // expected exception
        }
    }

    @Test
    public void test_validate() {
        // default implementation should fail.  This is abstract class.  Sub-class must do something sensible with isValid
        try {
            validator.validate(1L, Action.CREATE);
            Assertions.fail("Should have thrown exception!");
        } catch (Exception e) {
            // ok expected exception
            String message = e.getMessage();
            Assertions.assertTrue(message.contains("internal error"));
        }
    }

    @Test
    public void test_getServiceConfigParameters() {
        // reasonable protection against null values
        Set<String> parameters = validator.getServiceConfigParameters(null);
        Assertions.assertNotNull(parameters);
        Assertions.assertTrue(parameters.isEmpty());

        RangerService service = mock(RangerService.class);
        when(service.getConfigs()).thenReturn(null);
        parameters = validator.getServiceConfigParameters(service);
        Assertions.assertNotNull(parameters);
        Assertions.assertTrue(parameters.isEmpty());

        when(service.getConfigs()).thenReturn(new HashMap<>());
        parameters = validator.getServiceConfigParameters(service);
        Assertions.assertNotNull(parameters);
        Assertions.assertTrue(parameters.isEmpty());

        String[]            keys = new String[] {"a", "b", "c"};
        Map<String, String> map  = utils.createMap(keys);
        when(service.getConfigs()).thenReturn(map);
        parameters = validator.getServiceConfigParameters(service);
        for (String key : keys) {
            Assertions.assertTrue(parameters.contains(key), "key");
        }
    }

    @Test
    public void test_getRequiredParameters() {
        // reasonable protection against null things
        Set<String> parameters = validator.getRequiredParameters(null);
        Assertions.assertNotNull(parameters);
        Assertions.assertTrue(parameters.isEmpty());

        RangerServiceDef serviceDef = mock(RangerServiceDef.class);
        when(serviceDef.getConfigs()).thenReturn(null);
        parameters = validator.getRequiredParameters(null);
        Assertions.assertNotNull(parameters);
        Assertions.assertTrue(parameters.isEmpty());

        List<RangerServiceConfigDef> configs = new ArrayList<>();
        when(serviceDef.getConfigs()).thenReturn(configs);
        parameters = validator.getRequiredParameters(null);
        Assertions.assertNotNull(parameters);
        Assertions.assertTrue(parameters.isEmpty());

        Object[][] input = new Object[][] {
                {"param1", false},
                {"param2", true},
                {"param3", true},
                {"param4", false},
        };
        configs = utils.createServiceConditionDefs(input);
        when(serviceDef.getConfigs()).thenReturn(configs);
        parameters = validator.getRequiredParameters(serviceDef);
        Assertions.assertTrue(parameters.contains("param2"), "result does not contain: param2");
        Assertions.assertTrue(parameters.contains("param3"), "result does not contain: param3");
    }

    @Test
    public void test_getServiceDef() {
        try {
            // if service store returns null or throws an exception then service is deemed invalid
            when(store.getServiceDefByName("return null")).thenReturn(null);
            when(store.getServiceDefByName("throw")).thenThrow(new Exception());
            RangerServiceDef serviceDef = mock(RangerServiceDef.class);
            when(store.getServiceDefByName("good-service")).thenReturn(serviceDef);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("Unexpected exception during mocking!");
        }

        Assertions.assertNull(validator.getServiceDef("return null"));
        Assertions.assertNull(validator.getServiceDef("throw"));
        Assertions.assertNotNull(validator.getServiceDef("good-service"));
    }

    @Test
    public void test_getPolicy() throws Exception {
        // if service store returns null or throws an exception then return null policy
        when(store.getPolicy(1L)).thenReturn(null);
        when(store.getPolicy(2L)).thenThrow(new Exception());
        RangerPolicy policy = mock(RangerPolicy.class);
        when(store.getPolicy(3L)).thenReturn(policy);

        Assertions.assertNull(validator.getPolicy(1L));
        Assertions.assertNull(validator.getPolicy(2L));
        Assertions.assertNotNull(validator.getPolicy(3L));
    }

    @Test
    public final void test_getPoliciesForResourceSignature() throws Exception {
        // return null if store returns null or throws an exception
        String  hexSignature    = "aSignature";
        String  serviceName     = "service-name";
        boolean isPolicyEnabled = true;
        when(store.getPoliciesByResourceSignature(serviceName, hexSignature, isPolicyEnabled)).thenReturn(null);
        Assertions.assertNotNull(validator.getPoliciesForResourceSignature(serviceName, hexSignature));
        when(store.getPoliciesByResourceSignature(serviceName, hexSignature, isPolicyEnabled)).thenThrow(new Exception());
        Assertions.assertNotNull(validator.getPoliciesForResourceSignature(serviceName, hexSignature));

        // what ever store returns should come back
        hexSignature = "anotherSignature";
        List<RangerPolicy> policies = new ArrayList<>();
        RangerPolicy       policy1  = mock(RangerPolicy.class);
        policies.add(policy1);
        RangerPolicy policy2 = mock(RangerPolicy.class);
        policies.add(policy2);
        when(store.getPoliciesByResourceSignature(serviceName, hexSignature, isPolicyEnabled)).thenReturn(policies);
        List<RangerPolicy> result = validator.getPoliciesForResourceSignature(serviceName, hexSignature);
        Assertions.assertTrue(result.contains(policy1) && result.contains(policy2));
    }

    @Test
    public void test_getService_byId() throws Exception {
        // if service store returns null or throws an exception then service is deemed invalid
        when(store.getService(1L)).thenReturn(null);
        when(store.getService(2L)).thenThrow(new Exception());
        RangerService service = mock(RangerService.class);
        when(store.getService(3L)).thenReturn(service);

        Assertions.assertNull(validator.getService(1L));
        Assertions.assertNull(validator.getService(2L));
        Assertions.assertNotNull(validator.getService(3L));
    }

    @Test
    public void test_getService() {
        try {
            // if service store returns null or throws an exception then service is deemed invalid
            when(store.getServiceByName("return null")).thenReturn(null);
            when(store.getServiceByName("throw")).thenThrow(new Exception());
            RangerService service = mock(RangerService.class);
            when(store.getServiceByName("good-service")).thenReturn(service);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("Unexpected exception during mocking!");
        }

        Assertions.assertNull(validator.getService("return null"));
        Assertions.assertNull(validator.getService("throw"));
        Assertions.assertNotNull(validator.getService("good-service"));
    }

    @Test
    public void test_getAccessTypes() {
        // passing in null service def
        Set<String> accessTypes = validator.getAccessTypes(null);
        Assertions.assertTrue(accessTypes.isEmpty());
        // that has null or empty access type def
        RangerServiceDef serviceDef = mock(RangerServiceDef.class);
        when(serviceDef.getAccessTypes()).thenReturn(null);
        accessTypes = validator.getAccessTypes(serviceDef);
        Assertions.assertTrue(accessTypes.isEmpty());

        List<RangerAccessTypeDef> accessTypeDefs = new ArrayList<>();
        when(serviceDef.getAccessTypes()).thenReturn(accessTypeDefs);
        accessTypes = validator.getAccessTypes(serviceDef);
        Assertions.assertTrue(accessTypes.isEmpty());

        // having null accesstypedefs
        accessTypeDefs.add(null);
        accessTypes = validator.getAccessTypes(serviceDef);
        Assertions.assertTrue(accessTypes.isEmpty());

        // access type defs with null empty blank names are skipped, spaces within names are preserved
        String[] names = new String[] {null, "", "a", "  ", "b ", "\t\t", " C", "\tD\t"};

        accessTypeDefs.addAll(utils.createAccessTypeDefs(names));
        accessTypes = validator.getAccessTypes(serviceDef);
        accessTypes.removeAll(ServiceDefUtil.ACCESS_TYPE_MARKERS);
        Assertions.assertEquals(4, accessTypes.size());
        Assertions.assertTrue(accessTypes.contains("a"));
        Assertions.assertTrue(accessTypes.contains("b "));
        Assertions.assertTrue(accessTypes.contains(" C"));
        Assertions.assertTrue(accessTypes.contains("\tD\t"));
    }

    @Test
    public void test_getResourceNames() {
        // passing in null service def
        Set<String> accessTypes = validator.getMandatoryResourceNames(null);
        Assertions.assertTrue(accessTypes.isEmpty());
        // that has null or empty access type def
        RangerServiceDef serviceDef = mock(RangerServiceDef.class);
        when(serviceDef.getResources()).thenReturn(null);
        accessTypes = validator.getMandatoryResourceNames(serviceDef);
        Assertions.assertTrue(accessTypes.isEmpty());

        List<RangerResourceDef> resourceDefs = new ArrayList<>();
        when(serviceDef.getResources()).thenReturn(resourceDefs);
        accessTypes = validator.getMandatoryResourceNames(serviceDef);
        Assertions.assertTrue(accessTypes.isEmpty());

        // having null accesstypedefs
        resourceDefs.add(null);
        accessTypes = validator.getMandatoryResourceNames(serviceDef);
        Assertions.assertTrue(accessTypes.isEmpty());

        // access type defs with null empty blank names are skipped, spaces within names are preserved
        Object[][] data = {
                //  { name,  excludes     recursive    mandatory, reg-exp,       parent-level }
                //         Supported?,  Supported?,
                {"a", null, null, true}, // all good
                null,                                       // this should put a null element in the resource def!
                {"b", null, null, null}, // mandatory field is null, i.e. false
                {"c", null, null, false}, // non-mandatory field false - upper case
                {"D", null, null, true}, // resource specified in upper case
                {"E", null, null, false}, // all good
        };
        resourceDefs.addAll(utils.createResourceDefs(data));
        accessTypes = validator.getMandatoryResourceNames(serviceDef);
        Assertions.assertEquals(2, accessTypes.size());
        Assertions.assertTrue(accessTypes.contains("a"));
        Assertions.assertTrue(accessTypes.contains("d")); // name should come back lower case

        accessTypes = validator.getAllResourceNames(serviceDef);
        Assertions.assertEquals(5, accessTypes.size());
        Assertions.assertTrue(accessTypes.contains("b"));
        Assertions.assertTrue(accessTypes.contains("c"));
        Assertions.assertTrue(accessTypes.contains("e"));
    }

    @Test
    public void test_getValidationRegExes() {
        // passing in null service def
        Map<String, String> regExMap = validator.getValidationRegExes(null);
        Assertions.assertTrue(regExMap.isEmpty());
        // that has null or empty access type def
        RangerServiceDef serviceDef = mock(RangerServiceDef.class);
        when(serviceDef.getResources()).thenReturn(null);
        regExMap = validator.getValidationRegExes(serviceDef);
        Assertions.assertTrue(regExMap.isEmpty());

        List<RangerResourceDef> resourceDefs = new ArrayList<>();
        when(serviceDef.getResources()).thenReturn(resourceDefs);
        regExMap = validator.getValidationRegExes(serviceDef);
        Assertions.assertTrue(regExMap.isEmpty());

        // having null accesstypedefs
        resourceDefs.add(null);
        regExMap = validator.getValidationRegExes(serviceDef);
        Assertions.assertTrue(regExMap.isEmpty());

        // access type defs with null empty blank names are skipped, spaces within names are preserved
        String[][] data = {
                {"a", null},     // null-regex
                null,              // this should put a null element in the resource def!
                {"b", "regex1"}, // valid
                {"c", ""},       // empty regex
                {"d", "regex2"}, // valid
                {"e", "   "},    // blank regex
                {"f", "regex3"}, // all good
        };
        resourceDefs.addAll(utils.createResourceDefsWithRegEx(data));
        regExMap = validator.getValidationRegExes(serviceDef);
        Assertions.assertEquals(3, regExMap.size());
        Assertions.assertEquals("regex1", regExMap.get("b"));
        Assertions.assertEquals("regex2", regExMap.get("d"));
        Assertions.assertEquals("regex3", regExMap.get("f"));
    }

    @Test
    public void test_getIsAuditEnabled() {
        // null policy
        RangerPolicy policy = null;
        boolean      result = validator.getIsAuditEnabled(policy);
        Assertions.assertFalse(result);
        // null isAuditEnabled Boolean is supposed to be TRUE!!
        policy = mock(RangerPolicy.class);
        when(policy.getIsAuditEnabled()).thenReturn(null);
        result = validator.getIsAuditEnabled(policy);
        Assertions.assertTrue(result);
        // non-null value
        when(policy.getIsAuditEnabled()).thenReturn(Boolean.FALSE);
        result = validator.getIsAuditEnabled(policy);
        Assertions.assertFalse(result);

        when(policy.getIsAuditEnabled()).thenReturn(Boolean.TRUE);
        result = validator.getIsAuditEnabled(policy);
        Assertions.assertTrue(result);
    }

    @Test
    public void test_getServiceDef_byId() throws Exception {
        // if service store returns null or throws an exception then service is deemed invalid
        when(store.getServiceDef(1L)).thenReturn(null);
        when(store.getServiceDef(2L)).thenThrow(new Exception());
        RangerServiceDef serviceDef = mock(RangerServiceDef.class);
        when(store.getServiceDef(3L)).thenReturn(serviceDef);

        Assertions.assertNull(validator.getServiceDef(1L));
        Assertions.assertNull(validator.getServiceDef(2L));
        Assertions.assertNotNull(validator.getServiceDef(3L));
    }

    @Test
    public void test_getEnumDefaultIndex() {
        RangerEnumDef enumDef = mock(RangerEnumDef.class);
        Assertions.assertEquals(-1, validator.getEnumDefaultIndex(null));
        when(enumDef.getDefaultIndex()).thenReturn(null);
        Assertions.assertEquals(0, validator.getEnumDefaultIndex(enumDef));
        when(enumDef.getDefaultIndex()).thenReturn(-5);
        Assertions.assertEquals(-5, validator.getEnumDefaultIndex(enumDef));
    }

    @Test
    public void test_getImpliedGrants() {
        // passing in null gets back a null
        Collection<String> result = validator.getImpliedGrants(null);
        Assertions.assertNull(result);

        // null or empty implied grant collection gets back an empty collection
        RangerAccessTypeDef accessTypeDef = mock(RangerAccessTypeDef.class);
        when(accessTypeDef.getImpliedGrants()).thenReturn(null);
        result = validator.getImpliedGrants(accessTypeDef);
        Assertions.assertTrue(result.isEmpty());

        List<String> impliedGrants = new ArrayList<>();
        when(accessTypeDef.getImpliedGrants()).thenReturn(impliedGrants);
        result = validator.getImpliedGrants(accessTypeDef);
        Assertions.assertTrue(result.isEmpty());

        // null/empty values come back as is
        impliedGrants = Arrays.asList(new String[] {null, "", " ", "\t\t"});
        when(accessTypeDef.getImpliedGrants()).thenReturn(impliedGrants);
        result = validator.getImpliedGrants(accessTypeDef);
        Assertions.assertEquals(4, result.size());

        // non-empty values get lower cased
        impliedGrants = Arrays.asList(new String[] {"a", "B", "C\t", " d "});
        when(accessTypeDef.getImpliedGrants()).thenReturn(impliedGrants);
        result = validator.getImpliedGrants(accessTypeDef);
        Assertions.assertEquals(4, result.size());
        Assertions.assertTrue(result.contains("a"));
        Assertions.assertTrue(result.contains("b"));
        Assertions.assertTrue(result.contains("c\t"));
        Assertions.assertTrue(result.contains(" d "));
    }

    @Test
    public void test_isValid_string() {
        String      fieldName      = "value-field-Name";
        String      collectionName = "value-collection-Name";
        Set<String> alreadySeen    = new HashSet<>();
        // null/empty string value is invalid
        for (String value : new String[] {null, "", "  "}) {
            Assertions.assertFalse(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
            utils.checkFailureForMissingValue(failures, fieldName);
        }
        // value should not have been seen so far.
        String value = "blah";
        failures.clear();
        Assertions.assertTrue(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
        Assertions.assertTrue(failures.isEmpty());
        Assertions.assertTrue(alreadySeen.contains(value));

        // since "blah" has already been seen doing this test again should fail
        failures.clear();
        Assertions.assertFalse(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
        utils.checkFailureForSemanticError(failures, fieldName, value);

        // not see check is done in a case-insenstive manner
        value = "bLaH";
        failures.clear();
        Assertions.assertFalse(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
        utils.checkFailureForSemanticError(failures, fieldName, value);
    }

    @Test
    public void test_isValid_long() {
        String    fieldName      = "field-Name";
        String    collectionName = "field-collection-Name";
        Set<Long> alreadySeen    = new HashSet<>();
        Long      value          = null;
        // null value is invalid
        Assertions.assertFalse(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
        utils.checkFailureForMissingValue(failures, fieldName);

        // value should not have been seen so far.
        value = 7L;
        failures.clear();
        Assertions.assertTrue(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
        Assertions.assertTrue(failures.isEmpty());
        Assertions.assertTrue(alreadySeen.contains(value));

        // since 7L has already been seen doing this test again should fail
        failures.clear();
        Assertions.assertFalse(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
        utils.checkFailureForSemanticError(failures, fieldName, value.toString());
    }

    @Test
    public void test_isValid_integer() {
        String       fieldName      = "field-Name";
        String       collectionName = "field-collection-Name";
        Set<Integer> alreadySeen    = new HashSet<>();
        Integer      value          = null;
        // null value is invalid
        Assertions.assertFalse(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
        utils.checkFailureForMissingValue(failures, fieldName);

        // value should not have been seen so far.
        value = 49;
        failures.clear();
        Assertions.assertTrue(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
        Assertions.assertTrue(failures.isEmpty());
        Assertions.assertTrue(alreadySeen.contains(value));

        // since 7L has already been seen doing this test again should fail
        failures.clear();
        Assertions.assertFalse(validator.isUnique(value, alreadySeen, fieldName, collectionName, failures));
        utils.checkFailureForSemanticError(failures, fieldName, value.toString());
    }

    static class RangerValidatorForTest extends RangerValidator {
        public RangerValidatorForTest(ServiceStore store) {
            super(store);
        }

        boolean isValid(String behavior) {
            return "valid".equals(behavior);
        }
    }
}
