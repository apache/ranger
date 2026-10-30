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

import org.apache.ranger.plugin.errors.ValidationErrorCode;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerConstraintPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemConstraintInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerConstraintDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerConstraintParamDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.ServiceStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRangerPolicyValidatorConstraints {
    private ServiceStore                   store;
    private RangerPolicyValidator          validator;
    private RangerServiceDef               serviceDef;
    private List<ValidationFailureDetails> failures;

    @BeforeEach
    public void setUp() {
        store      = mock(ServiceStore.class);
        validator  = new RangerPolicyValidator(store);
        serviceDef = serviceDef();
        failures   = new ArrayList<>();
    }

    @Test
    public void testValidConstraintPolicy() {
        RangerPolicy policy = constraintPolicy(resources("database", "udf"), item("requireApproval", "execute", params("approver", "finance-approvers", "mode", "sync")));

        assertTrue(validator.isValidConstraintPolicy(policy, failures, serviceDef), String.valueOf(failures));
        assertTrue(validator.isValidConstraintResourceNames(policy.getResources(), Collections.singleton("requireApproval"), failures, serviceDef), String.valueOf(failures));
    }

    @Test
    public void testMixedConstraintsWithCompatibleResources() {
        RangerPolicy policy = constraintPolicy(resources("database"),
                item("requireApproval", "execute", params("approver", "finance-approvers")),
                item("maxRows", "select", params("rowCount", 1000)));

        assertTrue(validator.isValidConstraintPolicy(policy, failures, serviceDef), String.valueOf(failures));
        assertTrue(validator.isValidConstraintResourceNames(policy.getResources(), set("requireApproval", "maxRows"), failures, serviceDef), String.valueOf(failures));
    }

    @Test
    public void testResourcesNotValidForEveryConstraint() {
        // udf is allowed for requireApproval, but not for maxRows
        assertFalse(validator.isValidConstraintResourceNames(resources("database", "udf"), set("requireApproval", "maxRows"), failures, serviceDef));
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_RESOURCES_INVALID);

        failures.clear();

        // column is not in any constraint's resources
        assertFalse(validator.isValidConstraintResourceNames(resources("database", "table", "column"), set("maxRows"), failures, serviceDef));
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_RESOURCES_INVALID);
    }

    @Test
    public void testUnknownAndMissingConstraint() {
        RangerPolicy policy = constraintPolicy(resources("database"),
                item("noSuchConstraint", "execute", null),
                item(null, "execute", null));

        assertFalse(validator.isValidConstraintPolicy(policy, failures, serviceDef));
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_UNKNOWN);
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_MISSING);
    }

    @Test
    public void testAccessTypeNotAllowedByConstraint() {
        RangerPolicy policy = constraintPolicy(resources("database"), item("requireApproval", "select", params("approver", "finance-approvers")));

        assertFalse(validator.isValidConstraintPolicy(policy, failures, serviceDef));
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_ACCESS_TYPE_INVALID);
    }

    @Test
    public void testParameterErrors() {
        RangerPolicy policy = constraintPolicy(resources("database"),
                item("requireApproval", "execute", params("mode", "no-such-mode", "extra", "x")), // approver missing, invalid enum, unknown parameter
                item("maxRows", "select", params("rowCount", "abc")));                            // invalid int

        assertFalse(validator.isValidConstraintPolicy(policy, failures, serviceDef));
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_PARAM_MISSING);
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_PARAM_UNKNOWN);
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_PARAM_INVALID_VALUE);
        assertErrorCount(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_PARAM_INVALID_VALUE, 2);
    }

    @Test
    public void testParamValueTypes() {
        RangerConstraintParamDef intParam    = param("p", "int", null, false);
        RangerConstraintParamDef boolParam   = param("p", "bool", null, false);
        RangerConstraintParamDef stringParam = param("p", "string", null, false);
        RangerConstraintParamDef enumParam   = param("p", "enum", "approvalMode", false);

        assertTrue(validator.isValidConstraintParamValue(5, intParam, serviceDef));
        assertTrue(validator.isValidConstraintParamValue(5L, intParam, serviceDef));
        assertTrue(validator.isValidConstraintParamValue(5.0, intParam, serviceDef));
        assertTrue(validator.isValidConstraintParamValue("7", intParam, serviceDef));
        assertFalse(validator.isValidConstraintParamValue(1.5, intParam, serviceDef));
        assertFalse(validator.isValidConstraintParamValue("x", intParam, serviceDef));
        assertTrue(validator.isValidConstraintParamValue(true, boolParam, serviceDef));
        assertTrue(validator.isValidConstraintParamValue("FALSE", boolParam, serviceDef));
        assertFalse(validator.isValidConstraintParamValue("yes", boolParam, serviceDef));
        assertTrue(validator.isValidConstraintParamValue("abc", stringParam, serviceDef));
        assertFalse(validator.isValidConstraintParamValue(5, stringParam, serviceDef));
        assertTrue(validator.isValidConstraintParamValue("async", enumParam, serviceDef));
        assertFalse(validator.isValidConstraintParamValue("other", enumParam, serviceDef));
    }

    @Test
    public void testConstraintPolicyWithOtherItems() {
        RangerPolicy policy = constraintPolicy(resources("database"), item("maxRows", "select", params("rowCount", 10)));

        policy.getPolicyItems().add(new RangerPolicyItem(Collections.singletonList(new RangerPolicyItemAccess("select")), Collections.singletonList("alice"), null, null, null, false));

        assertFalse(validator.isValidConstraintPolicy(policy, failures, serviceDef));
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_POLICY_HAS_OTHER_ITEMS);
    }

    @Test
    public void testConstraintItemsInAccessPolicy() {
        RangerPolicy policy = constraintPolicy(resources("database"), item("maxRows", "select", params("rowCount", 10)));

        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);

        assertFalse(validator.isValidConstraintPolicy(policy, failures, serviceDef));
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_ITEMS_NOT_ALLOWED);
    }

    @Test
    public void testServiceDefWithoutConstraintDefs() {
        RangerPolicy policy = constraintPolicy(resources("database"), item("maxRows", "select", params("rowCount", 10)));

        serviceDef.setConstraintDefs(null);

        assertFalse(validator.isValidConstraintPolicy(policy, failures, serviceDef));
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINTS_NOT_SUPPORTED);
    }

    @Test
    public void testIsValidRunsConstraintChecks() throws Exception {
        RangerService service = new RangerService();

        service.setId(1L);
        service.setName("agent_svc");
        service.setType(serviceDef.getName());

        when(store.getServiceByName("agent_svc")).thenReturn(service);
        when(store.getServiceDefByName(serviceDef.getName())).thenReturn(serviceDef);
        when(store.getPolicyId(any(), any(), any())).thenReturn(null); // no policy with the same name; Mockito returns 0L by default

        RangerPolicy valid = constraintPolicy(resources("database", "udf"), item("requireApproval", "execute", params("approver", "finance-approvers")));

        assertTrue(validator.isValid(valid, Action.CREATE, true, failures), String.valueOf(failures));

        RangerPolicy invalid = constraintPolicy(resources("database", "udf"), item("maxRows", "select", params("rowCount", 10)));

        invalid.setName("p2");

        assertFalse(validator.isValid(invalid, Action.CREATE, true, failures));
        assertErrorCode(ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_RESOURCES_INVALID);
    }

    private void assertErrorCode(ValidationErrorCode error) {
        assertErrorCount(error, -1);
    }

    private void assertErrorCount(ValidationErrorCode error, int expectedCount) {
        int count = 0;

        for (ValidationFailureDetails failure : failures) {
            if (failure.errorCode == error.getErrorCode()) {
                count++;
            }
        }

        if (expectedCount < 0 ? count == 0 : count != expectedCount) {
            fail("expected error " + error + (expectedCount < 0 ? "" : (" x" + expectedCount)) + " in " + failures);
        }
    }

    private static RangerPolicy constraintPolicy(Map<String, RangerPolicyResource> resources, RangerConstraintPolicyItem... items) {
        RangerPolicy ret = new RangerPolicy();

        ret.setService("agent_svc");
        ret.setName("p1");
        ret.setPolicyType(RangerPolicy.POLICY_TYPE_CONSTRAINTS);
        ret.setResources(resources);
        ret.setConstraintPolicyItems(new ArrayList<>(Arrays.asList(items)));

        return ret;
    }

    private static RangerConstraintPolicyItem item(String constraintName, String accessType, Map<String, Object> parameters) {
        RangerPolicyItemConstraintInfo constraintInfo = new RangerPolicyItemConstraintInfo(constraintName, parameters);

        return new RangerConstraintPolicyItem(constraintInfo, new ArrayList<>(Collections.singletonList(new RangerPolicyItemAccess(accessType))), new ArrayList<>(Collections.singletonList("alice")), null, null, null, false);
    }

    private static Map<String, Object> params(Object... nameValues) {
        Map<String, Object> ret = new HashMap<>();

        for (int i = 0; i + 1 < nameValues.length; i += 2) {
            ret.put((String) nameValues[i], nameValues[i + 1]);
        }

        return ret;
    }

    private static Map<String, RangerPolicyResource> resources(String... names) {
        Map<String, RangerPolicyResource> ret = new HashMap<>();

        for (String name : names) {
            ret.put(name, new RangerPolicyResource(name + "-value"));
        }

        return ret;
    }

    private static Set<String> set(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    private static RangerServiceDef serviceDef() {
        RangerServiceDef ret = new RangerServiceDef();

        ret.setName("agent");
        ret.setResources(Arrays.asList(
                resourceDef(1L, "database", null, 10, true),
                resourceDef(2L, "table", "database", 20, false),
                resourceDef(3L, "udf", "database", 20, false),
                resourceDef(4L, "column", "table", 30, false)));
        ret.setAccessTypes(Arrays.asList(new RangerAccessTypeDef(1L, "select", "select", null, null), new RangerAccessTypeDef(2L, "execute", "execute", null, null)));
        ret.setEnums(Collections.singletonList(new RangerEnumDef(1L, "approvalMode", Arrays.asList(new RangerEnumElementDef(1L, "sync", "sync", null), new RangerEnumElementDef(2L, "async", "async", null)), 0)));

        RangerConstraintDef requireApproval = new RangerConstraintDef();

        requireApproval.setItemId(1L);
        requireApproval.setName("requireApproval");
        requireApproval.setResources(Arrays.asList("database", "udf"));
        requireApproval.setAccessTypes(Collections.singletonList("execute"));
        requireApproval.setParameters(Arrays.asList(param("approver", "string", null, true), param("mode", "enum", "approvalMode", false)));

        RangerConstraintDef maxRows = new RangerConstraintDef();

        maxRows.setItemId(2L);
        maxRows.setName("maxRows");
        maxRows.setResources(Arrays.asList("database", "table"));
        maxRows.setAccessTypes(Collections.singletonList("select"));
        maxRows.setParameters(Collections.singletonList(param("rowCount", "int", null, true)));

        ret.setConstraintDefs(Arrays.asList(requireApproval, maxRows));

        return ret;
    }

    private static RangerResourceDef resourceDef(Long itemId, String name, String parent, int level, boolean mandatory) {
        RangerResourceDef ret = new RangerResourceDef();

        ret.setItemId(itemId);
        ret.setName(name);
        ret.setType("string");
        ret.setParent(parent);
        ret.setLevel(level);
        ret.setMandatory(mandatory);
        ret.setLookupSupported(false);
        ret.setRecursiveSupported(false);
        ret.setExcludesSupported(false);
        ret.setMatcher("org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher");

        return ret;
    }

    private static RangerConstraintParamDef param(String name, String type, String subType, boolean mandatory) {
        RangerConstraintParamDef ret = new RangerConstraintParamDef();

        ret.setItemId(1L);
        ret.setName(name);
        ret.setType(type);
        ret.setSubType(subType);
        ret.setMandatory(mandatory);

        return ret;
    }
}
