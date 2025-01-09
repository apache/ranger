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
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.ServiceStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRangerServiceValidator {
    private final Action[]                       cu        = new Action[] {Action.CREATE, Action.UPDATE};
    private       ServiceStore                   store;
    private       RangerServiceValidator         validator;
    private       Action                         action;
    private final ValidationTestUtils            utils    = new ValidationTestUtils();
    private final List<ValidationFailureDetails> failures = new ArrayList<>();

    @Before
    public void before() {
        store     = mock(ServiceStore.class);
        action    = Action.CREATE; // by default we set action to create
        validator = new RangerServiceValidator(store);
    }

    @Test
    public void testIsValidServiceNameCreationWithOutSpecialCharacters() throws Exception {
        String serviceName  = "c1_yarn";

        RangerService rangerService = new RangerService();
        rangerService.setName(serviceName);
        rangerService.setDisplayName(serviceName);
        rangerService.setType("yarn");
        rangerService.setTagService("");

        RangerServiceConfigDef configDef = new RangerServiceConfigDef();
        configDef.setMandatory(true);

        List<RangerServiceConfigDef> listRangerServiceConfigDef = new ArrayList<>();
        listRangerServiceConfigDef.add(configDef);

        configDef.setName("myconfig1");

        Map<String, String> testMap = new HashMap<>();
        testMap.put("myconfig1", "myconfig1");

        rangerService.setConfigs(testMap);

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setConfigs(listRangerServiceConfigDef);

        when(store.getServiceDefByName("yarn")).thenReturn(rangerServiceDef);
        boolean valid = validator.isValid(rangerService, Action.CREATE, failures);
        Assert.assertEquals(0, failures.size());
        Assert.assertTrue(valid);
    }

    @Test
    public void testIsValidServiceNameUpdationWithOutSpecialCharacters() throws Exception {
        String serviceName = "c1_yarn";

        RangerService rangerService = new RangerService();
        rangerService.setId(1L);
        rangerService.setName(serviceName);
        rangerService.setDisplayName(serviceName);
        rangerService.setType("yarn");
        rangerService.setTagService("");

        RangerServiceConfigDef configDef = new RangerServiceConfigDef();
        configDef.setMandatory(true);

        List<RangerServiceConfigDef> listRangerServiceConfigDef = new ArrayList<>();
        listRangerServiceConfigDef.add(configDef);

        configDef.setName("myconfig1");

        Map<String, String> testMap = new HashMap<>();
        testMap.put("myconfig1", "myconfig1");

        rangerService.setConfigs(testMap);

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setConfigs(listRangerServiceConfigDef);

        when(store.getService(1L)).thenReturn(rangerService);
        when(store.getServiceDefByName("yarn")).thenReturn(rangerServiceDef);
        boolean valid = validator.isValid(rangerService, Action.UPDATE, failures);
        Assert.assertEquals(0, failures.size());
        Assert.assertTrue(valid);
    }

    @Test
    public void testIsValidServiceNameUpdationWithSpecialCharacters() throws Exception {
        String serviceName = "<alert>c1_yarn</alert>";

        ValidationErrorCode vErrCod      = ValidationErrorCode.SERVICE_VALIDATION_ERR_SPECIAL_CHARACTERS_SERVICE_NAME;
        String              errorMessage = vErrCod.getMessage(serviceName);
        int                 errorCode    = vErrCod.getErrorCode();

        RangerService rangerService = new RangerService();
        rangerService.setId(1L);
        rangerService.setName(serviceName);
        rangerService.setType("yarn");
        rangerService.setTagService("");

        RangerServiceConfigDef configDef = new RangerServiceConfigDef();
        configDef.setMandatory(true);

        List<RangerServiceConfigDef> listRangerServiceConfigDef = new ArrayList<>();
        listRangerServiceConfigDef.add(configDef);

        configDef.setName("myconfig1");

        Map<String, String> testMap = new HashMap<>();
        testMap.put("myconfig1", "myconfig1");

        rangerService.setConfigs(testMap);

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setConfigs(listRangerServiceConfigDef);

        when(store.getService(1L)).thenReturn(rangerService);
        when(store.getServiceDefByName("yarn")).thenReturn(rangerServiceDef);
        boolean                  valid          = validator.isValid(rangerService, Action.UPDATE, failures);
        ValidationFailureDetails failureMessage = failures.get(0);
        Assert.assertFalse(valid);
        Assert.assertEquals("name", failureMessage.getFieldName());
        Assert.assertEquals(errorMessage, failureMessage.reason);
        Assert.assertEquals(errorCode, failureMessage.errorCode);
    }

    @Test
    public void testIsValidServiceNameCreationWithSpecialCharacters() throws Exception {
        String serviceName = "<script>c1_yarn</script>";

        ValidationErrorCode vErrCod      = ValidationErrorCode.SERVICE_VALIDATION_ERR_SPECIAL_CHARACTERS_SERVICE_NAME;
        String              errorMessage = vErrCod.getMessage(serviceName);
        int                 errorCode    = vErrCod.getErrorCode();

        RangerService rangerService = new RangerService();
        rangerService.setName(serviceName);
        rangerService.setType("yarn");
        rangerService.setTagService("");

        RangerServiceConfigDef configDef = new RangerServiceConfigDef();
        configDef.setMandatory(true);

        List<RangerServiceConfigDef> listRangerServiceConfigDef = new ArrayList<>();
        listRangerServiceConfigDef.add(configDef);

        configDef.setName("myconfig1");

        Map<String, String> testMap = new HashMap<>();
        testMap.put("myconfig1", "myconfig1");

        rangerService.setConfigs(testMap);

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setConfigs(listRangerServiceConfigDef);

        when(store.getServiceDefByName("yarn")).thenReturn(rangerServiceDef);
        boolean                  valid          = validator.isValid(rangerService, action, failures);
        ValidationFailureDetails failureMessage = failures.get(0);
        Assert.assertFalse(valid);
        Assert.assertEquals("name", failureMessage.getFieldName());
        Assert.assertEquals(errorMessage, failureMessage.reason);
        Assert.assertEquals(errorCode, failureMessage.errorCode);
    }

    @Test
    public void testIsValidServiceNameCreationWithSpaceCharacter() throws Exception {
        String serviceName = "Cluster 1_c1_yarn";

        ValidationErrorCode vErrCod      = ValidationErrorCode.SERVICE_VALIDATION_ERR_SPECIAL_CHARACTERS_SERVICE_NAME;
        String              errorMessage = vErrCod.getMessage(serviceName);
        int                 errorCode    = vErrCod.getErrorCode();

        RangerService rangerService = new RangerService();
        rangerService.setName(serviceName);
        rangerService.setDisplayName(serviceName);
        rangerService.setType("yarn");
        rangerService.setTagService("");

        RangerServiceConfigDef configDef = new RangerServiceConfigDef();
        configDef.setMandatory(true);

        List<RangerServiceConfigDef> listRangerServiceConfigDef = new ArrayList<>();
        listRangerServiceConfigDef.add(configDef);

        configDef.setName("myconfig1");

        Map<String, String> testMap = new HashMap<>();
        testMap.put("myconfig1", "myconfig1");

        rangerService.setConfigs(testMap);

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setConfigs(listRangerServiceConfigDef);

        when(store.getServiceDefByName("yarn")).thenReturn(rangerServiceDef);
        boolean                  valid          = validator.isValid(rangerService, action, failures);
        ValidationFailureDetails failureMessage = failures.get(0);
        Assert.assertFalse(valid);
        Assert.assertEquals("name", failureMessage.getFieldName());
        Assert.assertEquals(errorMessage, failureMessage.reason);
        Assert.assertEquals(errorCode, failureMessage.errorCode);
    }

    @Test
    public void testIsValidServiceNameUpdationWithSpaceCharacter() throws Exception {
        String serviceName        = "Cluster 1_c1_yarn";

        ValidationErrorCode vErrCod      = ValidationErrorCode.SERVICE_VALIDATION_ERR_SPECIAL_CHARACTERS_SERVICE_NAME;
        String              errorMessage = vErrCod.getMessage(serviceName);
        int                 errorCode    = vErrCod.getErrorCode();

        RangerService rangerService = new RangerService();
        rangerService.setId(1L);
        rangerService.setName(serviceName);
        rangerService.setDisplayName(serviceName);
        rangerService.setType("yarn");
        rangerService.setTagService("");

        RangerServiceConfigDef configDef = new RangerServiceConfigDef();
        configDef.setMandatory(true);

        List<RangerServiceConfigDef> listRangerServiceConfigDef = new ArrayList<>();
        listRangerServiceConfigDef.add(configDef);

        configDef.setName("myconfig1");

        Map<String, String> testMap = new HashMap<>();
        testMap.put("myconfig1", "myconfig1");

        rangerService.setConfigs(testMap);

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setConfigs(listRangerServiceConfigDef);

        String        serviceNameWithoutSpace   = "Cluster_1_c1_yarn";
        RangerService rangerServiceWithoutSpace = new RangerService();

        rangerServiceWithoutSpace.setId(1L);
        rangerServiceWithoutSpace.setName(serviceNameWithoutSpace);
        rangerServiceWithoutSpace.setDisplayName(serviceNameWithoutSpace);
        rangerServiceWithoutSpace.setType("yarn");
        rangerServiceWithoutSpace.setTagService("");

        //Case: previous service name does not have space, updating with name containing space
        when(store.getService(1L)).thenReturn(rangerServiceWithoutSpace);
        when(store.getServiceDefByName("yarn")).thenReturn(rangerServiceDef);
        boolean                  valid          = validator.isValid(rangerService, Action.UPDATE, failures);
        ValidationFailureDetails failureMessage = failures.get(0);
        Assert.assertFalse(valid);
        Assert.assertEquals("name", failureMessage.getFieldName());
        Assert.assertEquals(errorMessage, failureMessage.reason);
        Assert.assertEquals(errorCode, failureMessage.errorCode);

        //Case: previous service name does have space, updating with name containing space
        when(store.getService(1L)).thenReturn(rangerService);
        when(store.getServiceDefByName("yarn")).thenReturn(rangerServiceDef);
        boolean validWithSpace = validator.isValid(rangerService, Action.UPDATE, failures);
        Assert.assertTrue(validWithSpace);
    }

    @Test
    public void testIsValidServiceNameCreationWithGreater255Characters() throws Exception {
        String serviceName = "c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1";

        ValidationErrorCode vErrCod      = ValidationErrorCode.SERVICE_VALIDATION_ERR_SPECIAL_CHARACTERS_SERVICE_NAME;
        String              errorMessage = vErrCod.getMessage(serviceName);
        int                 errorCode    = vErrCod.getErrorCode();

        RangerService rangerService = new RangerService();
        rangerService.setName(serviceName);
        rangerService.setType("yarn");
        rangerService.setTagService("");

        RangerServiceConfigDef configDef = new RangerServiceConfigDef();
        configDef.setMandatory(true);

        List<RangerServiceConfigDef> listRangerServiceConfigDef = new ArrayList<>();
        listRangerServiceConfigDef.add(configDef);

        configDef.setName("myconfig1");

        Map<String, String> testMap = new HashMap<>();
        testMap.put("myconfig1", "myconfig1");

        rangerService.setConfigs(testMap);

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setConfigs(listRangerServiceConfigDef);

        when(store.getServiceDefByName("yarn")).thenReturn(rangerServiceDef);
        boolean                  valid          = validator.isValid(rangerService, action, failures);
        ValidationFailureDetails failureMessage = failures.get(0);
        Assert.assertFalse(valid);
        Assert.assertEquals("name", failureMessage.getFieldName());
        Assert.assertEquals(errorMessage, failureMessage.reason);
        Assert.assertEquals(errorCode, failureMessage.errorCode);
    }

    @Test
    public void testIsValidServiceNameUpdationWithGreater255Characters() throws Exception {
        String serviceName = "c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1_yarn_c1";

        ValidationErrorCode vErrCod      = ValidationErrorCode.SERVICE_VALIDATION_ERR_SPECIAL_CHARACTERS_SERVICE_NAME;
        String              errorMessage = vErrCod.getMessage(serviceName);
        int                 errorCode    = vErrCod.getErrorCode();

        RangerService rangerService = new RangerService();
        rangerService.setId(1L);
        rangerService.setName(serviceName);
        rangerService.setType("yarn");
        rangerService.setTagService("");

        RangerServiceConfigDef configDef = new RangerServiceConfigDef();
        configDef.setMandatory(true);

        List<RangerServiceConfigDef> listRangerServiceConfigDef = new ArrayList<>();
        listRangerServiceConfigDef.add(configDef);

        configDef.setName("myconfig1");

        Map<String, String> testMap = new HashMap<>();
        testMap.put("myconfig1", "myconfig1");

        rangerService.setConfigs(testMap);

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setConfigs(listRangerServiceConfigDef);

        when(store.getService(1L)).thenReturn(rangerService);
        when(store.getServiceDefByName("yarn")).thenReturn(rangerServiceDef);
        boolean                  valid          = validator.isValid(rangerService, Action.UPDATE, failures);
        ValidationFailureDetails failureMessage = failures.get(0);
        Assert.assertFalse(valid);
        Assert.assertEquals("name", failureMessage.getFieldName());
        Assert.assertEquals(errorMessage, failureMessage.reason);
        Assert.assertEquals(errorCode, failureMessage.errorCode);
    }

    @Test
    public void testIsValid_failures() throws Exception {
        RangerService service = mock(RangerService.class);
        // passing in a null service to the check itself is an error
        Assert.assertFalse(validator.isValid((RangerService) null, action, failures));
        utils.checkFailureForMissingValue(failures, "service");

        // id is required for update
        when(service.getId()).thenReturn(null);
        // let's verify the failure and the sort of error information that is returned (for one of these)
        // Assert.assert that among the failure reason is one about id being missing.
        checkFailure_isValid(validator, service, Action.UPDATE, failures, "missing", "id");
        when(service.getId()).thenReturn(7L);

        for (Action action : cu) {
            // null, empty of blank name renders a service invalid
            for (String name : new String[] {null, "", " \t"}) { // spaces and tabs
                when(service.getName()).thenReturn(name);
                checkFailure_isValid(validator, service, action, failures, "missing", "name");
            }
            // same is true for the type
            for (String type : new String[] {null, "", "    "}) {
                when(service.getType()).thenReturn(type);
                checkFailure_isValid(validator, service, action, failures, "missing", "type");
            }
        }
        when(service.getName()).thenReturn("aName");

        // if non-empty, then the type should exist!
        when(store.getServiceDefByName("null-type")).thenReturn(null);
        when(store.getServiceDefByName("throwing-type")).thenThrow(new Exception());
        for (Action action : cu) {
            for (String type : new String[] {"null-type", "throwing-type"}) {
                when(service.getType()).thenReturn(type);
                checkFailure_isValid(validator, service, action, failures, "semantic", "type");
            }
        }
        when(service.getType()).thenReturn("aType");
        RangerServiceDef serviceDef = mock(RangerServiceDef.class);
        when(store.getServiceDefByName("aType")).thenReturn(serviceDef);

        // Create: No service should exist matching its id and/or name
        RangerService anExistingService = mock(RangerService.class);
        when(store.getServiceByName("aName")).thenReturn(anExistingService);
        checkFailure_isValid(validator, service, Action.CREATE, failures, "semantic", "name");

        // Update: service should exist matching its id and name specified should not belong to a different service
        when(store.getService(7L)).thenReturn(null);
        when(store.getServiceByName("aName")).thenReturn(anExistingService);
        checkFailure_isValid(validator, service, Action.UPDATE, failures, "semantic", "id");

        when(store.getService(7L)).thenReturn(anExistingService);
        RangerService anotherExistingService = mock(RangerService.class);
        when(anotherExistingService.getId()).thenReturn(49L);
        when(store.getServiceByName("aName")).thenReturn(anotherExistingService);
        checkFailure_isValid(validator, service, Action.UPDATE, failures, "semantic", "id/name");
    }

    @Test
    public void test_isValid_missingRequiredParameter() throws Exception {
        // Create/Update: simulate a condition where required parameters are missing
        Object[][] input = new Object[][] {
                {"param1", true},
                {"param2", true},
                {"param3", false},
                {"param4", false},
        };
        List<RangerServiceConfigDef> configDefs = utils.createServiceConditionDefs(input);
        RangerServiceDef             serviceDef = mock(RangerServiceDef.class);
        when(serviceDef.getConfigs()).thenReturn(configDefs);
        // wire this service def into store
        when(store.getServiceDefByName("aType")).thenReturn(serviceDef);
        // create a service with some require parameters missing
        RangerService service = mock(RangerService.class);
        when(service.getType()).thenReturn("aType");
        when(service.getName()).thenReturn("aName");
        // required parameters param2 is missing
        String[]            params   = new String[] {"param1", "param3", "param4", "param5"};
        Map<String, String> paramMap = utils.createMap(params);
        when(service.getConfigs()).thenReturn(paramMap);
        // service does not exist in the store
        when(store.getServiceByName("aService")).thenReturn(null);
        for (Action action : cu) {
            // it should be invalid
            checkFailure_isValid(validator, service, action, failures, "missing", "configuration", "param2");
        }
    }

    @Test
    public void test_isValid_happyPath() throws Exception {
        // create a service def with some required parameters
        Object[][] serviceDefInput = new Object[][] {
                {"param1", true},
                {"param2", true},
                {"param3", false},
                {"param4", false},
                {"param5", true},
        };
        List<RangerServiceConfigDef> configDefs = utils.createServiceConditionDefs(serviceDefInput);
        RangerServiceDef             serviceDef = mock(RangerServiceDef.class);
        when(serviceDef.getConfigs()).thenReturn(configDefs);
        // create a service with some parameters on it
        RangerService service = mock(RangerService.class);
        when(service.getName()).thenReturn("aName");
        when(service.getDisplayName()).thenReturn("aDisplayName");
        when(service.getType()).thenReturn("aType");
        // contains an extra parameter (param6) and one optional is missing(param4)
        String[]            configs   = new String[] {"param1", "param2", "param3", "param5", "param6"};
        Map<String, String> configMap = utils.createMap(configs);
        when(service.getConfigs()).thenReturn(configMap);
        // wire then into the store
        // service does not exists
        when(store.getServiceByName("aName")).thenReturn(null);
        // service def exists
        when(store.getServiceDefByName("aType")).thenReturn(serviceDef);

        Assert.assertTrue(validator.isValid(service, Action.CREATE, failures));

        // for update to work the only additional requirement is that id is required and service should exist
        // if name is not null and it points to a service then it should match the id
        when(service.getId()).thenReturn(7L);
        RangerService existingService = mock(RangerService.class);
        when(existingService.getId()).thenReturn(7L);
        when(store.getService(7L)).thenReturn(existingService);
        when(store.getServiceByName("aName")).thenReturn(existingService);
        Assert.assertTrue(validator.isValid(service, Action.UPDATE, failures));
        // name need not point to a service for update to work, of course.
        when(store.getServiceByName("aName")).thenReturn(null);
        Assert.assertTrue(validator.isValid(service, Action.UPDATE, failures));
    }

    @Test
    public void test_isValid_withId_errorConditions() throws Exception {
        // api that takes in long is only supported for delete currently
        Assert.assertFalse(validator.isValid(1L, Action.CREATE, failures));
        utils.checkFailureForInternalError(failures);
        // passing in a null id is a failure!
        validator = new RangerServiceValidator(store);
        failures.clear();
        Assert.assertFalse(validator.isValid((Long) null, Action.DELETE, failures));
        utils.checkFailureForMissingValue(failures, "id");
        // if service with that id does not exist then that, is ok because delete is idempotent
        when(store.getService(1L)).thenReturn(null);
        when(store.getService(2L)).thenThrow(new Exception());
        failures.clear();
        Assert.assertTrue(validator.isValid(1L, Action.DELETE, failures));
        Assert.assertTrue(failures.isEmpty());

        failures.clear();
        Assert.assertTrue(validator.isValid(2L, Action.DELETE, failures));
        Assert.assertTrue(failures.isEmpty());
    }

    @Test
    public void test_isValid_withId_happyPath() throws Exception {
        validator = new RangerServiceValidator(store);
        RangerService service = mock(RangerService.class);
        when(store.getService(1L)).thenReturn(service);
        Assert.assertTrue(validator.isValid(1L, Action.DELETE, failures));
    }

    void checkFailure_isValid(RangerServiceValidator validator, RangerService service, Action action, List<ValidationFailureDetails> failures, String errorType, String field) {
        checkFailure_isValid(validator, service, action, failures, errorType, field, null);
    }

    void checkFailure_isValid(RangerServiceValidator validator, RangerService service, Action action, List<ValidationFailureDetails> failures, String errorType, String field, String subField) {
        failures.clear();
        Assert.assertFalse(validator.isValid(service, action, failures));
        switch (errorType) {
            case "missing":
                utils.checkFailureForMissingValue(failures, field, subField);
                break;
            case "semantic":
                utils.checkFailureForSemanticError(failures, field, subField);
                break;
            case "internal error":
                utils.checkFailureForInternalError(failures);
                break;
            default:
                Assert.fail("Unsupported errorType[" + errorType + "]");
                break;
        }
    }
}
