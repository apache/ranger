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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.utils.TestStringUtil;
import org.apache.ranger.plugin.errors.ValidationErrorCode;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerSecurityZone.RangerSecurityZoneService;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.SecurityZoneStore;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RangerSecurityZoneValidatorTest {
    ServiceStore      store             = mock(ServiceStore.class);
    SecurityZoneStore securityZoneStore = mock(SecurityZoneStore.class);

    @InjectMocks
    RangerSecurityZoneValidator rangerSecurityZoneValidator = new RangerSecurityZoneValidator(store, securityZoneStore);

    @Test
    public void testValidateSecurityZoneForCreate() throws Exception {
        SearchFilter             filter                 = getSerachFilter();
        List<RangerSecurityZone> rangerSecurityZoneList = new ArrayList<>();
        RangerService            rangerSvc              = getRangerService();
        RangerServiceDef         rangerSvcDef           = rangerServiceDef();
        RangerSecurityZone       suppliedSecurityZone   = getRangerSecurityZone();

        rangerSecurityZoneList.add(suppliedSecurityZone);

        Mockito.when(store.getSecurityZone("MyZone")).thenReturn(null);
        Mockito.when(store.getServiceByName("hdfsSvc")).thenReturn(rangerSvc);
        Mockito.when(store.getServiceDefByName("1")).thenReturn(rangerSvcDef);
        Mockito.when(securityZoneStore.getSecurityZones(filter)).thenReturn(rangerSecurityZoneList);

        rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.CREATE);
        Mockito.verify(store).getSecurityZone("MyZone");
    }

    @Test
    public void testValidateSecurityZoneForUpdate() throws Exception {
        SearchFilter             filter                 = getSerachFilter();
        List<RangerSecurityZone> rangerSecurityZoneList = new ArrayList<>();
        RangerService            rangerSvc              = getRangerService();
        RangerServiceDef         rangerSvcDef           = rangerServiceDef();
        RangerSecurityZone       suppliedSecurityZone   = getRangerSecurityZone();
        rangerSecurityZoneList.add(suppliedSecurityZone);

        Mockito.when(store.getSecurityZone(1L)).thenReturn(suppliedSecurityZone);
        Mockito.when(store.getServiceByName("hdfsSvc")).thenReturn(rangerSvc);
        Mockito.when(store.getServiceDefByName("1")).thenReturn(rangerSvcDef);
        Mockito.when(securityZoneStore.getSecurityZones(filter)).thenReturn(rangerSecurityZoneList);

        rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.UPDATE);
        Mockito.verify(store, Mockito.atLeastOnce()).getSecurityZone(1L);
    }

    @Test
    public void testValidateSecurityZoneForDelete() throws Exception {
        List<ValidationFailureDetails> failures             = new ArrayList<>();
        RangerSecurityZone             suppliedSecurityZone = getRangerSecurityZone();

        Mockito.when(store.getSecurityZone(1L)).thenReturn(suppliedSecurityZone);

        rangerSecurityZoneValidator.isValid(1L, RangerValidator.Action.DELETE, failures);
        Mockito.verify(store).getSecurityZone(1L);
    }

    @Test
    public void testValidateSecurityZoneForDeleteThrowsError() throws Exception {
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();
        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.DELETE);
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals(ex.getMessage(), "isValid(RangerSecurityZone, ...) is only supported for create/update");
        }
    }

    @Test
    public void testValidateSecurityZoneWithoutNameForCreate() {
        RangerSecurityZone suppliedSecurityZone = new RangerSecurityZone();
        suppliedSecurityZone.setName(null);
        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.CREATE);
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "(0) Validation failure: error code[3035], reason[Internal error: missing field[name]], field[name], subfield[null], type[missing] ");
        }
    }

    @Test
    public void testValidateSecurityZoneForCreateWithExistingNameThrowsError() throws Exception {
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();
        RangerSecurityZone existingSecurityZone = getRangerSecurityZone();

        Mockito.when(store.getSecurityZone("MyZone")).thenReturn(existingSecurityZone);
        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.CREATE);
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "(0) Validation failure: error code[3036], reason[Another security zone already exists for this name: zone-id=[1]]], field[name], subfield[null], type[] ");
        }
    }

    @Test
    public void testValidateSecurityZoneNotExistForUpdateThrowsError() throws Exception {
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();
        Mockito.when(store.getSecurityZone(1L)).thenReturn(null);
        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.UPDATE);
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "(0) Validation failure: error code[3037], reason[No security zone found for [1]], field[id], subfield[null], type[] ");
        }
    }

    @Test
    public void testValidateSecurityZoneWitoutServicesAdminUserAdminUserGroupAuditUserAuditUserGroupForCreateThrowsError() throws Exception {
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();
        suppliedSecurityZone.setAdminUserGroups(null);
        suppliedSecurityZone.setAdminUsers(null);
        suppliedSecurityZone.setAuditUserGroups(null);
        suppliedSecurityZone.setAuditUsers(null);
        suppliedSecurityZone.setServices(null);

        Mockito.when(store.getSecurityZone("MyZone")).thenReturn(null);
        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.CREATE);
        } catch (Exception ex) {
            String              failureMessage   = ex.getMessage();
            ValidationErrorCode expectedError    = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_MISSING_USER_AND_GROUPS_AND_ROLES;
            boolean             hasExpectedError = StringUtils.contains(failureMessage, expectedError.getErrorCode() + "");

            Assert.assertTrue("validation failure message didn't include expected error code " + expectedError.getErrorCode() + ". Failure message: " + failureMessage, hasExpectedError);
        }
    }

    @Test
    public void testValidateSecurityZoneWitoutResourcesForCreateThrowsError() throws Exception {
        RangerSecurityZoneService rangerSecurityZoneService = new RangerSecurityZoneService();
        RangerService             rangerSvc                 = getRangerService();
        RangerServiceDef          rangerSvcDef              = rangerServiceDef();
        Mockito.when(store.getServiceDefByName("1")).thenReturn(rangerSvcDef);

        Map<String, RangerSecurityZone.RangerSecurityZoneService> map = new HashMap<>();
        map.put("hdfsSvc", rangerSecurityZoneService);
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();
        suppliedSecurityZone.setServices(map);

        Mockito.when(store.getSecurityZone("MyZone")).thenReturn(null);
        Mockito.when(store.getServiceByName("hdfsSvc")).thenReturn(rangerSvc);

        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.CREATE);
        } catch (Exception ex) {
            Assert.assertEquals(
                    ex.getMessage(),
                    "(0) Validation failure: error code[3039], reason[No resources specified for service [hdfsSvc]], field[security zone resources], subfield[null], type[missing] ");
        }
    }

    @Test
    public void testValidateSecurityZoneWitoutRangerServiceForCreateThrowsError() throws Exception {
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();

        Mockito.when(store.getSecurityZone("MyZone")).thenReturn(null);
        Mockito.when(store.getServiceByName("hdfsSvc")).thenReturn(null);

        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.CREATE);
        } catch (Exception ex) {
            Assert.assertEquals(
                    ex.getMessage(),
                    "(0) Validation failure: error code[3040], reason[Invalid service [hdfsSvc]], field[security zone resource service-name], subfield[null], type[] ");
        }
    }

    @Test
    public void testValidateSecurityZoneWitoutRangerServiceDefForCreateThrowsError() throws Exception {
        RangerService      rangerSvc            = getRangerService();
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();

        Mockito.when(store.getSecurityZone("MyZone")).thenReturn(null);
        Mockito.when(store.getServiceByName("hdfsSvc")).thenReturn(rangerSvc);
        Mockito.when(store.getServiceDefByName("1")).thenReturn(null);

        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.CREATE);
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(),
                    "(0) Validation failure: error code[3041], reason[Invalid service-type [1]], field[security zone resource service-type], subfield[null], type[] ");
        }
    }

    @Test
    public void testValidateSecurityZoneWitoutRangerServiceDefResourceForCreateThrowsError() throws Exception {
        RangerService    rangerSvc    = getRangerService();
        RangerServiceDef rangerSvcDef = rangerServiceDef();
        rangerSvcDef.setResources(null);
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();

        Mockito.when(store.getSecurityZone("MyZone")).thenReturn(null);
        Mockito.when(store.getServiceByName("hdfsSvc")).thenReturn(rangerSvc);
        Mockito.when(store.getServiceDefByName("1")).thenReturn(rangerSvcDef);

        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone, RangerValidator.Action.CREATE);
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(),
                    "(0) Validation failure: error code[3042], reason[Invalid resource hierarchy specified for service:[hdfsSvc], resource-hierarchy:[[hdfs]]], field[security zone resource hierarchy], subfield[null], type[] ");
        }
    }

    @Test
    public void testValidateWhileFetchingSecurityZoneForCreateThrowsError() throws Exception {
        SearchFilter  filter    = getSerachFilter();
        RangerService rangerSvc = getRangerService();

        RangerServiceDef   rangerSvcDef         = rangerServiceDef();
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();

        Mockito.when(store.getSecurityZone("MyZone")).thenReturn(null);
        Mockito.when(store.getServiceByName("hdfsSvc")).thenReturn(rangerSvc);
        Mockito.when(store.getServiceDefByName("1")).thenReturn(rangerSvcDef);
        Mockito.when(securityZoneStore.getSecurityZones(filter)).thenThrow(new NullPointerException());
        try {
            rangerSecurityZoneValidator.validate(suppliedSecurityZone,
                    RangerValidator.Action.CREATE);
        } catch (Exception ex) {
            Assert.assertEquals(
                    ex.getMessage(),
                    "(0) Validation failure: error code[3045], reason[Internal Error:[null]], field[null], subfield[null], type[] ");
        }
    }

    @Test
    public void testIsValidSecurityZoneForDeleteWithWrongActionTypeReturnFalse() {
        RangerSecurityZone             suppliedSecurityZone = getRangerSecurityZone();
        List<ValidationFailureDetails> failures             = new ArrayList<>();
        boolean isValid = rangerSecurityZoneValidator.isValid(suppliedSecurityZone.getName(),
                RangerValidator.Action.UPDATE, failures);

        Assert.assertFalse(isValid);
    }

    @Test
    public void testIsValidSecurityZoneForDeleteWithoutNameReturnFalse() {
        RangerSecurityZone suppliedSecurityZone = new RangerSecurityZone();
        suppliedSecurityZone.setName(null);
        List<ValidationFailureDetails> failures = new ArrayList<>();
        boolean isValid = rangerSecurityZoneValidator.isValid(suppliedSecurityZone.getName(),
                RangerValidator.Action.DELETE, failures);

        Assert.assertFalse(isValid);
    }

    @Test
    public void testIsValidSecurityZoneForDeleteWithWrongNameReturnFalse() throws Exception {
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();
        Mockito.when(store.getSecurityZone(suppliedSecurityZone.getName())).thenReturn(null);
        List<ValidationFailureDetails> failures = new ArrayList<>();
        boolean isValid = rangerSecurityZoneValidator.isValid(suppliedSecurityZone.getName(),
                RangerValidator.Action.DELETE, failures);
        Assert.assertFalse(isValid);
    }

    @Test
    public void testIsValidSecurityZoneIdForDeleteWithWrongActionTypeReturnFalse() {
        RangerSecurityZone             suppliedSecurityZone = getRangerSecurityZone();
        List<ValidationFailureDetails> failures             = new ArrayList<>();
        boolean isValid = rangerSecurityZoneValidator.isValid(suppliedSecurityZone.getId(),
                RangerValidator.Action.UPDATE, failures);
        Assert.assertFalse(isValid);
    }

    @Test
    public void testIsValidSecurityZoneForDeleteWithWrongIdReturnFalse() throws Exception {
        RangerSecurityZone suppliedSecurityZone = getRangerSecurityZone();
        Mockito.when(store.getSecurityZone(suppliedSecurityZone.getId())).thenReturn(null);
        List<ValidationFailureDetails> failures = new ArrayList<>();
        boolean isValid = rangerSecurityZoneValidator.isValid(suppliedSecurityZone.getId(),
                RangerValidator.Action.DELETE, failures);

        Assert.assertFalse(isValid);
    }

    @Test
    public void testValidatePathResourceInMultipleSecurityZones() throws Exception {
        List<HashMap<String, List<String>>> zone1Resources = new ArrayList<>();
        List<HashMap<String, List<String>>> zone2Resources = new ArrayList<>();

        zone1Resources.add(TestStringUtil.mapFromStringStringList("hdfs", Collections.singletonList("/zone1")));
        zone2Resources.add(TestStringUtil.mapFromStringStringList("hdfs", Collections.singletonList("/zone1/a")));

        RangerServiceDef          svcDef       = rangerServiceDef();
        RangerService             svc          = getRangerService();
        RangerSecurityZoneService zone1HdfsSvc = new RangerSecurityZoneService(zone1Resources);
        RangerSecurityZoneService zone2HdfsSvc = new RangerSecurityZoneService(zone2Resources);

        RangerSecurityZone zone1 = new RangerSecurityZone("zone1", Collections.singletonMap(svc.getName(), zone1HdfsSvc), null, Collections.singletonList("admin"), null, Collections.singletonList("auditor"), null, "Zone 1");
        RangerSecurityZone zone2 = new RangerSecurityZone("zone2", Collections.singletonMap(svc.getName(), zone2HdfsSvc), null, Collections.singletonList("admin"), null, Collections.singletonList("auditor"), null, "Zone 1");

        zone1.setId(1L);
        zone2.setId(2L);

        List<RangerSecurityZone> zones = new ArrayList<>(Collections.singletonList(zone1));

        Mockito.when(store.getServiceByName(svc.getName())).thenReturn(svc);
        Mockito.when(store.getServiceDefByName(svc.getType())).thenReturn(svcDef);
        Mockito.when(store.getSecurityZone(2L)).thenReturn(zone2);
        Mockito.when(securityZoneStore.getSecurityZones(Mockito.any())).thenReturn(zones);

        try {
            rangerSecurityZoneValidator.validate(zone2, RangerValidator.Action.UPDATE);

            Assert.fail("security-zone update should have failed in validation");
        } catch (Exception excp) {
            String              failureMessage   = excp.getMessage();
            ValidationErrorCode expectedError    = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_ZONE_RESOURCE_CONFLICT;
            boolean             hasExpectedError = StringUtils.contains(failureMessage, expectedError.getErrorCode() + "");

            Assert.assertTrue("validation failure message didn't include expected error code " + expectedError.getErrorCode() + ". Failure message: " + failureMessage, hasExpectedError);
        }
    }

    @Test
    public void testValidateHiveResourceInMultipleSecurityZones() throws Exception {
        List<HashMap<String, List<String>>> zone1Resources = new ArrayList<>();
        List<HashMap<String, List<String>>> zone2Resources = new ArrayList<>();

        zone1Resources.add(TestStringUtil.mapFromStringStringList("database", Collections.singletonList("db1")));
        zone2Resources.add(TestStringUtil.mapFromStringStringList("database", Collections.singletonList("db1"), "table", Collections.singletonList("tbl1")));

        RangerServiceDef          svcDef       = getHiveServiceDef();
        RangerService             svc          = getHiveService();
        RangerSecurityZoneService zone1HiveSvc = new RangerSecurityZoneService(zone1Resources);
        RangerSecurityZoneService zone2HiveSvc = new RangerSecurityZoneService(zone2Resources);

        RangerSecurityZone zone1 = new RangerSecurityZone("zone1", Collections.singletonMap(svc.getName(), zone1HiveSvc), null, Collections.singletonList("admin"), null, Collections.singletonList("auditor"), null, "Zone 1");
        RangerSecurityZone zone2 = new RangerSecurityZone("zone2", Collections.singletonMap(svc.getName(), zone2HiveSvc), null, Collections.singletonList("admin"), null, Collections.singletonList("auditor"), null, "Zone 1");

        zone1.setId(1L);
        zone2.setId(2L);

        List<RangerSecurityZone> zones = new ArrayList<>(Collections.singletonList(zone1));

        Mockito.when(store.getServiceByName(svc.getName())).thenReturn(svc);
        Mockito.when(store.getServiceDefByName(svc.getType())).thenReturn(svcDef);
        Mockito.when(store.getSecurityZone(2L)).thenReturn(zone2);
        Mockito.when(securityZoneStore.getSecurityZones(Mockito.any())).thenReturn(zones);

        try {
            rangerSecurityZoneValidator.validate(zone2, RangerValidator.Action.UPDATE);

            Assert.fail("security-zone update should have failed in validation");
        } catch (Exception excp) {
            String  failureMessage           = excp.getMessage();
            boolean hasResourceConflictError = StringUtils.contains(failureMessage, ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_ZONE_RESOURCE_CONFLICT.getErrorCode() + "");

            Assert.assertTrue("validation failure message didn't include expected error code " + ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_ZONE_RESOURCE_CONFLICT.getErrorCode() + ". Failure message: " + excp.getMessage(), hasResourceConflictError);
        }
    }

    @Test
    public void test2ValidateHiveResourceInMultipleSecurityZones() throws Exception {
        List<HashMap<String, List<String>>> zone1Resources = new ArrayList<>();
        List<HashMap<String, List<String>>> zone2Resources = new ArrayList<>();

        zone1Resources.add(TestStringUtil.mapFromStringStringList("database", Collections.singletonList("*")));
        zone2Resources.add(TestStringUtil.mapFromStringStringList("database", Collections.singletonList("db1"), "table", Collections.singletonList("tbl1")));

        RangerServiceDef          svcDef       = getHiveServiceDef();
        RangerService             svc          = getHiveService();
        RangerSecurityZoneService zone1HiveSvc = new RangerSecurityZoneService(zone1Resources);
        RangerSecurityZoneService zone2HiveSvc = new RangerSecurityZoneService(zone2Resources);

        RangerSecurityZone zone1 = new RangerSecurityZone("zone1", Collections.singletonMap(svc.getName(), zone1HiveSvc), null, Collections.singletonList("admin"), null, Collections.singletonList("auditor"), null, "Zone 1");
        RangerSecurityZone zone2 = new RangerSecurityZone("zone2", Collections.singletonMap(svc.getName(), zone2HiveSvc), null, Collections.singletonList("admin"), null, Collections.singletonList("auditor"), null, "Zone 1");

        zone1.setId(1L);
        zone2.setId(2L);

        List<RangerSecurityZone> zones = new ArrayList<>(Collections.singletonList(zone1));

        Mockito.when(store.getServiceByName(svc.getName())).thenReturn(svc);
        Mockito.when(store.getServiceDefByName(svc.getType())).thenReturn(svcDef);
        Mockito.when(store.getSecurityZone(2L)).thenReturn(zone2);
        Mockito.when(securityZoneStore.getSecurityZones(Mockito.any())).thenReturn(zones);

        try {
            rangerSecurityZoneValidator.validate(zone2, RangerValidator.Action.UPDATE);

            Assert.fail("security-zone update should have failed in validation");
        } catch (Exception excp) {
            String  failureMessage           = excp.getMessage();
            boolean hasResourceConflictError = StringUtils.contains(failureMessage, ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_ZONE_RESOURCE_CONFLICT.getErrorCode() + "");

            Assert.assertTrue("validation failure message didn't include expected error code " + ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_ZONE_RESOURCE_CONFLICT.getErrorCode() + ". Failure message: " + excp.getMessage(), hasResourceConflictError);
        }
    }

    @Test
    public void testValidateDuplicateResourceEntries() throws Exception {
        List<HashMap<String, List<String>>> zone1Resources = new ArrayList<>();

        zone1Resources.add(TestStringUtil.mapFromStringStringList("database", Collections.singletonList("db1"), "table", Collections.singletonList("tbl1")));
        zone1Resources.add(TestStringUtil.mapFromStringStringList("database", Collections.singletonList("db1"), "table", Collections.singletonList("tbl1")));
        RangerServiceDef          svcDef       = getHiveServiceDef();
        RangerService             svc          = getHiveService();
        RangerSecurityZoneService zone1HiveSvc = new RangerSecurityZoneService(zone1Resources);

        RangerSecurityZone zone1 = new RangerSecurityZone("zone1", Collections.singletonMap(svc.getName(), zone1HiveSvc), null, Collections.singletonList("admin"), null, Collections.singletonList("auditor"), null, "Zone 1");

        zone1.setId(1L);

        Mockito.when(store.getServiceByName(svc.getName())).thenReturn(svc);
        Mockito.when(store.getServiceDefByName(svc.getType())).thenReturn(svcDef);
        Mockito.when(store.getSecurityZone(zone1.getId())).thenReturn(zone1);

        try {
            rangerSecurityZoneValidator.validate(zone1, RangerValidator.Action.UPDATE);

            Assert.fail("security-zone update should have failed in validation");
        } catch (Exception excp) {
            String  failureMessage           = excp.getMessage();
            boolean hasResourceConflictError = StringUtils.contains(failureMessage, ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_DUPLICATE_RESOURCE_ENTRY.getErrorCode() + "");

            Assert.assertTrue("validation failure message didn't include expected error code " + ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_DUPLICATE_RESOURCE_ENTRY.getErrorCode() + ". Failure message: " + excp.getMessage(), hasResourceConflictError);
        }
    }

    private RangerService getRangerService() {
        Map<String, String> configs = new HashMap<>();

        configs.put("username", "servicemgr");
        configs.put("password", "servicemgr");
        configs.put("namenode", "servicemgr");
        configs.put("hadoop.security.authorization", "No");
        configs.put("hadoop.security.authentication", "Simple");
        configs.put("hadoop.security.auth_to_local", "");
        configs.put("dfs.datanode.kerberos.principal", "");
        configs.put("dfs.namenode.kerberos.principal", "");
        configs.put("dfs.secondary.namenode.kerberos.principal", "");
        configs.put("hadoop.rpc.protection", "Privacy");
        configs.put("commonNameForCertificate", "");

        RangerService rangerService = new RangerService();
        rangerService.setId(1L);
        rangerService.setConfigs(configs);
        rangerService.setCreateTime(new Date());
        rangerService.setDescription("service policy");
        rangerService.setGuid("1427365526516_835_0");
        rangerService.setIsEnabled(true);
        rangerService.setName("hdfsSvc");
        rangerService.setPolicyUpdateTime(new Date());
        rangerService.setType("1");
        rangerService.setUpdatedBy("Admin");
        rangerService.setUpdateTime(new Date());

        return rangerService;
    }

    private RangerServiceDef rangerServiceDef() {
        RangerResourceDef rangerResourceDef = new RangerResourceDef();
        rangerResourceDef.setName("hdfs");
        rangerResourceDef.setRecursiveSupported(true);
        rangerResourceDef.setMatcher("org.apache.ranger.plugin.resourcematcher.RangerPathResourceMatcher");

        List<RangerServiceConfigDef> configs   = new ArrayList<>();
        List<RangerResourceDef>      resources = new ArrayList<>();
        resources.add(rangerResourceDef);
        List<RangerAccessTypeDef>      accessTypes      = new ArrayList<>();
        List<RangerPolicyConditionDef> policyConditions = new ArrayList<>();
        List<RangerContextEnricherDef> contextEnrichers = new ArrayList<>();
        List<RangerEnumDef>            enums            = new ArrayList<>();

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setId(1L);
        rangerServiceDef.setImplClass("RangerServiceHdfs");
        rangerServiceDef.setName("HDFS Repository");
        rangerServiceDef.setLabel("HDFS Repository");
        rangerServiceDef.setDescription("HDFS Repository");
        rangerServiceDef.setRbKeyDescription(null);
        rangerServiceDef.setUpdatedBy("Admin");
        rangerServiceDef.setUpdateTime(new Date());
        rangerServiceDef.setConfigs(configs);
        rangerServiceDef.setResources(resources);
        rangerServiceDef.setAccessTypes(accessTypes);
        rangerServiceDef.setPolicyConditions(policyConditions);
        rangerServiceDef.setContextEnrichers(contextEnrichers);
        rangerServiceDef.setEnums(enums);

        return rangerServiceDef;
    }

    private RangerService getHiveService() {
        RangerService ret = new RangerService(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_HIVE_NAME, "hiveSvc", "Test Hive Service", null, new HashMap<>());

        ret.setId(1L);

        return ret;
    }

    private RangerServiceDef getHiveServiceDef() throws Exception {
        return EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_HIVE_NAME);
    }

    private RangerSecurityZone getRangerSecurityZone() {
        List<String> resourceList = new ArrayList<>();
        resourceList.add("/path/myfolder");

        HashMap<String, List<String>> resourcesMap = new HashMap<>();
        resourcesMap.put("hdfs", resourceList);

        List<HashMap<String, List<String>>> resources = new ArrayList<>();
        resources.add(resourcesMap);

        List<String> adminUsers = new ArrayList<>();
        adminUsers.add("adminUser1");

        List<String> adminGrpUsers = new ArrayList<>();
        adminGrpUsers.add("adminGrpUser1");

        List<String> aduitUsers = new ArrayList<>();
        aduitUsers.add("aduitUser1");

        List<String> aduitGrpUsers = new ArrayList<>();
        aduitUsers.add("aduitGrpUser1");

        RangerSecurityZoneService rangerSecurityZoneService = new RangerSecurityZoneService();
        rangerSecurityZoneService.setResources(resources);
        Map<String, RangerSecurityZone.RangerSecurityZoneService> map = new HashMap<>();
        map.put("hdfsSvc", rangerSecurityZoneService);

        RangerSecurityZone rangerSecurityZone = new RangerSecurityZone();
        rangerSecurityZone.setId(1L);
        rangerSecurityZone.setAdminUsers(adminUsers);
        rangerSecurityZone.setAuditUsers(aduitUsers);
        rangerSecurityZone.setAdminUserGroups(adminGrpUsers);
        rangerSecurityZone.setAuditUserGroups(aduitGrpUsers);
        rangerSecurityZone.setName("MyZone");
        rangerSecurityZone.setServices(map);
        rangerSecurityZone.setDescription("MyZone");

        return rangerSecurityZone;
    }

    private SearchFilter getSerachFilter() {
        SearchFilter filter = new SearchFilter();

        filter.setParam(SearchFilter.SERVICE_NAME, "hdfsSvc");
        filter.setParam(SearchFilter.NOT_ZONE_NAME, "MyZone");

        return filter;
    }
}
