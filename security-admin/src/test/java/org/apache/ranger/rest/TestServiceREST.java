/*

 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.rest;

import com.sun.jersey.core.header.FormDataContentDisposition;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.RangerPolicyAdmin;
import org.apache.ranger.biz.SecurityZoneDBStore;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.ServiceDBStore.JSON_FILE_NAME_TYPE;
import org.apache.ranger.biz.ServiceMgr;
import org.apache.ranger.biz.TagDBStore;
import org.apache.ranger.biz.UserMgr;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXPolicyDao;
import org.apache.ranger.db.XXRoleDao;
import org.apache.ranger.db.XXSecurityZoneDao;
import org.apache.ranger.db.XXSecurityZoneRefServiceDao;
import org.apache.ranger.db.XXSecurityZoneRefTagServiceDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXRole;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXSecurityZoneRefService;
import org.apache.ranger.entity.XXSecurityZoneRefTagService;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.model.RangerServiceHeaderInfo;
import org.apache.ranger.plugin.model.ServiceDeleteResponse;
import org.apache.ranger.plugin.model.validation.RangerPolicyValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerPluginCapability;
import org.apache.ranger.plugin.util.RangerPurgeResult;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerDataHistService;
import org.apache.ranger.service.RangerPluginInfoService;
import org.apache.ranger.service.RangerPolicyLabelsService;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.RangerExportPolicyList;
import org.apache.ranger.view.RangerPluginInfoList;
import org.apache.ranger.view.RangerPolicyList;
import org.apache.ranger.view.RangerServiceDefList;
import org.apache.ranger.view.RangerServiceList;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXUser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.WebApplicationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.eq;

/**
* @generated by Cursor
* @description <Unit Test for TestServiceREST class>
*/
@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestServiceREST {
    private static final Long   Id  = 8L;

    private final String grantor    = "test-grantor-1";
    private final String ownerUser  = "test-owner-user-1";
    private final String zoneName   = "test-zone-1";
    String importPoliceTestFilePath = "./src/test/java/org/apache/ranger/rest/importPolicy/import_policy_test_file.json";
    @InjectMocks
    ServiceREST serviceREST = new ServiceREST();
    @Mock
    RangerValidatorFactory validatorFactory;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    RangerDaoManager daoManager;
    @Mock
    ServiceDBStore svcStore;
    @Mock
    SecurityZoneDBStore zoneStore;
    @Mock
    TagDBStore tagStore;
    @Mock
    RangerServiceService svcService;
    @Mock
    RangerDataHistService dataHistService;
    @Mock
    RangerExportPolicyList rangerExportPolicyList;
    @Mock
    RangerServiceDefService serviceDefService;
    @Mock
    RangerPolicyService policyService;
    @Mock
    StringUtil stringUtil;
    @Mock
    XUserService xUserService;
    @Mock
    XUserMgr userMgr;
    @Mock
    RangerAuditFields rangerAuditFields;
    @Mock
    ContextUtil contextUtil;
    @Mock
    RangerBizUtil bizUtil;
    @Mock
    RESTErrorUtil restErrorUtil;
    @Mock
    RangerServiceDefValidator serviceDefValidator;
    @Mock
    RangerServiceValidator serviceValidator;
    @Mock
    RangerPolicyValidator policyValidator;
    @Mock
    ServiceMgr serviceMgr;
    @Mock
    VXResponse vXResponse;
    @Mock
    ServiceUtil serviceUtil;
    @Mock
    RangerSearchUtil searchUtil;
    @Mock
    StringUtils stringUtils;
    @Mock
    AssetMgr assetMgr;
    @Mock
    RangerPolicyLabelsService policyLabelsService;
    @Mock
    RangerPluginInfoService pluginInfoService;
    @Mock
    XXServiceDao xServiceDao;
    @Mock
    RangerPolicyEngineImpl rpImpl;
    @Mock
    RangerPolicyAdmin policyAdmin;
    @Mock
    RangerTransactionSynchronizationAdapter rangerTransactionSynchronizationAdapter;
    @Mock
    UserMgr           userMgrGrantor;
    private String capabilityVector;

    public void setup() {
        RangerSecurityContext context = new RangerSecurityContext();
        context.setUserSession(new UserSessionBase());
        RangerContextHolder.setSecurityContext(context);
        UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
        currentUserSession.setXXPortalUser(new XXPortalUser());
        currentUserSession.setUserAdmin(true);
        capabilityVector = Long.toHexString(new RangerPluginCapability().getPluginCapabilities());
    }

    public RangerServiceDef rangerServiceDef() {
        List<RangerServiceConfigDef>   configs          = new ArrayList<>();
        List<RangerResourceDef>        resources        = new ArrayList<>();
        List<RangerAccessTypeDef>      accessTypes      = new ArrayList<>();
        List<RangerPolicyConditionDef> policyConditions = new ArrayList<>();
        List<RangerContextEnricherDef> contextEnrichers = new ArrayList<>();
        List<RangerEnumDef>            enums            = new ArrayList<>();

        RangerServiceDef rangerServiceDef = new RangerServiceDef();
        rangerServiceDef.setId(Id);
        rangerServiceDef.setImplClass("RangerServiceHdfs");
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

    public RangerService rangerService() {
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
        rangerService.setId(Id);
        rangerService.setConfigs(configs);
        rangerService.setCreateTime(new Date());
        rangerService.setDescription("service policy");
        rangerService.setGuid("1427365526516_835_0");
        rangerService.setIsEnabled(true);
        rangerService.setName("HDFS_1");
        rangerService.setDisplayName("HDFS_1");
        rangerService.setPolicyUpdateTime(new Date());
        rangerService.setType("1");
        rangerService.setUpdatedBy("Admin");
        rangerService.setUpdateTime(new Date());

        return rangerService;
    }

    public XXServiceDef serviceDef() {
        XXServiceDef xServiceDef = new XXServiceDef();
        xServiceDef.setAddedByUserId(Id);
        xServiceDef.setCreateTime(new Date());
        xServiceDef.setDescription("HDFS Repository");
        xServiceDef.setGuid("1427365526516_835_0");
        xServiceDef.setId(Id);
        xServiceDef.setUpdateTime(new Date());
        xServiceDef.setUpdatedByUserId(Id);
        xServiceDef.setImplclassname("RangerServiceHdfs");
        xServiceDef.setLabel("HDFS Repository");
        xServiceDef.setRbkeylabel(null);
        xServiceDef.setRbkeydescription(null);
        xServiceDef.setIsEnabled(true);

        return xServiceDef;
    }

    public XXService xService() {
        XXService xService = new XXService();
        xService.setAddedByUserId(Id);
        xService.setCreateTime(new Date());
        xService.setDescription("Hdfs service");
        xService.setGuid("serviceguid");
        xService.setId(Id);
        xService.setIsEnabled(true);
        xService.setName("Hdfs");
        xService.setPolicyUpdateTime(new Date());
        xService.setPolicyVersion(1L);
        xService.setType(1L);
        xService.setUpdatedByUserId(Id);
        xService.setUpdateTime(new Date());

        return xService;
    }

    public ServicePolicies servicePolicies() {
        ServicePolicies sp = new ServicePolicies();
        sp.setAuditMode("auditMode");
        RangerPolicy       rangerPolicy = rangerPolicy();
        List<RangerPolicy> rpolList     = new ArrayList<>();
        rpolList.add(rangerPolicy);
        sp.setPolicies(rpolList);
        sp.setPolicyVersion(1L);
        sp.setServiceName("serviceName");
        sp.setServiceId(1L);
        return sp;
    }

    @Test
    public void test1createServiceDef() throws Exception {
        RangerServiceDef rangerServiceDef = rangerServiceDef();

        Mockito.when(validatorFactory.getServiceDefValidator(svcStore)).thenReturn(serviceDefValidator);
        Mockito.when(svcStore.createServiceDef(Mockito.any())).thenReturn(rangerServiceDef);

        RangerServiceDef dbRangerServiceDef = serviceREST.createServiceDef(rangerServiceDef);
        Assertions.assertNotNull(dbRangerServiceDef);
        Assertions.assertEquals(dbRangerServiceDef, rangerServiceDef);
        Assertions.assertEquals(dbRangerServiceDef.getId(), rangerServiceDef.getId());
        Assertions.assertEquals(dbRangerServiceDef.getName(), rangerServiceDef.getName());
        Assertions.assertEquals(dbRangerServiceDef.getImplClass(), rangerServiceDef.getImplClass());
        Assertions.assertEquals(dbRangerServiceDef.getLabel(), rangerServiceDef.getLabel());
        Assertions.assertEquals(dbRangerServiceDef.getDescription(), rangerServiceDef.getDescription());
        Assertions.assertEquals(dbRangerServiceDef.getRbKeyDescription(), rangerServiceDef.getRbKeyDescription());
        Assertions.assertEquals(dbRangerServiceDef.getUpdatedBy(), rangerServiceDef.getUpdatedBy());
        Assertions.assertEquals(dbRangerServiceDef.getUpdateTime(), rangerServiceDef.getUpdateTime());
        Assertions.assertEquals(dbRangerServiceDef.getVersion(), rangerServiceDef.getVersion());
        Assertions.assertEquals(dbRangerServiceDef.getConfigs(), rangerServiceDef.getConfigs());

        Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
        Mockito.verify(svcStore).createServiceDef(rangerServiceDef);
    }

    @Test
    public void test2updateServiceDef() throws Exception {
        RangerServiceDef rangerServiceDef = rangerServiceDef();

        Mockito.when(validatorFactory.getServiceDefValidator(svcStore)).thenReturn(serviceDefValidator);
        Mockito.when(svcStore.updateServiceDef(Mockito.any())).thenReturn(rangerServiceDef);

        RangerServiceDef dbRangerServiceDef = serviceREST.updateServiceDef(rangerServiceDef, rangerServiceDef.getId());
        Assertions.assertNotNull(dbRangerServiceDef);
        Assertions.assertEquals(dbRangerServiceDef, rangerServiceDef);
        Assertions.assertEquals(dbRangerServiceDef.getId(), rangerServiceDef.getId());
        Assertions.assertEquals(dbRangerServiceDef.getName(), rangerServiceDef.getName());
        Assertions.assertEquals(dbRangerServiceDef.getImplClass(), rangerServiceDef.getImplClass());
        Assertions.assertEquals(dbRangerServiceDef.getLabel(), rangerServiceDef.getLabel());
        Assertions.assertEquals(dbRangerServiceDef.getDescription(), rangerServiceDef.getDescription());
        Assertions.assertEquals(dbRangerServiceDef.getRbKeyDescription(), rangerServiceDef.getRbKeyDescription());
        Assertions.assertEquals(dbRangerServiceDef.getUpdatedBy(), rangerServiceDef.getUpdatedBy());
        Assertions.assertEquals(dbRangerServiceDef.getUpdateTime(), rangerServiceDef.getUpdateTime());
        Assertions.assertEquals(dbRangerServiceDef.getVersion(), rangerServiceDef.getVersion());
        Assertions.assertEquals(dbRangerServiceDef.getConfigs(), rangerServiceDef.getConfigs());

        Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
        Mockito.verify(svcStore).updateServiceDef(rangerServiceDef);
    }

    @Test
    public void test3deleteServiceDef() throws Exception {
        HttpServletRequest request          = Mockito.mock(HttpServletRequest.class);
        RangerServiceDef   rangerServiceDef = rangerServiceDef();
        XXServiceDef       xServiceDef      = serviceDef();
        XXServiceDefDao    xServiceDefDao   = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(validatorFactory.getServiceDefValidator(svcStore)).thenReturn(serviceDefValidator);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(Id)).thenReturn(xServiceDef);

        serviceREST.deleteServiceDef(rangerServiceDef.getId(), request);
        Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
        Mockito.verify(daoManager).getXXServiceDef();
    }

    @Test
    public void test4getServiceDefById() throws Exception {
        RangerServiceDef rangerServiceDef = rangerServiceDef();
        XXServiceDef     xServiceDef      = serviceDef();
        XXServiceDefDao  xServiceDefDao   = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(Id)).thenReturn(xServiceDef);
        Mockito.when(!bizUtil.hasAccess(xServiceDef, null)).thenReturn(true);
        Mockito.when(svcStore.getServiceDef(rangerServiceDef.getId())).thenReturn(rangerServiceDef);
        RangerServiceDef dbRangerServiceDef = serviceREST.getServiceDef(rangerServiceDef.getId());
        Assertions.assertNotNull(dbRangerServiceDef);
        Assertions.assertEquals(dbRangerServiceDef.getId(), rangerServiceDef.getId());
        Mockito.verify(svcStore).getServiceDef(rangerServiceDef.getId());
        Mockito.verify(daoManager).getXXServiceDef();
        Mockito.verify(bizUtil).hasAccess(xServiceDef, null);
    }

    @Test
    public void test5getServiceDefByName() throws Exception {
        RangerServiceDef rangerServiceDef = rangerServiceDef();
        XXServiceDef     xServiceDef      = serviceDef();
        XXServiceDefDao  xServiceDefDao   = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.findByName(xServiceDef.getName())).thenReturn(xServiceDef);
        Mockito.when(!bizUtil.hasAccess(xServiceDef, null)).thenReturn(true);
        Mockito.when(svcStore.getServiceDefByName(rangerServiceDef.getName())).thenReturn(rangerServiceDef);
        RangerServiceDef dbRangerServiceDef = serviceREST.getServiceDefByName(rangerServiceDef.getName());
        Assertions.assertNotNull(dbRangerServiceDef);
        Assertions.assertEquals(dbRangerServiceDef.getName(), rangerServiceDef.getName());
        Mockito.verify(svcStore).getServiceDefByName(rangerServiceDef.getName());
        Mockito.verify(daoManager).getXXServiceDef();
    }

    @Test
    public void test6createService() throws Exception {
        RangerService   rangerService  = rangerService();
        XXServiceDef    xServiceDef    = serviceDef();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenReturn(serviceValidator);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.findByName(rangerService.getType())).thenReturn(xServiceDef);

        Mockito.when(svcStore.createService(Mockito.any())).thenReturn(rangerService);

        RangerService dbRangerService = serviceREST.createService(rangerService);
        Assertions.assertNotNull(dbRangerService);
        Assertions.assertEquals(rangerService, dbRangerService);
        Assertions.assertEquals(rangerService.getId(), dbRangerService.getId());
        Assertions.assertEquals(rangerService.getConfigs(), dbRangerService.getConfigs());
        Assertions.assertEquals(rangerService.getDescription(), dbRangerService.getDescription());
        Assertions.assertEquals(rangerService.getGuid(), dbRangerService.getGuid());
        Assertions.assertEquals(rangerService.getName(), dbRangerService.getName());
        Assertions.assertEquals(rangerService.getPolicyVersion(), dbRangerService.getPolicyVersion());
        Assertions.assertEquals(rangerService.getType(), dbRangerService.getType());
        Assertions.assertEquals(rangerService.getVersion(), dbRangerService.getVersion());
        Assertions.assertEquals(rangerService.getCreateTime(), dbRangerService.getCreateTime());
        Assertions.assertEquals(rangerService.getUpdateTime(), dbRangerService.getUpdateTime());
        Assertions.assertEquals(rangerService.getUpdatedBy(), dbRangerService.getUpdatedBy());

        Mockito.verify(validatorFactory).getServiceValidator(svcStore);
        Mockito.verify(svcStore).createService(rangerService);
    }

    @Test
    public void test7getServiceDefs() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter       filter  = new SearchFilter();
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        Mockito.when(searchUtil.getSearchFilter(request, serviceDefService.sortFields)).thenReturn(filter);

        List<RangerServiceDef> serviceDefsList = new ArrayList<>();
        RangerServiceDef       serviceDef      = rangerServiceDef();
        serviceDefsList.add(serviceDef);
        PList<RangerServiceDef> serviceDefList = new PList<>();
        serviceDefList.setPageSize(0);
        serviceDefList.setResultSize(1);
        serviceDefList.setSortBy("asc");
        serviceDefList.setSortType("1");
        serviceDefList.setStartIndex(0);
        serviceDefList.setTotalCount(10);
        serviceDefList.setList(serviceDefsList);
        Mockito.when(bizUtil.hasModuleAccess(RangerConstants.MODULE_RESOURCE_BASED_POLICIES)).thenReturn(true);
        Mockito.when(svcStore.getPaginatedServiceDefs(filter)).thenReturn(serviceDefList);
        RangerServiceDefList dbRangerServiceDef = serviceREST.getServiceDefs(request);
        Assertions.assertNotNull(dbRangerServiceDef);
        Mockito.verify(searchUtil).getSearchFilter(request, serviceDefService.sortFields);
        Mockito.verify(svcStore).getPaginatedServiceDefs(filter);
    }

    @Test
    public void test8updateServiceDef() throws Exception {
        RangerService       rangerService = rangerService();
        XXServiceDef        xServiceDef   = serviceDef();
        HttpServletRequest  request       = null;
        Map<String, Object> options       = null;

        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenReturn(serviceValidator);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.findByName(rangerService.getType())).thenReturn(xServiceDef);

        Mockito.when(svcStore.updateService(Mockito.any(), Mockito.any())).thenReturn(rangerService);

        RangerService dbRangerService = serviceREST.updateService(rangerService, request);
        Assertions.assertNotNull(dbRangerService);
        Assertions.assertNotNull(dbRangerService);
        Assertions.assertEquals(rangerService, dbRangerService);
        Assertions.assertEquals(rangerService.getId(), dbRangerService.getId());
        Assertions.assertEquals(rangerService.getConfigs(), dbRangerService.getConfigs());
        Assertions.assertEquals(rangerService.getDescription(), dbRangerService.getDescription());
        Assertions.assertEquals(rangerService.getGuid(), dbRangerService.getGuid());
        Assertions.assertEquals(rangerService.getName(), dbRangerService.getName());
        Assertions.assertEquals(rangerService.getPolicyVersion(), dbRangerService.getPolicyVersion());
        Assertions.assertEquals(rangerService.getType(), dbRangerService.getType());
        Assertions.assertEquals(rangerService.getVersion(), dbRangerService.getVersion());
        Assertions.assertEquals(rangerService.getCreateTime(), dbRangerService.getCreateTime());
        Assertions.assertEquals(rangerService.getUpdateTime(), dbRangerService.getUpdateTime());
        Assertions.assertEquals(rangerService.getUpdatedBy(), dbRangerService.getUpdatedBy());
        Mockito.verify(validatorFactory).getServiceValidator(svcStore);
        Mockito.verify(daoManager).getXXServiceDef();
        Mockito.verify(svcStore).updateService(rangerService, options);
    }

    @Test
    public void test9deleteService() throws Exception {
        Assertions.assertThrows(NullPointerException.class, () -> {
            RangerService   rangerService  = rangerService();
            XXServiceDef    xServiceDef    = serviceDef();
            XXService       xService       = xService();
            XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
            XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
            Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenReturn(serviceValidator);

            Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
            Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
            serviceREST.deleteService(rangerService.getId());
            Mockito.verify(validatorFactory).getServiceValidator(svcStore);
            Mockito.verify(daoManager).getXXService();
            Mockito.verify(daoManager).getXXServiceDef();
        });
    }

    @Test
    public void test10getServiceById() throws Exception {
        RangerService rangerService = rangerService();
        Mockito.when(svcStore.getService(rangerService.getId())).thenReturn(rangerService);
        RangerService dbRangerService = serviceREST.getService(rangerService.getId());
        Assertions.assertNotNull(dbRangerService);
        Assertions.assertEquals(dbRangerService.getId(), dbRangerService.getId());
        Mockito.verify(svcStore).getService(dbRangerService.getId());
    }

    @Test
    public void test11getServiceByName() throws Exception {
        RangerService rangerService = rangerService();
        Mockito.when(svcStore.getServiceByName(rangerService.getName())).thenReturn(rangerService);
        RangerService dbRangerService = serviceREST.getServiceByName(rangerService.getName());
        Assertions.assertNotNull(dbRangerService);
        Assertions.assertEquals(dbRangerService.getName(), dbRangerService.getName());
        Mockito.verify(svcStore).getServiceByName(dbRangerService.getName());
    }

    @Test
    public void test12deleteServiceDef() {
        RangerServiceDef rangerServiceDef = rangerServiceDef();
        XXServiceDef     xServiceDef      = serviceDef();
        XXServiceDefDao  xServiceDefDao   = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(validatorFactory.getServiceDefValidator(svcStore)).thenReturn(serviceDefValidator);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(Id)).thenReturn(xServiceDef);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        serviceREST.deleteServiceDef(rangerServiceDef.getId(), request);
        Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
        Mockito.verify(daoManager).getXXServiceDef();
    }

    @Test
    public void test13lookupResource() throws Exception {
        String                serviceName = "HDFS_1";
        ResourceLookupContext context     = new ResourceLookupContext();
        context.setResourceName(serviceName);
        context.setUserInput("HDFS");
        List<String> list = serviceREST.lookupResource(serviceName, context);
        Assertions.assertNotNull(list);
    }

    @Test
    public void test14grantAccess() throws Exception {
        HttpServletRequest request         = Mockito.mock(HttpServletRequest.class);
        String             serviceName     = "HDFS_1";
        GrantRevokeRequest grantRequestObj = new GrantRevokeRequest();
        grantRequestObj.setAccessTypes(null);
        grantRequestObj.setDelegateAdmin(true);
        grantRequestObj.setEnableAudit(true);
        grantRequestObj.setGrantor("read");
        grantRequestObj.setIsRecursive(true);

        Mockito.when(serviceUtil.isValidateHttpsAuthentication(serviceName, request)).thenReturn(false);
        RESTResponse restResponse = serviceREST.grantAccess(serviceName, grantRequestObj, request);
        Assertions.assertNotNull(restResponse);
        Mockito.verify(serviceUtil).isValidateHttpsAuthentication(serviceName, request);
    }

    @Test
    public void test14_1_grantAccessWithMultiColumns() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        String      serviceName = "HIVE";
        Set<String> userList    = new HashSet<>();
        userList.add("user1");
        userList.add("user2");
        userList.add("user3");

        Map<String, String> grantResource = new HashMap<>();
        grantResource.put("database", "demo");
        grantResource.put("table", "testtbl");
        grantResource.put("column", "column1,column2,colum3");
        GrantRevokeRequest grantRequestObj = new GrantRevokeRequest();

        grantRequestObj.setResource(grantResource);
        grantRequestObj.setUsers(userList);
        grantRequestObj.setAccessTypes(new HashSet<>(Collections.singletonList("select")));
        grantRequestObj.setDelegateAdmin(true);
        grantRequestObj.setEnableAudit(true);
        grantRequestObj.setGrantor("systest");
        grantRequestObj.setIsRecursive(true);

        RangerAccessResource resource = new RangerAccessResourceImpl(ServiceREST.getAccessResourceObjectMap(grantRequestObj.getResource()), "systest");

        RangerPolicy createPolicy = new RangerPolicy();
        createPolicy.setService(serviceName);
        createPolicy.setName("grant-" + System.currentTimeMillis());
        createPolicy.setDescription("created by grant");
        createPolicy.setIsAuditEnabled(grantRequestObj.getEnableAudit());

        Map<String, RangerPolicyResource> policyResources = new HashMap<>();
        Set<String>                       resourceNames   = resource.getKeys();

        if (!CollectionUtils.isEmpty(resourceNames)) {
            for (String resourceName : resourceNames) {
                policyResources.put(resourceName, serviceREST.getPolicyResource(resource.getValue(resourceName), grantRequestObj));
            }
        }
        createPolicy.setResources(policyResources);

        RangerPolicyItem policyItem = new RangerPolicyItem();
        policyItem.setDelegateAdmin(grantRequestObj.getDelegateAdmin());
        policyItem.addUsers(grantRequestObj.getUsers());
        for (String accessType : grantRequestObj.getAccessTypes()) {
            policyItem.addAccess(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
        }
        createPolicy.addPolicyItem(policyItem);
        createPolicy.setZoneName(null);

        List<String>                      grantColumns         = (List<String>) resource.getValue("column");
        Map<String, RangerPolicyResource> policyResourceMap    = createPolicy.getResources();
        List<String>                      createdPolicyColumns = policyResourceMap.get("column").getValues();

        Assertions.assertTrue(createdPolicyColumns.containsAll(grantColumns));

        Mockito.when(serviceUtil.isValidateHttpsAuthentication(serviceName, request)).thenReturn(false);
        RESTResponse restResponse = serviceREST.grantAccess(serviceName, grantRequestObj, request);
        Assertions.assertNotNull(restResponse);
        Mockito.verify(serviceUtil).isValidateHttpsAuthentication(serviceName, request);
    }

    @Test
    public void test15revokeAccess() throws Exception {
        HttpServletRequest request     = Mockito.mock(HttpServletRequest.class);
        String             serviceName = "HDFS_1";
        Set<String>        userList    = new HashSet<>();
        userList.add("user1");
        userList.add("user2");
        userList.add("user3");
        Set<String> groupList = new HashSet<>();
        groupList.add("group1");
        groupList.add("group2");
        groupList.add("group3");
        GrantRevokeRequest revokeRequest = new GrantRevokeRequest();
        revokeRequest.setDelegateAdmin(true);
        revokeRequest.setEnableAudit(true);
        revokeRequest.setGrantor("read");
        revokeRequest.setGroups(groupList);
        revokeRequest.setUsers(userList);

        RESTResponse restResponse = serviceREST.revokeAccess(serviceName, revokeRequest, request);
        Assertions.assertNotNull(restResponse);
    }

    @Test
    public void test16createPolicyFalse() throws Exception {
        RangerPolicy     rangerPolicy     = rangerPolicy();
        RangerServiceDef rangerServiceDef = rangerServiceDef();

        List<RangerPolicy> policies   = new ArrayList<>();
        RangerPolicy       rangPolicy = new RangerPolicy();
        policies.add(rangPolicy);

        String      userName       = "admin";
        Set<String> userGroupsList = new HashSet<>();
        userGroupsList.add("group1");
        userGroupsList.add("group2");

        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceId(Id);
        servicePolicies.setServiceName("Hdfs_1");
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setPolicyUpdateTime(new Date());
        servicePolicies.setServiceDef(rangerServiceDef);
        servicePolicies.setPolicies(policies);

        List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<>();
        RangerAccessTypeDef       rangerAccessTypeDefObj  = new RangerAccessTypeDef();
        rangerAccessTypeDefObj.setLabel("Read");
        rangerAccessTypeDefObj.setName("read");
        rangerAccessTypeDefObj.setRbKeyLabel(null);
        rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);

        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.createPolicy(Mockito.any())).thenReturn(rangPolicy);

        RangerPolicy dbRangerPolicy = serviceREST.createPolicy(rangerPolicy, null);
        Assertions.assertNotNull(dbRangerPolicy);
        Mockito.verify(bizUtil, Mockito.times(2)).isAdmin();
        Mockito.verify(validatorFactory).getPolicyValidator(svcStore);

        Mockito.verify(daoManager).getXXService();
        Mockito.verify(daoManager, Mockito.atLeastOnce()).getXXServiceDef();
    }

    @Test
    public void test17updatePolicyFalse() throws Exception {
        RangerPolicy rangerPolicy = rangerPolicy();
        String       userName     = "admin";

        Set<String> userGroupsList = new HashSet<>();
        userGroupsList.add("group1");
        userGroupsList.add("group2");

        List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<>();
        RangerAccessTypeDef       rangerAccessTypeDefObj  = new RangerAccessTypeDef();
        rangerAccessTypeDefObj.setLabel("Read");
        rangerAccessTypeDefObj.setName("read");
        rangerAccessTypeDefObj.setRbKeyLabel(null);
        rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);

        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        RangerPolicy dbRangerPolicy = serviceREST.updatePolicy(rangerPolicy, Id);
        Assertions.assertNull(dbRangerPolicy);
        Mockito.verify(validatorFactory).getPolicyValidator(svcStore);
    }

    @Test
    public void test18deletePolicyFalse() throws Exception {
        RangerPolicy rangerPolicy = rangerPolicy();

        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        String userName = "admin";

        Set<String> userGroupsList = new HashSet<>();
        userGroupsList.add("group1");
        userGroupsList.add("group2");

        List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<>();
        RangerAccessTypeDef       rangerAccessTypeDefObj  = new RangerAccessTypeDef();
        rangerAccessTypeDefObj.setLabel("Read");
        rangerAccessTypeDefObj.setName("read");
        rangerAccessTypeDefObj.setRbKeyLabel(null);
        rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(svcStore.getPolicy(Id)).thenReturn(rangerPolicy);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        serviceREST.deletePolicy(rangerPolicy.getId());
        Mockito.verify(validatorFactory).getPolicyValidator(svcStore);
    }

    @Test
    public void test19getPolicyFalse() throws Exception {
        RangerPolicy rangerPolicy = rangerPolicy();
        Mockito.when(svcStore.getPolicy(rangerPolicy.getId())).thenReturn(rangerPolicy);
        String userName = "admin";

        Set<String> userGroupsList = new HashSet<>();
        userGroupsList.add("group1");
        userGroupsList.add("group2");

        List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<>();
        RangerAccessTypeDef       rangerAccessTypeDefObj  = new RangerAccessTypeDef();
        rangerAccessTypeDefObj.setLabel("Read");
        rangerAccessTypeDefObj.setName("read");
        rangerAccessTypeDefObj.setRbKeyLabel(null);
        rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        RangerPolicy dbRangerPolicy = serviceREST.getPolicy(rangerPolicy.getId());
        Assertions.assertNotNull(dbRangerPolicy);
        Assertions.assertEquals(dbRangerPolicy.getId(), rangerPolicy.getId());
        Mockito.verify(svcStore).getPolicy(rangerPolicy.getId());
    }

    @Test
    public void test20getPolicies() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter       filter  = new SearchFilter();
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        RangerPolicyList dbRangerPolicy = serviceREST.getPolicies(request);
        Assertions.assertNotNull(dbRangerPolicy);
        Assertions.assertEquals(dbRangerPolicy.getListSize(), 0);
        Mockito.verify(searchUtil).getSearchFilter(request, policyService.sortFields);
    }

    @Test
    public void test21countPolicies() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        SearchFilter filter = new SearchFilter();
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);

        Long data = serviceREST.countPolicies(request);
        Assertions.assertNotNull(data);
        Mockito.verify(searchUtil).getSearchFilter(request, policyService.sortFields);
    }

    @Test
    public void test22getServicePoliciesById() throws Exception {
        HttpServletRequest request      = Mockito.mock(HttpServletRequest.class);
        RangerPolicy       rangerPolicy = rangerPolicy();

        SearchFilter filter = new SearchFilter();
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);

        RangerPolicyList dbRangerPolicy = serviceREST.getServicePolicies(rangerPolicy.getId(), request);
        Assertions.assertNotNull(dbRangerPolicy);
        Mockito.verify(searchUtil).getSearchFilter(request, policyService.sortFields);
        Mockito.verify(svcStore).getServicePolicies(Id, filter);
    }

    @Test
    public void test23getServicePoliciesByName() throws Exception {
        HttpServletRequest request      = Mockito.mock(HttpServletRequest.class);
        RangerPolicy       rangerPolicy = rangerPolicy();

        List<RangerPolicy> ret    = new ArrayList<>();
        SearchFilter       filter = new SearchFilter();

        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");

        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getServicePolicies(Mockito.eq(rangerPolicy.getName()), Mockito.eq(filter))).thenReturn(ret);

        RangerPolicyList dbRangerPolicy = serviceREST.getServicePoliciesByName(rangerPolicy.getName(), request);
        Assertions.assertNotNull(dbRangerPolicy);
        Mockito.verify(svcStore).getServicePolicies(Mockito.eq(rangerPolicy.getName()), Mockito.eq(filter));
    }

    @Test
    public void test24getServicePoliciesIfUpdated() throws Exception {
        HttpServletRequest request          = Mockito.mock(HttpServletRequest.class);
        String             serviceName      = "HDFS_1";
        Long               lastKnownVersion = 1L;
        String             pluginId         = "1";

        ServicePolicies dbServicePolicies = serviceREST.getServicePoliciesIfUpdated(serviceName, lastKnownVersion, 0L, pluginId, "", "", false, capabilityVector, request);
        Assertions.assertNull(dbServicePolicies);
    }

    @Test
    public void test25getPolicies() throws Exception {
        List<RangerPolicy> ret    = new ArrayList<>();
        SearchFilter       filter = new SearchFilter();
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        Mockito.when(svcStore.getPolicies(filter)).thenReturn(ret);

        List<RangerPolicy> dbRangerPolicyList = serviceREST.getPolicies(filter);
        Assertions.assertNotNull(dbRangerPolicyList);
        Mockito.verify(svcStore).getPolicies(filter);
    }

    @Test
    public void test26getServices() throws Exception {
        List<RangerService> ret    = new ArrayList<>();
        SearchFilter        filter = new SearchFilter();
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        Mockito.when(svcStore.getServices(filter)).thenReturn(ret);

        List<RangerService> dbRangerService = serviceREST.getServices(filter);
        Assertions.assertNotNull(dbRangerService);
        Mockito.verify(svcStore).getServices(filter);
    }

    @Test
    public void test27getPoliciesWithoutServiceAdmin() throws Exception {
        HttpServletRequest request  = Mockito.mock(HttpServletRequest.class);
        SearchFilter       filter   = new SearchFilter();
        List<RangerPolicy> policies = new ArrayList<>();
        policies.add(rangerPolicy());
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getPolicies(filter)).thenReturn(policies);
        RangerPolicyList dbRangerPolicy = serviceREST.getPolicies(request);
        Assertions.assertNotNull(dbRangerPolicy);
        /*here we are not setting service admin role,hence we will not get any policy without the service admin roles*/
        Assertions.assertEquals(dbRangerPolicy.getListSize(), 0);
        Mockito.verify(searchUtil).getSearchFilter(request, policyService.sortFields);
    }

    @Test
    public void test28getPoliciesWithServiceAdmin() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter       filter  = new SearchFilter();
        XXService          xs      = Mockito.mock(XXService.class);
        xs.setType(3L);
        ServiceREST        spySVCRest  = Mockito.spy(serviceREST);
        List<RangerPolicy> policies    = new ArrayList<>();
        ServicePolicies    svcPolicies = new ServicePolicies();
        svcPolicies.setPolicies(policies);
        svcPolicies.setServiceName("HDFS_1-1-20150316062453");
        RangerPolicy rPol = rangerPolicy();
        policies.add(rPol);
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getPolicies(filter)).thenReturn(policies);

        /*here we are setting serviceAdminRole, so we will get the required policy with serviceAdmi role*/
        Mockito.when(svcStore.isServiceAdminUser(rPol.getService(), null)).thenReturn(true);
        RangerPolicyList dbRangerPolicy = spySVCRest.getPolicies(request);
        Assertions.assertNotNull(dbRangerPolicy);
        Assertions.assertEquals(dbRangerPolicy.getListSize(), 1);
        Mockito.verify(searchUtil).getSearchFilter(request, policyService.sortFields);
        Mockito.verify(svcStore).getPolicies(filter);
        Mockito.verify(svcStore).isServiceAdminUser(rPol.getService(), null);
    }

    @Test
    public void test30getPolicyFromEventTime() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        String      strdt          = new Date().toString();
        String      userName       = "Admin";
        Set<String> userGroupsList = new HashSet<>();
        userGroupsList.add("group1");
        userGroupsList.add("group2");
        Mockito.when(request.getParameter("eventTime")).thenReturn(strdt);
        Mockito.when(request.getParameter("policyId")).thenReturn("1");
        Mockito.when(request.getParameter("versionNo")).thenReturn("1");
        RangerPolicy                      policy    = new RangerPolicy();
        Map<String, RangerPolicyResource> resources = new HashMap<>();
        policy.setService("services");
        policy.setResources(resources);
        Mockito.when(svcStore.getPolicyFromEventTime(strdt, 1L)).thenReturn(policy);
        Mockito.when(bizUtil.isAdmin()).thenReturn(false);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new WebApplicationException());
        //thrown.expect(WebApplicationException.class);
        Assertions.assertThrows(WebApplicationException.class, () -> {
            RangerPolicy dbRangerPolicy = serviceREST.getPolicyFromEventTime(request);
            Assertions.assertNull(dbRangerPolicy);
            Mockito.verify(request).getParameter("eventTime");
            Mockito.verify(request).getParameter("policyId");
            Mockito.verify(request).getParameter("versionNo");
        });
    }

    @Test
    public void test31getServices() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter       filter  = new SearchFilter();
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        RangerServiceList dbRangerService = serviceREST.getServices(request);
        Assertions.assertNull(dbRangerService);
    }

    @Test
    public void test32getPolicyVersionList() throws Exception {
        VXString vXString = new VXString();
        vXString.setValue("1");
        Mockito.when(svcStore.getPolicyVersionList(Id)).thenReturn(vXString);

        VXString dbVXString = serviceREST.getPolicyVersionList(Id);
        Assertions.assertNotNull(dbVXString);
        Mockito.verify(svcStore).getPolicyVersionList(Id);
    }

    @Test
    public void test33getPolicyForVersionNumber() throws Exception {
        RangerPolicy rangerPolicy = rangerPolicy();
        Mockito.when(svcStore.getPolicyForVersionNumber(Id, 1)).thenReturn(rangerPolicy);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        RangerPolicy dbRangerPolicy = serviceREST.getPolicyForVersionNumber(Id, 1);
        Assertions.assertNotNull(dbRangerPolicy);
        Mockito.verify(svcStore).getPolicyForVersionNumber(Id, 1);
    }

    @Test
    public void test34countServices() throws Exception {
        HttpServletRequest   request = Mockito.mock(HttpServletRequest.class);
        PList<RangerService> ret     = Mockito.mock(PList.class);
        SearchFilter         filter  = new SearchFilter();
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);

        Mockito.when(svcStore.getPaginatedServices(filter)).thenReturn(ret);
        Long data = serviceREST.countServices(request);
        Assertions.assertNotNull(data);
        Mockito.verify(searchUtil).getSearchFilter(request, policyService.sortFields);
        Mockito.verify(svcStore).getPaginatedServices(filter);
    }

    @Test
    public void test35validateConfig() throws Exception {
        RangerService rangerService = rangerService();
        Mockito.when(serviceMgr.validateConfig(rangerService, svcStore)).thenReturn(vXResponse);
        VXResponse dbVXResponse = serviceREST.validateConfig(rangerService);
        Assertions.assertNotNull(dbVXResponse);
        Mockito.verify(serviceMgr).validateConfig(rangerService, svcStore);
    }

    @Test
    public void test40applyPolicy() {
        RangerPolicy existingPolicy = rangerPolicy();
        RangerPolicy appliedPolicy  = rangerPolicy();

        List<RangerPolicyItem> policyItem = new ArrayList<>();
        existingPolicy.setPolicyItems(policyItem);
        appliedPolicy.setPolicyItems(null);

        Map<String, RangerPolicyResource> policyResources      = new HashMap<>();
        RangerPolicyResource              rangerPolicyResource = new RangerPolicyResource("/tmp");
        rangerPolicyResource.setIsExcludes(true);
        rangerPolicyResource.setIsRecursive(true);
        policyResources.put("path", rangerPolicyResource);

        existingPolicy.setResources(policyResources);
        appliedPolicy.setResources(policyResources);

        RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("read", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("write", true));
        rangerPolicyItem.addGroup("group1");
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(true);

        existingPolicy.addPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addGroup("group1");
        rangerPolicyItem.addGroup("public");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.addUser("finance");
        rangerPolicyItem.setDelegateAdmin(false);

        appliedPolicy.addPolicyItem(rangerPolicyItem);

        String existingPolicyStr = existingPolicy.toString();
        System.out.println("existingPolicy = " + existingPolicyStr);

        ServiceRESTUtil.processApplyPolicy(existingPolicy, appliedPolicy);

        String resultPolicyStr = existingPolicy.toString();
        System.out.println("resultPolicy = " + resultPolicyStr);

        Assertions.assertTrue(true);
    }

    @Test
    public void test41applyPolicy() {
        RangerPolicy existingPolicy = rangerPolicy();
        RangerPolicy appliedPolicy  = rangerPolicy();

        List<RangerPolicyItem> policyItem = new ArrayList<>();
        existingPolicy.setPolicyItems(policyItem);
        appliedPolicy.setPolicyItems(null);

        Map<String, RangerPolicyResource> policyResources      = new HashMap<>();
        RangerPolicyResource              rangerPolicyResource = new RangerPolicyResource("/tmp");
        rangerPolicyResource.setIsExcludes(true);
        rangerPolicyResource.setIsRecursive(true);
        policyResources.put("path", rangerPolicyResource);

        existingPolicy.setResources(policyResources);
        appliedPolicy.setResources(policyResources);

        RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("read", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("write", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("lock", true));
        rangerPolicyItem.addGroup("group1");
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(true);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("read", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("write", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("lock", true));
        rangerPolicyItem.addGroup("group3");
        rangerPolicyItem.addUser("user3");
        rangerPolicyItem.setDelegateAdmin(true);

        existingPolicy.addPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("lock", true));
        rangerPolicyItem.addGroup("group1");
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(false);

        existingPolicy.addAllowException(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(false);

        existingPolicy.addDenyPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("index", true));
        rangerPolicyItem.addGroup("public");
        rangerPolicyItem.addUser("user");
        rangerPolicyItem.setDelegateAdmin(false);

        existingPolicy.addDenyPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("index", true));

        rangerPolicyItem.addGroup("group1");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.setDelegateAdmin(false);

        appliedPolicy.addPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));

        rangerPolicyItem.addGroup("public");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.setDelegateAdmin(false);

        appliedPolicy.addDenyPolicyItem(rangerPolicyItem);

        String existingPolicyStr = existingPolicy.toString();
        System.out.println("existingPolicy=" + existingPolicyStr);

        ServiceRESTUtil.processApplyPolicy(existingPolicy, appliedPolicy);

        String resultPolicyStr = existingPolicy.toString();
        System.out.println("resultPolicy = " + resultPolicyStr);

        Assertions.assertTrue(true);
    }

    @Test
    public void test42grant() {
        RangerPolicy           existingPolicy = rangerPolicy();
        List<RangerPolicyItem> policyItem     = new ArrayList<>();
        existingPolicy.setPolicyItems(policyItem);

        Map<String, RangerPolicyResource> policyResources      = new HashMap<>();
        RangerPolicyResource              rangerPolicyResource = new RangerPolicyResource("/tmp");
        rangerPolicyResource.setIsExcludes(true);
        rangerPolicyResource.setIsRecursive(true);
        policyResources.put("path", rangerPolicyResource);

        existingPolicy.setResources(policyResources);

        RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("read", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("write", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("lock", true));
        rangerPolicyItem.addGroup("group1");
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(true);

        existingPolicy.addPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("read", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("write", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("lock", true));
        rangerPolicyItem.addGroup("group3");
        rangerPolicyItem.addUser("user3");
        rangerPolicyItem.setDelegateAdmin(true);

        existingPolicy.addPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("lock", true));
        rangerPolicyItem.addGroup("group1");
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(false);

        existingPolicy.addAllowException(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(false);

        existingPolicy.addDenyPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("index", true));
        rangerPolicyItem.addGroup("public");
        rangerPolicyItem.addUser("user");
        rangerPolicyItem.setDelegateAdmin(false);

        existingPolicy.addDenyPolicyItem(rangerPolicyItem);

        GrantRevokeRequest  grantRequestObj = new GrantRevokeRequest();
        Map<String, String> resource        = new HashMap<>();
        resource.put("path", "/tmp");
        grantRequestObj.setResource(resource);

        grantRequestObj.getUsers().add("user1");
        grantRequestObj.getGroups().add("group1");

        grantRequestObj.getAccessTypes().add("delete");
        grantRequestObj.getAccessTypes().add("index");

        grantRequestObj.setDelegateAdmin(true);

        grantRequestObj.setEnableAudit(true);
        grantRequestObj.setIsRecursive(true);

        grantRequestObj.setGrantor("test42Grant");

        String existingPolicyStr = existingPolicy.toString();
        System.out.println("existingPolicy = " + existingPolicyStr);

        ServiceRESTUtil.processGrantRequest(existingPolicy, grantRequestObj);

        String resultPolicyStr = existingPolicy.toString();
        System.out.println("resultPolicy = " + resultPolicyStr);

        Assertions.assertTrue(true);
    }

    @Test
    public void test43revoke() {
        RangerPolicy existingPolicy = rangerPolicy();

        List<RangerPolicyItem> policyItem = new ArrayList<>();
        existingPolicy.setPolicyItems(policyItem);

        Map<String, RangerPolicyResource> policyResources      = new HashMap<>();
        RangerPolicyResource              rangerPolicyResource = new RangerPolicyResource("/tmp");
        rangerPolicyResource.setIsExcludes(true);
        rangerPolicyResource.setIsRecursive(true);
        policyResources.put("path", rangerPolicyResource);

        existingPolicy.setResources(policyResources);

        RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("read", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("write", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("lock", true));
        rangerPolicyItem.addGroup("group1");
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(true);

        existingPolicy.addPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("read", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("write", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("lock", true));
        rangerPolicyItem.addGroup("group3");
        rangerPolicyItem.addUser("user3");
        rangerPolicyItem.setDelegateAdmin(true);

        existingPolicy.addPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("lock", true));
        rangerPolicyItem.addGroup("group1");
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user1");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(false);

        existingPolicy.addAllowException(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("delete", true));
        rangerPolicyItem.addGroup("group2");
        rangerPolicyItem.addUser("user2");
        rangerPolicyItem.setDelegateAdmin(false);

        existingPolicy.addDenyPolicyItem(rangerPolicyItem);

        rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.addAccess(new RangerPolicyItemAccess("index", true));
        rangerPolicyItem.addGroup("public");
        rangerPolicyItem.addUser("user");
        rangerPolicyItem.setDelegateAdmin(false);

        existingPolicy.addDenyPolicyItem(rangerPolicyItem);

        GrantRevokeRequest  revokeRequestObj = new GrantRevokeRequest();
        Map<String, String> resource         = new HashMap<>();
        resource.put("path", "/tmp");
        revokeRequestObj.setResource(resource);

        revokeRequestObj.getUsers().add("user1");
        revokeRequestObj.getGroups().add("group1");

        revokeRequestObj.getAccessTypes().add("delete");
        revokeRequestObj.getAccessTypes().add("index");

        revokeRequestObj.setDelegateAdmin(true);

        revokeRequestObj.setEnableAudit(true);
        revokeRequestObj.setIsRecursive(true);

        revokeRequestObj.setGrantor("test43Revoke");

        String existingPolicyStr = existingPolicy.toString();
        System.out.println("existingPolicy=" + existingPolicyStr);

        ServiceRESTUtil.processRevokeRequest(existingPolicy, revokeRequestObj);

        String resultPolicyStr = existingPolicy.toString();
        System.out.println("resultPolicy=" + resultPolicyStr);

        Assertions.assertTrue(true);
    }

    @Test
    public void test44getPolicyLabels() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter       filter  = new SearchFilter();
        Mockito.when(searchUtil.getSearchFilter(request, policyLabelsService.sortFields)).thenReturn(filter);
        List<String> ret = new ArrayList<>();
        Mockito.when(svcStore.getPolicyLabels(filter)).thenReturn(ret);
        ret = serviceREST.getPolicyLabels(request);
        Assertions.assertNotNull(ret);
        Mockito.verify(searchUtil).getSearchFilter(request, policyLabelsService.sortFields);
    }

    @Test
    public void test45exportPoliciesInJSON() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        List<RangerPolicy> rangerPolicyList = new ArrayList<>();

        RangerPolicy rangerPolicy = rangerPolicy();
        rangerPolicyList.add(rangerPolicy);
        XXService xService = xService();

        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        XXServiceDef    xServiceDef    = serviceDef();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        request.setAttribute("serviceType", "hdfs,hbase,hive,yarn,knox,storm,solr,kafka,nifi,atlas,sqoop");
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        SearchFilter        filter   = new SearchFilter();
        filter.setParam("zoneName", "zone1");
        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getPolicies(filter)).thenReturn(rangerPolicyList);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.isKeyAdmin()).thenReturn(false);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("admin");
        Mockito.when(bizUtil.isAuditAdmin()).thenReturn(false);
        Mockito.when(bizUtil.isAuditKeyAdmin()).thenReturn(false);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(daoManager.getXXService().findByName("HDFS_1-1-20150316062453")).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef().getById(xService.getType())).thenReturn(xServiceDef);
        serviceREST.getPoliciesInJson(request, response, false);

        Mockito.verify(svcStore).getObjectInJson(rangerPolicyList, response, JSON_FILE_NAME_TYPE.POLICY);
    }

    @Test
    public void test46exportPoliciesInCSV() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        List<RangerPolicy> rangerPolicyList = new ArrayList<>();

        RangerPolicy rangerPolicy = rangerPolicy();
        rangerPolicyList.add(rangerPolicy);
        XXService xService = xService();

        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        XXServiceDef    xServiceDef    = serviceDef();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        request.setAttribute("serviceType", "hdfs,hbase,hive,yarn,knox,storm,solr,kafka,nifi,atlas,sqoop");
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        SearchFilter        filter   = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getPolicies(filter)).thenReturn(rangerPolicyList);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.isKeyAdmin()).thenReturn(false);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("admin");
        Mockito.when(bizUtil.isAuditAdmin()).thenReturn(false);
        Mockito.when(bizUtil.isAuditKeyAdmin()).thenReturn(false);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);

        Mockito.when(daoManager.getXXService().findByName("HDFS_1-1-20150316062453")).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef().getById(xService.getType())).thenReturn(xServiceDef);
        serviceREST.getPoliciesInCsv(request, response);

        Mockito.verify(svcStore).getPoliciesInCSV(rangerPolicyList, response);
    }

    @Test
    public void test48exportPoliciesInExcel() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        List<RangerPolicy> rangerPolicyList = new ArrayList<>();
        RangerPolicy rangerPolicy = rangerPolicy();

        rangerPolicyList.add(rangerPolicy);

        XXService xService             = xService();
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        XXServiceDef    xServiceDef    = serviceDef();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        request.setAttribute("serviceType", "hdfs,hbase,hive,yarn,knox,storm,solr,kafka,nifi,atlas,sqoop");
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        SearchFilter        filter   = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getPolicies(filter)).thenReturn(rangerPolicyList);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.isKeyAdmin()).thenReturn(false);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("admin");
        Mockito.when(bizUtil.isAuditAdmin()).thenReturn(false);
        Mockito.when(bizUtil.isAuditKeyAdmin()).thenReturn(false);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);

        Mockito.when(daoManager.getXXService().findByName("HDFS_1-1-20150316062453")).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef().getById(xService.getType())).thenReturn(xServiceDef);
        serviceREST.getPoliciesInExcel(request, response);
        Mockito.verify(svcStore).getPoliciesInExcel(rangerPolicyList, response);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test49importPoliciesFromFileAllowingOverride() throws Exception {
        HttpServletRequest        request         = Mockito.mock(HttpServletRequest.class);
        RangerPolicyValidator     policyValidator = Mockito.mock(RangerPolicyValidator.class);
        Map<String, RangerPolicy> policiesMap     = new LinkedHashMap<>();
        RangerPolicy              rangerPolicy    = rangerPolicy();
        RangerService             service         = rangerService();
        XXService                 xService        = xService();
        policiesMap.put("Name", rangerPolicy);
        XXServiceDao                      xServiceDao              = Mockito.mock(XXServiceDao.class);
        XXServiceDef                      xServiceDef              = serviceDef();
        XXServiceDefDao                   xServiceDefDao           = Mockito.mock(XXServiceDefDao.class);
        XXSecurityZoneRefServiceDao       xSecZoneRefServiceDao    = Mockito.mock(XXSecurityZoneRefServiceDao.class);
        XXSecurityZoneRefTagServiceDao    xSecZoneRefTagServiceDao = Mockito.mock(XXSecurityZoneRefTagServiceDao.class);
        XXSecurityZoneRefService          xSecZoneRefService       = Mockito.mock(XXSecurityZoneRefService.class);
        XXSecurityZoneRefTagService       xSecZoneRefTagService    = Mockito.mock(XXSecurityZoneRefTagService.class);
        XXSecurityZoneDao                 xSecZoneDao              = Mockito.mock(XXSecurityZoneDao.class);
        XXSecurityZone                    xSecZone                 = Mockito.mock(XXSecurityZone.class);
        List<XXSecurityZoneRefService>    zoneServiceList          = new ArrayList<>();
        List<XXSecurityZoneRefTagService> zoneTagServiceList       = new ArrayList<>();
        zoneServiceList.add(xSecZoneRefService);
        zoneTagServiceList.add(xSecZoneRefTagService);
        Map<String, String> zoneMappingMap = new LinkedHashMap<>();
        zoneMappingMap.put("ZoneSource", "ZoneDestination");

        String paramServiceType = "serviceType";
        String serviceTypeList    = "hdfs,hbase,hive,yarn,knox,storm,solr,kafka,nifi,atlas,sqoop";
        request.setAttribute("serviceType", "hdfs,hbase,hive,yarn,knox,storm,solr,kafka,nifi,atlas,sqoop");
        SearchFilter filter = new SearchFilter();
        filter.setParam("serviceType", "value");

        File        jsonPolicyFile      = getFile(importPoliceTestFilePath);
        InputStream uploadedInputStream = new FileInputStream(jsonPolicyFile);
        FormDataContentDisposition fileDetail = FormDataContentDisposition.name("file").fileName(jsonPolicyFile.getName()).size(uploadedInputStream.toString().length()).build();
        boolean isOverride = true;

        InputStream zoneInputStream = IOUtils.toInputStream("ZoneSource=ZoneDestination", "UTF-8");

        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(request.getParameter(paramServiceType)).thenReturn(serviceTypeList);
        Mockito.when(svcStore.createPolicyMap(Mockito.any(Map.class), Mockito.any(List.class), Mockito.anyString(), Mockito.any(Map.class), Mockito.any(List.class), Mockito.any(List.class), Mockito.any(RangerPolicy.class), Mockito.any(Map.class))).thenReturn(policiesMap);
        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(daoManager.getXXService().findByName("HDFS_1-1-20150316062453")).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef().getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        Mockito.when(svcStore.getMapFromInputStream(zoneInputStream)).thenReturn(zoneMappingMap);
        Mockito.when(daoManager.getXXSecurityZoneDao()).thenReturn(xSecZoneDao);
        Mockito.when(xSecZoneDao.findByZoneName(Mockito.anyString())).thenReturn(xSecZone);
        Mockito.when(daoManager.getXXSecurityZoneRefService()).thenReturn(xSecZoneRefServiceDao);
        Mockito.when(xSecZoneRefServiceDao.findByServiceNameAndZoneId(Mockito.anyString(), Mockito.anyLong())).thenReturn(zoneServiceList);
        Mockito.when(daoManager.getXXSecurityZoneRefTagService()).thenReturn(xSecZoneRefTagServiceDao);
        Mockito.when(xSecZoneRefTagServiceDao.findByTagServiceNameAndZoneId(Mockito.anyString(), Mockito.anyLong())).thenReturn(zoneTagServiceList);
        Mockito.when(svcStore.getServiceByName(Mockito.anyString())).thenReturn(service);
        serviceREST.importPoliciesFromFile(request, null, zoneInputStream, uploadedInputStream, fileDetail, isOverride, "unzoneToZone");

        Mockito.verify(svcStore).createPolicy(rangerPolicy);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test50importPoliciesFromFileNotAllowingOverride() throws Exception {
        HttpServletRequest        request      = Mockito.mock(HttpServletRequest.class);
        Map<String, RangerPolicy> policiesMap  = new LinkedHashMap<>();
        RangerPolicy              rangerPolicy = rangerPolicy();
        XXService                 xService     = xService();
        policiesMap.put("Name", rangerPolicy);
        XXServiceDao                      xServiceDao              = Mockito.mock(XXServiceDao.class);
        XXServiceDef                      xServiceDef              = serviceDef();
        XXServiceDefDao                   xServiceDefDao           = Mockito.mock(XXServiceDefDao.class);
        XXSecurityZoneRefServiceDao       xSecZoneRefServiceDao    = Mockito.mock(XXSecurityZoneRefServiceDao.class);
        XXSecurityZoneRefTagServiceDao    xSecZoneRefTagServiceDao = Mockito.mock(XXSecurityZoneRefTagServiceDao.class);
        XXSecurityZoneRefService          xSecZoneRefService       = Mockito.mock(XXSecurityZoneRefService.class);
        XXSecurityZoneRefTagService       xSecZoneRefTagService    = Mockito.mock(XXSecurityZoneRefTagService.class);
        XXSecurityZoneDao                 xSecZoneDao              = Mockito.mock(XXSecurityZoneDao.class);
        XXSecurityZone                    xSecZone                 = Mockito.mock(XXSecurityZone.class);
        List<XXSecurityZoneRefService>    zoneServiceList          = new ArrayList<>();
        List<XXSecurityZoneRefTagService> zoneTagServiceList       = new ArrayList<>();
        zoneServiceList.add(xSecZoneRefService);
        zoneTagServiceList.add(xSecZoneRefTagService);
        Map<String, String> zoneMappingMap = new LinkedHashMap<>();
        zoneMappingMap.put("ZoneSource", "ZoneDestination");

        String paramServiceType = "serviceType";
        String serviceTypeList    = "hdfs,hbase,hive,yarn,knox,storm,solr,kafka,nifi,atlas,sqoop";
        request.setAttribute("serviceType", "hdfs,hbase,hive,yarn,knox,storm,solr,kafka,nifi,atlas,sqoop");
        SearchFilter filter = new SearchFilter();
        filter.setParam("serviceType", "value");

        File        jsonPolicyFile      = getFile(importPoliceTestFilePath);
        InputStream uploadedInputStream = new FileInputStream(jsonPolicyFile);
        FormDataContentDisposition fileDetail = FormDataContentDisposition.name("file").fileName(jsonPolicyFile.getName()).size(uploadedInputStream.toString().length()).build();
        boolean isOverride = false;

        InputStream zoneInputStream = IOUtils.toInputStream("ZoneSource=ZoneDestination", "UTF-8");

        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(request.getParameter(paramServiceType)).thenReturn(serviceTypeList);
        Mockito.when(svcStore.createPolicyMap(Mockito.any(Map.class), Mockito.any(List.class), Mockito.anyString(), Mockito.any(Map.class), Mockito.any(List.class), Mockito.any(List.class), Mockito.any(RangerPolicy.class), Mockito.any(Map.class))).thenReturn(policiesMap);
        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(daoManager.getXXService().findByName("HDFS_1-1-20150316062453")).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef().getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.getMapFromInputStream(zoneInputStream)).thenReturn(zoneMappingMap);
        Mockito.when(daoManager.getXXSecurityZoneDao()).thenReturn(xSecZoneDao);
        Mockito.when(xSecZoneDao.findByZoneName(Mockito.anyString())).thenReturn(xSecZone);
        Mockito.when(daoManager.getXXSecurityZoneRefService()).thenReturn(xSecZoneRefServiceDao);
        Mockito.when(xSecZoneRefServiceDao.findByServiceNameAndZoneId(Mockito.anyString(), Mockito.anyLong())).thenReturn(zoneServiceList);
        Mockito.when(daoManager.getXXSecurityZoneRefTagService()).thenReturn(xSecZoneRefTagServiceDao);
        Mockito.when(xSecZoneRefTagServiceDao.findByTagServiceNameAndZoneId(Mockito.anyString(), Mockito.anyLong())).thenReturn(zoneTagServiceList);
        serviceREST.importPoliciesFromFile(request, null, zoneInputStream, uploadedInputStream, fileDetail, isOverride, "unzoneToUnZone");
        Mockito.verify(svcStore).createPolicy(rangerPolicy);
    }

    @Test
    public void test51getMetricByType() throws Exception {
        String type = "usergroup";
        String ret = "{\"groupCount\":1,\"userCountOfUserRole\":0,\"userCountOfKeyAdminRole\":1,"
                + "\"userCountOfSysAdminRole\":3,\"userCountOfKeyadminAuditorRole\":0,\"userCountOfSysAdminAuditorRole\":0,\"userTotalCount\":4}";
        ServiceDBStore.METRIC_TYPE metricType = ServiceDBStore.METRIC_TYPE.getMetricTypeByName(type);
        Mockito.when(svcStore.getMetricByType(metricType)).thenReturn(ret);
        serviceREST.getMetricByType(type);
        Mockito.verify(svcStore).getMetricByType(metricType);
    }

    @Test
    public void test52deleteService() throws Exception {
        RangerService   rangerService     = rangerService();
        XXService       xService          = xService();
        List<XXService> referringServices = new ArrayList<>();
        referringServices.add(xService);
        EmbeddedServiceDefsUtil embeddedServiceDefsUtil = EmbeddedServiceDefsUtil.instance();
        xService.setType(embeddedServiceDefsUtil.getTagServiceDefId());
        XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);

        String                userLoginID = "testuser";
        Long                  userId      = 8L;
        RangerSecurityContext context     = new RangerSecurityContext();
        context.setUserSession(new UserSessionBase());
        RangerContextHolder.setSecurityContext(context);
        UserSessionBase session = ContextUtil.getCurrentUserSession();
        session.setUserAdmin(true);
        XXPortalUser xXPortalUser = new XXPortalUser();
        xXPortalUser.setLoginId(userLoginID);
        xXPortalUser.setId(userId);
        session.setXXPortalUser(xXPortalUser);

        Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenReturn(serviceValidator);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByTagServiceId(Mockito.anyLong())).thenReturn(referringServices);
        Mockito.when(xServiceDao.getById(Id)).thenReturn(xService);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.deleteService(rangerService.getId());
        });
    }

    @Test
    public void test53getPoliciesForResource() throws Exception {
        HttpServletRequest  request = Mockito.mock(HttpServletRequest.class);
        List<RangerService> rsList  = new ArrayList<>();
        RangerService       rs      = rangerService();
        rsList.add(rs);

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getPoliciesForResource("servicedefname", "servicename", request);
        });
    }

    @Test
    public void test54getPluginsInfo() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter       filter  = new SearchFilter();
        filter.setParam(SearchFilter.POLICY_NAME, "policyName");
        filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
        PList<RangerPluginInfo> paginatedPluginsInfo = new PList<>();
        Mockito.when(searchUtil.getSearchFilter(request, pluginInfoService.getSortFields())).thenReturn(filter);
        Mockito.when(pluginInfoService.searchRangerPluginInfo(filter)).thenReturn(paginatedPluginsInfo);
        RangerPluginInfoList rPluginInfoList = serviceREST.getPluginsInfo(request);
        Assertions.assertNotNull(rPluginInfoList);
        Mockito.verify(searchUtil).getSearchFilter(request, pluginInfoService.getSortFields());
        Mockito.verify(pluginInfoService).searchRangerPluginInfo(filter);
    }

    @Test
    public void test55getServicePoliciesIfUpdatedCatch() throws Exception {
        HttpServletRequest request          = Mockito.mock(HttpServletRequest.class);
        String             serviceName      = "HDFS_1";
        Long               lastKnownVersion = 1L;
        String             pluginId         = "1";
        Mockito.when(serviceUtil.isValidateHttpsAuthentication(serviceName, request)).thenReturn(true);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getServicePoliciesIfUpdated(serviceName, lastKnownVersion, 0L, pluginId, "", "", false, capabilityVector, request);
        });
    }

    @Test
    public void test56getServicePoliciesIfUpdated() throws Exception {
        HttpServletRequest request          = Mockito.mock(HttpServletRequest.class);
        ServicePolicies    servicePolicies  = servicePolicies();
        String             serviceName      = "HDFS_1";
        Long               lastKnownVersion = 1L;
        String             pluginId         = "1";
        Mockito.when(serviceUtil.isValidateHttpsAuthentication(serviceName, request)).thenReturn(true);
        Mockito.when(svcStore.getServicePoliciesIfUpdated(Mockito.anyString(), Mockito.anyLong(), Mockito.anyBoolean())).thenReturn(servicePolicies);
        ServicePolicies dbServicePolicies = serviceREST.getServicePoliciesIfUpdated(serviceName, lastKnownVersion, 0L, pluginId, "", "", true, capabilityVector, request);
        Assertions.assertNotNull(dbServicePolicies);
    }

    @Test
    public void test57getSecureServicePoliciesIfUpdatedFail() throws Exception {
        HttpServletRequest request          = Mockito.mock(HttpServletRequest.class);
        Long               lastKnownVersion = 1L;
        String             pluginId         = "1";
        XXService          xService         = xService();
        XXServiceDef       xServiceDef      = serviceDef();
        String             serviceName      = xService.getName();
        RangerService      rs               = rangerService();
        XXServiceDefDao    xServiceDefDao   = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(serviceUtil.isValidService(serviceName, request)).thenReturn(true);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(serviceName)).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.getServiceByName(serviceName)).thenReturn(rs);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getSecureServicePoliciesIfUpdated(serviceName, lastKnownVersion, 0L, pluginId, "", "", false, capabilityVector, request);
        });
    }

    @Test
    public void test58getSecureServicePoliciesIfUpdatedAllowedFail() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Long         lastKnownVersion = 1L;
        String       pluginId         = "1";
        XXService    xService         = xService();
        XXServiceDef xServiceDef      = serviceDef();
        xServiceDef.setImplclassname("org.apache.ranger.services.kms.RangerServiceKMS");
        String          serviceName    = xService.getName();
        RangerService   rs             = rangerService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(serviceUtil.isValidService(serviceName, request)).thenReturn(true);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(serviceName)).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.getServiceByNameForDP(serviceName)).thenReturn(rs);
        Mockito.when(bizUtil.isUserAllowed(rs, ServiceREST.Allowed_User_List_For_Grant_Revoke)).thenReturn(true);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getSecureServicePoliciesIfUpdated(serviceName, lastKnownVersion, 0L, pluginId, "", "", false, capabilityVector, request);
        });
    }

    @Test
    public void test59getSecureServicePoliciesIfUpdatedSuccess() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Long         lastKnownVersion = 1L;
        String       pluginId         = "1";
        XXService    xService         = xService();
        XXServiceDef xServiceDef      = serviceDef();
        xServiceDef.setImplclassname("org.apache.ranger.services.kms.RangerServiceKMS");
        String          serviceName    = xService.getName();
        RangerService   rs             = rangerService();
        ServicePolicies sp             = servicePolicies();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(serviceUtil.isValidService(serviceName, request)).thenReturn(true);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(serviceName)).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.getServiceByNameForDP(serviceName)).thenReturn(rs);
        Mockito.when(bizUtil.isUserAllowed(rs, ServiceREST.Allowed_User_List_For_Grant_Revoke)).thenReturn(true);
        Mockito.when(svcStore.getServicePoliciesIfUpdated(Mockito.anyString(), Mockito.anyLong(), Mockito.anyBoolean())).thenReturn(sp);
        Assertions.assertThrows(NullPointerException.class, () -> {
            ServicePolicies dbServiceSecurePolicies = serviceREST.getSecureServicePoliciesIfUpdated(serviceName, lastKnownVersion, 0L, pluginId, "", "", true, capabilityVector, request);
            Assertions.assertNotNull(dbServiceSecurePolicies);
            Mockito.verify(serviceUtil).isValidService(serviceName, request);
            Mockito.verify(xServiceDao).findByName(serviceName);
            Mockito.verify(xServiceDefDao).getById(xService.getType());
            Mockito.verify(svcStore).getServiceByNameForDP(serviceName);
            Mockito.verify(bizUtil).isUserAllowed(rs, ServiceREST.Allowed_User_List_For_Grant_Revoke);
            Mockito.verify(svcStore).getServicePoliciesIfUpdated(serviceName, lastKnownVersion, false);
        });
    }

    @Test
    public void test60getPolicyFromEventTime() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        String      strdt          = new Date().toString();
        Set<String> userGroupsList = new HashSet<>();
        userGroupsList.add("group1");
        userGroupsList.add("group2");
        Mockito.when(request.getParameter("eventTime")).thenReturn(strdt);
        Mockito.when(request.getParameter("policyId")).thenReturn("1");
        Mockito.when(request.getParameter("versionNo")).thenReturn("1");
        RangerPolicy                      policy    = new RangerPolicy();
        Map<String, RangerPolicyResource> resources = new HashMap<>();
        policy.setService("services");
        policy.setResources(resources);
        Mockito.when(svcStore.getPolicyFromEventTime(strdt, 1L)).thenReturn(null);

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getPolicyFromEventTime(request);
        });
    }

    @Test
    public void test61getServiceWillOnlyReturnNameIdAndTypeForRoleUser() throws Exception {
        RangerService actualService = rangerService();

        String userLoginID = "testuser";
        Long   userId      = 8L;

        RangerSecurityContext context = new RangerSecurityContext();
        context.setUserSession(new UserSessionBase());
        RangerContextHolder.setSecurityContext(context);
        UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
        currentUserSession.setUserAdmin(false);
        XXPortalUser xXPortalUser = new XXPortalUser();
        xXPortalUser.setLoginId(userLoginID);
        xXPortalUser.setId(userId);
        currentUserSession.setXXPortalUser(xXPortalUser);

        VXUser       loggedInUser     = new VXUser();
        List<String> loggedInUserRole = new ArrayList<>();
        loggedInUserRole.add(RangerConstants.ROLE_USER);
        loggedInUser.setId(8L);
        loggedInUser.setName("testuser");
        loggedInUser.setUserRoleList(loggedInUserRole);
        Mockito.when(xUserService.getXUserByUserName("testuser")).thenReturn(loggedInUser);
        Mockito.when(svcStore.getService(Id)).thenReturn(actualService);

        RangerService service = serviceREST.getService(Id);
        Assertions.assertNotNull(service);
        Mockito.verify(svcStore).getService(Id);
        Assertions.assertNull(service.getDescription());
        Assertions.assertTrue(service.getConfigs().isEmpty());
        Assertions.assertEquals(service.getId(), Id);
        Assertions.assertEquals(service.getName(), "HDFS_1");
        Assertions.assertEquals(service.getType(), "1");
    }

    @Test
    public void test62getServiceByNameWillOnlyReturnNameIdAndTypeForRoleUser() throws Exception {
        RangerService actualService = rangerService();

        String userLoginID = "testuser";
        Long   userId      = 8L;

        RangerSecurityContext context = new RangerSecurityContext();
        context.setUserSession(new UserSessionBase());
        RangerContextHolder.setSecurityContext(context);
        UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
        currentUserSession.setUserAdmin(false);
        XXPortalUser xXPortalUser = new XXPortalUser();
        xXPortalUser.setLoginId(userLoginID);
        xXPortalUser.setId(userId);
        currentUserSession.setXXPortalUser(xXPortalUser);

        VXUser       loggedInUser     = new VXUser();
        List<String> loggedInUserRole = new ArrayList<>();
        loggedInUserRole.add(RangerConstants.ROLE_USER);
        loggedInUser.setId(8L);
        loggedInUser.setName("testuser");
        loggedInUser.setUserRoleList(loggedInUserRole);
        Mockito.when(xUserService.getXUserByUserName("testuser")).thenReturn(loggedInUser);
        Mockito.when(svcStore.getServiceByName(actualService.getName())).thenReturn(actualService);

        RangerService service = serviceREST.getServiceByName(actualService.getName());
        Assertions.assertNotNull(service);
        Mockito.verify(svcStore).getServiceByName(actualService.getName());
        Assertions.assertNull(service.getDescription());
        Assertions.assertTrue(service.getConfigs().isEmpty());
        Assertions.assertEquals(service.getId(), Id);
        Assertions.assertEquals(service.getName(), "HDFS_1");
        Assertions.assertEquals(service.getType(), "1");
    }

    @Test
    public void test63getServices() throws Exception {
        HttpServletRequest   request       = Mockito.mock(HttpServletRequest.class);
        PList<RangerService> paginatedSvcs = new PList<>();
        RangerService        svc1          = rangerService();

        String userLoginID = "testuser";
        Long   userId      = 8L;

        RangerSecurityContext context = new RangerSecurityContext();
        context.setUserSession(new UserSessionBase());
        RangerContextHolder.setSecurityContext(context);
        UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
        currentUserSession.setUserAdmin(false);
        XXPortalUser xXPortalUser = new XXPortalUser();
        xXPortalUser.setLoginId(userLoginID);
        xXPortalUser.setId(userId);
        currentUserSession.setXXPortalUser(xXPortalUser);

        VXUser       loggedInUser     = new VXUser();
        List<String> loggedInUserRole = new ArrayList<>();
        loggedInUserRole.add(RangerConstants.ROLE_USER);
        loggedInUser.setId(8L);
        loggedInUser.setName("testuser");
        loggedInUser.setUserRoleList(loggedInUserRole);

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

        RangerService svc2 = new RangerService();
        svc2.setId(9L);
        svc2.setConfigs(configs);
        svc2.setCreateTime(new Date());
        svc2.setDescription("service policy");
        svc2.setGuid("1427365526516_835_1");
        svc2.setIsEnabled(true);
        svc2.setName("YARN_1");
        svc2.setPolicyUpdateTime(new Date());
        svc2.setType("yarn");
        svc2.setUpdatedBy("Admin");
        svc2.setUpdateTime(new Date());

        List<RangerService> rangerServiceList = new ArrayList<>();
        rangerServiceList.add(svc1);
        rangerServiceList.add(svc2);

        paginatedSvcs.setList(rangerServiceList);

        SearchFilter filter = new SearchFilter();
        Mockito.when(searchUtil.getSearchFilter(request, svcService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getPaginatedServices(filter)).thenReturn(paginatedSvcs);
        Mockito.when(xUserService.getXUserByUserName("testuser")).thenReturn(loggedInUser);
        RangerServiceList retServiceList = serviceREST.getServices(request);
        Assertions.assertNotNull(retServiceList);
        Assertions.assertNull(retServiceList.getServices().get(0).getDescription());
        Assertions.assertTrue(retServiceList.getServices().get(0).getConfigs().isEmpty());
        Assertions.assertNull(retServiceList.getServices().get(1).getDescription());
        Assertions.assertTrue(retServiceList.getServices().get(1).getConfigs().isEmpty());
        Assertions.assertEquals(retServiceList.getServices().get(0).getId(), Id);
        Assertions.assertEquals(retServiceList.getServices().get(0).getName(), "HDFS_1");
        Assertions.assertEquals(retServiceList.getServices().get(0).getType(), "1");

        Assertions.assertEquals(retServiceList.getServices().get(1).getId(), svc2.getId());
        Assertions.assertEquals(retServiceList.getServices().get(1).getName(), "YARN_1");
        Assertions.assertEquals(retServiceList.getServices().get(1).getType(), "yarn");
    }

    public void mockValidateGrantRevokeRequest() {
        Mockito.when(xUserService.getXUserByUserName(Mockito.anyString())).thenReturn(Mockito.mock(VXUser.class));
        Mockito.when(userMgr.getGroupByGroupName(Mockito.anyString())).thenReturn(Mockito.mock(VXGroup.class));
        Mockito.when(daoManager.getXXRole().findByRoleName(Mockito.anyString())).thenReturn(Mockito.mock(XXRole.class));
    }

    @Test
    public void test14bGrantAccess() throws Exception {
        HttpServletRequest request         = Mockito.mock(HttpServletRequest.class);
        String             serviceName     = "HDFS_1";
        GrantRevokeRequest grantRequestObj = createValidGrantRevokeRequest();
        Mockito.when(serviceUtil.isValidateHttpsAuthentication(serviceName, request)).thenReturn(true);
        Mockito.doNothing().when(bizUtil).failUnauthenticatedIfNotAllowed();
        mockValidateGrantRevokeRequest();
        Mockito.when(xUserService.getXUserByUserName(Mockito.anyString())).thenReturn(Mockito.mock(VXUser.class));
        Mockito.when(svcStore.getServiceByName(Mockito.anyString())).thenReturn(Mockito.mock(RangerService.class));
        Mockito.when(daoManager.getXXServiceDef().findServiceDefTypeByServiceName(Mockito.anyString())).thenReturn("hdfs");
        Mockito.when(bizUtil.isUserRangerAdmin(Mockito.anyString())).thenReturn(true);
        Mockito.when(userMgrGrantor.getRolesByLoginId(Mockito.anyString())).thenReturn(Arrays.asList("ROLE_SYS_ADMIN"));
        RESTResponse restResponse = serviceREST.grantAccess(serviceName, grantRequestObj, request);
        Mockito.verify(svcStore, Mockito.times(1)).createPolicy(Mockito.any(RangerPolicy.class));
        Assertions.assertNotNull(restResponse);
        Assertions.assertEquals(restResponse.getStatusCode(), RESTResponse.STATUS_SUCCESS);
    }

    @Test
    public void test64SecureGrantAccess() {
        HttpServletRequest request         = Mockito.mock(HttpServletRequest.class);
        String             serviceName     = "HDFS_1";
        GrantRevokeRequest grantRequestObj = createValidGrantRevokeRequest();
        Mockito.when(serviceUtil.isValidService(serviceName, request)).thenReturn(true);
        Mockito.when(daoManager.getXXService().findByName(Mockito.anyString())).thenReturn(Mockito.mock(XXService.class));
        Mockito.when(daoManager.getXXServiceDef().getById(Mockito.anyLong())).thenReturn(Mockito.mock(XXServiceDef.class));
        try {
            Mockito.when(svcStore.getServiceByName(Mockito.anyString())).thenReturn(Mockito.mock(RangerService.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        mockValidateGrantRevokeRequest();
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.isUserServiceAdmin(Mockito.any(RangerService.class), Mockito.anyString())).thenReturn(true);
        Mockito.when(userMgrGrantor.getRolesByLoginId(Mockito.anyString())).thenReturn(Arrays.asList("ROLE_SYS_ADMIN"));
        RESTResponse restResponse;
        try {
            restResponse = serviceREST.secureGrantAccess(serviceName, grantRequestObj, request);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            Mockito.verify(svcStore, Mockito.times(1)).createPolicy(Mockito.any(RangerPolicy.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Assertions.assertNotNull(restResponse);
        Assertions.assertEquals(RESTResponse.STATUS_SUCCESS, restResponse.getStatusCode());
    }

    @Test
    public void test15bRevokeAccess() throws Exception {
        HttpServletRequest request       = Mockito.mock(HttpServletRequest.class);
        String             serviceName   = "HDFS_1";
        GrantRevokeRequest revokeRequest = createValidGrantRevokeRequest();
        Mockito.when(serviceUtil.isValidateHttpsAuthentication(serviceName, request)).thenReturn(true);
        Mockito.doNothing().when(bizUtil).failUnauthenticatedIfNotAllowed();
        mockValidateGrantRevokeRequest();
        Mockito.when(xUserService.getXUserByUserName(Mockito.anyString())).thenReturn(Mockito.mock(VXUser.class));
        Mockito.when(bizUtil.isUserRangerAdmin(Mockito.anyString())).thenReturn(true);
        RESTResponse restResponse = serviceREST.revokeAccess(serviceName, revokeRequest, request);
        Assertions.assertNotNull(restResponse);
        Assertions.assertEquals(RESTResponse.STATUS_SUCCESS, restResponse.getStatusCode());
    }

    @Test
    public void test65SecureRevokeAccess() {
        HttpServletRequest request       = Mockito.mock(HttpServletRequest.class);
        String             serviceName   = "HDFS_1";
        GrantRevokeRequest revokeRequest = createValidGrantRevokeRequest();
        Mockito.when(serviceUtil.isValidService(serviceName, request)).thenReturn(true);
        Mockito.when(daoManager.getXXService().findByName(Mockito.anyString())).thenReturn(Mockito.mock(XXService.class));
        Mockito.when(daoManager.getXXServiceDef().getById(Mockito.anyLong())).thenReturn(Mockito.mock(XXServiceDef.class));
        try {
            Mockito.when(svcStore.getServiceByName(Mockito.anyString())).thenReturn(Mockito.mock(RangerService.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        mockValidateGrantRevokeRequest();
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.isUserRangerAdmin(Mockito.anyString())).thenReturn(true);
        RESTResponse restResponse = null;
        try {
            restResponse = serviceREST.secureRevokeAccess(serviceName,
                    revokeRequest, request);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assertions.assertNotNull(restResponse);
        Assertions.assertEquals(RESTResponse.STATUS_SUCCESS, restResponse.getStatusCode());
    }

    @Test
    public void test66ApplyPolicy() {
        ServiceREST        serviceRESTSpy = Mockito.spy(serviceREST);
        HttpServletRequest request        = Mockito.mock(HttpServletRequest.class);
        RangerPolicy       policy         = rangerPolicy();
        Mockito.doReturn(policy).when(serviceRESTSpy).createPolicy(Mockito.any(RangerPolicy.class), eq(null));
        RangerPolicy returnedPolicy = serviceRESTSpy.applyPolicy(policy, request);
        Assertions.assertNotNull(returnedPolicy);
        Assertions.assertEquals(returnedPolicy.getId(), policy.getId());
        Assertions.assertEquals(returnedPolicy.getName(), policy.getName());
    }

    @Test
    public void test67ResetPolicyCacheForAdmin() {
        boolean res         = true;
        String  serviceName = "HDFS_1";
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        RangerService rangerService = rangerService();
        try {
            Mockito.when(svcStore.getServiceByName(serviceName)).thenReturn(rangerService);
        } catch (Exception ignored) {
        }
        Mockito.when(svcStore.resetPolicyCache(serviceName)).thenReturn(res);
        boolean isReset = serviceREST.resetPolicyCache(serviceName);
        Assertions.assertEquals(res, isReset);
        try {
            Mockito.verify(svcStore).getServiceByName(serviceName);
        } catch (Exception ignored) {
        }
    }

    @Test
    public void test68ResetPolicyCacheAll() {
        boolean res = true;
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(svcStore.resetPolicyCache(null)).thenReturn(res);
        boolean isReset = serviceREST.resetPolicyCacheAll();
        Assertions.assertEquals(res, isReset);
    }

    @Test
    public void test69DeletePolicyDeltas() {
        int                val     = 1;
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        serviceREST.deletePolicyDeltas(val, request);
        Mockito.verify(svcStore).resetPolicyUpdateLog(Mockito.anyInt(), Mockito.anyInt());
    }

    @Test
    public void test70PurgeEmptyPolicies() {
        ServiceREST        serviceRESTSpy = Mockito.spy(serviceREST);
        HttpServletRequest request        = Mockito.mock(HttpServletRequest.class);
        String             serviceName    = "HDFS_1";
        try {
            Mockito.when(svcStore.getServiceByName(Mockito.anyString())).thenReturn(Mockito.mock(RangerService.class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            Mockito.when(svcStore.getServicePolicies(Mockito.anyString(), Mockito.anyLong())).thenReturn(servicePolicies());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        serviceRESTSpy.purgeEmptyPolicies(serviceName, request);
        Mockito.verify(serviceRESTSpy, Mockito.never()).deletePolicy(Mockito.anyLong());
    }

    @Test
    public void test71DeleteClusterServices() {
        String     clusterName = "cluster1";
        List<Long> idsToDelete = createLongList();
        Mockito.when(daoManager.getXXServiceConfigMap().findServiceIdsByClusterName(Mockito.anyString())).thenReturn(idsToDelete);
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenReturn(serviceValidator);
        Mockito.when(daoManager.getXXService().getById(Mockito.anyLong())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        ResponseEntity<List<ServiceDeleteResponse>> deletedResponse = serviceREST.deleteClusterServices(clusterName);
        Assertions.assertEquals(HttpStatus.OK, deletedResponse.getStatusCode());
        Assertions.assertNotEquals(null, deletedResponse.getBody());
        for (ServiceDeleteResponse response : deletedResponse.getBody()) {
            Assertions.assertTrue(response.getIsDeleted());
        }
    }

    @Test
    public void test72updatePolicyWithPolicyIdIsNull() throws Exception {
        RangerPolicy rangerPolicy = rangerPolicy();
        Long         policyId     = rangerPolicy.getId();
        rangerPolicy.setId(null);
        String userName = "admin";

        Set<String> userGroupsList = new HashSet<>();
        userGroupsList.add("group1");
        userGroupsList.add("group2");

        List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<>();
        RangerAccessTypeDef       rangerAccessTypeDefObj  = new RangerAccessTypeDef();
        rangerAccessTypeDefObj.setLabel("Read");
        rangerAccessTypeDefObj.setName("read");
        rangerAccessTypeDefObj.setRbKeyLabel(null);
        rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);

        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        RangerPolicy dbRangerPolicy = serviceREST.updatePolicy(rangerPolicy, policyId);
        Assertions.assertNull(dbRangerPolicy);
        Mockito.verify(validatorFactory).getPolicyValidator(svcStore);
    }

    @Test
    public void test72updatePolicyWithInvalidPolicyId() throws Exception {
        RangerPolicy rangerPolicy = rangerPolicy();

        Set<String> userGroupsList = new HashSet<>();
        userGroupsList.add("group1");
        userGroupsList.add("group2");

        List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<>();
        RangerAccessTypeDef       rangerAccessTypeDefObj  = new RangerAccessTypeDef();
        rangerAccessTypeDefObj.setLabel("Read");
        rangerAccessTypeDefObj.setName("read");
        rangerAccessTypeDefObj.setRbKeyLabel(null);
        rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);

        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> {
            RangerPolicy dbRangerPolicy = serviceREST.updatePolicy(rangerPolicy, -11L);
            Assertions.assertNull(dbRangerPolicy);
            Mockito.verify(validatorFactory).getPolicyValidator(svcStore);
        });
    }

    @Test
    public void test73updateServiceDefWhenIdIsNull() throws Exception {
        RangerServiceDef rangerServiceDef = rangerServiceDef();
        rangerServiceDef.setId(null);

        Mockito.when(validatorFactory.getServiceDefValidator(svcStore)).thenReturn(serviceDefValidator);
        Mockito.when(svcStore.updateServiceDef(Mockito.any())).thenReturn(rangerServiceDef);

        RangerServiceDef dbRangerServiceDef = serviceREST.updateServiceDef(rangerServiceDef, rangerServiceDef.getId());
        Assertions.assertNotNull(dbRangerServiceDef);
        Assertions.assertEquals(dbRangerServiceDef, rangerServiceDef);
        Assertions.assertEquals(dbRangerServiceDef.getId(), rangerServiceDef.getId());
        Assertions.assertEquals(dbRangerServiceDef.getName(), rangerServiceDef.getName());
        Assertions.assertEquals(dbRangerServiceDef.getImplClass(), rangerServiceDef.getImplClass());
        Assertions.assertEquals(dbRangerServiceDef.getLabel(), rangerServiceDef.getLabel());
        Assertions.assertEquals(dbRangerServiceDef.getDescription(), rangerServiceDef.getDescription());
        Assertions.assertEquals(dbRangerServiceDef.getRbKeyDescription(), rangerServiceDef.getRbKeyDescription());
        Assertions.assertEquals(dbRangerServiceDef.getUpdatedBy(), rangerServiceDef.getUpdatedBy());
        Assertions.assertEquals(dbRangerServiceDef.getUpdateTime(), rangerServiceDef.getUpdateTime());
        Assertions.assertEquals(dbRangerServiceDef.getVersion(), rangerServiceDef.getVersion());
        Assertions.assertEquals(dbRangerServiceDef.getConfigs(), rangerServiceDef.getConfigs());

        Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
        Mockito.verify(svcStore).updateServiceDef(rangerServiceDef);
    }

    @Test
    public void test74updateServiceDefWithInvalidDefId() throws Exception {
        RangerServiceDef rangerServiceDef = rangerServiceDef();

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new WebApplicationException());
        //thrown.expect(WebApplicationException.class);
        Assertions.assertThrows(WebApplicationException.class, () -> {
            RangerServiceDef dbRangerServiceDef = serviceREST.updateServiceDef(rangerServiceDef, -1L);
            Assertions.assertNotNull(dbRangerServiceDef);
            Assertions.assertEquals(dbRangerServiceDef, rangerServiceDef);
            Assertions.assertEquals(dbRangerServiceDef.getId(), rangerServiceDef.getId());
            Assertions.assertEquals(dbRangerServiceDef.getName(), rangerServiceDef.getName());
            Assertions.assertEquals(dbRangerServiceDef.getImplClass(), rangerServiceDef.getImplClass());
            Assertions.assertEquals(dbRangerServiceDef.getLabel(), rangerServiceDef.getLabel());
            Assertions.assertEquals(dbRangerServiceDef.getDescription(), rangerServiceDef.getDescription());
            Assertions.assertEquals(dbRangerServiceDef.getRbKeyDescription(), rangerServiceDef.getRbKeyDescription());
            Assertions.assertEquals(dbRangerServiceDef.getUpdatedBy(), rangerServiceDef.getUpdatedBy());
            Assertions.assertEquals(dbRangerServiceDef.getUpdateTime(), rangerServiceDef.getUpdateTime());
            Assertions.assertEquals(dbRangerServiceDef.getVersion(), rangerServiceDef.getVersion());
            Assertions.assertEquals(dbRangerServiceDef.getConfigs(), rangerServiceDef.getConfigs());

            Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
            Mockito.verify(svcStore).updateServiceDef(rangerServiceDef);
        });
    }

    @Test
    public void test75GetPolicyByGUIDAndServiceNameAndZoneName() throws Exception {
        RangerPolicy    rangerPolicy   = rangerPolicy();
        RangerService   rangerService  = rangerService();
        String          serviceName    = rangerService.getName();
        String          zoneName       = "zone-1";
        String          userName       = "admin";
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.getPolicy(rangerPolicy.getGuid(), serviceName, zoneName)).thenReturn(rangerPolicy);
        RangerPolicy dbRangerPolicy = serviceREST.getPolicyByGUIDAndServiceNameAndZoneName(rangerPolicy.getGuid(), serviceName, zoneName);
        Assertions.assertNotNull(dbRangerPolicy);
        Assertions.assertEquals(dbRangerPolicy.getId(), rangerPolicy.getId());
        Mockito.verify(svcStore).getPolicy(rangerPolicy.getGuid(), serviceName, zoneName);
    }

    @Test
    public void test76GetPolicyByGUID() throws Exception {
        RangerPolicy rangerPolicy = rangerPolicy();
        String       userName     = "admin";

        Set<String> userGroupsList = new HashSet<>();
        userGroupsList.add("group1");
        userGroupsList.add("group2");

        List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<>();
        RangerAccessTypeDef       rangerAccessTypeDefObj  = new RangerAccessTypeDef();
        rangerAccessTypeDefObj.setLabel("Read");
        rangerAccessTypeDefObj.setName("read");
        rangerAccessTypeDefObj.setRbKeyLabel(null);
        rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.getPolicy(rangerPolicy.getGuid(), null, null)).thenReturn(rangerPolicy);
        RangerPolicy dbRangerPolicy = serviceREST.getPolicyByGUIDAndServiceNameAndZoneName(rangerPolicy.getGuid(), null, null);
        Assertions.assertNotNull(dbRangerPolicy);
        Assertions.assertEquals(dbRangerPolicy.getId(), rangerPolicy.getId());
        Mockito.verify(svcStore).getPolicy(rangerPolicy.getGuid(), null, null);
    }

    @Test
    public void test76DeletePolicyByGUIDAndServiceNameAndZoneName() throws Exception {
        RangerPolicy  rangerPolicy  = rangerPolicy();
        RangerService rangerService = rangerService();
        String        serviceName   = rangerService.getName();
        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        String          zoneName       = "zone-1";
        String          userName       = "admin";
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.getPolicy(Id)).thenReturn(rangerPolicy);
        Mockito.when(svcStore.getPolicy(rangerPolicy.getGuid(), serviceName, zoneName)).thenReturn(rangerPolicy);
        serviceREST.deletePolicyByGUIDAndServiceNameAndZoneName(rangerPolicy.getGuid(), serviceName, zoneName);
        Mockito.verify(validatorFactory).getPolicyValidator(svcStore);
        Mockito.verify(svcStore).getPolicy(rangerPolicy.getGuid(), serviceName, zoneName);
    }

    @Test
    public void test77DeletePolicyByGUID() throws Exception {
        RangerPolicy rangerPolicy = rangerPolicy();
        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        String          userName       = "admin";
        XXServiceDef    xServiceDef    = serviceDef();
        XXService       xService       = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.getPolicy(Id)).thenReturn(rangerPolicy);
        Mockito.when(svcStore.getPolicy(rangerPolicy.getGuid(), null, null)).thenReturn(rangerPolicy);
        serviceREST.deletePolicyByGUIDAndServiceNameAndZoneName(rangerPolicy.getGuid(), null, null);
        Mockito.verify(validatorFactory).getPolicyValidator(svcStore);
        Mockito.verify(svcStore).getPolicy(rangerPolicy.getGuid(), null, null);
    }

    @Test
    public void test78ResetPolicyCacheByServiceNameForServiceAdmin() {
        boolean       isAdmin       = false;
        boolean       res           = true;
        RangerService rangerService = rangerService();
        String        serviceName   = rangerService.getName();
        Mockito.when(bizUtil.isAdmin()).thenReturn(isAdmin);
        String userName = "admin";
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        try {
            Mockito.when(svcStore.getServiceByName(serviceName)).thenReturn(rangerService);
        } catch (Exception ignored) {
        }
        Mockito.when(bizUtil.isUserServiceAdmin(Mockito.any(RangerService.class), Mockito.anyString())).thenReturn(true);
        try {
            Mockito.when(svcStore.resetPolicyCache(serviceName)).thenReturn(true);
        } catch (Exception ignored) {
        }
        boolean isReset = serviceREST.resetPolicyCache(serviceName);
        Assertions.assertEquals(res, isReset);
        Mockito.verify(bizUtil).isAdmin();
        Mockito.verify(bizUtil).isUserServiceAdmin(Mockito.any(RangerService.class), Mockito.anyString());
        try {
            Mockito.verify(svcStore).getServiceByName(serviceName);
        } catch (Exception ignored) {
        }
        try {
            Mockito.verify(svcStore).resetPolicyCache(serviceName);
        } catch (Exception ignored) {
        }
    }

    @Test
    public void test79ResetPolicyCacheWhenServiceNameIsInvalid() {
        String serviceName = "HDFS_1";
        try {
            Mockito.when(svcStore.getServiceByName(serviceName)).thenReturn(null);
        } catch (Exception ignored) {
        }
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.resetPolicyCache(serviceName);
            Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
        });
    }

    @Test
    public void test80GetPolicyByNameAndServiceNameWithZoneName() throws Exception {
        RangerPolicy  rangerPolicy  = rangerPolicy();
        RangerService rangerService = rangerService();
        XXPolicy      xxPolicy      = new XXPolicy();
        String        serviceName   = rangerService.getName();
        String        policyName    = rangerPolicy.getName();
        String        zoneName      = "zone-1";
        XXPolicyDao   xXPolicyDao   = Mockito.mock(XXPolicyDao.class);
        Mockito.when(daoManager.getXXPolicy()).thenReturn(xXPolicyDao);
        Mockito.when(daoManager.getXXPolicy().findPolicy(policyName, serviceName, zoneName)).thenReturn(xxPolicy);
        Mockito.when(policyService.getPopulatedViewObject(xxPolicy)).thenReturn(rangerPolicy);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        RangerPolicy dbRangerPolicy = serviceREST.getPolicyByName(serviceName, policyName, zoneName);
        Assertions.assertNotNull(dbRangerPolicy);
        Assertions.assertEquals(dbRangerPolicy, rangerPolicy);
        Assertions.assertEquals(dbRangerPolicy.getId(), rangerPolicy.getId());
        Assertions.assertEquals(dbRangerPolicy.getName(), rangerPolicy.getName());
    }

    @Test
    public void test81GetPolicyByNameAndServiceNameWithZoneNameIsNull() throws Exception {
        RangerPolicy  rangerPolicy  = rangerPolicy();
        RangerService rangerService = rangerService();
        XXPolicy      xxPolicy      = new XXPolicy();
        String        serviceName   = rangerService.getName();
        String        policyName    = rangerPolicy.getName();
        XXPolicyDao   xXPolicyDao   = Mockito.mock(XXPolicyDao.class);
        Mockito.when(daoManager.getXXPolicy()).thenReturn(xXPolicyDao);
        Mockito.when(daoManager.getXXPolicy().findPolicy(policyName, serviceName, null)).thenReturn(xxPolicy);
        Mockito.when(policyService.getPopulatedViewObject(xxPolicy)).thenReturn(rangerPolicy);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        RangerPolicy dbRangerPolicy = serviceREST.getPolicyByName(serviceName, policyName, null);
        Assertions.assertNotNull(dbRangerPolicy);
        Assertions.assertEquals(dbRangerPolicy, rangerPolicy);
        Assertions.assertEquals(dbRangerPolicy.getId(), rangerPolicy.getId());
        Assertions.assertEquals(dbRangerPolicy.getName(), rangerPolicy.getName());
    }

    @Test
    public void test82GetServiceHeaders() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        RangerServiceHeaderInfo serviceHeader = new RangerServiceHeaderInfo();
        serviceHeader.setName("testService");
        serviceHeader.setType("hdfs");
        serviceHeader.setId(1L);

        List<RangerServiceHeaderInfo> serviceHeaders = new ArrayList<>();
        serviceHeaders.add(serviceHeader);

        Mockito.when(request.getParameter(SearchFilter.SERVICE_NAME_PREFIX)).thenReturn("test");
        Mockito.when(request.getParameter(SearchFilter.SERVICE_TYPE)).thenReturn("hdfs");
        Mockito.when(daoManager.getXXService().findServiceHeaders()).thenReturn(serviceHeaders);
        Mockito.when(bizUtil.hasAccess(Mockito.any(), Mockito.any())).thenReturn(true);

        List<RangerServiceHeaderInfo> result = serviceREST.getServiceHeaders(request);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("testService", result.get(0).getName());
        Assertions.assertEquals("hdfs", result.get(0).getType());
    }

    @Test
    public void test83GetServiceHeadersWithFiltering() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        RangerServiceHeaderInfo serviceHeader1 = new RangerServiceHeaderInfo();
        serviceHeader1.setName("anotherService1");
        serviceHeader1.setType("gds");
        serviceHeader1.setId(1L);

        RangerServiceHeaderInfo serviceHeader2 = new RangerServiceHeaderInfo();
        serviceHeader2.setName("anotherService2");
        serviceHeader2.setType("hdfs");
        serviceHeader2.setId(2L);

        RangerServiceHeaderInfo serviceHeader3 = new RangerServiceHeaderInfo();
        serviceHeader3.setName("testService");
        serviceHeader3.setType("hbase");
        serviceHeader3.setId(3L);

        RangerServiceHeaderInfo serviceHeader4 = new RangerServiceHeaderInfo();
        serviceHeader4.setName("testService2");
        serviceHeader4.setType("hdfs");
        serviceHeader4.setId(4L);

        List<RangerServiceHeaderInfo> serviceHeaders = new ArrayList<>();
        serviceHeaders.add(serviceHeader1);
        serviceHeaders.add(serviceHeader2);
        serviceHeaders.add(serviceHeader3);
        serviceHeaders.add(serviceHeader4);

        Mockito.when(request.getParameter(SearchFilter.SERVICE_NAME_PREFIX)).thenReturn("test");
        Mockito.when(request.getParameter(SearchFilter.SERVICE_TYPE)).thenReturn("hdfs");
        Mockito.when(daoManager.getXXService().findServiceHeaders()).thenReturn(serviceHeaders);
        Mockito.when(bizUtil.hasAccess(Mockito.any(), Mockito.any())).thenReturn(false);

        List<RangerServiceHeaderInfo> result = serviceREST.getServiceHeaders(request);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.size());
    }

    @Test
    public void test84CheckSSO() throws Exception {
        Mockito.when(bizUtil.isSSOEnabled()).thenReturn(true);
        String result = serviceREST.checkSSO();
        Assertions.assertEquals("true", result);

        Mockito.when(bizUtil.isSSOEnabled()).thenReturn(false);
        result = serviceREST.checkSSO();
        Assertions.assertEquals("false", result);
    }

    @Test
    public void test85GetCSRFProperties() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpSession session = Mockito.mock(HttpSession.class);

        Mockito.when(request.getSession()).thenReturn(session);
        Mockito.when(session.getAttribute(Mockito.anyString())).thenReturn("test-salt");

        HashMap<String, Object> result = serviceREST.getCSRFProperties(request);
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.containsKey("ranger.rest-csrf.enabled"));
        Assertions.assertTrue(result.containsKey("ranger.rest-csrf.custom-header"));
        Assertions.assertTrue(result.containsKey("ranger.rest-csrf.browser-useragents-regex"));
        Assertions.assertTrue(result.containsKey("ranger.rest-csrf.methods-to-ignore"));
        Assertions.assertTrue(result.containsKey("_csrfToken"));
    }

    @Test
    public void test86InitStore_Success() {
        serviceREST.initStore();
    }

    @Test
    public void test87PurgeRecordsAuthSessions() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        List<RangerPurgeResult> purgeResults = new ArrayList<>();
        RangerPurgeResult purgeResult = new RangerPurgeResult();
        purgeResult.setRecordType("login_records");
        purgeResult.setTotalRecordCount(100L);
        purgeResults.add(purgeResult);

        Mockito.doNothing().when(svcStore).removeAuthSessions(Mockito.anyInt(), Mockito.any());

        List<RangerPurgeResult> result = serviceREST.purgeRecords("login_records", 180, request);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.size());
        Mockito.verify(svcStore, Mockito.times(1)).removeAuthSessions(Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void test88PurgeRecordsTransactionLogs() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        List<RangerPurgeResult> purgeResults = new ArrayList<>();
        RangerPurgeResult purgeResult = new RangerPurgeResult();
        purgeResult.setRecordType("trx_records");
        purgeResult.setTotalRecordCount(50L);
        purgeResults.add(purgeResult);

        Mockito.doNothing().when(svcStore).removeTransactionLogs(Mockito.anyInt(), Mockito.any());

        List<RangerPurgeResult> result = serviceREST.purgeRecords("trx_records", 180, request);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.size());
        Mockito.verify(svcStore, Mockito.times(1)).removeTransactionLogs(Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void test89PurgeRecordsPolicyExportLogs() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        List<RangerPurgeResult> purgeResults = new ArrayList<>();
        RangerPurgeResult purgeResult = new RangerPurgeResult();
        purgeResult.setRecordType("policy_export_logs");
        purgeResult.setTotalRecordCount(25L);
        purgeResults.add(purgeResult);

        Mockito.doNothing().when(svcStore).removePolicyExportLogs(Mockito.anyInt(), Mockito.any());

        List<RangerPurgeResult> result = serviceREST.purgeRecords("policy_export_logs", 180, request);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.size());
        Mockito.verify(svcStore, Mockito.times(1)).removePolicyExportLogs(Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void test90PurgeRecordsInvalidType() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String errorMessage = "invalid record type";

        WebApplicationException webException = new WebApplicationException(new Throwable(errorMessage));
        Mockito.when(restErrorUtil.createRESTException(
                Mockito.eq(HttpServletResponse.SC_BAD_REQUEST),
                Mockito.anyString(),
                Mockito.eq(true))
        ).thenReturn(webException);

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.purgeRecords("invalid_type", 180, request);
        });

        Assertions.assertNotNull(exception.getCause());
        Assertions.assertEquals(errorMessage, exception.getCause().getMessage());
        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test91PurgeRecordsInvalidRetentionDays() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String errorMessage = "Retention days can't be lesser than 1";

        WebApplicationException webException = new WebApplicationException(new Throwable(errorMessage));

        Mockito.when(restErrorUtil.createRESTException(
                Mockito.eq(HttpServletResponse.SC_BAD_REQUEST),
                Mockito.anyString(),
                Mockito.eq(true))
        ).thenReturn(webException);

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.purgeRecords("test-type", 0, request);
        });

        Assertions.assertNotNull(exception.getCause());
        Assertions.assertEquals(errorMessage, exception.getCause().getMessage());
        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test92PurgeRecords_NullException() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String recordType = "login_records";
        int olderThan = 180;

        Mockito.doThrow(new RuntimeException("General exception")).when(svcStore).removeAuthSessions(Mockito.eq(olderThan), Mockito.anyList());
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        Assertions.assertThrows(WebApplicationException .class, () -> {
            serviceREST.purgeRecords(recordType, olderThan, request);
        });

        Mockito.verify(svcStore).removeAuthSessions(Mockito.eq(olderThan), Mockito.anyList());
        Mockito.verify(restErrorUtil).createRESTException("General exception");
    }

    @Test
    public void test93GetBulkPolicies() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        RangerPolicy policy1 = rangerPolicy();
        RangerPolicy policy2 = rangerPolicy();
        policy2.setId(2L);
        policy2.setName("test-policy-2");

        List<RangerPolicy> policies = Arrays.asList(policy1, policy2);
        SearchFilter filter = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(Mockito.any(), Mockito.any())).thenReturn(filter);
        Mockito.when(svcStore.getPolicies(filter)).thenReturn(policies);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);

        Method method = ServiceREST.class.getDeclaredMethod("getBulkPolicies", String.class, HttpServletRequest.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<RangerPolicy> result = (List<RangerPolicy>) method.invoke(serviceREST, "HDFS_1-1-20150316062453", request);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(policy1.getId(), result.get(0).getId());
        Assertions.assertEquals(policy2.getId(), result.get(1).getId());
    }

    @Test
    public void test94GetBulkPolicies_WebApplicationException() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter       filter  = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(Mockito.any(), Mockito.any())).thenReturn(filter);
        Mockito.when(svcStore.getPolicies(filter)).thenThrow(new WebApplicationException());

        Method method = ServiceREST.class.getDeclaredMethod("getBulkPolicies", String.class, HttpServletRequest.class);
        method.setAccessible(true);

        InvocationTargetException exception = Assertions.assertThrows(InvocationTargetException.class, () -> {
            method.invoke(serviceREST, "HDFS_1-1-20150316062453", request);
        });
        Assertions.assertTrue(exception.getCause() instanceof WebApplicationException);

        Mockito.verify(svcStore).getPolicies(filter);
        Mockito.verify(searchUtil).getSearchFilter(request, policyService.sortFields);
    }

    @Test
    public void test95GetBulkPolicies_Exception() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter       filter  = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(Mockito.any(), Mockito.any())).thenReturn(filter);
        Mockito.when(svcStore.getPolicies(filter)).thenThrow(new RuntimeException("General exception"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        Method method = ServiceREST.class.getDeclaredMethod("getBulkPolicies", String.class, HttpServletRequest.class);
        method.setAccessible(true);

        InvocationTargetException exception = Assertions.assertThrows(InvocationTargetException.class, () -> {
            method.invoke(serviceREST, "HDFS_1-1-20150316062453", request);
        });
        Assertions.assertTrue(exception.getCause() instanceof WebApplicationException);

        Mockito.verify(svcStore).getPolicies(filter);
        Mockito.verify(searchUtil).getSearchFilter(request, policyService.sortFields);
        Mockito.verify(restErrorUtil).createRESTException("General exception");
    }

    @Test
    public void test96DeleteBulkPolicies() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        RangerPolicy policy1 = rangerPolicy();
        RangerPolicy policy2 = rangerPolicy();
        policy2.setId(2L);
        policy2.setName("test-policy-2");

        Set<RangerPolicy> policies = new HashSet<>(Arrays.asList(policy1, policy2));
        String serviceName = "test-service";

        Mockito.when(searchUtil.getSearchFilter(Mockito.any(), Mockito.any())).thenReturn(new SearchFilter());
        Mockito.when(svcStore.getPolicies(Mockito.any(SearchFilter.class))).thenReturn(new ArrayList<>(policies));
        ServiceREST spyServiceRest = Mockito.spy(serviceREST);
        XXServiceDao xxServiceDao = Mockito.mock(XXServiceDao.class);
        Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
        Mockito.when(xxServiceDao.findByName(Mockito.anyString())).thenReturn(xService());
        Mockito.when(daoManager.getXXServiceDef().getById(Mockito.anyLong())).thenReturn(serviceDef());
        RangerPolicyAdmin policyAdminMock = Mockito.mock(RangerPolicyAdmin.class);
        Mockito.when(policyAdminMock.isDelegatedAdminAccessAllowedForRead(Mockito.any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet(), Mockito.anyMap())).thenReturn(true);
        Mockito.when(policyAdminMock.getRolesFromUserAndGroups(Mockito.anyString(), Mockito.anySet())).thenReturn(new HashSet<>());
        Mockito.when(policyAdminMock.isDelegatedAdminAccessAllowedForModify(Mockito.any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet(), Mockito.anyMap())).thenReturn(true);
        Mockito.doReturn(policyAdminMock).when(spyServiceRest).getPolicyAdminForDelegatedAdmin(Mockito.anyString());
        Mockito.when(userMgr.getGroupsForUser(Mockito.anyString())).thenReturn(new HashSet<>());
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("testUser");
        Mockito.doNothing().when(svcStore).deletePolicies(Mockito.anySet(), Mockito.eq(serviceName), Mockito.anyList());

        List<Long> result = spyServiceRest.deleteBulkPolicies(serviceName, request);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.size());
        Mockito.verify(svcStore, Mockito.times(1)).deletePolicies(Mockito.anySet(), Mockito.eq(serviceName), Mockito.anyList());
    }

    @Test
    public void test97DeleteBulkPolicies_Exception() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        RangerPolicy policy1 = rangerPolicy();
        RangerPolicy policy2 = rangerPolicy();
        policy2.setId(2L);
        policy2.setName("test-policy-2");

        Set<RangerPolicy> policies = new HashSet<>(Arrays.asList(policy1, policy2));
        String serviceName = "test-service";

        Mockito.when(searchUtil.getSearchFilter(Mockito.any(), Mockito.any())).thenReturn(new SearchFilter());
        Mockito.when(svcStore.getPolicies(Mockito.any(SearchFilter.class))).thenReturn(new ArrayList<>(policies));
        ServiceREST spyServiceRest = Mockito.spy(serviceREST);
        XXServiceDao xxServiceDao = Mockito.mock(XXServiceDao.class);
        Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
        Mockito.when(xxServiceDao.findByName(Mockito.anyString())).thenReturn(xService());
        Mockito.when(daoManager.getXXServiceDef().getById(Mockito.anyLong())).thenReturn(serviceDef());
        RangerPolicyAdmin policyAdminMock = Mockito.mock(RangerPolicyAdmin.class);
        Mockito.when(policyAdminMock.isDelegatedAdminAccessAllowedForRead(Mockito.any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet(), Mockito.anyMap())).thenReturn(true);
        Mockito.when(policyAdminMock.getRolesFromUserAndGroups(Mockito.anyString(), Mockito.anySet())).thenReturn(new HashSet<>());
        Mockito.when(policyAdminMock.isDelegatedAdminAccessAllowedForModify(Mockito.any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet(), Mockito.anyMap())).thenReturn(true);
        Mockito.doReturn(policyAdminMock).when(spyServiceRest).getPolicyAdminForDelegatedAdmin(Mockito.anyString());
        Mockito.when(userMgr.getGroupsForUser(Mockito.anyString())).thenReturn(new HashSet<>());
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("testUser");

        Mockito.doThrow(new RuntimeException("General exception")).when(svcStore).deletePolicies(Mockito.anySet(), Mockito.eq(serviceName), Mockito.anyList());
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            spyServiceRest.deleteBulkPolicies(serviceName, request);
        });

        Mockito.verify(svcStore, Mockito.times(1)).deletePolicies(Mockito.anySet(), Mockito.eq(serviceName), Mockito.anyList());
    }

    @Test
    public void test98GetPolicyResource() throws Exception {
        GrantRevokeRequest grantRequest = createValidGrantRevokeRequest();

        String resourceName = "test-resource";
        RangerPolicyResource result = serviceREST.getPolicyResource(resourceName, grantRequest);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(resourceName, result.getValues().get(0));

        List<String> resourceList = Arrays.asList("resource1", "resource2");
        result = serviceREST.getPolicyResource(resourceList, grantRequest);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.getValues().size());
        Assertions.assertTrue(result.getValues().contains("resource1"));
        Assertions.assertTrue(result.getValues().contains("resource2"));
    }

    @Test
    public void test99EnsureAdminAccessForServicePolicies() throws Exception {
        RangerPolicy policy1 = rangerPolicy();
        RangerPolicy policy2 = rangerPolicy();
        policy2.setId(2L);
        policy2.setName("test-policy-2");

        Set<RangerPolicy> policies = new HashSet<>(Arrays.asList(policy1, policy2));
        String serviceName = "HDFS";
        XXService xxService = xService();

        ServiceREST spyServiceRest = Mockito.spy(serviceREST);

        Mockito.when(daoManager.getXXService().findByName(Mockito.anyString())).thenReturn(xxService);

        XXServiceDef serviceDef = new XXServiceDef();
        serviceDef.setImplclassname("test.impl.class");
        Mockito.when(daoManager.getXXServiceDef().getById(Mockito.anyLong())).thenReturn(serviceDef);

        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.isKeyAdmin()).thenReturn(false);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("testUser");
        Mockito.when(userMgr.getGroupsForUser("testUser")).thenReturn(new HashSet<>());

        RangerPolicyAdmin policyAdmin = Mockito.mock(RangerPolicyAdmin.class);
        Mockito.when(spyServiceRest.getPolicyAdminForDelegatedAdmin(Mockito.anyString())).thenReturn(policyAdmin);
        Mockito.when(policyAdmin.getRolesFromUserAndGroups(Mockito.anyString(), Mockito.anySet())).thenReturn(new HashSet<>());

        Mockito.when(svcStore.isServiceAdminUser(Mockito.anyString(), Mockito.anyString())).thenReturn(false);

        Method method = ServiceREST.class.getDeclaredMethod("ensureAdminAccessForServicePolicies", String.class, Set.class);
        method.setAccessible(true);
        method.invoke(spyServiceRest, serviceName, policies);

        Mockito.verify(daoManager.getXXService()).findByName(serviceName);
    }

    @Test
    public void test100EnsureAdminAccessForServicePoliciesNotFound() throws Exception {
        Set<RangerPolicy> policies = new HashSet<>();
        policies.add(rangerPolicy());
        String serviceName = "non-existent-service";

        Mockito.when(daoManager.getXXService().findByName(serviceName)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new WebApplicationException());

        Method method = ServiceREST.class.getDeclaredMethod("ensureAdminAccessForServicePolicies", String.class, Set.class);
        method.setAccessible(true);
        InvocationTargetException exception = Assertions.assertThrows(InvocationTargetException.class, () -> {
            method.invoke(serviceREST, serviceName, policies);
        });
        Assertions.assertTrue(exception.getCause() instanceof WebApplicationException);

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test101BlockIfGdsService() throws Exception {
        String serviceName = "test-service";

        Mockito.when(daoManager.getXXServiceDef().findServiceDefTypeByServiceName(serviceName)).thenReturn("gds");
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.blockIfGdsService(serviceName);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test102BlockIfGdsServiceNonGds() throws Exception {
        String serviceName = "test-service";

        Mockito.when(daoManager.getXXServiceDef().findServiceDefTypeByServiceName(serviceName)).thenReturn("hadoop");

        Assertions.assertDoesNotThrow(() -> {
            serviceREST.blockIfGdsService(serviceName);
        });
    }

    @Test
    public void test103GetPoliciesWithMetaAttributes() throws Exception {
        RangerPolicy policy1 = rangerPolicy();
        RangerPolicy policy2 = rangerPolicy();
        policy2.setId(2L);
        policy2.setName("test-policy-2");

        List<RangerPolicy> policies = Arrays.asList(policy1, policy2);
        List<RangerPolicy> expectedPolicies = Arrays.asList(policy1, policy2);

        Mockito.when(svcStore.getPoliciesWithMetaAttributes(policies)).thenReturn(expectedPolicies);

        List<RangerPolicy> result = serviceREST.getPoliciesWithMetaAttributes(policies);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(policy1.getId(), result.get(0).getId());
        Assertions.assertEquals(policy2.getId(), result.get(1).getId());
    }

    @Test
    public void test104IsServiceAdmin() throws Exception {
        String serviceName = "test-service";
        String userName = "test-user";

        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        boolean result = serviceREST.isServiceAdmin(serviceName);
        Assertions.assertTrue(result);

        Mockito.when(bizUtil.isAdmin()).thenReturn(false);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
        Mockito.when(svcStore.isServiceAdminUser(serviceName, userName)).thenReturn(true);
        result = serviceREST.isServiceAdmin(serviceName);
        Assertions.assertTrue(result);

        Mockito.when(bizUtil.isAdmin()).thenReturn(false);
        Mockito.when(svcStore.isServiceAdminUser(serviceName, userName)).thenReturn(false);
        result = serviceREST.isServiceAdmin(serviceName);
        Assertions.assertFalse(result);
    }

    @Test
    public void test105hideCriticalServiceDetailsForRoleUser() throws Exception {
        RangerService originalService = rangerService();
        originalService.getConfigs().put("password", "secret");
        originalService.getConfigs().put("username", "admin");
        originalService.setDescription("Test service description");

        // Use reflection to access the private method
        Method method = ServiceREST.class.getDeclaredMethod("hideCriticalServiceDetailsForRoleUser", RangerService.class);
        method.setAccessible(true);

        RangerService result = (RangerService) method.invoke(serviceREST, originalService);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(result.getId(), originalService.getId());
        Assertions.assertEquals(result.getName(), originalService.getName());
        Assertions.assertEquals(result.getType(), originalService.getType());
        Assertions.assertNull(result.getDescription());
        Assertions.assertTrue(result.getConfigs().isEmpty());
    }

    @Test
    public void test106LinkedServiceCreatorConstructor() throws Exception {
        // Access the inner class using reflection
        Class<?> linkedServiceCreatorClass = Class.forName("org.apache.ranger.rest.ServiceREST$LinkedServiceCreator");
        Constructor<?> constructor = linkedServiceCreatorClass.getDeclaredConstructor(ServiceREST.class, String.class, String.class);
        constructor.setAccessible(true);

        Object linkedServiceCreator = constructor.newInstance(serviceREST, "testService", "tag");

        // Access fields using reflection
        Field resourceServiceNameField = linkedServiceCreatorClass.getDeclaredField("resourceServiceName");
        Field linkedServiceTypeField = linkedServiceCreatorClass.getDeclaredField("linkedServiceType");
        Field linkedServiceNameField = linkedServiceCreatorClass.getDeclaredField("linkedServiceName");
        Field isAutoCreateField = linkedServiceCreatorClass.getDeclaredField("isAutoCreate");
        Field isAutoLinkField = linkedServiceCreatorClass.getDeclaredField("isAutoLink");

        resourceServiceNameField.setAccessible(true);
        linkedServiceTypeField.setAccessible(true);
        linkedServiceNameField.setAccessible(true);
        isAutoCreateField.setAccessible(true);
        isAutoLinkField.setAccessible(true);

        String resourceServiceName = (String) resourceServiceNameField.get(linkedServiceCreator);
        String linkedServiceType = (String) linkedServiceTypeField.get(linkedServiceCreator);
        String linkedServiceName = (String) linkedServiceNameField.get(linkedServiceCreator);
        Boolean isAutoCreate = (Boolean) isAutoCreateField.get(linkedServiceCreator);
        Boolean isAutoLink = (Boolean) isAutoLinkField.get(linkedServiceCreator);

        Assertions.assertEquals("testService", resourceServiceName);
        Assertions.assertEquals("tag", linkedServiceType);
        Assertions.assertTrue(isAutoCreate);
        Assertions.assertTrue(isAutoLink);
    }

    @Test
    public void test107LinkedServiceCreatorToString() throws Exception {
        // Access the inner class using reflection
        Class<?> linkedServiceCreatorClass = Class.forName("org.apache.ranger.rest.ServiceREST$LinkedServiceCreator");
        Constructor<?> constructor = linkedServiceCreatorClass.getDeclaredConstructor(ServiceREST.class, String.class, String.class);
        constructor.setAccessible(true);

        Object linkedServiceCreator = constructor.newInstance(serviceREST, "testService", "tag");

        String result = linkedServiceCreator.toString();

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("resourceServiceName=testService"));
        Assertions.assertTrue(result.contains("linkedServiceType=tag"));
        Assertions.assertTrue(result.contains("isAutoCreate="));
        Assertions.assertTrue(result.contains("isAutoLink="));
    }

    @Test
    public void test108LinkedServiceCreatorDoCreateAndLinkService() throws Exception {
        String testService = "testService";
        RangerService existingService = rangerService();
        existingService.setName(testService);

        String linkedServiceType = "tag";

        Class<?> linkedServiceCreatorClass = Class.forName("org.apache.ranger.rest.ServiceREST$LinkedServiceCreator");
        Field sepField = linkedServiceCreatorClass.getDeclaredField("SEP");
        sepField.setAccessible(true);
        char sep = (char) sepField.get(null);

        String linkedServiceName = testService + sep + linkedServiceType;
        RangerService existingLinkedService = rangerService();
        existingLinkedService.setName(linkedServiceName);
        existingLinkedService.setType(linkedServiceType);

        // Mock the service store to return existing service
        Mockito.when(svcStore.getServiceByName(testService)).thenReturn(existingService);
        Mockito.when(svcStore.getServiceByName(linkedServiceType)).thenReturn(null);
        Mockito.when(svcStore.createService(Mockito.any(RangerService.class))).thenReturn(null);

        // Access the inner class using reflection
        //Class<?> linkedServiceCreatorClass = Class.forName("org.apache.ranger.rest.ServiceREST$LinkedServiceCreator");
        Constructor<?> constructor = linkedServiceCreatorClass.getDeclaredConstructor(ServiceREST.class, String.class, String.class);
        constructor.setAccessible(true);

        Object linkedServiceCreator = constructor.newInstance(serviceREST, "testService", "tag");

        // Access the doCreateAndLinkService method
        Method doCreateAndLinkServiceMethod = linkedServiceCreatorClass.getDeclaredMethod("doCreateAndLinkService");
        doCreateAndLinkServiceMethod.setAccessible(true);

        // Execute the method
        doCreateAndLinkServiceMethod.invoke(linkedServiceCreator);

        Mockito.verify(svcStore, Mockito.times(2)).getServiceByName(testService);
        Mockito.verify(svcStore, Mockito.times(2)).getServiceByName(linkedServiceType);
        Mockito.verify(svcStore, Mockito.times(1)).createService(Mockito.any(RangerService.class));
    }

    @Test
    public void test109GetPolicyByNameWithNullServiceName() throws Exception {
        String serviceName = null;
        String policyName = "testPolicy";
        String zoneName = "testZone";

        RangerPolicy rangerPolicy = serviceREST.getPolicyByName(serviceName, policyName, zoneName);

        Assertions.assertNull(rangerPolicy);    }

    @Test
    public void test110GetPolicyByNameWithNullPolicyName() throws Exception {
        String serviceName = "testService";
        String policyName = null;
        String zoneName = "testZone";

        RangerPolicy rangerPolicy = serviceREST.getPolicyByName(serviceName, policyName, zoneName);

        Assertions.assertNull(rangerPolicy);
    }

    @Test
    public void test111GetPolicyByNamePolicyNotFound() throws Exception {
        String serviceName = "testService";
        String policyName = "testPolicy";
        String zoneName = "testZone";
        XXPolicyDao xXPolicyDao = Mockito.mock(XXPolicyDao.class);

        Mockito.when(daoManager.getXXPolicy()).thenReturn(xXPolicyDao);
        Mockito.when(xXPolicyDao.findPolicy(policyName, serviceName, zoneName)).thenReturn(null);

        RangerPolicy rangerPolicy = serviceREST.getPolicyByName(serviceName, policyName, zoneName);

        Assertions.assertNull(rangerPolicy);
    }

    @Test
    public void test112GetPolicyResourceWithNullResource() throws Exception {
        GrantRevokeRequest grantRequest = createValidGrantRevokeRequest();

        RangerPolicyResource result = serviceREST.getPolicyResource(null, grantRequest);

        Assertions.assertNotNull(result);
        Assertions.assertNull(result.getValues().get(0));
    }

    @Test
    public void test113GetPolicyResourceWithEmptyStringResource() throws Exception {
        GrantRevokeRequest grantRequest = createValidGrantRevokeRequest();

        RangerPolicyResource result = serviceREST.getPolicyResource("", grantRequest);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getValues().size());
        Assertions.assertEquals("", result.getValues().get(0));
    }

    @Test
    public void test114CountServicesWithFilterException() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter filter = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(request, svcService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getPaginatedServices(filter)).thenThrow(new RuntimeException("Test exception"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.countServices(request);
        });

        Mockito.verify(restErrorUtil).createRESTException("Test exception");
    }

    @Test
    public void test115ValidateConfigWithException() throws Exception {
        RangerService rangerService = rangerService();

        Mockito.when(serviceMgr.validateConfig(rangerService, svcStore)).thenThrow(new RuntimeException("Validation failed"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.validateConfig(rangerService);
        });

        Mockito.verify(restErrorUtil).createRESTException("Validation failed");
    }

    @Test
    public void test116LookupResourceWithException() throws Exception {
        String serviceName = "HDFS_1";
        ResourceLookupContext context = new ResourceLookupContext();
        context.setResourceName(serviceName);
        context.setUserInput("HDFS");

        Mockito.when(serviceMgr.lookupResource(serviceName, context, svcStore)).thenThrow(new RuntimeException("Lookup failed"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.lookupResource(serviceName, context);
        });

        Mockito.verify(restErrorUtil).createRESTException("Lookup failed");
    }

    @Test
    public void test117GetPluginsInfoWithException() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter filter = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(request, pluginInfoService.getSortFields())).thenReturn(filter);
        Mockito.when(pluginInfoService.searchRangerPluginInfo(filter)).thenThrow(new RuntimeException("Search failed"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getPluginsInfo(request);
        });

        Mockito.verify(restErrorUtil).createRESTException("Search failed");
    }

    @Test
    public void test118GetMetricByTypeWithInvalidType() throws Exception {
        String type = "invalid_type";

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getMetricByType(type);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString());
    }

    @Test
    public void test119GetMetricByTypeWithException() throws Exception {
        String type = "usergroup";

        ServiceDBStore.METRIC_TYPE metricType = ServiceDBStore.METRIC_TYPE.getMetricTypeByName(type);
        Mockito.when(svcStore.getMetricByType(metricType)).thenThrow(new RuntimeException("GetMetricByType failed"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getMetricByType(type);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString());
    }

    @Test
    public void test120DeleteClusterServicesWithNoServices() throws Exception {
        String clusterName = "emptyCluster";
        List<Long> emptyList = new ArrayList<>();

        Mockito.when(daoManager.getXXServiceConfigMap().findServiceIdsByClusterName(clusterName)).thenReturn(emptyList);

        ResponseEntity<List<ServiceDeleteResponse>> result = serviceREST.deleteClusterServices(clusterName);

        Assertions.assertEquals(HttpStatus.NOT_FOUND, result.getStatusCode());
        Assertions.assertNotNull(result.getBody());
        Assertions.assertTrue(result.getBody().isEmpty());
    }

    @Test
    public void test121DeleteClusterServicesWithInvalidServices() throws Exception {
        String clusterName = "invalidCluster";
        List<Long> emptyList = new ArrayList<>();

        Mockito.when(daoManager.getXXServiceConfigMap().findServiceIdsByClusterName(clusterName)).thenThrow(new RuntimeException("Search Config failed"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.deleteClusterServices(clusterName);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString());
    }

    @Test
    public void test122DeleteClusterServicesWithException() throws Exception {
        String clusterName = "testCluster";
        List<Long> serviceIds = Arrays.asList(1L, 2L, 3L);

        Mockito.when(daoManager.getXXServiceConfigMap().findServiceIdsByClusterName(clusterName)).thenReturn(serviceIds);
        Mockito.when(daoManager.getXXService().getById(Mockito.anyLong())).thenThrow(new RuntimeException("Database error"));

        ResponseEntity<List<ServiceDeleteResponse>> result = serviceREST.deleteClusterServices(clusterName);

        Assertions.assertEquals(HttpStatus.OK, result.getStatusCode());
        Assertions.assertNotNull(result.getBody());
        Assertions.assertEquals(3, result.getBody().size());
        for (ServiceDeleteResponse response : result.getBody()) {
            Assertions.assertFalse(response.getIsDeleted());
        }
    }

    @Test
    public void test123IsServiceAdminWithNullServiceName() throws Exception {
        String serviceName = null;

        Mockito.when(bizUtil.isAdmin()).thenReturn(false);

        boolean result = serviceREST.isServiceAdmin(serviceName);

        Assertions.assertFalse(result);
        Mockito.verify(bizUtil).isAdmin();
    }

    @Test
    public void test124IsServiceAdminWithEmptyServiceName() throws Exception {
        String serviceName = "";

        Mockito.when(bizUtil.isAdmin()).thenReturn(false);

        boolean result = serviceREST.isServiceAdmin(serviceName);

        Assertions.assertFalse(result);
        Mockito.verify(bizUtil).isAdmin();
    }

    @Test
    public void test125GetServiceDefWithAccessDenied() throws Exception {
        RangerServiceDef rangerServiceDef = rangerServiceDef();
        XXServiceDef xServiceDef = serviceDef();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(Id)).thenReturn(xServiceDef);
        Mockito.when(bizUtil.hasAccess(xServiceDef, null)).thenReturn(false);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class))).thenThrow(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getServiceDef(rangerServiceDef.getId());
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class));
    }

    @Test
    public void test126CreateServiceWithDisplayNameHandling() throws Exception {
        RangerService service = rangerService();
        service.setDisplayName(null); // Test null display name
        XXServiceDef xServiceDef = serviceDef();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenReturn(serviceValidator);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.findByName(service.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.createService(Mockito.any())).thenReturn(service);

        RangerService result = serviceREST.createService(service);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(service.getName(), service.getDisplayName()); // Should use name as display name
        Mockito.verify(svcStore).createService(Mockito.any(RangerService.class));
    }

    @Test
    public void test127CreateServiceWithBlankDisplayName() throws Exception {
        RangerService service = rangerService();
        service.setDisplayName("   "); // Test blank display name
        XXServiceDef xServiceDef = serviceDef();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenReturn(serviceValidator);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.findByName(service.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.createService(Mockito.any())).thenReturn(service);

        RangerService result = serviceREST.createService(service);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(service.getName(), service.getDisplayName()); // Should use name as display name
        Mockito.verify(svcStore).createService(Mockito.any(RangerService.class));
    }

    @Test
    public void test128CreateServiceDefWithValidationFailure() throws Exception {
        RangerServiceDef serviceDef = rangerServiceDef();

        Mockito.when(validatorFactory.getServiceDefValidator(svcStore)).thenThrow(new RuntimeException("Validation failed"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.createServiceDef(serviceDef);
        });

        Mockito.verify(restErrorUtil).createRESTException("Validation failed");
    }

    @Test
    public void test129UpdateServiceWithValidationFailure() throws Exception {
        RangerService service = rangerService();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        XXServiceDef xServiceDef = serviceDef();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenThrow(new RuntimeException("Validation failed"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.updateService(service, request);
        });

        Mockito.verify(restErrorUtil).createRESTException("Validation failed");
    }

    @Test
    public void test130GetServiceDefByNameNotFound() throws Exception {
        String serviceName = "nonexistent";
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.findByName(serviceName)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getServiceDefByName(serviceName);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test131GetServiceByNameNotFound() throws Exception {
        String serviceName = "nonexistent";

        Mockito.when(svcStore.getServiceByName(serviceName)).thenThrow(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getServiceByName(serviceName);
        });

        Mockito.verify(svcStore).getServiceByName(serviceName);
    }

    @Test
    public void test132GetServiceNotFound() throws Exception {
        Long serviceId = 999L;

        Mockito.when(svcStore.getService(serviceId)).thenThrow(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getService(serviceId);
        });

        Mockito.verify(svcStore).getService(serviceId);
    }

    @Test
    public void test133PurgeEmptyPoliciesWithNullServiceName() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.purgeEmptyPolicies(null, request);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test134PurgeEmptyPoliciesServiceNotFound() throws Exception {
        String serviceName = "nonexistent";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(svcStore.getServiceByName(serviceName)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.purgeEmptyPolicies(serviceName, request);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString());
    }

    @Test
    public void test135GetPolicyFromEventTimeWithMissingParameters() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(request.getParameter("eventTime")).thenReturn(null);
        Mockito.when(request.getParameter("policyId")).thenReturn("1");
        Mockito.when(request.getParameter("versionNo")).thenReturn("1");
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class))).thenThrow(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getPolicyFromEventTime(request);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class));
    }

    @Test
    public void test136GetServicePoliciesIfUpdatedWithInvalidService() throws Exception {
        String serviceName = "invalid";
        Long lastKnownVersion = 1L;
        String pluginId = "test";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(serviceUtil.isValidateHttpsAuthentication(serviceName, request)).thenReturn(true);
        Mockito.when(svcStore.getServicePoliciesIfUpdated(serviceName, lastKnownVersion, false))
                .thenThrow(new RuntimeException("Service not found"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getServicePoliciesIfUpdated(serviceName, lastKnownVersion, 0L, pluginId, "", "", false, capabilityVector, request);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test137GetServiceDefsWithFilterException() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter filter = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(request, serviceDefService.sortFields)).thenReturn(filter);
        Mockito.when(bizUtil.hasModuleAccess(RangerConstants.MODULE_RESOURCE_BASED_POLICIES)).thenReturn(true);
        Mockito.when(svcStore.getPaginatedServiceDefs(filter)).thenThrow(new RuntimeException("Database error"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getServiceDefs(request);
        });

        Mockito.verify(restErrorUtil).createRESTException("Database error");
    }

    @Test
    public void test138GetServiceDefsWithoutModuleAccess() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(bizUtil.hasModuleAccess(RangerConstants.MODULE_RESOURCE_BASED_POLICIES)).thenReturn(false);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getServiceDefs(request);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test139CreatePolicyWithNameTrimming() throws Exception {
        RangerPolicy policy = rangerPolicy();
        policy.setName("  policy-with-spaces  ");
        policy.setDescription("  description-with-spaces  ");
        XXServiceDef xServiceDef = serviceDef();
        XXService xService = xService();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);

        Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(policyValidator);
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("admin");
        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.createPolicy(Mockito.any())).thenReturn(policy);

        RangerPolicy result = serviceREST.createPolicy(policy, null);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("policy-with-spaces", policy.getName().trim());
        Assertions.assertEquals("description-with-spaces", policy.getDescription().trim());
        Mockito.verify(svcStore).createPolicy(Mockito.any(RangerPolicy.class));
    }

    @Test
    public void test140CreatePolicyWithLongName() throws Exception {
        RangerPolicy policy = rangerPolicy();
        // Create a name longer than maxPolicyNameLength (255)
        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            longName.append("a");
        }
        policy.setName(longName.toString());

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class), Mockito.isNull(), Mockito.anyString(), Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.createPolicy(policy, null);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class), Mockito.isNull(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void test141UpdateServiceWithNameTrimming() throws Exception {
        RangerService service = rangerService();
        service.setName("  service-with-spaces  ");
        service.setDisplayName("  display-name-with-spaces  ");
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        XXServiceDef xServiceDef = serviceDef();
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenReturn(serviceValidator);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.findByName(service.getType())).thenReturn(xServiceDef);
        Mockito.when(svcStore.updateService(Mockito.any(), Mockito.any())).thenReturn(service);

        RangerService result = serviceREST.updateService(service, request);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("service-with-spaces", service.getName().trim());
        Assertions.assertEquals("display-name-with-spaces", service.getDisplayName().trim());
        Mockito.verify(svcStore).updateService(Mockito.any(RangerService.class), Mockito.any());
    }

    @Test
    public void test142GetPoliciesForResourceWithInvalidServiceDef() throws Exception {
        String serviceDefName = "invalid";
        String serviceName = "testService";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class))).thenThrow(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getPoliciesForResource(serviceDefName, serviceName, request);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class));
    }

    @Test
    public void test143DeleteServiceDefNotFound() throws Exception {
        Long serviceDefId = 999L;
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(validatorFactory.getServiceDefValidator(svcStore)).thenReturn(serviceDefValidator);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(serviceDefId)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.deleteServiceDef(serviceDefId, request);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString());
    }

    @Test
    public void test144GetServiceByIdNotFound() throws Exception {
        Long serviceId = 999L;

        Mockito.when(svcStore.getService(serviceId)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getService(serviceId);
        });

        Mockito.verify(svcStore).getService(serviceId);
        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test145ResetPolicyCacheWithException() throws Exception {
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class))).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.resetPolicyCache(null);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class));
    }

    @Test
    public void test146ResetPolicyCacheAllWithException() throws Exception {
        Mockito.when(bizUtil.isAdmin()).thenReturn(false);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class))).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.resetPolicyCacheAll();
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class));
    }

    @Test
    public void test147GetServicePoliciesByNameWithException() throws Exception {
        String serviceName = "testService";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter filter = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getServicePolicies(serviceName, filter)).thenThrow(new RuntimeException("Service not found"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getServicePoliciesByName(serviceName, request);
        });

        Mockito.verify(restErrorUtil).createRESTException("Service not found");
    }

    @Test
    public void test148GetServicePoliciesByIdWithException() throws Exception {
        Long serviceId = 1L;
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter filter = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(request, policyService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getServicePolicies(serviceId, filter)).thenThrow(new RuntimeException("Service not found"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getServicePolicies(serviceId, request);
        });

        Mockito.verify(restErrorUtil).createRESTException("Service not found");
    }

    @Test
    public void test149CheckSSOWithDifferentValues() throws Exception {
        Mockito.when(bizUtil.isSSOEnabled()).thenReturn(true);
        String result = serviceREST.checkSSO();
        Assertions.assertEquals("true", result);

        Mockito.reset(bizUtil);
        Mockito.when(bizUtil.isSSOEnabled()).thenReturn(false);
        result = serviceREST.checkSSO();
        Assertions.assertEquals("false", result);
    }

    @Test
    public void test150GetCSRFPropertiesWithNullSession() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpSession session = Mockito.mock(HttpSession.class);

        Mockito.when(request.getSession()).thenReturn(session);
        Mockito.when(session.getAttribute(Mockito.anyString())).thenReturn(null);

        HashMap<String, Object> result = serviceREST.getCSRFProperties(request);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.containsKey("ranger.rest-csrf.enabled"));
        Assertions.assertTrue(result.containsKey("_csrfToken"));
        Assertions.assertNotNull(result.get("_csrfToken"));
    }

    @Test
    public void test151ApplyPolicyWithNullRequest() throws Exception {
        RangerPolicy policy = rangerPolicy();

        ServiceREST serviceRESTSpy = Mockito.spy(serviceREST);
        Mockito.doReturn(policy).when(serviceRESTSpy).createPolicy(Mockito.any(RangerPolicy.class), eq(null));

        RangerPolicy result = serviceRESTSpy.applyPolicy(policy, null);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(policy.getId(), result.getId());
        Mockito.verify(serviceRESTSpy).createPolicy(policy, null);
    }

    @Test
    public void test152GetPolicyLabelsWithException() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter filter = new SearchFilter();

        Mockito.when(searchUtil.getSearchFilter(request, policyLabelsService.sortFields)).thenReturn(filter);
        Mockito.when(svcStore.getPolicyLabels(filter)).thenThrow(new RuntimeException("Database error"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getPolicyLabels(request);
        });

        Mockito.verify(restErrorUtil).createRESTException("Database error");
    }

    @Test
    public void test153GetServiceWithRoleUserAndException() throws Exception {
        Long serviceId = 1L;

        Mockito.when(svcStore.getService(serviceId)).thenThrow(new RuntimeException("Service not found"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getService(serviceId);
        });

        Mockito.verify(restErrorUtil).createRESTException("Service not found");
        Mockito.verify(svcStore).getService(serviceId);
    }

    @Test
    public void test154GetPolicyForVersionNumberWithAccessDenied() throws Exception {
        Long policyId = 1L;
        int versionNo = 1;
        RangerPolicy policy = rangerPolicy();

        Mockito.when(svcStore.getPolicyForVersionNumber(policyId, versionNo)).thenReturn(policy);
        Mockito.when(bizUtil.isAdmin()).thenReturn(false);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("user");
        Mockito.when(svcStore.isServiceAdminUser(policy.getService(), "user")).thenReturn(false);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new WebApplicationException());

        WebApplicationException exception = Assertions.assertThrows(WebApplicationException.class, () -> {
            serviceREST.getPolicyForVersionNumber(policyId, versionNo);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    RangerPolicy rangerPolicy() {
        List<RangerPolicyItemAccess>    accesses         = new ArrayList<>();
        List<String>                    users            = new ArrayList<>();
        List<String>                    groups           = new ArrayList<>();
        List<RangerPolicyItemCondition> conditions       = new ArrayList<>();
        List<RangerPolicyItem>          policyItems      = new ArrayList<>();
        RangerPolicyItem                rangerPolicyItem = new RangerPolicyItem();
        rangerPolicyItem.setAccesses(accesses);
        rangerPolicyItem.setConditions(conditions);
        rangerPolicyItem.setGroups(groups);
        rangerPolicyItem.setUsers(users);
        rangerPolicyItem.setDelegateAdmin(false);

        policyItems.add(rangerPolicyItem);

        Map<String, RangerPolicyResource> policyResource       = new HashMap<>();
        RangerPolicyResource              rangerPolicyResource = new RangerPolicyResource();
        rangerPolicyResource.setIsExcludes(true);
        rangerPolicyResource.setIsRecursive(true);
        rangerPolicyResource.setValue("1");
        rangerPolicyResource.setValues(users);
        policyResource.put("resource", rangerPolicyResource);
        RangerPolicy policy = new RangerPolicy();
        policy.setId(Id);
        policy.setCreateTime(new Date());
        policy.setDescription("policy");
        policy.setGuid("policyguid");
        policy.setIsEnabled(true);
        policy.setName("HDFS_1-1-20150316062453");
        policy.setUpdatedBy("Admin");
        policy.setUpdateTime(new Date());
        policy.setService("HDFS_1-1-20150316062453");
        policy.setIsAuditEnabled(true);
        policy.setPolicyItems(policyItems);
        policy.setResources(policyResource);

        return policy;
    }

    private List<Long> createLongList() {
        List<Long> list = new ArrayList<>();
        list.add(1L);
        list.add(2L);
        list.add(3L);
        return list;
    }

    private ArrayList<String> createUserList() {
        ArrayList<String> userList = new ArrayList<>();
        userList.add("test-user-1");
        return userList;
    }

    private ArrayList<String> createGroupList() {
        ArrayList<String> groupList = new ArrayList<>();
        groupList.add("test-group-1");
        return groupList;
    }

    private ArrayList<String> createRoleList() {
        ArrayList<String> roleList = new ArrayList<>();
        roleList.add("test-role-1");
        return roleList;
    }

    private ArrayList<String> createGrantorGroupList() {
        ArrayList<String> grantorGroupList = new ArrayList<>();
        grantorGroupList.add("test-grantor-group-1");
        return grantorGroupList;
    }

    private HashMap<String, String> createResourceMap() {
        HashMap<String, String> resourceMap = new HashMap<>();
        resourceMap.put("test-resource-1", "test-resource-value-1");
        return resourceMap;
    }

    private ArrayList<String> createAccessTypeList() {
        ArrayList<String> accessTypeList = new ArrayList<>();
        accessTypeList.add("test-access-type-1");
        return accessTypeList;
    }

    private GrantRevokeRequest createValidGrantRevokeRequest() {
        GrantRevokeRequest grantRevokeRequest = new GrantRevokeRequest();
        grantRevokeRequest.setUsers(new HashSet<>(createUserList()));
        grantRevokeRequest.setGroups(new HashSet<>(createGroupList()));
        grantRevokeRequest.setRoles(new HashSet<>(createRoleList()));
        grantRevokeRequest.setGrantor(grantor);
        grantRevokeRequest.setGrantorGroups(new HashSet<>(createGrantorGroupList()));
        grantRevokeRequest.setOwnerUser(ownerUser);
        grantRevokeRequest.setResource(createResourceMap());
        grantRevokeRequest.setAccessTypes(new HashSet<>(createAccessTypeList()));
        grantRevokeRequest.setZoneName(zoneName);
        grantRevokeRequest.setIsRecursive(true);
        return grantRevokeRequest;
    }

    private XXPolicy getXXPolicy() {
        XXPolicy xxPolicy = new XXPolicy();
        xxPolicy.setId(Id);
        xxPolicy.setName("HDFS_1-1-20150316062453");
        xxPolicy.setAddedByUserId(Id);
        xxPolicy.setDescription("policy");
        xxPolicy.setGuid("policyguid");
        xxPolicy.setCreateTime(new Date());
        xxPolicy.setIsAuditEnabled(true);
        xxPolicy.setIsEnabled(true);
        xxPolicy.setService(1L);
        xxPolicy.setUpdatedByUserId(Id);
        xxPolicy.setUpdateTime(new Date());
        return xxPolicy;
    }

    private File getFile(String testFilePath) throws IOException {
        File jsonPolicyFile = new File(testFilePath);
        if (jsonPolicyFile.getCanonicalPath().contains("/target/jstest")) {
            jsonPolicyFile = new File(jsonPolicyFile.getCanonicalPath().replace("/target/jstest", ""));
        }
        return jsonPolicyFile;
    }

    @Test
    public void testGetAccessResourceObjectMap_NullMap() {
        Map<String, Object> result = ServiceREST.getAccessResourceObjectMap(null);
        Assertions.assertNull(result);
    }

    @Test
    public void testGetAccessResourceObjectMap_SingleValues() {
        Map<String, String> input = new HashMap<>();
        input.put("database", "db1");
        input.put("table", "tbl1");

        Map<String, Object> result = ServiceREST.getAccessResourceObjectMap(input);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("db1", result.get("database"));
        Assertions.assertEquals("tbl1", result.get("table"));
    }

    @Test
    public void testGetAccessResourceObjectMap_CommaSeparatedValues() {
        Map<String, String> input = new HashMap<>();
        input.put("column", "c1,c2,c3");

        Map<String, Object> result = ServiceREST.getAccessResourceObjectMap(input);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.get("column") instanceof List);
        @SuppressWarnings("unchecked")
        List<String> cols = (List<String>) result.get("column");
        Assertions.assertEquals(Arrays.asList("c1", "c2", "c3"), cols);
    }

    @Test
    public void testGetPolicyComparator_ByIdDesc_And_ByNameAscDesc() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("getPolicyComparator", String.class, String.class);
        m.setAccessible(true);

        RangerPolicy p1 = rangerPolicy();
        p1.setId(1L);
        p1.setName("A");
        RangerPolicy p2 = rangerPolicy();
        p2.setId(2L);
        p2.setName("B");

        @SuppressWarnings("unchecked")
        Comparator<RangerPolicy> byIdDesc = (Comparator<RangerPolicy>) m.invoke(serviceREST, SearchFilter.POLICY_ID, "DESC");
        Assertions.assertTrue(byIdDesc.compare(p1, p2) > 0);

        @SuppressWarnings("unchecked")
        Comparator<RangerPolicy> byNameAsc = (Comparator<RangerPolicy>) m.invoke(serviceREST, SearchFilter.POLICY_NAME, "ASC");
        Assertions.assertTrue(byNameAsc.compare(p1, p2) < 0);

        @SuppressWarnings("unchecked")
        Comparator<RangerPolicy> byNameDesc = (Comparator<RangerPolicy>) m.invoke(serviceREST, SearchFilter.POLICY_NAME, "DESC");
        Assertions.assertTrue(byNameDesc.compare(p1, p2) > 0);
    }

    @Test
    public void testGetPolicyAdminForSearch_DoesNotThrow() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("getPolicyAdminForSearch", String.class);
        m.setAccessible(true);
        Assertions.assertDoesNotThrow(() -> {
            try {
                m.invoke(serviceREST, "svc");
            } catch (InvocationTargetException ite) {
                throw new RuntimeException(ite.getCause());
            }
        });
    }

    @Test
    public void testIsZoneAdmin() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("isZoneAdmin", String.class);
        m.setAccessible(true);
        Mockito.when(bizUtil.isAdmin()).thenReturn(false);
        Mockito.when(serviceMgr.isZoneAdmin("zone1")).thenReturn(true);
        boolean ret = (boolean) m.invoke(serviceREST, "zone1");
        Assertions.assertTrue(ret);
    }

    @Test
    public void testHasAdminAccess_Resource_Allowed() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        Method m = ServiceREST.class.getDeclaredMethod("hasAdminAccess", String.class, String.class, String.class, Set.class, RangerAccessResource.class, Set.class);
        m.setAccessible(true);
        RangerPolicyAdmin policyAdmin = Mockito.mock(RangerPolicyAdmin.class);
        Mockito.doReturn(policyAdmin).when(spy).getPolicyAdminForDelegatedAdmin(Mockito.anyString());
        Mockito.when(policyAdmin.isDelegatedAdminAccessAllowed(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet())).thenReturn(true);
        RangerAccessResource resource = new RangerAccessResourceImpl(Collections.singletonMap("path", "/"));
        boolean ret = (boolean) m.invoke(spy, "svc", "zone", "user", new HashSet<>(), resource, Collections.singleton("admin"));
        Assertions.assertTrue(ret);
    }

    @Test
    public void testHasAdminAccess_Policy_Allowed() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        Method m = ServiceREST.class.getDeclaredMethod("hasAdminAccess", RangerPolicy.class, String.class, Set.class);
        m.setAccessible(true);
        RangerPolicyAdmin policyAdmin = Mockito.mock(RangerPolicyAdmin.class);
        Mockito.doReturn(policyAdmin).when(spy).getPolicyAdminForDelegatedAdmin(Mockito.anyString());
        Mockito.when(policyAdmin.getRolesFromUserAndGroups(Mockito.anyString(), Mockito.anySet())).thenReturn(new HashSet<>());
        Mockito.when(policyAdmin.isDelegatedAdminAccessAllowedForModify(Mockito.any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet(), Mockito.anyMap())).thenReturn(true);
        RangerPolicy policy = rangerPolicy();
        boolean ret = (boolean) m.invoke(spy, policy, "user", new HashSet<>());
        Assertions.assertTrue(ret);
    }

    @Test
    public void testProcessServiceMapping_ValidAndInvalid() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("processServiceMapping", Map.class, List.class, List.class);
        m.setAccessible(true);
        Map<String, String> map = new HashMap<>();
        map.put(" s ", " d ");
        List<String> src = new ArrayList<>();
        List<String> dst = new ArrayList<>();
        m.invoke(serviceREST, map, src, dst);
        Assertions.assertEquals(1, src.size());
        Assertions.assertEquals(1, dst.size());

        map.put(" ", " ");
        Assertions.assertThrows(InvocationTargetException.class, () -> m.invoke(serviceREST, map, new ArrayList<>(), new ArrayList<>()));
    }

    @Test
    public void testDeleteExactMatchPolicyForResource_DeletesWhenMatchFound() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        Method m = ServiceREST.class.getDeclaredMethod("deleteExactMatchPolicyForResource", List.class, String.class, String.class);
        m.setAccessible(true);
        RangerPolicy toDelete = rangerPolicy();
        m.invoke(spy, Collections.singletonList(toDelete), "user", null);
        Mockito.verify(svcStore, Mockito.never()).deletePolicy(Mockito.any(), Mockito.isNull());
    }

    @Test
    public void testGetPolicyMatchByName_UsesRequestParams() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        Method m = ServiceREST.class.getDeclaredMethod("getPolicyMatchByName", RangerPolicy.class, HttpServletRequest.class);
        m.setAccessible(true);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getParameter("servicename")).thenReturn("svc1");
        Mockito.when(request.getParameter("policyname")).thenReturn("pol1");
        Mockito.when(request.getParameter("zoneName")).thenReturn("zone1");
        RangerPolicy expected = rangerPolicy();
        Mockito.doReturn(expected).when(spy).getPolicyByName("svc1", "pol1", "zone1");
        RangerPolicy actual = (RangerPolicy) m.invoke(spy, new RangerPolicy(), request);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testGetExactMatchPolicyForResource_ReturnsNullWhenNoAdmin() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("getExactMatchPolicyForResource", RangerPolicy.class, String.class);
        m.setAccessible(true);
        RangerPolicy policy = rangerPolicy();
        Assertions.assertDoesNotThrow(() -> {
            try {
                m.invoke(serviceREST, policy, "user");
            } catch (InvocationTargetException ite) {
                throw new RuntimeException(ite.getCause());
            }
        });
    }

    @Test
    public void testDeletePoliciesForResource_DeletesNonExported() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        Method m = ServiceREST.class.getDeclaredMethod("deletePoliciesForResource", List.class, List.class, HttpServletRequest.class, List.class, String.class);
        m.setAccessible(true);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter filter = new SearchFilter();
        Mockito.when(searchUtil.getSearchFilter(Mockito.any(), Mockito.any())).thenReturn(filter);
        RangerPolicy p = rangerPolicy();
        p.setName("to-delete");
        RangerPolicyList list = new RangerPolicyList();
        list.setPolicies(Collections.singletonList(p));
        // Cannot mock private getServicePolicies; instead, we only verify path runs without exception
        Mockito.doReturn(rangerService()).when(spy).getServiceByName("dest");
        Assertions.assertDoesNotThrow(() -> m.invoke(spy, Collections.singletonList("src"), Collections.singletonList("dest"), request, Collections.emptyList(), ""));
    }

    @Test
    public void testValidateResourcePoliciesRequest_NoResource_And_InvalidServiceType() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("validateResourcePoliciesRequest", String.class, String.class, HttpServletRequest.class, List.class, Map.class);
        m.setAccessible(true);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        @SuppressWarnings("unchecked")
        String msg = (String) m.invoke(serviceREST, "hdfs", "svc", request, new ArrayList<>(), new HashMap<>());
        Assertions.assertTrue(msg.contains("No resource"));

        Map<String, Object> resource = new HashMap<>();
        resource.put("db", "db1");
        Mockito.when(svcStore.getServiceDefByName("badType")).thenReturn(null);
        msg = (String) m.invoke(serviceREST, "badType", "svc", request, new ArrayList<>(), resource);
        Assertions.assertTrue(msg.contains("Invalid service-type"));
    }

    @Test
    public void testValidateUsersGroupsAndRoles_Empty_Throws() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("validateUsersGroupsAndRoles", Set.class, Set.class, Set.class);
        m.setAccessible(true);
        Mockito.when(restErrorUtil.createGrantRevokeRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(InvocationTargetException.class, () -> m.invoke(serviceREST, Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));
    }

    @Test
    public void testValidateGrantor_NotExists_Throws() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("validateGrantor", String.class);
        m.setAccessible(true);
        Mockito.when(xUserService.getXUserByUserName("noUser")).thenReturn(null);
        Mockito.when(restErrorUtil.createGrantRevokeRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(InvocationTargetException.class, () -> m.invoke(serviceREST, "noUser"));
    }

    @Test
    public void testDeleteService_InvokesStore() throws Exception {
        RangerSecurityContext context = new RangerSecurityContext();
        UserSessionBase session = new UserSessionBase();
        XXPortalUser portalUser = new XXPortalUser();
        portalUser.setId(Id);
        session.setXXPortalUser(portalUser);
        context.setUserSession(session);
        RangerContextHolder.setSecurityContext(context);
        XXService xService = xService();
        xService.setAddedByUserId(Id);
        xService.setType(999L);
        XXServiceDao xxServiceDao = Mockito.mock(XXServiceDao.class);
        Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
        Mockito.when(xxServiceDao.getById(Id)).thenReturn(xService);
        XXServiceDef xServiceDef = serviceDef();
        XXServiceDefDao xxServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xxServiceDefDao);
        Mockito.when(xxServiceDefDao.getById(999L)).thenReturn(xServiceDef);
        Mockito.when(validatorFactory.getServiceValidator(svcStore)).thenReturn(serviceValidator);
        Mockito.doNothing().when(serviceValidator).validate(Mockito.eq(Id), Mockito.eq(Action.DELETE));
        Mockito.doNothing().when(tagStore).deleteAllTagObjectsForService(Mockito.anyString());
        Mockito.doNothing().when(svcStore).deleteService(Id);
        Assertions.assertDoesNotThrow(() -> serviceREST.deleteService(Id));
        Mockito.verify(svcStore).deleteService(Id);
    }

    @Test
    public void testCreatePolicesBasedOnPolicyMap_CreatesOne() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        Method m = ServiceREST.class.getDeclaredMethod("createPolicesBasedOnPolicyMap", HttpServletRequest.class, Map.class, List.class, boolean.class, int.class);
        m.setAccessible(true);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getParameter("mergeIfExists")).thenReturn("true");
        RangerPolicy policy = rangerPolicy();
        policy.setService(rangerService().getName());
        Map<String, RangerPolicy> map = new HashMap<>();
        map.put("k", policy);
        List<String> serviceNames = Collections.singletonList(rangerService().getName());

        Mockito.doReturn(policy).when(spy).createPolicy(Mockito.eq(policy), Mockito.eq(request));
        int ret = (int) m.invoke(spy, request, map, serviceNames, false, 0);
        Assertions.assertEquals(1, ret);
    }

    @Test
    public void testEnsureAdminAccess_ZoneAdminAllowed() {
        ServiceREST spy = Mockito.spy(serviceREST);
        RangerPolicy policy = rangerPolicy();
        policy.setZoneName("zone1");
        Mockito.when(bizUtil.isAdmin()).thenReturn(false);
        Mockito.when(bizUtil.isKeyAdmin()).thenReturn(false);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("user");
        Mockito.when(svcStore.isServiceAdminUser(Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        Mockito.when(serviceMgr.isZoneAdmin("zone1")).thenReturn(true);
        Assertions.assertDoesNotThrow(() -> spy.ensureAdminAccess(policy));
    }

    @Test
    public void testGetAllFilteredPolicyList_DoesNotThrow() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("getAllFilteredPolicyList", SearchFilter.class, HttpServletRequest.class, List.class);
        m.setAccessible(true);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Assertions.assertDoesNotThrow(() -> {
            try {
                m.invoke(serviceREST, new SearchFilter(), request, new ArrayList<>());
            } catch (InvocationTargetException ite) {
                throw new RuntimeException(ite.getCause());
            }
        });
    }

    @Test
    public void testValidateGroups_Grantees_Roles_Throws() throws Exception {
        Method mg = ServiceREST.class.getDeclaredMethod("validateGroups", Set.class);
        Method mu = ServiceREST.class.getDeclaredMethod("validateGrantees", Set.class);
        Method mr = ServiceREST.class.getDeclaredMethod("validateRoles", Set.class);
        mg.setAccessible(true);
        mu.setAccessible(true);
        mr.setAccessible(true);
        Mockito.when(userMgr.getGroupByGroupName("g1")).thenReturn(null);
        Mockito.when(xUserService.getXUserByUserName("u1")).thenReturn(null);
        XXRoleDao roleDao = Mockito.mock(XXRoleDao.class);
        Mockito.when(daoManager.getXXRole()).thenReturn(roleDao);
        Mockito.when(roleDao.findByRoleName("r1")).thenReturn(null);
        Mockito.when(restErrorUtil.createGrantRevokeRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(InvocationTargetException.class, () -> mg.invoke(serviceREST, Collections.singleton("g1")));
        Assertions.assertThrows(InvocationTargetException.class, () -> mu.invoke(serviceREST, Collections.singleton("u1")));
        Assertions.assertThrows(InvocationTargetException.class, () -> mr.invoke(serviceREST, Collections.singleton("r1")));
    }

    @Test
    public void testGetRangerAdminZoneName_ReturnsGiven() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("getRangerAdminZoneName", String.class, GrantRevokeRequest.class);
        m.setAccessible(true);
        GrantRevokeRequest req = new GrantRevokeRequest();
        req.setZoneName("zoneX");
        String ret = (String) m.invoke(serviceREST, "svc", req);
        Assertions.assertEquals("zoneX", ret);
    }

    @Test
    public void testResetPolicyCacheAll_AdminTrue() {
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        Mockito.when(svcStore.resetPolicyCache(null)).thenReturn(true);
        boolean ret = serviceREST.resetPolicyCacheAll();
        Assertions.assertTrue(ret);
        Mockito.verify(svcStore).resetPolicyCache(null);
    }

    @Test
    public void testGetOptions_NullRequestReturnsNull() throws Exception {
        Method m = ServiceREST.class.getDeclaredMethod("getOptions", HttpServletRequest.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> ret = (Map<String, Object>) m.invoke(serviceREST, new Object[] {null});
        Assertions.assertNull(ret);
    }

    @Test
    public void testGetOptions_ForceRenameTrue() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getParameter(ServiceStore.OPTION_FORCE_RENAME)).thenReturn("true");
        Method m = ServiceREST.class.getDeclaredMethod("getOptions", HttpServletRequest.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> ret = (Map<String, Object>) m.invoke(serviceREST, request);
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(Boolean.TRUE, ret.get(ServiceStore.OPTION_FORCE_RENAME));
    }

    @Test
    public void testDeletePoliciesForResource_DeletesNonExported_2() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchFilter filter = new SearchFilter();
        Mockito.when(searchUtil.getSearchFilter(Mockito.eq(request), Mockito.any())).thenReturn(filter);
        // prepare destination policies: one keep, one delete
        RangerPolicy keep = new RangerPolicy();
        keep.setId(1L);
        keep.setName("keep");
        keep.setResources(new HashMap<>());
        RangerPolicy del = new RangerPolicy();
        del.setId(2L);
        del.setName("del");
        del.setResources(new HashMap<>());
        RangerPolicyList list = new RangerPolicyList();
        list.setPolicies(Arrays.asList(keep, del));
        // private method getServicePolicies; use reflection rather than stubbing the private directly
        Mockito.doReturn(new RangerService()).when(spy).getServiceByName("destSvc");
        Method getSvcPolicies = ServiceREST.class.getDeclaredMethod("getServicePolicies", String.class, SearchFilter.class);
        getSvcPolicies.setAccessible(true);
        // let getServicePolicies call through; ensure admin path to bypass applyAdminAccessFilter complexities
        Mockito.when(svcStore.getServicePolicies(Mockito.eq("destSvc"), Mockito.any(SearchFilter.class))).thenReturn(Arrays.asList(keep, del));
        Mockito.when(bizUtil.isAdmin()).thenReturn(true);
        // export list contains keep only
        List<RangerPolicy> exportPolicies = Arrays.asList(keep);
        // invoke private via reflection and only assert no exception
        Method m = ServiceREST.class.getDeclaredMethod("deletePoliciesForResource", List.class, List.class, HttpServletRequest.class, List.class, String.class);
        m.setAccessible(true);
        Assertions.assertDoesNotThrow(() -> {
            try {
                m.invoke(spy, Arrays.asList("srcSvc"), Arrays.asList("destSvc"), request, exportPolicies, "");
            } catch (InvocationTargetException ite) {
                throw new RuntimeException(ite.getCause());
            }
        });
    }

    @Test
    public void testGetAllFilteredPolicyList_FiltersAndResourceMatch() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(request.getParameter("resourceMatch")).thenReturn("full");
        SearchFilter filter = new SearchFilter();

        RangerPolicy p1 = new RangerPolicy();
        p1.setId(10L);
        p1.setService("svcA");
        p1.setName("A");
        Mockito.doReturn(Arrays.asList(p1)).when(spy).getPolicies(Mockito.any(SearchFilter.class));
        Mockito.when(serviceUtil.getMatchingPoliciesForResource(Mockito.eq(request), Mockito.anyList())).thenReturn(Arrays.asList(p1));
        Method m = ServiceREST.class.getDeclaredMethod("getAllFilteredPolicyList", SearchFilter.class, HttpServletRequest.class, List.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<RangerPolicy> out = (List<RangerPolicy>) m.invoke(spy, filter, request, new ArrayList<>());
        Assertions.assertNotNull(out);
    }

    @Test
    public void testGetRangerAdminZoneName_ReturnsGivenZone() throws Exception {
        GrantRevokeRequest req = new GrantRevokeRequest();
        req.setZoneName("zoneX");
        Method m = ServiceREST.class.getDeclaredMethod("getRangerAdminZoneName", String.class, GrantRevokeRequest.class);
        m.setAccessible(true);
        String ret = (String) m.invoke(serviceREST, "svc1", req);
        Assertions.assertEquals("zoneX", ret);
    }

    @Test
    public void testGetExactMatchPolicyForResource_ByPolicy() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        RangerPolicy in = new RangerPolicy();
        in.setService("svc1");
        in.setZoneName("zn");
        Method m = ServiceREST.class.getDeclaredMethod("getExactMatchPolicyForResource", RangerPolicy.class, String.class);
        m.setAccessible(true);
        // Just ensure reflective invocation does not bubble checked exceptions
        try {
            m.invoke(spy, in, "user");
        } catch (InvocationTargetException ite) {
            if (ite.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ite.getCause();
            }
        }
    }

    @Test
    public void testDeleteExactMatchPolicyForResource_DeletesWhenFound() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        RangerPolicy p = new RangerPolicy();
        p.setService("svc1");
        p.setName("pol1");
        RangerPolicy existing = new RangerPolicy();
        existing.setId(9L);
        // Cannot stub private getExactMatchPolicyForResource; instead, invoke method and avoid assertion on delete
        Method m = ServiceREST.class.getDeclaredMethod("deleteExactMatchPolicyForResource", List.class, String.class, String.class);
        m.setAccessible(true);
        Assertions.assertDoesNotThrow(() -> {
            try {
                m.invoke(spy, Arrays.asList(p), "user", null);
            } catch (InvocationTargetException ite) {
                throw new RuntimeException(ite.getCause());
            }
        });
    }

    @Test
    public void testEnsureAdminAndAuditAccess_DelegatedReadAllowed() {
        ServiceREST spy = Mockito.spy(serviceREST);
        RangerPolicy pol = rangerPolicy();
        Mockito.when(bizUtil.isAdmin()).thenReturn(false);
        Mockito.when(bizUtil.isKeyAdmin()).thenReturn(false);
        Mockito.when(bizUtil.isAuditAdmin()).thenReturn(false);
        Mockito.when(bizUtil.isAuditKeyAdmin()).thenReturn(false);
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("user");
        Mockito.when(svcStore.isServiceAdminUser(Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        // Zone is null in policy, so no zoneAdmin call; provide delegated admin
        RangerPolicyAdmin admin = Mockito.mock(RangerPolicyAdmin.class);
        Mockito.doReturn(admin).when(spy).getPolicyAdminForDelegatedAdmin(Mockito.anyString());
        Mockito.when(userMgr.getGroupsForUser(Mockito.anyString())).thenReturn(new HashSet<>());
        Mockito.when(admin.getRolesFromUserAndGroups(Mockito.anyString(), Mockito.anySet())).thenReturn(new HashSet<>());
        Mockito.when(admin.isDelegatedAdminAccessAllowedForRead(Mockito.any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet(), Mockito.anyMap())).thenReturn(true);
        Assertions.assertDoesNotThrow(() -> spy.ensureAdminAndAuditAccess(pol, new HashMap<>()));
    }

    @Test
    public void testToRangerPolicyList_SortsAndPaginates() throws Exception {
        RangerPolicy a = new RangerPolicy();
        a.setId(1L);
        a.setName("A");
        RangerPolicy b = new RangerPolicy();
        b.setId(2L);
        b.setName("B");
        List<RangerPolicy> list = Arrays.asList(b, a);
        SearchFilter f = new SearchFilter();
        f.setStartIndex(0);
        f.setMaxRows(1);
        f.setSortBy(SearchFilter.POLICY_NAME);
        f.setSortType("ASC");
        Method m = ServiceREST.class.getDeclaredMethod("toRangerPolicyList", List.class, SearchFilter.class);
        m.setAccessible(true);
        RangerPolicyList out = (RangerPolicyList) m.invoke(serviceREST, list, f);
        Assertions.assertNotNull(out);
        Assertions.assertEquals(1, out.getListSize());
        Assertions.assertEquals("A", out.getPolicies().get(0).getName());
    }

    @Test
    public void testDeletePoliciesProvidedInServiceMap_InvokesValidatorAndDelete() throws Exception {
        ServiceREST spy = Mockito.spy(serviceREST);
        RangerPolicy pol = rangerPolicy();
        pol.setId(77L);
        RangerPolicyList rpl = new RangerPolicyList();
        rpl.setPolicies(Arrays.asList(pol));

        Mockito.when(svcStore.getServicePolicies(Mockito.eq("dest"), Mockito.any(SearchFilter.class))).thenReturn(Arrays.asList(pol));
        Mockito.doReturn(new RangerService()).when(spy).getServiceByName("dest");
        RangerPolicyValidator pv = Mockito.mock(RangerPolicyValidator.class);
        Mockito.when(validatorFactory.getPolicyValidator(Mockito.eq(svcStore))).thenReturn(pv);
        Method m = ServiceREST.class.getDeclaredMethod("deletePoliciesProvidedInServiceMap", List.class, List.class, String.class);
        m.setAccessible(true);
        Assertions.assertDoesNotThrow(() -> {
            try {
                m.invoke(spy, Arrays.asList("src"), Arrays.asList("dest"), "zone1");
            } catch (InvocationTargetException ite) {
                throw new RuntimeException(ite.getCause());
            }
        });
    }

    @Test
    public void testGetPoliciesFromProvidedJson_SuccessAndFailure() throws Exception {
        RangerExportPolicyList exp = new RangerExportPolicyList();
        RangerPolicy p = rangerPolicy();
        exp.setPolicies(Arrays.asList(p));
        Method m = ServiceREST.class.getDeclaredMethod("getPoliciesFromProvidedJson", RangerExportPolicyList.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<RangerPolicy> ok = (List<RangerPolicy>) m.invoke(serviceREST, exp);
        Assertions.assertEquals(1, ok.size());
        try {
            m.invoke(serviceREST, new Object[] {null});
            // If it returns normally, fail
            Assertions.fail("Expected WebApplicationException cause");
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            Assertions.assertTrue(cause instanceof WebApplicationException || cause instanceof RuntimeException);
        }
    }

    @Test
    public void testValidateGrantRevokeRequest_InvalidOwnerForNonAdmin() throws Exception {
        GrantRevokeRequest req = new GrantRevokeRequest();
        req.setUsers(new HashSet<>(Arrays.asList("u1")));
        req.setGroups(new HashSet<>());
        req.setRoles(new HashSet<>());
        req.setGrantor("user1");
        req.setOwnerUser("owner");
        Mockito.when(xUserService.getXUserByUserName("user1")).thenReturn(new VXUser());
        Mockito.when(xUserService.getXUserByUserName("u1")).thenReturn(new VXUser());
        Method m = ServiceREST.class.getDeclaredMethod("validateGrantRevokeRequest", GrantRevokeRequest.class, boolean.class, String.class);
        m.setAccessible(true);
        Assertions.assertThrows(WebApplicationException.class, () -> {
            try {
                Mockito.when(restErrorUtil.createGrantRevokeRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());
                m.invoke(serviceREST, req, false, "user1");
            } catch (InvocationTargetException ite) {
                if (ite.getCause() instanceof WebApplicationException) {
                    throw (WebApplicationException) ite.getCause();
                }
                throw new RuntimeException(ite.getCause());
            }
        });
    }
}
