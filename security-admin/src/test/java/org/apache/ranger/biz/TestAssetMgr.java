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

package org.apache.ranger.biz;

import org.apache.ranger.amazon.cloudwatch.CloudWatchAccessAuditsService;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXPermMapDao;
import org.apache.ranger.db.XXPluginInfoDao;
import org.apache.ranger.db.XXPolicyExportAuditDao;
import org.apache.ranger.db.XXPortalUserDao;
import org.apache.ranger.db.XXSecurityZoneDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXUserDao;
import org.apache.ranger.elasticsearch.ElasticSearchAccessAuditsService;
import org.apache.ranger.entity.XXPermMap;
import org.apache.ranger.entity.XXPluginInfo;
import org.apache.ranger.entity.XXPolicyExportAudit;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.service.RangerPluginInfoService;
import org.apache.ranger.service.RangerTrxLogV2Service;
import org.apache.ranger.service.XAccessAuditService;
import org.apache.ranger.service.XAuditMapService;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XPolicyExportAuditService;
import org.apache.ranger.service.XPolicyService;
import org.apache.ranger.service.XResourceService;
import org.apache.ranger.service.XUgsyncAuditInfoService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.solr.SolrAccessAuditsService;
import org.apache.ranger.view.VXAccessAuditList;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXAuditMap;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXGroupList;
import org.apache.ranger.view.VXPermMap;
import org.apache.ranger.view.VXPolicyExportAuditList;
import org.apache.ranger.view.VXResource;
import org.apache.ranger.view.VXTrxLog;
import org.apache.ranger.view.VXTrxLogList;
import org.apache.ranger.view.VXTrxLogV2;
import org.apache.ranger.view.VXTrxLogV2.AttributeChangeInfo;
import org.apache.ranger.view.VXTrxLogV2.ObjectChangeInfo;
import org.apache.ranger.view.VXTrxLogV2List;
import org.apache.ranger.view.VXUgsyncAuditInfoList;
import org.apache.ranger.view.VXUser;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;

import java.lang.reflect.Method;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
* @generated by Cursor
* @description <Unit Test for TestAssetMgr class>
*/
@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestAssetMgr {
    @InjectMocks
    AssetMgr assetMgr;

    @Mock
    XPermMapService xPermMapService;

    @Mock
    XAuditMapService xAuditMapService;

    @Mock
    JSONUtil jsonUtil;

    @Mock
    RangerBizUtil msBizUtil;

    @Mock
    StringUtil stringUtil;

    @Mock
    RangerDaoManager rangerDaoManager;

    @Mock
    XUserService xUserService;

    @Mock
    RangerBizUtil xaBizUtil;

    @Mock
    RangerTrxLogV2Service xTrxLogService;

    @Mock
    XAccessAuditService xAccessAuditService;

    @Mock
    XGroupService xGroupService;

    @Mock
    XUserMgr xUserMgr;

    @Mock
    SolrAccessAuditsService solrAccessAuditsService;

    @Mock
    ElasticSearchAccessAuditsService elasticSearchAccessAuditsService;

    @Mock
    CloudWatchAccessAuditsService cloudWatchAccessAuditsService;

    @Mock
    XPolicyService xPolicyService;

    @Mock
    RangerTransactionSynchronizationAdapter transactionSynchronizationAdapter;

    @Mock
    RangerPluginInfoService pluginInfoService;

    @Mock
    XUgsyncAuditInfoService xUgsyncAuditInfoService;

    @Mock
    ServiceMgr serviceMgr;

    @Mock
    XResourceService xResourceService;

    @Mock
    RESTErrorUtil restErrorUtil;

    @Mock
    XPolicyExportAuditService xPolicyExportAuditService;

    @Test
    public void testInit() {
        try (MockedStatic<RangerAdminConfig> mockedConfig = mockStatic(RangerAdminConfig.class)) {
            RangerAdminConfig mockConfig = mock(RangerAdminConfig.class);
            mockedConfig.when(RangerAdminConfig::getInstance).thenReturn(mockConfig);
            when(mockConfig.getBoolean(anyString(), eq(false))).thenReturn(true);

            assetMgr.init();

            assertTrue(assetMgr.rangerLogNotModified);
            assertTrue(assetMgr.pluginActivityAuditLogNotModified);
            assertTrue(assetMgr.pluginActivityAuditCommitInline);
        }
    }

    @Test
    public void testGetLatestRepoPolicyAssetNull() {
        when(restErrorUtil.createRESTException("No Data Found.", MessageEnums.DATA_NOT_FOUND))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> assetMgr.getLatestRepoPolicy(null,
                Collections.emptyList(), 0L, null, true, "0", "127.0.0.1", true, "0", "agent1"));
    }

    @Test
    public void testGetLatestRepoPolicyResourceListNull() {
        VXAsset mockAsset = mock(VXAsset.class);
        when(restErrorUtil.createRESTException("No Data Found.", MessageEnums.DATA_NOT_FOUND))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> assetMgr.getLatestRepoPolicy(mockAsset,
                null, 0L, null, true, "0", "127.0.0.1", true, "0", "agent1"));
    }

    @Test
    public void testGetLatestRepoPolicyAssetDisabled() {
        VXAsset mockAsset = mock(VXAsset.class);
        mockAsset.setActiveStatus(RangerCommonEnums.ACT_STATUS_DISABLED);
        List<VXResource> mockResourceList = Collections.emptyList();

        when(restErrorUtil.createRESTException(anyString(), any(MessageEnums.class))).thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> assetMgr.getLatestRepoPolicy(mockAsset,
                mockResourceList, 0L, null, true, "0", "127.0.0.1", true, "0", "agent1"));
    }

    @Test
    public void testGetLatestRepoPolicySuccessHDFS() {
        VXAsset mockAsset = createMockVXAsset();
        mockAsset.setAssetType(AppConstants.ASSET_HDFS);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = createMockVXResource();
        mockResource.setIsRecursive(1);
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        XXPolicyExportAuditDao mockAuditDao = mock(XXPolicyExportAuditDao.class);
        XXPolicyExportAudit mockAudit = new XXPolicyExportAudit();

        when(restErrorUtil.parseLong(anyString(), anyString(), any(), any(), anyString())).thenReturn(3L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, "0", "127.0.0.1", true, "1", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"result\"}", result);
    }

    @Test
    public void testGetLatestRepoPolicyNotModified() {
        VXAsset mockAsset = mock(VXAsset.class);
        mockAsset.setAssetType(AppConstants.ASSET_HDFS);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");

        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = mock(VXResource.class);
        mockResource.setIsRecursive(1);
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        XXPolicyExportAuditDao mockAuditDao = mock(XXPolicyExportAuditDao.class);
        XXPolicyExportAudit mockAudit = new XXPolicyExportAudit();

        when(restErrorUtil.parseLong(anyString(), anyString(), any(), any(), anyString())).thenReturn(1L);
        when(restErrorUtil.createRESTException(anyInt(), anyString(), anyBoolean())).thenReturn(new WebApplicationException());

        WebApplicationException thrown = assertThrows(WebApplicationException.class, () -> {
            assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L, null, true, "0", "127.0.0.1", true, "1", "agent1");
        });
    }

    @Test
    public void testUpdateDefaultPolicyUserAndPermWithExistingUser() {
        VXResource mockResource = mock(VXResource.class);
        when(mockResource.getId()).thenReturn(1L);
        String userName = "testUser";
        XXUser mockXXUser = mock(XXUser.class);
        VXUser mockVXUser = mock(VXUser.class);
        XXUserDao mockUserDao = mock(XXUserDao.class);
        XXPermMapDao xXPermMapDao = mock(XXPermMapDao.class);
        List<XXPermMap> xXPermMap = new ArrayList<>();
        XXPermMap xXPermMap1 = new XXPermMap();
        xXPermMap1.setId(1L);
        xXPermMap1.setResourceId(1L);
        xXPermMap.add(xXPermMap1);

        when(rangerDaoManager.getXXPermMap()).thenReturn(xXPermMapDao);
        when(xXPermMapDao.findByResourceId(xXPermMap1.getId())).thenReturn(xXPermMap);
        when(rangerDaoManager.getXXUser()).thenReturn(mockUserDao);
        when(mockUserDao.findByUserName(userName)).thenReturn(mockXXUser);
        when(xUserService.populateViewBean(mockXXUser)).thenReturn(mockVXUser);
        when(mockVXUser.getId()).thenReturn(1L);

        assetMgr.updateDefaultPolicyUserAndPerm(mockResource, userName);

        verify(xUserService).populateViewBean(mockXXUser);
        verify(xUserService, never()).createResource(any());
    }

    @Test
    public void testUpdateDefaultPolicyUserAndPermWithNewUser() {
        VXResource mockResource = mock(VXResource.class);
        when(mockResource.getId()).thenReturn(1L);
        String userName = "newUser";
        VXUser mockVXUser = mock(VXUser.class);
        XXUserDao mockUserDao = mock(XXUserDao.class);
        XXPermMapDao xXPermMapDao = mock(XXPermMapDao.class);
        List<XXPermMap> xXPermMap = new ArrayList<>();
        XXPermMap xXPermMap1 = new XXPermMap();
        xXPermMap1.setId(1L);
        xXPermMap1.setResourceId(1L);
        xXPermMap.add(xXPermMap1);

        when(rangerDaoManager.getXXPermMap()).thenReturn(xXPermMapDao);
        when(xXPermMapDao.findByResourceId(xXPermMap1.getId())).thenReturn(xXPermMap);
        when(rangerDaoManager.getXXUser()).thenReturn(mockUserDao);
        when(mockUserDao.findByUserName(userName)).thenReturn(null);
        when(xUserService.createResource(any())).thenReturn(mockVXUser);
        when(mockVXUser.getId()).thenReturn(1L);

        assetMgr.updateDefaultPolicyUserAndPerm(mockResource, userName);

        verify(xUserService).createResource(any());
    }

    @Test
    public void testUpdateDefaultPolicyUserAndPermNullUserName() {
        VXResource mockResource = mock(VXResource.class);

        assetMgr.updateDefaultPolicyUserAndPerm(mockResource, null);

        verify(rangerDaoManager, never()).getXXUser();
    }

    @Test
    public void testCreatePolicyAuditNotModifiedNotLogged() {
        XXPolicyExportAudit mockAudit = mock(XXPolicyExportAudit.class);
        when(mockAudit.getHttpRetCode()).thenReturn(HttpServletResponse.SC_NOT_MODIFIED);
        assetMgr.rangerLogNotModified = false;

        assetMgr.createPolicyAudit(mockAudit);

        verify(transactionSynchronizationAdapter, never()).executeOnTransactionCompletion(any());
        verify(transactionSynchronizationAdapter, never()).executeAsyncOnTransactionComplete(any());
    }

    @Test
    public void testCreatePolicyAuditNotModifiedLogged() {
        XXPolicyExportAudit mockAudit = mock(XXPolicyExportAudit.class);
        when(mockAudit.getHttpRetCode()).thenReturn(HttpServletResponse.SC_NOT_MODIFIED);
        assetMgr.rangerLogNotModified = true;
        assetMgr.pluginActivityAuditCommitInline = true;

        assetMgr.createPolicyAudit(mockAudit);

        verify(transactionSynchronizationAdapter).executeOnTransactionCompletion(any());
    }

    @Test
    public void testCreatePolicyAuditOtherHttpCode() {
        XXPolicyExportAudit mockAudit = mock(XXPolicyExportAudit.class);
        when(mockAudit.getHttpRetCode()).thenReturn(HttpServletResponse.SC_OK);
        assetMgr.pluginActivityAuditCommitInline = false;

        assetMgr.createPolicyAudit(mockAudit);

        verify(transactionSynchronizationAdapter).executeAsyncOnTransactionComplete(any());
    }

    @Test
    public void testCreatePluginInfoPolicies() {
        String serviceName = "testService";
        String pluginId = "testPlugin@host1";
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getRemoteHost()).thenReturn("host1");

        assetMgr.createPluginInfo(serviceName, pluginId, mockRequest,
                RangerPluginInfo.ENTITY_TYPE_POLICIES, 1L, 2L, 123456789L, 200, "cluster1", "capabilities");

        verify(mockRequest).getRemoteHost();
    }

    @Test
    public void testGetReportLogsWithEmptyList() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        PList<VXTrxLogV2> mockPList = mock(PList.class);
        when(mockPList.getList()).thenReturn(Collections.emptyList());
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xTrxLogService.searchTrxLogs(any())).thenReturn(mockPList);

        VXTrxLogList result = assetMgr.getReportLogs(mockCriteria);

        assertNotNull(result);
        assertTrue(result.getList().isEmpty());
    }

    @Test
    public void testGetReportLogsV2WithEmptyList() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        PList<VXTrxLogV2> mockPList = mock(PList.class);
        when(mockPList.getList()).thenReturn(Collections.emptyList());
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xTrxLogService.searchTrxLogs(any())).thenReturn(mockPList);

        VXTrxLogV2List result = assetMgr.getReportLogsV2(mockCriteria);

        assertNotNull(result);
        assertTrue(result.getList().isEmpty());
    }

    @Test
    public void testGetVXTrxLogsV2AdminAccess() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        PList<VXTrxLogV2> mockPList = mock(PList.class);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xTrxLogService.searchTrxLogs(any())).thenReturn(mockPList);

        PList<VXTrxLogV2> result = assetMgr.getVXTrxLogsV2(mockCriteria);

        assertNotNull(result);
        verify(xTrxLogService).searchTrxLogs(any());
    }

    @Test
    public void testGetVXTrxLogsV2NoAccess() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        when(xaBizUtil.isAdmin()).thenReturn(false);
        when(xaBizUtil.isKeyAdmin()).thenReturn(false);
        when(xaBizUtil.isAuditAdmin()).thenReturn(false);
        when(xaBizUtil.isAuditKeyAdmin()).thenReturn(false);
        when(restErrorUtil.create403RESTException("Permission Denied !")).thenReturn(new WebApplicationException());

        assertThrows(RuntimeException.class, () -> assetMgr.getVXTrxLogsV2(mockCriteria));
    }

    @Test
    public void testGetAccessLogsSolrStore() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn(RangerBizUtil.AUDIT_STORE_SOLR);
        when(solrAccessAuditsService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(solrAccessAuditsService).searchXAccessAudits(any());
    }

    @Test
    public void testGetAccessLogsOpenSearchStore() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn(RangerBizUtil.AUDIT_STORE_ELASTIC_SEARCH);
        when(elasticSearchAccessAuditsService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(elasticSearchAccessAuditsService).searchXAccessAudits(any());
    }

    @Test
    public void testGetAccessLogsCloudWatchStore() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn(RangerBizUtil.AUDIT_STORE_CLOUD_WATCH);
        when(cloudWatchAccessAuditsService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(cloudWatchAccessAuditsService).searchXAccessAudits(any());
    }

    @Test
    public void testGetAccessLogsDefaultStore() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn("other");
        when(xAccessAuditService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(xAccessAuditService).searchXAccessAudits(any());
    }

    @Test
    public void testGetTransactionReport() {
        String transactionId = "txn123";
        List<VXTrxLogV2> mockLogsV2 = createMockTrxLogsV2();
        when(xTrxLogService.findByTransactionId(transactionId)).thenReturn(mockLogsV2);

        VXTrxLogList result = assetMgr.getTransactionReport(transactionId);

        assertNotNull(result);
        verify(xTrxLogService).findByTransactionId(transactionId);
    }

    @Test
    public void testGetTransactionReportV2() {
        String transactionId = "txn123";
        List<VXTrxLogV2> mockLogsV2 = createMockTrxLogsV2();
        when(xTrxLogService.findByTransactionId(transactionId)).thenReturn(mockLogsV2);

        VXTrxLogV2List result = assetMgr.getTransactionReportV2(transactionId);

        assertNotNull(result);
        verify(xTrxLogService).findByTransactionId(transactionId);
    }

    @Test
    public void testValidateXXTrxLogListPasswordMasking() {
        List<VXTrxLog> mockLogs = new ArrayList<>();
        VXTrxLog mockLog = mock(VXTrxLog.class);
        when(mockLog.getAttributeName()).thenReturn("Password");
        when(mockLog.getPreviousValue()).thenReturn("oldPassword");
        when(mockLog.getNewValue()).thenReturn("newPassword");
        mockLogs.add(mockLog);

        List<VXTrxLog> result = assetMgr.validateXXTrxLogList(mockLogs);

        assertNotNull(result);
        verify(mockLog).setPreviousValue("*********");
        verify(mockLog).setNewValue("***********");
    }

    @Test
    public void testValidateXXTrxLogListNullValues() {
        List<VXTrxLog> mockLogs = new ArrayList<>();
        VXTrxLog mockLog = mock(VXTrxLog.class);
        when(mockLog.getAttributeName()).thenReturn("SomeAttribute");
        when(mockLog.getPreviousValue()).thenReturn(null);
        when(mockLog.getNewValue()).thenReturn("null");
        mockLogs.add(mockLog);

        List<VXTrxLog> result = assetMgr.validateXXTrxLogList(mockLogs);

        assertNotNull(result);
        verify(mockLog).setPreviousValue("");
        verify(mockLog).setNewValue("");
    }

    @Test
    public void testValidateXXTrxLogListConnectionConfigurationsPreviousValue() {
        List<VXTrxLog> mockLogs = new ArrayList<>();
        VXTrxLog mockLog = mock(VXTrxLog.class);
        when(mockLog.getAttributeName()).thenReturn("Connection Configurations");
        when(mockLog.getPreviousValue()).thenReturn("{\"password\":\"oldpassword\"},config2");
        when(mockLog.getNewValue()).thenReturn("newvalue");
        mockLogs.add(mockLog);

        List<VXTrxLog> result = assetMgr.validateXXTrxLogList(mockLogs);

        assertNotNull(result);
        verify(mockLog).setPreviousValue("{\"password\":\"*****\"},config2");
    }

    @Test
    public void testValidateXXTrxLogListConnectionConfigurationsNewValue() {
        List<VXTrxLog> mockLogs = new ArrayList<>();
        VXTrxLog mockLog = mock(VXTrxLog.class);
        when(mockLog.getAttributeName()).thenReturn("Connection Configurations");
        when(mockLog.getPreviousValue()).thenReturn("");
        when(mockLog.getNewValue()).thenReturn("config1,{\"password\":\"newpassword\"}");
        mockLogs.add(mockLog);

        List<VXTrxLog> result = assetMgr.validateXXTrxLogList(mockLogs);

        assertNotNull(result);
        verify(mockLog).setNewValue("config1,{\"password\":\"*****\"}");
    }

    @Test
    public void testValidateXXTrxLogV2ListPasswordMasking() {
        List<VXTrxLogV2> mockLogsV2 = new ArrayList<>();
        VXTrxLogV2 mockLogV2 = createMockTrxLogV2WithPassword();
        mockLogsV2.add(mockLogV2);

        List<VXTrxLogV2> result = assetMgr.validateXXTrxLogV2List(mockLogsV2);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testValidateXXTrxLogV2ListConnectionConfigurations() {
        List<VXTrxLogV2> mockLogsV2 = new ArrayList<>();
        VXTrxLogV2 mockLogV2 = mock(VXTrxLogV2.class);

        ObjectChangeInfo changeInfo = mock(ObjectChangeInfo.class);
        AttributeChangeInfo attrChangeInfo = mock(AttributeChangeInfo.class);

        when(attrChangeInfo.getAttributeName()).thenReturn("Connection Configurations");
        when(attrChangeInfo.getOldValue()).thenReturn("{\"password\":\"oldpassword\"}");
        when(attrChangeInfo.getNewValue()).thenReturn("{\"password\":\"newpassword\"}");

        List<AttributeChangeInfo> attributes = new ArrayList<>();
        attributes.add(attrChangeInfo);

        when(changeInfo.getAttributes()).thenReturn(attributes);
        when(mockLogV2.getChangeInfo()).thenReturn(changeInfo);
        mockLogsV2.add(mockLogV2);

        List<VXTrxLogV2> result = assetMgr.validateXXTrxLogV2List(mockLogsV2);

        assertNotNull(result);
        verify(attrChangeInfo).setOldValue("{\"password\":\"*****\"}");
        verify(attrChangeInfo).setNewValue("{\"password\":\"*****\"}");
    }

    @Test
    public void testGetLatestRepoPolicyHiveAsset() {
        VXAsset mockAsset = mock(VXAsset.class);
        mockAsset.setAssetType(AppConstants.ASSET_HIVE);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");
        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);
        when(mockAsset.getAssetType()).thenReturn(AppConstants.ASSET_HIVE);

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = mock(VXResource.class);
        mockResource.setResourceType(AppConstants.RESOURCE_UDF);
        mockResource.setUdfs("test_udf");
        mockResource.setDatabases("test_db");
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        when(mockResource.getId()).thenReturn(1L);
        when(mockResource.getAuditList()).thenReturn(new ArrayList<>());
        when(mockResource.getPermMapList()).thenReturn(new ArrayList<>());
        when(restErrorUtil.parseLong(anyString(), anyString(), any(), any(), anyString())).thenReturn(3L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"hive_result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, "0", "127.0.0.1", true, "1", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"hive_result\"}", result);
    }

    @Test
    public void testGetLatestRepoPolicyHBaseAsset() {
        VXAsset mockAsset = mock(VXAsset.class);
        mockAsset.setAssetType(AppConstants.ASSET_HBASE);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");
        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);
        when(mockAsset.getAssetType()).thenReturn(AppConstants.ASSET_HBASE);

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = mock(VXResource.class);
        mockResource.setTables("test_table");
        mockResource.setColumns("test_column");
        mockResource.setColumnFamilies("test_cf");
        mockResource.setIsEncrypt(1);
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        when(mockResource.getId()).thenReturn(1L);
        when(mockResource.getAuditList()).thenReturn(new ArrayList<>());
        when(mockResource.getPermMapList()).thenReturn(new ArrayList<>());
        when(restErrorUtil.parseLong(anyString(), anyString(), any(), any(), anyString())).thenReturn(3L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"hbase_result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, "0", "127.0.0.1", true, "1", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"hbase_result\"}", result);
    }

    @Test
    public void testGetLatestRepoPolicyKnoxAsset() {
        VXAsset mockAsset = mock(VXAsset.class);
        mockAsset.setAssetType(AppConstants.ASSET_KNOX);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");
        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);
        when(mockAsset.getAssetType()).thenReturn(AppConstants.ASSET_KNOX);

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = mock(VXResource.class);
        mockResource.setTopologies("test_topology");
        mockResource.setServices("test_service");
        mockResource.setIsEncrypt(0);
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        when(mockResource.getId()).thenReturn(1L);
        when(mockResource.getAuditList()).thenReturn(new ArrayList<>());
        when(mockResource.getPermMapList()).thenReturn(new ArrayList<>());
        when(restErrorUtil.parseLong(anyString(), anyString(), any(), any(), anyString())).thenReturn(3L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"knox_result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, "0", "127.0.0.1", true, "1", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"knox_result\"}", result);
    }

    @Test
    public void testGetLatestRepoPolicyStormAsset() {
        VXAsset mockAsset = mock(VXAsset.class);
        mockAsset.setAssetType(AppConstants.ASSET_STORM);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");
        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);
        when(mockAsset.getAssetType()).thenReturn(AppConstants.ASSET_STORM);

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = mock(VXResource.class);
        mockResource.setTopologies("test_topology");
        mockResource.setIsEncrypt(1);
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        when(mockResource.getId()).thenReturn(1L);
        when(mockResource.getAuditList()).thenReturn(new ArrayList<>());
        when(mockResource.getPermMapList()).thenReturn(new ArrayList<>());
        when(restErrorUtil.parseLong(anyString(), anyString(), any(), any(), anyString())).thenReturn(3L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"storm_result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, "0", "127.0.0.1", true, "1", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"storm_result\"}", result);
    }

    @Test
    public void testGetLatestRepoPolicyUnsupportedAssetType() {
        VXAsset mockAsset = mock(VXAsset.class);
        mockAsset.setAssetType(999);
        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = mock(VXResource.class);
        mockResourceList.add(mockResource);

        when(restErrorUtil.createRESTException(anyString(), any(MessageEnums.class)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                    null, true, "0", "127.0.0.1", true, "1", "agent1");
        });
    }

    @Test
    public void testGetLatestRepoPolicyHttpsRequired() {
        VXAsset mockAsset = mock(VXAsset.class);
        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);

        VXResource resource = mock(VXResource.class);
        List<VXResource> mockResourceList = Collections.singletonList(resource);

        when(restErrorUtil.createRESTException(anyString(), any(MessageEnums.class)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                    null, false, "0", "127.0.0.1", false, "1", "agent1");
        });
    }

    @Test
    public void testGetLatestRepoPolicyNoCertificates() {
        VXAsset mockAsset = mock(VXAsset.class);
        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);

        VXResource resource = mock(VXResource.class);
        List<VXResource> mockResourceList = Collections.singletonList(resource);

        when(restErrorUtil.createRESTException(anyString(), any(MessageEnums.class)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                    null, false, "0", "127.0.0.1", true, "1", "agent1");
        });
    }

    @Test
    public void testUpdateDefaultPolicyUserAndPermEmptyUserName() {
        VXResource mockResource = mock(VXResource.class);

        assetMgr.updateDefaultPolicyUserAndPerm(mockResource, "");

        verify(rangerDaoManager, never()).getXXUser();
    }

    @Test
    public void testCreatePolicyAuditOtherHttpCodeWithInline() {
        XXPolicyExportAudit mockAudit = mock(XXPolicyExportAudit.class);
        when(mockAudit.getHttpRetCode()).thenReturn(HttpServletResponse.SC_OK);
        assetMgr.pluginActivityAuditCommitInline = true;

        assetMgr.createPolicyAudit(mockAudit);

        verify(transactionSynchronizationAdapter).executeOnTransactionCompletion(any());
    }

    @Test
    public void testCreatePluginInfoTags() {
        String serviceName = "testService";
        String pluginId = "testPlugin@host1";
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getRemoteHost()).thenReturn("host1");

        assetMgr.createPluginInfo(serviceName, pluginId, mockRequest,
                RangerPluginInfo.ENTITY_TYPE_TAGS, 1L, 2L, 123456789L, 200, "cluster1", "capabilities");

        verify(mockRequest).getRemoteHost();
    }

    @Test
    public void testCreatePluginInfoRoles() {
        String serviceName = "testService";
        String pluginId = "testPlugin@host1";
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getRemoteHost()).thenReturn("host1");

        assetMgr.createPluginInfo(serviceName, pluginId, mockRequest,
                RangerPluginInfo.ENTITY_TYPE_ROLES, 1L, 2L, 123456789L, 200, "cluster1", "capabilities");

        verify(mockRequest).getRemoteHost();
    }

    @Test
    public void testCreatePluginInfoUserStore() {
        String serviceName = "testService";
        String pluginId = "testPlugin@host1";
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getRemoteHost()).thenReturn("host1");

        assetMgr.createPluginInfo(serviceName, pluginId, mockRequest,
                RangerPluginInfo.ENTITY_TYPE_USERSTORE, 1L, 2L, 123456789L, 200, "cluster1", "capabilities");

        verify(mockRequest).getRemoteHost();
    }

    @Test
    public void testCreatePluginInfoGds() {
        String serviceName = "testService";
        String pluginId = "testPlugin@host1";
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getRemoteHost()).thenReturn("host1");

        assetMgr.createPluginInfo(serviceName, pluginId, mockRequest,
                RangerPluginInfo.ENTITY_TYPE_GDS, 1L, 2L, 123456789L, 200, "cluster1", "capabilities");

        verify(mockRequest).getRemoteHost();
    }

    @Test
    public void testGetAccessLogsWithSearchCriteriaParamList() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        HashMap<String, Object> paramList = new HashMap<>();
        paramList.put("startDate", new Date());
        paramList.put("endDate", new Date());

        when(mockCriteria.getParamList()).thenReturn(paramList);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn("other");
        when(xAccessAuditService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(xAccessAuditService).searchXAccessAudits(any());
    }

    @Test
    public void testGetAccessLogsNonAdminWithZones() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        VXGroupList mockGroupList = mock(VXGroupList.class);
        VXGroup mockGroup = mock(VXGroup.class);

        when(mockCriteria.getSortType()).thenReturn("asc");
        when(mockCriteria.getParamValue("zoneName")).thenReturn(null);
        when(xaBizUtil.isAdmin()).thenReturn(false);
        when(xaBizUtil.getXUserId()).thenReturn(1L);
        when(rangerDaoManager.getXXSecurityZoneDao()).thenReturn(mock(XXSecurityZoneDao.class));
        when(rangerDaoManager.getXXSecurityZoneDao().findZoneNamesByUserId(1L)).thenReturn(Arrays.asList("zone1"));
        when(xUserMgr.getXUserGroups(1L)).thenReturn(mockGroupList);
        when(mockGroupList.getList()).thenReturn(Arrays.asList(mockGroup));
        when(mockGroup.getId()).thenReturn(2L);
        when(rangerDaoManager.getXXSecurityZoneDao().findZoneNamesByGroupId(2L)).thenReturn(Arrays.asList("zone2"));
        when(xaBizUtil.getAuditDBType()).thenReturn("other");
        when(xAccessAuditService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
    }

    @Test
    public void testGetVXTrxLogsV2WithParamList() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        PList<VXTrxLogV2> mockPList = mock(PList.class);
        HashMap<String, Object> paramList = new HashMap<>();
        paramList.put("startDate", new Date());
        paramList.put("endDate", new Date());
        paramList.put("owner", "testOwner");

        XXPortalUser mockPortalUser = mock(XXPortalUser.class);
        when(mockPortalUser.getId()).thenReturn(1L);

        when(mockCriteria.getParamList()).thenReturn(paramList);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(rangerDaoManager.getXXPortalUser()).thenReturn(mock(XXPortalUserDao.class));
        when(rangerDaoManager.getXXPortalUser().findByLoginId("testOwner")).thenReturn(mockPortalUser);
        when(xTrxLogService.searchTrxLogs(any())).thenReturn(mockPList);

        PList<VXTrxLogV2> result = assetMgr.getVXTrxLogsV2(mockCriteria);

        assertNotNull(result);
        verify(xTrxLogService).searchTrxLogs(any());
    }

    @Test
    public void testIsTagDownloadRequestFalse() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("isTagDownloadRequest", int.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(assetMgr, RangerPluginInfo.ENTITY_TYPE_POLICIES);

        assertFalse(result);
    }

    @Test
    public void testIsRoleDownloadRequestFalse() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("isRoleDownloadRequest", int.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(assetMgr, RangerPluginInfo.ENTITY_TYPE_POLICIES);

        assertFalse(result);
    }

    @Test
    public void testIsUserStoreDownloadRequestFalse() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("isUserStoreDownloadRequest", int.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(assetMgr, RangerPluginInfo.ENTITY_TYPE_POLICIES);

        assertFalse(result);
    }

    @Test
    public void testIsGdsDownloadRequestFalse() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("isGdsDownloadRequest", int.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(assetMgr, RangerPluginInfo.ENTITY_TYPE_POLICIES);

        assertFalse(result);
    }

    @Test
    public void testSearchXPolicyExportAuditsWithParamList() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXPolicyExportAuditList mockResult = mock(VXPolicyExportAuditList.class);
        HashMap<String, Object> paramList = new HashMap<>();
        paramList.put("startDate", new Date());
        paramList.put("endDate", new Date());

        when(mockCriteria.getParamList()).thenReturn(paramList);
        when(xPolicyExportAuditService.searchXPolicyExportAudits(any())).thenReturn(mockResult);

        VXPolicyExportAuditList result = assetMgr.searchXPolicyExportAudits(mockCriteria);

        assertNotNull(result);
        verify(xPolicyExportAuditService).searchXPolicyExportAudits(any());
    }

    @Test
    public void testGetUgsyncAuditsWithParamList() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXUgsyncAuditInfoList mockResult = mock(VXUgsyncAuditInfoList.class);
        HashMap<String, Object> paramList = new HashMap<>();
        paramList.put("startDate", new Date());
        paramList.put("endDate", new Date());

        when(mockCriteria.getParamList()).thenReturn(paramList);
        when(mockCriteria.getSortType()).thenReturn("invalid");
        when(msBizUtil.hasModuleAccess(anyString())).thenReturn(true);
        when(xUgsyncAuditInfoService.searchXUgsyncAuditInfoList(any())).thenReturn(mockResult);

        VXUgsyncAuditInfoList result = assetMgr.getUgsyncAudits(mockCriteria);

        assertNotNull(result);
        verify(xUgsyncAuditInfoService).searchXUgsyncAuditInfoList(any());
    }

    @Test
    public void testGetUgsyncAuditsBySyncSourceSuccess() {
        String syncSource = "testSource";
        VXUgsyncAuditInfoList mockResult = mock(VXUgsyncAuditInfoList.class);
        when(msBizUtil.hasModuleAccess(anyString())).thenReturn(true);
        when(xUgsyncAuditInfoService.searchXUgsyncAuditInfoBySyncSource(syncSource))
                .thenReturn(mockResult);

        VXUgsyncAuditInfoList result = assetMgr.getUgsyncAuditsBySyncSource(syncSource);

        assertNotNull(result);
        verify(xUgsyncAuditInfoService).searchXUgsyncAuditInfoBySyncSource(syncSource);
    }

    @Test
    public void testGetUgsyncAuditsBySyncSourceInvalidSource() {
        when(msBizUtil.hasModuleAccess(anyString())).thenReturn(true);
        when(restErrorUtil.createRESTException(anyString(), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> assetMgr.getUgsyncAuditsBySyncSource(""));
    }

    @Test
    public void testGetUgsyncAuditsBySyncSourceWithoutModuleAccess() {
        String syncSource = "testSource";

        when(msBizUtil.hasModuleAccess(anyString())).thenReturn(false);
        when(restErrorUtil.createRESTException(anyInt(), anyString(), anyBoolean()))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> assetMgr.getUgsyncAuditsBySyncSource(syncSource));
    }

    @Test
    public void testGetBooleanValueTrue() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("getBooleanValue", int.class);
        method.setAccessible(true);

        String result = (String) method.invoke(assetMgr, 1);

        assertEquals("1", result);
    }

    @Test
    public void testGetBooleanValueFalse() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("getBooleanValue", int.class);
        method.setAccessible(true);

        String result = (String) method.invoke(assetMgr, 0);

        assertEquals("0", result);
    }

    @Test
    public void testGetRemoteAddressXForwarded() throws Exception {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getHeader("X-Forwarded-For")).thenReturn("192.168.1.1,10.0.0.1");

        Method method = AssetMgr.class.getDeclaredMethod("getRemoteAddress", HttpServletRequest.class);
        method.setAccessible(true);

        String result = (String) method.invoke(assetMgr, mockRequest);

        assertEquals("192.168.1.1", result);
    }

    @Test
    public void testGetRemoteAddressRemoteAddr() throws Exception {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getHeader("X-Forwarded-For")).thenReturn(null);
        when(mockRequest.getRemoteAddr()).thenReturn("127.0.0.1");

        Method method = AssetMgr.class.getDeclaredMethod("getRemoteAddress", HttpServletRequest.class);
        method.setAccessible(true);

        String result = (String) method.invoke(assetMgr, mockRequest);

        assertEquals("127.0.0.1", result);
    }

    @Test
    public void testGetRemoteAddressNullRequest() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("getRemoteAddress", HttpServletRequest.class);
        method.setAccessible(true);

        String result = (String) method.invoke(assetMgr, (HttpServletRequest) null);

        assertNull(result);
    }

    @Test
    public void testIsPolicyDownloadRequest() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("isPolicyDownloadRequest", int.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(assetMgr, RangerPluginInfo.ENTITY_TYPE_POLICIES);

        assertTrue(result);
    }

    @Test
    public void testIsTagDownloadRequest() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("isTagDownloadRequest", int.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(assetMgr, RangerPluginInfo.ENTITY_TYPE_TAGS);

        assertTrue(result);
    }

    @Test
    public void testIsRoleDownloadRequest() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("isRoleDownloadRequest", int.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(assetMgr, RangerPluginInfo.ENTITY_TYPE_ROLES);

        assertTrue(result);
    }

    @Test
    public void testIsUserStoreDownloadRequest() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("isUserStoreDownloadRequest", int.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(assetMgr, RangerPluginInfo.ENTITY_TYPE_USERSTORE);

        assertTrue(result);
    }

    @Test
    public void testIsGdsDownloadRequest() throws Exception {
        Method method = AssetMgr.class.getDeclaredMethod("isGdsDownloadRequest", int.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(assetMgr, RangerPluginInfo.ENTITY_TYPE_GDS);

        assertTrue(result);
    }

    // Helper methods to create mock objects
    private VXResource createMockVXResource() {
        VXResource resource = mock(VXResource.class);
        when(resource.getId()).thenReturn(1L);
        when(resource.getName()).thenReturn("testResource");
        when(resource.getAuditList()).thenReturn(new ArrayList<>());
        when(resource.getPermMapList()).thenReturn(new ArrayList<>());
        return resource;
    }

    private VXAsset createMockVXAsset() {
        VXAsset asset = mock(VXAsset.class);
        when(asset.getName()).thenReturn("testAsset");
        when(asset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);
        when(asset.getAssetType()).thenReturn(AppConstants.ASSET_HDFS);
        return asset;
    }

    private Map<String, String> createConfigMap() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("commonNameForCertificate", "testCN");
        return configMap;
    }

    private List<VXTrxLogV2> createMockTrxLogsV2() {
        List<VXTrxLogV2> logs = new ArrayList<>();
        VXTrxLogV2 log = mock(VXTrxLogV2.class);
        when(log.getChangeInfo()).thenReturn(null);
        logs.add(log);
        return logs;
    }

    private VXTrxLogV2 createMockTrxLogV2WithPassword() {
        VXTrxLogV2 logV2 = mock(VXTrxLogV2.class);
        ObjectChangeInfo changeInfo = mock(ObjectChangeInfo.class);
        AttributeChangeInfo attrChangeInfo = mock(AttributeChangeInfo.class);

        when(attrChangeInfo.getAttributeName()).thenReturn("Password");
        when(attrChangeInfo.getOldValue()).thenReturn("oldPassword");
        when(attrChangeInfo.getNewValue()).thenReturn("newPassword");

        List<AttributeChangeInfo> attributes = new ArrayList<>();
        attributes.add(attrChangeInfo);

        when(changeInfo.getAttributes()).thenReturn(attributes);
        when(logV2.getChangeInfo()).thenReturn(changeInfo);

        return logV2;
    }

    @Test
    public void testGetUgsyncAuditsNoModuleAccess() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        when(msBizUtil.hasModuleAccess(anyString())).thenReturn(false);
        when(restErrorUtil.createRESTException(anyInt(), anyString(), anyBoolean()))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> assetMgr.getUgsyncAudits(mockCriteria));
    }

    @Test
    public void testValidateXXTrxLogV2ListNullChangeInfo() {
        List<VXTrxLogV2> mockLogsV2 = new ArrayList<>();
        VXTrxLogV2 mockLogV2 = mock(VXTrxLogV2.class);
        when(mockLogV2.getChangeInfo()).thenReturn(null);
        mockLogsV2.add(mockLogV2);

        List<VXTrxLogV2> result = assetMgr.validateXXTrxLogV2List(mockLogsV2);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testGetLatestRepoPolicyInvalidNameException() {
        VXAsset mockAsset = mock(VXAsset.class);
        mockAsset.setAssetType(AppConstants.ASSET_HDFS);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");

        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = mock(VXResource.class);
        mockResourceList.add(mockResource);

        X509Certificate mockCert = mock(X509Certificate.class);
        X509Certificate[] certchain = {mockCert};

        when(mockCert.getSubjectX500Principal()).thenReturn(mock(X500Principal.class));
        when(mockCert.getSubjectX500Principal().getName()).thenReturn("invalid_dn");
        when(restErrorUtil.createRESTException(anyString(), any(MessageEnums.class)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                    certchain, true, "0", "127.0.0.1", true, "1", "agent1");
        });
    }

    @Test
    public void testGetLatestRepoPolicyCommonNameMismatch() {
        VXAsset mockAsset = mock(VXAsset.class);
        mockAsset.setAssetType(AppConstants.ASSET_HDFS);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"expectedCN\"}");

        when(mockAsset.getName()).thenReturn("testAsset");
        when(mockAsset.getActiveStatus()).thenReturn(RangerCommonEnums.ACT_STATUS_ACTIVE);

        Map<String, String> map = new HashMap<>();
        map.put("commonNameForCertificate", "testCN");

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = mock(VXResource.class);
        mockResourceList.add(mockResource);

        X509Certificate mockCert = mock(X509Certificate.class);
        X509Certificate[] certchain = {mockCert};

        when(mockCert.getSubjectX500Principal()).thenReturn(mock(X500Principal.class));
        when(mockCert.getSubjectX500Principal().getName()).thenReturn("CN=differentCN,OU=test");
        when(restErrorUtil.createRESTException(anyString(), any(MessageEnums.class)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                    certchain, true, "0", "127.0.0.1", true, "1", "agent1");
        });
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotFound() {
        String serviceName = "testService";
        String pluginId = "testPlugin@host1";
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getRemoteHost()).thenReturn("host1");

        assetMgr.createPluginInfo(serviceName, pluginId, mockRequest,
                RangerPluginInfo.ENTITY_TYPE_POLICIES, -1L, -1L, 123456789L, 404, "cluster1", "capabilities");

        verify(mockRequest).getRemoteHost();
    }

    @Test
    public void testDoDeleteXXPluginInfoWithRangerPluginInfo() {
        RangerPluginInfo mockPluginInfo = mock(RangerPluginInfo.class);
        when(mockPluginInfo.getServiceName()).thenReturn("testService");
        when(mockPluginInfo.getHostName()).thenReturn("testHost");
        when(mockPluginInfo.getAppType()).thenReturn("testApp");

        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockXXPluginInfo);
        when(mockXXPluginInfo.getId()).thenReturn(1L);

        // Use reflection to access the private method
        try {
            Method method = AssetMgr.class.getDeclaredMethod("doDeleteXXPluginInfo", RangerPluginInfo.class);
            method.setAccessible(true);
            method.invoke(assetMgr, mockPluginInfo);
        } catch (Exception e) {
            // Handle reflection exception
        }

        verify(mockDao).remove(1L);
    }

    @Test
    public void testGetAccessLogsZoneAdminPermission() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        VXGroupList mockGroupList = mock(VXGroupList.class);
        VXGroup mockGroup = mock(VXGroup.class);

        when(mockCriteria.getParamValue("zoneName")).thenReturn(Arrays.asList("zone1"));
        when(xaBizUtil.isAdmin()).thenReturn(false);
        when(xaBizUtil.getXUserId()).thenReturn(1L);
        when(rangerDaoManager.getXXSecurityZoneDao()).thenReturn(mock(XXSecurityZoneDao.class));
        when(rangerDaoManager.getXXSecurityZoneDao().findZoneNamesByUserId(1L)).thenReturn(Arrays.asList("zone1"));
        when(xUserMgr.getXUserGroups(1L)).thenReturn(mockGroupList);
        when(mockGroupList.getList()).thenReturn(Arrays.asList(mockGroup));
        when(mockGroup.getId()).thenReturn(2L);
        when(rangerDaoManager.getXXSecurityZoneDao().findZoneNamesByGroupId(2L)).thenReturn(Arrays.asList("zone2"));
        when(serviceMgr.isZoneAdmin("zone1")).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn("other");
        when(xAccessAuditService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(serviceMgr).isZoneAdmin("zone1");
    }

    @Test
    public void testGetAccessLogsZoneAuditorPermission() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        VXGroupList mockGroupList = mock(VXGroupList.class);
        VXGroup mockGroup = mock(VXGroup.class);

        when(mockCriteria.getParamValue("zoneName")).thenReturn(Arrays.asList("zone1"));
        when(xaBizUtil.isAdmin()).thenReturn(false);
        when(xaBizUtil.getXUserId()).thenReturn(1L);
        when(rangerDaoManager.getXXSecurityZoneDao()).thenReturn(mock(XXSecurityZoneDao.class));
        when(rangerDaoManager.getXXSecurityZoneDao().findZoneNamesByUserId(1L)).thenReturn(Arrays.asList("zone1"));
        when(xUserMgr.getXUserGroups(1L)).thenReturn(mockGroupList);
        when(mockGroupList.getList()).thenReturn(Arrays.asList(mockGroup));
        when(mockGroup.getId()).thenReturn(2L);
        when(rangerDaoManager.getXXSecurityZoneDao().findZoneNamesByGroupId(2L)).thenReturn(Arrays.asList("zone2"));
        when(serviceMgr.isZoneAdmin("zone1")).thenReturn(false);
        when(serviceMgr.isZoneAuditor("zone1")).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn("other");
        when(xAccessAuditService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(serviceMgr).isZoneAuditor("zone1");
    }

    @Test
    public void testGetAccessLogsZonePermissionDenied() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXGroupList mockGroupList = mock(VXGroupList.class);
        VXGroup mockGroup = mock(VXGroup.class);

        when(mockCriteria.getParamValue("zoneName")).thenReturn(Arrays.asList("zone1"));
        when(xaBizUtil.isAdmin()).thenReturn(false);
        when(xaBizUtil.getXUserId()).thenReturn(1L);
        when(rangerDaoManager.getXXSecurityZoneDao()).thenReturn(mock(XXSecurityZoneDao.class));
        when(rangerDaoManager.getXXSecurityZoneDao().findZoneNamesByUserId(1L)).thenReturn(Arrays.asList("zone1"));
        when(xUserMgr.getXUserGroups(1L)).thenReturn(mockGroupList);
        when(mockGroupList.getList()).thenReturn(Arrays.asList(mockGroup));
        when(mockGroup.getId()).thenReturn(2L);
        when(rangerDaoManager.getXXSecurityZoneDao().findZoneNamesByGroupId(2L)).thenReturn(Arrays.asList("zone2"));
        when(serviceMgr.isZoneAdmin("zone1")).thenReturn(false);
        when(serviceMgr.isZoneAuditor("zone1")).thenReturn(false);
        when(restErrorUtil.createRESTException(anyInt(), anyString(), anyBoolean())).thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> assetMgr.getAccessLogs(mockCriteria));
    }

    @Test
    public void testGetVXTrxLogsV2KeyAdminAccess() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        PList<VXTrxLogV2> mockPList = mock(PList.class);
        when(xaBizUtil.isAdmin()).thenReturn(false);
        when(xaBizUtil.isKeyAdmin()).thenReturn(true);
        when(xTrxLogService.searchTrxLogs(any())).thenReturn(mockPList);

        PList<VXTrxLogV2> result = assetMgr.getVXTrxLogsV2(mockCriteria);

        assertNotNull(result);
        verify(xTrxLogService).searchTrxLogs(any());
    }

    @Test
    public void testGetVXTrxLogsV2AuditAdminAccess() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        PList<VXTrxLogV2> mockPList = mock(PList.class);
        when(xaBizUtil.isAdmin()).thenReturn(false);
        when(xaBizUtil.isKeyAdmin()).thenReturn(false);
        when(xaBizUtil.isAuditAdmin()).thenReturn(true);
        when(xTrxLogService.searchTrxLogs(any())).thenReturn(mockPList);

        PList<VXTrxLogV2> result = assetMgr.getVXTrxLogsV2(mockCriteria);

        assertNotNull(result);
        verify(xTrxLogService).searchTrxLogs(any());
    }

    @Test
    public void testGetVXTrxLogsV2AuditKeyAdminAccess() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        PList<VXTrxLogV2> mockPList = mock(PList.class);
        when(xaBizUtil.isAdmin()).thenReturn(false);
        when(xaBizUtil.isKeyAdmin()).thenReturn(false);
        when(xaBizUtil.isAuditAdmin()).thenReturn(false);
        when(xaBizUtil.isAuditKeyAdmin()).thenReturn(true);
        when(xTrxLogService.searchTrxLogs(any())).thenReturn(mockPList);

        PList<VXTrxLogV2> result = assetMgr.getVXTrxLogsV2(mockCriteria);

        assertNotNull(result);
        verify(xTrxLogService).searchTrxLogs(any());
    }

    @Test
    public void testValidateXXTrxLogListNonPasswordAttribute() {
        List<VXTrxLog> mockLogs = new ArrayList<>();
        VXTrxLog mockLog = mock(VXTrxLog.class);
        when(mockLog.getAttributeName()).thenReturn("SomeOtherAttribute");
        when(mockLog.getPreviousValue()).thenReturn("oldValue");
        when(mockLog.getNewValue()).thenReturn("newValue");
        mockLogs.add(mockLog);

        List<VXTrxLog> result = assetMgr.validateXXTrxLogList(mockLogs);

        assertNotNull(result);
        verify(mockLog, never()).setPreviousValue("*********");
        verify(mockLog, never()).setNewValue("***********");
    }

    @Test
    public void testValidateXXTrxLogV2ListNonPasswordAttribute() {
        List<VXTrxLogV2> mockLogsV2 = new ArrayList<>();
        VXTrxLogV2 mockLogV2 = mock(VXTrxLogV2.class);

        ObjectChangeInfo changeInfo = mock(ObjectChangeInfo.class);
        AttributeChangeInfo attrChangeInfo = mock(AttributeChangeInfo.class);

        when(attrChangeInfo.getAttributeName()).thenReturn("SomeOtherAttribute");
        when(attrChangeInfo.getOldValue()).thenReturn("oldValue");
        when(attrChangeInfo.getNewValue()).thenReturn("newValue");

        List<AttributeChangeInfo> attributes = new ArrayList<>();
        attributes.add(attrChangeInfo);

        when(changeInfo.getAttributes()).thenReturn(attributes);
        when(mockLogV2.getChangeInfo()).thenReturn(changeInfo);
        mockLogsV2.add(mockLogV2);

        List<VXTrxLogV2> result = assetMgr.validateXXTrxLogV2List(mockLogsV2);

        assertNotNull(result);
        verify(attrChangeInfo, never()).setOldValue("*********");
        verify(attrChangeInfo, never()).setNewValue("***********");
    }

    @Test
    public void testGetLatestRepoPolicyWithNullPolicyCount() {
        VXAsset mockAsset = createMockVXAsset();
        mockAsset.setAssetType(AppConstants.ASSET_HDFS);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = createMockVXResource();
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        when(restErrorUtil.parseLong(eq(null), anyString(), any(), any(), anyString())).thenReturn(null);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, "0", "127.0.0.1", true, null, "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"result\"}", result);
    }

    @Test
    public void testGetLatestRepoPolicyWithNullEpoch() {
        VXAsset mockAsset = createMockVXAsset();
        mockAsset.setAssetType(AppConstants.ASSET_HDFS);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = createMockVXResource();
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        when(restErrorUtil.parseLong(eq("0"), anyString(), any(), any(), anyString())).thenReturn(0L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, null, "127.0.0.1", true, "0", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"result\"}", result);
    }

    @Test
    public void testGetLatestRepoPolicyWithNullEpochValue() {
        VXAsset mockAsset = createMockVXAsset();
        mockAsset.setAssetType(AppConstants.ASSET_HDFS);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = createMockVXResource();
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        when(restErrorUtil.parseLong(eq("0"), anyString(), any(), any(), anyString())).thenReturn(0L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, null, "127.0.0.1", true, "0", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"result\"}", result);
    }

    @Test
    public void testCreatePolicyAuditNotModifiedLoggedAsync() {
        XXPolicyExportAudit mockAudit = mock(XXPolicyExportAudit.class);
        when(mockAudit.getHttpRetCode()).thenReturn(HttpServletResponse.SC_NOT_MODIFIED);
        assetMgr.rangerLogNotModified = true;
        assetMgr.pluginActivityAuditCommitInline = false;

        assetMgr.createPolicyAudit(mockAudit);

        verify(transactionSynchronizationAdapter).executeAsyncOnTransactionComplete(any());
    }

    @Test
    public void testGetAccessLogsNullSortType() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        when(mockCriteria.getSortType()).thenReturn(null);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn("other");
        when(xAccessAuditService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(mockCriteria).setSortType("desc");
    }

    @Test
    public void testGetAccessLogsInvalidSortType() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        when(mockCriteria.getSortType()).thenReturn("invalid");
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn("other");
        when(xAccessAuditService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(mockCriteria).setSortType("desc");
    }

    @Test
    public void testGetAccessLogsValidAscSortType() {
        SearchCriteria mockCriteria = mock(SearchCriteria.class);
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        when(mockCriteria.getSortType()).thenReturn("asc");
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn("other");
        when(xAccessAuditService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(mockCriteria);

        assertNotNull(result);
        verify(mockCriteria, never()).setSortType(anyString());
    }

    @Test
    public void testPopulatePermMapWithGroupPermissions() throws Exception {
        VXResource mockResource = mock(VXResource.class);
        when(mockResource.getPermMapList()).thenReturn(new ArrayList<>());
        HashMap<String, Object> resourceMap = new HashMap<>();

        VXPermMap permMap1 = new VXPermMap();
        permMap1.setId(1L);
        permMap1.setPermGroup("group1");
        permMap1.setGroupId(10L);
        permMap1.setGroupName("testGroup1");
        permMap1.setPermType(1);

        VXPermMap permMap2 = new VXPermMap();
        permMap2.setId(2L);
        permMap2.setPermGroup("group1");
        permMap2.setGroupId(11L);
        permMap2.setGroupName("testGroup2");
        permMap2.setPermType(2);

        List<VXPermMap> permMapList = Arrays.asList(permMap1, permMap2);
        when(mockResource.getPermMapList()).thenReturn(permMapList);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("populatePermMap", VXResource.class, HashMap.class, int.class);
        method.setAccessible(true);
        HashMap<String, Object> result = (HashMap<String, Object>) method.invoke(assetMgr, mockResource, resourceMap, AppConstants.ASSET_HDFS);

        assertNotNull(result);
        assertTrue(result.containsKey("permission"));
    }

    @Test
    public void testPopulatePermMapWithUserPermissions() throws Exception {
        VXResource mockResource = mock(VXResource.class);
        when(mockResource.getPermMapList()).thenReturn(new ArrayList<>());
        HashMap<String, Object> resourceMap = new HashMap<>();

        VXPermMap permMap1 = new VXPermMap();
        permMap1.setId(1L);
        permMap1.setPermGroup("group1");
        permMap1.setUserId(10L);
        permMap1.setUserName("testUser1");
        permMap1.setPermType(1);

        VXPermMap permMap2 = new VXPermMap();
        permMap2.setId(2L);
        permMap2.setPermGroup("group1");
        permMap2.setUserId(11L);
        permMap2.setUserName("testUser2");
        permMap2.setPermType(2);

        List<VXPermMap> permMapList = Arrays.asList(permMap1, permMap2);
        when(mockResource.getPermMapList()).thenReturn(permMapList);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("populatePermMap", VXResource.class, HashMap.class, int.class);
        method.setAccessible(true);
        HashMap<String, Object> result = (HashMap<String, Object>) method.invoke(assetMgr, mockResource, resourceMap, AppConstants.ASSET_HDFS);

        assertNotNull(result);
        assertTrue(result.containsKey("permission"));
    }

    @Test
    public void testPopulatePermMapWithKnoxAssetType() throws Exception {
        VXResource mockResource = mock(VXResource.class);
        when(mockResource.getPermMapList()).thenReturn(new ArrayList<>());
        HashMap<String, Object> resourceMap = new HashMap<>();

        VXPermMap permMap = new VXPermMap();
        permMap.setId(1L);
        permMap.setPermGroup("group1");
        permMap.setGroupId(10L);
        permMap.setGroupName("testGroup");
        permMap.setPermType(1);
        permMap.setIpAddress("192.168.1.1,10.0.0.1");

        List<VXPermMap> permMapList = Arrays.asList(permMap);
        when(mockResource.getPermMapList()).thenReturn(permMapList);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("populatePermMap", VXResource.class, HashMap.class, int.class);
        method.setAccessible(true);
        HashMap<String, Object> result = (HashMap<String, Object>) method.invoke(assetMgr, mockResource, resourceMap, AppConstants.ASSET_KNOX);

        assertNotNull(result);
        assertTrue(result.containsKey("permission"));
    }

    @Test
    public void testPopulatePermMapWithNullIpAddress() throws Exception {
        VXResource mockResource = mock(VXResource.class);
        when(mockResource.getPermMapList()).thenReturn(new ArrayList<>());
        HashMap<String, Object> resourceMap = new HashMap<>();

        VXPermMap permMap = new VXPermMap();
        permMap.setId(1L);
        permMap.setPermGroup("group1");
        permMap.setGroupId(10L);
        permMap.setGroupName("testGroup");
        permMap.setPermType(1);
        permMap.setIpAddress(null);

        List<VXPermMap> permMapList = Arrays.asList(permMap);
        when(mockResource.getPermMapList()).thenReturn(permMapList);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("populatePermMap", VXResource.class, HashMap.class, int.class);
        method.setAccessible(true);
        HashMap<String, Object> result = (HashMap<String, Object>) method.invoke(assetMgr, mockResource, resourceMap, AppConstants.ASSET_KNOX);

        assertNotNull(result);
        assertTrue(result.containsKey("permission"));
    }

    @Test
    public void testCreateOrUpdatePluginInfoWithNullServiceName() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName(null);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, false, "cluster1");

        assertNull(result);
    }

    @Test
    public void testCreateOrUpdatePluginInfoWithEmptyServiceName() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("");

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, false, "cluster1");

        assertNull(result);
    }

    @Test
    public void testCreateOrUpdatePluginInfoNewPlugin() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setPolicyActiveVersion(1L);
        pluginInfo.setPolicyDownloadedVersion(1L);
        pluginInfo.setPolicyActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(null);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockXXPluginInfo);
        when(mockDao.create(any())).thenReturn(mockXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).create(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoUpdateExistingPlugin() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setPolicyActiveVersion(2L);
        pluginInfo.setPolicyDownloadedVersion(2L);
        pluginInfo.setPolicyActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setPolicyDownloadedVersion(1L);
        existingPluginInfo.setPolicyActiveVersion(1L);
        existingPluginInfo.setPolicyActivationTime(System.currentTimeMillis() - 1000);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoTagVersionReset() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.1");

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setTagDownloadedVersion(1L);
        existingPluginInfo.setTagActiveVersion(1L);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, true, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testGetTransactionReportWithObjectChangeInfo() {
        String transactionId = "txn123";

        VXTrxLogV2 mockLogV2 = mock(VXTrxLogV2.class);
        ObjectChangeInfo changeInfo = mock(ObjectChangeInfo.class);
        AttributeChangeInfo attrChangeInfo1 = mock(AttributeChangeInfo.class);
        AttributeChangeInfo attrChangeInfo2 = mock(AttributeChangeInfo.class);

        when(attrChangeInfo1.getAttributeName()).thenReturn("attr1");
        when(attrChangeInfo1.getOldValue()).thenReturn("oldValue1");
        when(attrChangeInfo1.getNewValue()).thenReturn("newValue1");

        when(attrChangeInfo2.getAttributeName()).thenReturn("attr2");
        when(attrChangeInfo2.getOldValue()).thenReturn("oldValue2");
        when(attrChangeInfo2.getNewValue()).thenReturn("newValue2");

        List<AttributeChangeInfo> attributes = Arrays.asList(attrChangeInfo1, attrChangeInfo2);
        when(changeInfo.getAttributes()).thenReturn(attributes);
        when(mockLogV2.getChangeInfo()).thenReturn(changeInfo);

        List<VXTrxLogV2> mockLogsV2 = Arrays.asList(mockLogV2);
        when(xTrxLogService.findByTransactionId(transactionId)).thenReturn(mockLogsV2);

        VXTrxLogList result = assetMgr.getTransactionReport(transactionId);

        assertNotNull(result);
        assertEquals(2, result.getList().size());
        verify(xTrxLogService).findByTransactionId(transactionId);
    }

    @Test
    public void testGetTransactionReportWithNullChangeInfo() {
        String transactionId = "txn123";

        VXTrxLogV2 mockLogV2 = mock(VXTrxLogV2.class);
        when(mockLogV2.getChangeInfo()).thenReturn(null);

        List<VXTrxLogV2> mockLogsV2 = Arrays.asList(mockLogV2);
        when(xTrxLogService.findByTransactionId(transactionId)).thenReturn(mockLogsV2);

        VXTrxLogList result = assetMgr.getTransactionReport(transactionId);

        assertNotNull(result);
        assertEquals(1, result.getList().size());
        verify(xTrxLogService).findByTransactionId(transactionId);
    }

    @Test
    public void testGetTransactionReportWithEmptyAttributes() {
        String transactionId = "txn123";

        VXTrxLogV2 mockLogV2 = mock(VXTrxLogV2.class);
        ObjectChangeInfo changeInfo = mock(ObjectChangeInfo.class);
        when(changeInfo.getAttributes()).thenReturn(Collections.emptyList());
        when(mockLogV2.getChangeInfo()).thenReturn(changeInfo);

        List<VXTrxLogV2> mockLogsV2 = Arrays.asList(mockLogV2);
        when(xTrxLogService.findByTransactionId(transactionId)).thenReturn(mockLogsV2);

        VXTrxLogList result = assetMgr.getTransactionReport(transactionId);

        assertNotNull(result);
        assertEquals(1, result.getList().size());
        verify(xTrxLogService).findByTransactionId(transactionId);
    }

    @Test
    public void testGetLatestRepoPolicyWithAuditMaps() {
        VXAsset mockAsset = createMockVXAsset();
        mockAsset.setAssetType(AppConstants.ASSET_HDFS);
        mockAsset.setConfig("{\"commonNameForCertificate\":\"testCN\"}");

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = createMockVXResource();
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setPermMapList(Collections.emptyList());

        VXAuditMap auditMap = new VXAuditMap();
        auditMap.setId(1L);
        List<VXAuditMap> auditList = Arrays.asList(auditMap);
        mockResource.setAuditList(auditList);
        mockResourceList.add(mockResource);

        when(restErrorUtil.parseLong(anyString(), anyString(), any(), any(), anyString())).thenReturn(3L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"result_with_audit\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, "0", "127.0.0.1", true, "1", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"result_with_audit\"}", result);
    }

    @Test
    public void testValidateXXTrxLogListConnectionConfigurationsComplexPassword() {
        List<VXTrxLog> mockLogs = new ArrayList<>();
        VXTrxLog mockLog = mock(VXTrxLog.class);
        when(mockLog.getAttributeName()).thenReturn("Connection Configurations");
        when(mockLog.getPreviousValue()).thenReturn("config1,{\"password\":\"complex123\"},config2");
        when(mockLog.getNewValue()).thenReturn("config1,\"password\":\"simple456\",config2");
        mockLogs.add(mockLog);

        List<VXTrxLog> result = assetMgr.validateXXTrxLogList(mockLogs);

        assertNotNull(result);
        verify(mockLog).setPreviousValue("config1,{\"password\":\"*****\"},config2");
        verify(mockLog).setNewValue("config1,\"password\":\"******\",config2");
    }

    @Test
    public void testValidateXXTrxLogV2ListConnectionConfigurationsComplexPassword() {
        List<VXTrxLogV2> mockLogsV2 = new ArrayList<>();
        VXTrxLogV2 mockLogV2 = mock(VXTrxLogV2.class);

        ObjectChangeInfo changeInfo = mock(ObjectChangeInfo.class);
        AttributeChangeInfo attrChangeInfo = mock(AttributeChangeInfo.class);

        when(attrChangeInfo.getAttributeName()).thenReturn("Connection Configurations");
        when(attrChangeInfo.getOldValue()).thenReturn("config1,{\"password\":\"complex123\"},config2");
        when(attrChangeInfo.getNewValue()).thenReturn("config1,\"password\":\"simple456\",config2");

        List<AttributeChangeInfo> attributes = new ArrayList<>();
        attributes.add(attrChangeInfo);

        when(changeInfo.getAttributes()).thenReturn(attributes);
        when(mockLogV2.getChangeInfo()).thenReturn(changeInfo);
        mockLogsV2.add(mockLogV2);

        List<VXTrxLogV2> result = assetMgr.validateXXTrxLogV2List(mockLogsV2);

        assertNotNull(result);
        verify(attrChangeInfo).setOldValue("config1,{\"password\":\"*****\"},config2");
        verify(attrChangeInfo).setNewValue("config1,\"password\":\"******\",config2");
    }

    @Test
    public void testGetLatestRepoPolicyHiveTableResource() {
        VXAsset mockAsset = createMockVXAsset();
        mockAsset.setAssetType(AppConstants.ASSET_HIVE);

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = createMockVXResource();
        mockResource.setResourceType(AppConstants.RESOURCE_TABLE);
        mockResource.setTables("test_table");
        mockResource.setDatabases("test_db");
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        when(restErrorUtil.parseLong(anyString(), anyString(), any(), any(), anyString())).thenReturn(3L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"hive_table_result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, "0", "127.0.0.1", true, "1", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"hive_table_result\"}", result);
    }

    @Test
    public void testGetLatestRepoPolicyHiveColumnResource() {
        VXAsset mockAsset = createMockVXAsset();
        mockAsset.setAssetType(AppConstants.ASSET_HIVE);

        List<VXResource> mockResourceList = new ArrayList<>();
        VXResource mockResource = createMockVXResource();
        mockResource.setResourceType(AppConstants.RESOURCE_COLUMN);
        mockResource.setTables("test_table");
        mockResource.setColumns("test_column");
        mockResource.setDatabases("test_db");
        mockResource.setResourceStatus(RangerCommonEnums.ACT_STATUS_ACTIVE);
        mockResource.setAuditList(Collections.emptyList());
        mockResource.setPermMapList(Collections.emptyList());
        mockResourceList.add(mockResource);

        when(restErrorUtil.parseLong(anyString(), anyString(), any(), any(), anyString())).thenReturn(3L);
        when(jsonUtil.readMapToString(any())).thenReturn("{\"test\":\"hive_column_result\"}");

        String result = assetMgr.getLatestRepoPolicy(mockAsset, mockResourceList, 0L,
                null, true, "0", "127.0.0.1", true, "1", "agent1");

        assertNotNull(result);
        assertEquals("{\"test\":\"hive_column_result\"}", result);
    }

    @Test
    public void testCreateOrUpdatePluginInfoForTagsEntityType() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setTagActiveVersion(1L);
        pluginInfo.setTagDownloadedVersion(1L);
        pluginInfo.setTagActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(null);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockXXPluginInfo);
        when(mockDao.create(any())).thenReturn(mockXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_TAGS, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).create(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoForRolesEntityType() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setRoleActiveVersion(1L);
        pluginInfo.setRoleDownloadedVersion(1L);
        pluginInfo.setRoleActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(null);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockXXPluginInfo);
        when(mockDao.create(any())).thenReturn(mockXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_ROLES, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).create(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoForUserStoreEntityType() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setUserStoreActiveVersion(1L);
        pluginInfo.setUserStoreDownloadedVersion(1L);
        pluginInfo.setUserStoreActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(null);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockXXPluginInfo);
        when(mockDao.create(any())).thenReturn(mockXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_USERSTORE, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).create(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoForGdsEntityType() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setGdsActiveVersion(1L);
        pluginInfo.setGdsDownloadedVersion(1L);
        pluginInfo.setGdsActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(null);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockXXPluginInfo);
        when(mockDao.create(any())).thenReturn(mockXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_GDS, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).create(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoUpdateTagsEntity() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setTagActiveVersion(2L);
        pluginInfo.setTagDownloadedVersion(2L);
        pluginInfo.setTagActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setTagDownloadedVersion(1L);
        existingPluginInfo.setTagActiveVersion(1L);
        existingPluginInfo.setTagActivationTime(System.currentTimeMillis() - 1000);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_TAGS, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoUpdateRolesEntity() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setRoleActiveVersion(2L);
        pluginInfo.setRoleDownloadedVersion(2L);
        pluginInfo.setRoleActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setRoleDownloadedVersion(1L);
        existingPluginInfo.setRoleActiveVersion(1L);
        existingPluginInfo.setRoleActivationTime(System.currentTimeMillis() - 1000);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_ROLES, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoUpdateUserStoreEntity() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setUserStoreActiveVersion(2L);
        pluginInfo.setUserStoreDownloadedVersion(2L);
        pluginInfo.setUserStoreActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setUserStoreDownloadedVersion(1L);
        existingPluginInfo.setUserStoreActiveVersion(1L);
        existingPluginInfo.setUserStoreActivationTime(System.currentTimeMillis() - 1000);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_USERSTORE, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoUpdateGdsEntity() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setGdsActiveVersion(2L);
        pluginInfo.setGdsDownloadedVersion(2L);
        pluginInfo.setGdsActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setGdsDownloadedVersion(1L);
        existingPluginInfo.setGdsActiveVersion(1L);
        existingPluginInfo.setGdsActivationTime(System.currentTimeMillis() - 1000);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_GDS, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testDoDeleteXXPluginInfoWithRangerPluginInfoNotFound() {
        RangerPluginInfo mockPluginInfo = mock(RangerPluginInfo.class);
        when(mockPluginInfo.getServiceName()).thenReturn("testService");
        when(mockPluginInfo.getHostName()).thenReturn("testHost");
        when(mockPluginInfo.getAppType()).thenReturn("testApp");

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(null);

        // Use reflection to access the private method
        try {
            Method method = AssetMgr.class.getDeclaredMethod("doDeleteXXPluginInfo", RangerPluginInfo.class);
            method.setAccessible(true);
            method.invoke(assetMgr, mockPluginInfo);
        } catch (Exception e) {
            // Handle reflection exception
        }

        verify(mockDao, never()).remove(anyLong());
    }

    @Test
    public void testGetRemoteAddressWithBlankXForwardedFor() throws Exception {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getHeader("X-Forwarded-For")).thenReturn("");
        when(mockRequest.getRemoteAddr()).thenReturn("127.0.0.1");

        Method method = AssetMgr.class.getDeclaredMethod("getRemoteAddress", HttpServletRequest.class);
        method.setAccessible(true);

        String result = (String) method.invoke(assetMgr, mockRequest);

        assertEquals("127.0.0.1", result);
    }

    @Test
    public void testGetRemoteAddressWithEmptyForwardedAddresses() throws Exception {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getHeader("X-Forwarded-For")).thenReturn("   ");
        when(mockRequest.getRemoteAddr()).thenReturn("127.0.0.1");

        Method method = AssetMgr.class.getDeclaredMethod("getRemoteAddress", HttpServletRequest.class);
        method.setAccessible(true);

        String result = (String) method.invoke(assetMgr, mockRequest);

        assertEquals("127.0.0.1", result);
    }

    @Test
    public void testGetAccessLogsNullCriteria() {
        VXAccessAuditList mockResult = mock(VXAccessAuditList.class);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xaBizUtil.getAuditDBType()).thenReturn("other");
        when(xAccessAuditService.searchXAccessAudits(any())).thenReturn(mockResult);

        VXAccessAuditList result = assetMgr.getAccessLogs(null);

        assertNotNull(result);
        verify(xAccessAuditService).searchXAccessAudits(any());
    }

    @Test
    public void testGetVXTrxLogsV2NullCriteria() {
        PList<VXTrxLogV2> mockPList = mock(PList.class);
        when(xaBizUtil.isAdmin()).thenReturn(true);
        when(xTrxLogService.searchTrxLogs(any())).thenReturn(mockPList);

        PList<VXTrxLogV2> result = assetMgr.getVXTrxLogsV2(null);

        assertNotNull(result);
        verify(xTrxLogService).searchTrxLogs(any());
    }

    @Test
    public void testSearchXPolicyExportAuditsNullCriteria() {
        VXPolicyExportAuditList mockResult = mock(VXPolicyExportAuditList.class);
        when(xPolicyExportAuditService.searchXPolicyExportAudits(any())).thenReturn(mockResult);

        VXPolicyExportAuditList result = assetMgr.searchXPolicyExportAudits(null);

        assertNotNull(result);
        verify(xPolicyExportAuditService).searchXPolicyExportAudits(any());
    }

    @Test
    public void testGetUgsyncAuditsNullCriteria() {
        VXUgsyncAuditInfoList mockResult = mock(VXUgsyncAuditInfoList.class);
        when(msBizUtil.hasModuleAccess(anyString())).thenReturn(true);
        when(xUgsyncAuditInfoService.searchXUgsyncAuditInfoList(any())).thenReturn(mockResult);

        VXUgsyncAuditInfoList result = assetMgr.getUgsyncAudits(null);

        assertNotNull(result);
        verify(xUgsyncAuditInfoService).searchXUgsyncAuditInfoList(any());
    }

    @Test
    public void testGetUgsyncAuditsBySyncSourceNullSource() {
        when(msBizUtil.hasModuleAccess(anyString())).thenReturn(true);
        when(restErrorUtil.createRESTException(anyString(), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> assetMgr.getUgsyncAuditsBySyncSource(null));
    }

    @Test
    public void testGetUgsyncAuditsBySyncSourceEmptySource() {
        when(msBizUtil.hasModuleAccess(anyString())).thenReturn(true);
        when(restErrorUtil.createRESTException(anyString(), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> assetMgr.getUgsyncAuditsBySyncSource("   "));
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotModifiedWithActivationTimeUpdate() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setPolicyActivationTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setPolicyActivationTime(System.currentTimeMillis() - 10000);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockXXPluginInfo)).thenReturn(existingPluginInfo);
        when(rangerDaoManager.getXXService()).thenReturn(mock(XXServiceDao.class));
        when(rangerDaoManager.getXXService().findAssociatedTagService("testService")).thenReturn(null);

        assetMgr.pluginActivityAuditLogNotModified = false;
        assetMgr.pluginActivityAuditCommitInline = true;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, HttpServletResponse.SC_NOT_MODIFIED, "cluster1");

        verify(transactionSynchronizationAdapter).executeOnTransactionCompletion(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotModifiedNoUpdate() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setPolicyActivationTime(1000L);

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setPolicyActivationTime(1000L);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockXXPluginInfo)).thenReturn(existingPluginInfo);
        when(rangerDaoManager.getXXService()).thenReturn(mock(XXServiceDao.class));
        when(rangerDaoManager.getXXService().findAssociatedTagService("testService")).thenReturn(null);

        assetMgr.pluginActivityAuditLogNotModified = false;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, HttpServletResponse.SC_NOT_MODIFIED, "cluster1");

        verify(transactionSynchronizationAdapter, never()).executeOnTransactionCompletion(any());
        verify(transactionSynchronizationAdapter, never()).executeAsyncOnTransactionComplete(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotFoundWithActiveVersion() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setPolicyActiveVersion(1L);

        assetMgr.pluginActivityAuditCommitInline = true;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, HttpServletResponse.SC_NOT_FOUND, "cluster1");

        verify(transactionSynchronizationAdapter).executeOnTransactionCompletion(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotFoundWithNullActiveVersion() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setPolicyActiveVersion(null);

        assetMgr.pluginActivityAuditCommitInline = false;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, HttpServletResponse.SC_NOT_FOUND, "cluster1");

        verify(transactionSynchronizationAdapter).executeAsyncOnTransactionComplete(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotModifiedNoUpdateForTag() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setTagActivationTime(1000L);

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setPolicyActivationTime(1000L);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockXXPluginInfo)).thenReturn(existingPluginInfo);

        assetMgr.pluginActivityAuditLogNotModified = false;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_TAGS, HttpServletResponse.SC_NOT_MODIFIED, "cluster1");
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotModifiedNoUpdateForRole() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setRoleActivationTime(1000L);

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setPolicyActivationTime(1000L);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockXXPluginInfo)).thenReturn(existingPluginInfo);

        assetMgr.pluginActivityAuditLogNotModified = false;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_ROLES, HttpServletResponse.SC_NOT_MODIFIED, "cluster1");
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotModifiedNoUpdateForUserStore() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setUserStoreActivationTime(1000L);

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setPolicyActivationTime(1000L);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockXXPluginInfo)).thenReturn(existingPluginInfo);

        assetMgr.pluginActivityAuditLogNotModified = false;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_USERSTORE, HttpServletResponse.SC_NOT_MODIFIED, "cluster1");
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotModifiedNoUpdateForGds() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setGdsActivationTime(1000L);

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setPolicyActivationTime(1000L);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockXXPluginInfo)).thenReturn(existingPluginInfo);

        assetMgr.pluginActivityAuditLogNotModified = false;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_GDS, HttpServletResponse.SC_NOT_MODIFIED, "cluster1");
    }

    @Test
    public void testCreateOrUpdatePluginInfoNotFoundWithMinusOneActiveVersion() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setPolicyActiveVersion(-1L);

        assetMgr.pluginActivityAuditCommitInline = true;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, HttpServletResponse.SC_NOT_FOUND, "cluster1");

        verify(transactionSynchronizationAdapter).executeOnTransactionCompletion(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoOtherHttpCode() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");

        assetMgr.pluginActivityAuditCommitInline = false;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, HttpServletResponse.SC_OK, "cluster1");

        verify(transactionSynchronizationAdapter).executeAsyncOnTransactionComplete(any());
    }

    @Test
    public void testCreateOrUpdatePluginInfoTagVersionReset2() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockXXPluginInfo)).thenReturn(new RangerPluginInfo());
        when(rangerDaoManager.getXXService()).thenReturn(mock(XXServiceDao.class));
        when(rangerDaoManager.getXXService().findAssociatedTagService("testService")).thenReturn(null);

        assetMgr.pluginActivityAuditLogNotModified = true;
        assetMgr.pluginActivityAuditCommitInline = true;

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("createOrUpdatePluginInfo", RangerPluginInfo.class, int.class, int.class, String.class);
        method.setAccessible(true);
        method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, HttpServletResponse.SC_NOT_MODIFIED, "cluster1");

        verify(transactionSynchronizationAdapter).executeOnTransactionCompletion(any());
    }

    @Test
    public void testCreatePluginInfoWithBlankPluginId() {
        String serviceName = "testService";
        String pluginId = "";
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getRemoteHost()).thenReturn("host1");

        assetMgr.createPluginInfo(serviceName, pluginId, mockRequest,
                RangerPluginInfo.ENTITY_TYPE_POLICIES, 1L, 2L, 123456789L, 200, "cluster1", "capabilities");

        verify(mockRequest).getRemoteHost();
    }

    @Test
    public void testCreatePluginInfoWithNullRequest() {
        String serviceName = "testService";
        String pluginId = "testPlugin@host1";

        assetMgr.createPluginInfo(serviceName, pluginId, null,
                RangerPluginInfo.ENTITY_TYPE_POLICIES, 1L, 2L, 123456789L, 200, "cluster1", "capabilities");

        // Should not throw exception
    }

    @Test
    public void testCreatePluginInfoWithEmptyPluginCapabilities() {
        String serviceName = "testService";
        String pluginId = "testPlugin@host1";
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getRemoteHost()).thenReturn("host1");

        assetMgr.createPluginInfo(serviceName, pluginId, mockRequest,
                RangerPluginInfo.ENTITY_TYPE_POLICIES, 1L, 2L, 123456789L, 200, "cluster1", "");

        verify(mockRequest).getRemoteHost();
    }

    @Test
    public void testCreatePluginInfoWithNullPluginCapabilities() {
        String serviceName = "testService";
        String pluginId = "testPlugin@host1";
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getRemoteHost()).thenReturn("host1");

        assetMgr.createPluginInfo(serviceName, pluginId, mockRequest,
                RangerPluginInfo.ENTITY_TYPE_POLICIES, 1L, 2L, 123456789L, 200, "cluster1", null);

        verify(mockRequest).getRemoteHost();
    }

    @Test
    public void testDoCreateOrUpdateXXPluginInfoWithClusterNameAndInfoMap() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");

        Map<String, String> infoMap = new HashMap<>();
        infoMap.put("existingKey", "existingValue");
        pluginInfo.setInfo(infoMap);

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(null);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockXXPluginInfo);
        when(mockDao.create(any())).thenReturn(mockXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, false, "testCluster");

        assertNotNull(result);
        verify(mockDao).create(any());
    }

    @Test
    public void testDoCreateOrUpdateXXPluginInfoWithNullInfoMap() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setInfo(null);

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockXXPluginInfo = mock(XXPluginInfo.class);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(null);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockXXPluginInfo);
        when(mockDao.create(any())).thenReturn(mockXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, false, "testCluster");

        assertNotNull(result);
        verify(mockDao).create(any());
    }

    @Test
    public void testDoCreateOrUpdateXXPluginInfoUpdateWithClusterNameChange() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.1");

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        Map<String, String> existingInfoMap = new HashMap<>();
        existingInfoMap.put("CLUSTER_NAME", "oldCluster");
        existingPluginInfo.setInfo(existingInfoMap);

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, false, "newCluster");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testDoCreateOrUpdateXXPluginInfoUpdateNoChangesNeeded() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.1");

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setInfo(new HashMap<>());

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, false, "");

        assertNull(result);
    }

    @Test
    public void testDoCreateOrUpdateXXPluginInfoPolicyDownloadVersionMinus1() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setPolicyActiveVersion(-1L);
        pluginInfo.setPolicyDownloadTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setInfo(new HashMap<>());

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_POLICIES, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testDoCreateOrUpdateXXPluginInfoTagDownloadVersionMinus1() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setTagActiveVersion(-1L);
        pluginInfo.setTagDownloadTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setInfo(new HashMap<>());

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_TAGS, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testDoCreateOrUpdateXXPluginInfoRoleDownloadVersionMinus1() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setRoleActiveVersion(-1L);
        pluginInfo.setRoleDownloadTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setInfo(new HashMap<>());

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_ROLES, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testDoCreateOrUpdateXXPluginInfoUserStoreDownloadVersionMinus1() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setUserStoreActiveVersion(-1L);
        pluginInfo.setUserStoreDownloadTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setInfo(new HashMap<>());

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_USERSTORE, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testDoCreateOrUpdateXXPluginInfoGdsDownloadVersionMinus1() throws Exception {
        RangerPluginInfo pluginInfo = new RangerPluginInfo();
        pluginInfo.setServiceName("testService");
        pluginInfo.setHostName("testHost");
        pluginInfo.setAppType("testApp");
        pluginInfo.setIpAddress("192.168.1.2");
        pluginInfo.setGdsActiveVersion(-1L);
        pluginInfo.setGdsDownloadTime(System.currentTimeMillis());

        XXPluginInfoDao mockDao = mock(XXPluginInfoDao.class);
        XXPluginInfo mockExistingXXPluginInfo = mock(XXPluginInfo.class);
        XXPluginInfo mockUpdatedXXPluginInfo = mock(XXPluginInfo.class);

        RangerPluginInfo existingPluginInfo = new RangerPluginInfo();
        existingPluginInfo.setIpAddress("192.168.1.1");
        existingPluginInfo.setInfo(new HashMap<>());

        when(rangerDaoManager.getXXPluginInfo()).thenReturn(mockDao);
        when(mockDao.find("testService", "testHost", "testApp")).thenReturn(mockExistingXXPluginInfo);
        when(pluginInfoService.populateViewObject(mockExistingXXPluginInfo)).thenReturn(existingPluginInfo);
        when(pluginInfoService.populateDBObject(any())).thenReturn(mockUpdatedXXPluginInfo);
        when(mockDao.update(any())).thenReturn(mockUpdatedXXPluginInfo);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("doCreateOrUpdateXXPluginInfo", RangerPluginInfo.class, int.class, boolean.class, String.class);
        method.setAccessible(true);
        XXPluginInfo result = (XXPluginInfo) method.invoke(assetMgr, pluginInfo, RangerPluginInfo.ENTITY_TYPE_GDS, false, "cluster1");

        assertNotNull(result);
        verify(mockDao).update(any());
    }

    @Test
    public void testPopulatePermMapWithNullPermGroup() throws Exception {
        VXResource mockResource = mock(VXResource.class);
        when(mockResource.getPermMapList()).thenReturn(new ArrayList<>());
        HashMap<String, Object> resourceMap = new HashMap<>();

        VXPermMap permMap = new VXPermMap();
        permMap.setId(1L);
        permMap.setPermGroup(null);
        permMap.setGroupId(10L);
        permMap.setGroupName("testGroup");
        permMap.setPermType(1);

        List<VXPermMap> permMapList = Arrays.asList(permMap);
        when(mockResource.getPermMapList()).thenReturn(permMapList);

        // Use reflection to test the private method
        Method method = AssetMgr.class.getDeclaredMethod("populatePermMap", VXResource.class, HashMap.class, int.class);
        method.setAccessible(true);
        HashMap<String, Object> result = (HashMap<String, Object>) method.invoke(assetMgr, mockResource, resourceMap, AppConstants.ASSET_HDFS);

        assertNotNull(result);
        assertTrue(result.containsKey("permission"));
    }
}
