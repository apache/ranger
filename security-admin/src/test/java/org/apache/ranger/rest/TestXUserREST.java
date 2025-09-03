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

import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.SessionMgr;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAuditMapDao;
import org.apache.ranger.db.XXGroupDao;
import org.apache.ranger.db.XXGroupPermissionDao;
import org.apache.ranger.db.XXGroupUserDao;
import org.apache.ranger.db.XXPermMapDao;
import org.apache.ranger.db.XXPolicyDao;
import org.apache.ranger.db.XXResourceDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.db.XXUserDao;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.entity.XXAuditMap;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXGroupGroup;
import org.apache.ranger.entity.XXGroupPermission;
import org.apache.ranger.entity.XXPermMap;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXResource;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.ranger.plugin.model.RangerPrincipal;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.service.AuthSessionService;
import org.apache.ranger.service.XAuditMapService;
import org.apache.ranger.service.XGroupGroupService;
import org.apache.ranger.service.XGroupPermissionService;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XGroupUserService;
import org.apache.ranger.service.XModuleDefService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XResourceService;
import org.apache.ranger.service.XUserPermissionService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.ugsyncutil.model.GroupUserInfo;
import org.apache.ranger.ugsyncutil.model.UsersGroupRoleAssignments;
import org.apache.ranger.view.VXAuditMap;
import org.apache.ranger.view.VXAuditMapList;
import org.apache.ranger.view.VXAuthSession;
import org.apache.ranger.view.VXAuthSessionList;
import org.apache.ranger.view.VXDataObject;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXGroupList;
import org.apache.ranger.view.VXGroupPermission;
import org.apache.ranger.view.VXGroupPermissionList;
import org.apache.ranger.view.VXGroupUser;
import org.apache.ranger.view.VXGroupUserInfo;
import org.apache.ranger.view.VXGroupUserList;
import org.apache.ranger.view.VXLong;
import org.apache.ranger.view.VXModuleDef;
import org.apache.ranger.view.VXModuleDefList;
import org.apache.ranger.view.VXModulePermissionList;
import org.apache.ranger.view.VXPermMap;
import org.apache.ranger.view.VXPermMapList;
import org.apache.ranger.view.VXResource;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXStringList;
import org.apache.ranger.view.VXUgsyncAuditInfo;
import org.apache.ranger.view.VXUser;
import org.apache.ranger.view.VXUserGroupInfo;
import org.apache.ranger.view.VXUserList;
import org.apache.ranger.view.VXUserPermission;
import org.apache.ranger.view.VXUserPermissionList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;

/**
* @generated by Cursor
* @description <Unit Test for TestXUserREST class>
*/
@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestXUserREST {
    @InjectMocks
    XUserREST xUserRest = new XUserREST();
    Long   id     = 1L;
    VXUser vxUser = createVXUser();
    @Mock XUserMgr                  xUserMgr;
    @Mock VXGroup                   vxGroup;
    @Mock SearchCriteria            searchCriteria;
    @Mock XGroupService             xGroupService;
    @Mock SearchUtil                searchUtil;
    @Mock StringUtil                stringUtil;
    @Mock VXLong                    vXLong;
    @Mock HttpServletRequest        request;
    @Mock VXUser                    vXUser1;
    @Mock VXUserGroupInfo           vXUserGroupInfo;
    @Mock RangerBizUtil             bizUtil;
    @Mock ServiceDBStore            svcStore;
    @Mock XUserService              xUserService;
    @Mock VXUserList                vXUserList;
    @Mock VXGroupUser               vXGroupUser;
    @Mock XGroupUserService         xGroupUserService;
    @Mock VXGroupUserList           vXGroupUserList;
    @Mock XGroupGroupService        xGroupGroupService;
    @Mock VXPermMap                 vXPermMap;
    @Mock RESTErrorUtil             restErrorUtil;
    @Mock WebApplicationException   webApplicationException;
    @Mock XResourceService          xResourceService;
    @Mock VXDataObject              vxDataObject;
    @Mock AppConstants              appConstants;
    @Mock RangerConstants           rangerConstants;
    @Mock VXResource                vxResource;
    @Mock VXResponse                vxResponse;
    @Mock XXResource                xxResource;
    @Mock XXAuditMap                xxAuditMap;
    @Mock XAuditMapService          xAuditMapService;
    @Mock XPermMapService           xPermMapService;
    @Mock XXAsset                   xxAsset;
    @Mock RangerDaoManager          rangerDaoManager;
    @Mock XXPermMap                 xxPermMap;
    @Mock Response                  response;
    @Mock VXPermMapList             vXPermMapList;
    @Mock VXAuditMap                vXAuditMap;
    @Mock VXAuditMapList            vXAuditMapList;
    @Mock AuthSessionService        authSessionService;
    @Mock SessionMgr                sessionMgr;
    @Mock VXAuthSessionList         vXAuthSessionList;
    @Mock VXModuleDef               vXModuleDef;
    @Mock VXUserPermission          vXUserPermission;
    @Mock VXUserPermissionList      vXUserPermissionList;
    @Mock VXGroupPermission         vXGroupPermission;
    @Mock XModuleDefService         xModuleDefService;
    @Mock VXModuleDefList           vxModuleDefList;
    @Mock XUserPermissionService    xUserPermissionService;
    @Mock VXGroupPermissionList     vXGroupPermissionList;
    @Mock XGroupPermissionService   xGroupPermissionService;
    @Mock VXStringList              vXStringList;
    @Mock VXString                  vXString;
    @Mock XXGroupDao                xXGroupDao;
    @Mock XXGroup                   xXGroup;
    @Mock XXGroupGroup              xXGroupGroup;
    @Mock XXGroupPermission         xXGroupPermission;
    @Mock XXGroupPermissionDao      xXGroupPermissionDao;
    @Mock XXPolicyDao               xXPolicyDao;
    @Mock XXPolicy                  xXPolicy;
    @Mock XXGroupUserDao            xXGroupUserDao;
    @Mock XXUserDao                 xXUserDao;
    @Mock XXUser                    xXUser;
    @Mock XXPermMapDao              xXPermMapDao;
    @Mock XXResourceDao             xXResourceDao;
    @Mock XXAuditMapDao             xXAuditMapDao;
    @Mock RangerPolicy              rangerPolicy;
    @Mock RangerPolicyItem          rangerPolicyItem;
    @Mock RangerDataMaskPolicyItem  rangerDataMaskPolicyItem;
    @Mock RangerRowFilterPolicyItem rangerRowFilterPolicyItem;
    @Mock ServiceUtil               serviceUtil;
    @Mock XXServiceDao              xXServiceDao;
    @Mock XXService                 xXService;
    @Mock AssetMgr                  assetMgr;

    @Test
    public void test1getXGroup() {
        VXGroup compareTestVXGroup = createVXGroup();

        Mockito.when(xUserMgr.getXGroup(id)).thenReturn(compareTestVXGroup);
        VXGroup retVxGroup = xUserRest.getXGroup(id);

        Assertions.assertNotNull(retVxGroup);
        Assertions.assertEquals(compareTestVXGroup.getId(), retVxGroup.getId());
        Assertions.assertEquals(compareTestVXGroup.getName(), retVxGroup.getName());
        Mockito.verify(xUserMgr).getXGroup(id);
    }

    @Test
    public void test2secureGetXGroup() {
        VXGroup compareTestVXGroup = createVXGroup();

        Mockito.when(xUserMgr.getXGroup(id)).thenReturn(compareTestVXGroup);
        VXGroup retVxGroup = xUserRest.secureGetXGroup(id);

        Assertions.assertNotNull(retVxGroup);
        Assertions.assertEquals(compareTestVXGroup.getId(), retVxGroup.getId());
        Assertions.assertEquals(compareTestVXGroup.getName(), retVxGroup.getName());
        Mockito.verify(xUserMgr).getXGroup(id);
    }

    @Test
    public void test3createXGroup() {
        VXGroup compareTestVXGroup = createVXGroup();

        Mockito.when(xUserMgr.createXGroupWithoutLogin(compareTestVXGroup)).thenReturn(compareTestVXGroup);
        VXGroup retVxGroup = xUserRest.createXGroup(compareTestVXGroup);

        Assertions.assertNotNull(retVxGroup);
        Assertions.assertEquals(compareTestVXGroup.getId(), retVxGroup.getId());
        Assertions.assertEquals(compareTestVXGroup.getName(), retVxGroup.getName());
        Mockito.verify(xUserMgr).createXGroupWithoutLogin(compareTestVXGroup);
    }

    @Test
    public void test4secureCreateXGroup() {
        VXGroup compareTestVXGroup = createVXGroup();

        Mockito.when(xUserMgr.createXGroup(compareTestVXGroup)).thenReturn(compareTestVXGroup);
        VXGroup retVxGroup = xUserRest.secureCreateXGroup(compareTestVXGroup);

        Assertions.assertNotNull(retVxGroup);
        Assertions.assertEquals(compareTestVXGroup.getId(), retVxGroup.getId());
        Assertions.assertEquals(compareTestVXGroup.getName(), retVxGroup.getName());
        Mockito.verify(xUserMgr).createXGroup(compareTestVXGroup);
    }

    @Test
    public void test5updateXGroup() {
        VXGroup compareTestVXGroup = createVXGroup();

        Mockito.when(xUserMgr.updateXGroup(compareTestVXGroup)).thenReturn(compareTestVXGroup);
        VXGroup retVxGroup = xUserRest.updateXGroup(compareTestVXGroup);

        Assertions.assertNotNull(retVxGroup);
        Assertions.assertEquals(compareTestVXGroup.getId(), retVxGroup.getId());
        Assertions.assertEquals(compareTestVXGroup.getName(), retVxGroup.getName());
        Mockito.verify(xUserMgr).updateXGroup(compareTestVXGroup);
    }

    @Test
    public void test6secureUpdateXGroup() {
        VXGroup compareTestVXGroup = createVXGroup();

        Mockito.when(xUserMgr.updateXGroup(compareTestVXGroup)).thenReturn(compareTestVXGroup);
        VXGroup retVxGroup = xUserRest.secureUpdateXGroup(compareTestVXGroup);

        Assertions.assertNotNull(retVxGroup);
        Assertions.assertEquals(compareTestVXGroup.getId(), retVxGroup.getId());
        Assertions.assertEquals(compareTestVXGroup.getName(), retVxGroup.getName());
        Mockito.verify(xUserMgr).updateXGroup(compareTestVXGroup);
    }

    @Test
    public void test7modifyGroupsVisibility() {
        HashMap<Long, Integer> groupVisibilityMap = creategroupVisibilityMap();
        xUserRest.modifyGroupsVisibility(groupVisibilityMap);

        Mockito.verify(xUserMgr).modifyGroupsVisibility(groupVisibilityMap);
    }

    @Test
    public void test8deleteXGroupTrue() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "true";
        boolean            forceDelete        = false;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);

        forceDelete = true;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(id, forceDelete);
        xUserRest.deleteXGroup(id, request);
        Mockito.verify(xUserMgr).deleteXGroup(id, forceDelete);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test9deleteXGroupFalse() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testForceDeleteStr = "false";
        boolean            forceDelete;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testForceDeleteStr);

        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(id, forceDelete);
        xUserRest.deleteXGroup(id, request);
        Mockito.verify(xUserMgr).deleteXGroup(id, forceDelete);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test10deleteXGroupNotEmpty() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = null;
        boolean            forceDelete;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);

        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(id, forceDelete);
        xUserRest.deleteXGroup(id, request);
        Mockito.verify(xUserMgr).deleteXGroup(id, forceDelete);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test11searchXGroups() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();
        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "name", "group name", null)).thenReturn("");
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "isVisible", "Group Visibility")).thenReturn(1);
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "groupSource", "group source")).thenReturn(1);
        VXGroupList testvXGroupList = createXGroupList();
        Mockito.when(xUserMgr.searchXGroups(testSearchCriteria)).thenReturn(testvXGroupList);
        VXGroupList outputvXGroupList = xUserRest.searchXGroups(request);

        Mockito.verify(xUserMgr).searchXGroups(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "name", "group name", null);
        Mockito.verify(searchUtil).extractInt(request, testSearchCriteria, "isVisible", "Group Visibility");
        Mockito.verify(searchUtil).extractInt(request, testSearchCriteria, "groupSource", "group source");
        Assertions.assertNotNull(outputvXGroupList);
        Assertions.assertEquals(outputvXGroupList.getTotalCount(), testvXGroupList.getTotalCount());
        Assertions.assertEquals(outputvXGroupList.getClass(), testvXGroupList.getClass());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test12countXGroups() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        vXLong.setValue(1);

        Mockito.when(xUserMgr.getXGroupSearchCount(testSearchCriteria)).thenReturn(vXLong);
        VXLong testvxLong = xUserRest.countXGroups(request);
        Mockito.verify(xUserMgr).getXGroupSearchCount(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());

        Assertions.assertNotNull(testvxLong);
        Assertions.assertEquals(testvxLong.getValue(), vXLong.getValue());
        Assertions.assertEquals(testvxLong.getClass(), vXLong.getClass());
    }

    @Test
    public void test13getXUser() {
        Mockito.when(xUserMgr.getXUser(id)).thenReturn(vxUser);
        VXUser gotVXUser = xUserRest.getXUser(id);
        Mockito.verify(xUserMgr).getXUser(id);

        Assertions.assertNotNull(gotVXUser);
        Assertions.assertEquals(vxUser.getId(), gotVXUser.getId());
        Assertions.assertEquals(vxUser.getName(), gotVXUser.getName());
    }

    @Test
    public void test14secureGetXUser() {
        Mockito.when(xUserMgr.getXUser(id)).thenReturn(vxUser);
        VXUser gotVXUser = xUserRest.secureGetXUser(id);
        Mockito.verify(xUserMgr).getXUser(id);

        Assertions.assertNotNull(gotVXUser);
        Assertions.assertEquals(vxUser.getId(), gotVXUser.getId());
        Assertions.assertEquals(vxUser.getName(), gotVXUser.getName());
    }

    @Test
    public void test15createXUser() {
        Mockito.when(xUserMgr.createXUserWithOutLogin(vxUser)).thenReturn(vxUser);
        VXUser gotVXUser = xUserRest.createXUser(vxUser);
        Mockito.verify(xUserMgr).createXUserWithOutLogin(vxUser);

        Assertions.assertNotNull(gotVXUser);
        Assertions.assertEquals(vxUser.getId(), gotVXUser.getId());
        Assertions.assertEquals(vxUser.getName(), gotVXUser.getName());
    }

    @Test
    public void test16createXUserGroupFromMap() {
        VXUserGroupInfo vXUserGroupInfo = new VXUserGroupInfo();
        vXUserGroupInfo.setXuserInfo(vxUser);

        Mockito.when(xUserMgr.createXUserGroupFromMap(vXUserGroupInfo)).thenReturn(vXUserGroupInfo);
        VXUserGroupInfo gotVXUserGroupInfo = xUserRest.createXUserGroupFromMap(vXUserGroupInfo);
        Mockito.verify(xUserMgr).createXUserGroupFromMap(vXUserGroupInfo);

        Assertions.assertNotNull(gotVXUserGroupInfo);
        Assertions.assertEquals(vXUserGroupInfo.getId(), gotVXUserGroupInfo.getId());
        Assertions.assertEquals(vXUserGroupInfo.getOwner(), gotVXUserGroupInfo.getOwner());
    }

    @Test
    public void test17secureCreateXUser() {
        Boolean val = true;
        Mockito.when(bizUtil.checkUserAccessible(vxUser)).thenReturn(val);
        Mockito.when(xUserMgr.createXUser(vxUser)).thenReturn(vxUser);
        VXUser gotVXUser = xUserRest.secureCreateXUser(vxUser);
        Mockito.verify(xUserMgr).createXUser(vxUser);
        Mockito.verify(bizUtil).checkUserAccessible(vxUser);
        Assertions.assertNotNull(gotVXUser);
        Assertions.assertEquals(vxUser.getId(), gotVXUser.getId());
        Assertions.assertEquals(vxUser.getName(), gotVXUser.getName());
    }

    @Test
    public void test18updateXUser() {
        Mockito.when(xUserMgr.updateXUser(vxUser)).thenReturn(vxUser);
        VXUser gotVXUser = xUserRest.updateXUser(vxUser);
        Mockito.verify(xUserMgr).updateXUser(vxUser);
        Assertions.assertNotNull(gotVXUser);
        Assertions.assertEquals(vxUser.getId(), gotVXUser.getId());
        Assertions.assertEquals(vxUser.getName(), gotVXUser.getName());
    }

    @Test
    public void test19secureUpdateXUser() {
        Boolean val = true;
        Mockito.when(bizUtil.checkUserAccessible(vxUser)).thenReturn(val);
        Mockito.when(xUserMgr.updateXUser(vxUser)).thenReturn(vxUser);
        VXUser gotVXUser = xUserRest.secureUpdateXUser(vxUser);
        Mockito.verify(xUserMgr).updateXUser(vxUser);
        Mockito.verify(bizUtil).checkUserAccessible(vxUser);

        Assertions.assertNotNull(gotVXUser);
        Assertions.assertEquals(vxUser.getId(), gotVXUser.getId());
        Assertions.assertEquals(vxUser.getName(), gotVXUser.getName());
    }

    @Test
    public void test20modifyUserVisibility() {
        HashMap<Long, Integer> testVisibilityMap = new HashMap<>();
        testVisibilityMap.put(1L, 0);
        Mockito.doNothing().when(xUserMgr).modifyUserVisibility(testVisibilityMap);
        xUserRest.modifyUserVisibility(testVisibilityMap);
        Mockito.verify(xUserMgr).modifyUserVisibility(testVisibilityMap);
    }

    @Test
    public void test21deleteXUser() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        boolean            forceDelete        = false;
        String             testforceDeleteStr = "true";
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        forceDelete = true;
        Mockito.doNothing().when(xUserMgr).deleteXUser(id, forceDelete);
        xUserRest.deleteXUser(id, request);
        Mockito.verify(xUserMgr).deleteXUser(id, forceDelete);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test22deleteXUserFalse() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";
        boolean            forceDelete;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);

        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXUser(id, forceDelete);
        xUserRest.deleteXUser(id, request);
        Mockito.verify(xUserMgr).deleteXUser(id, forceDelete);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test23deleteXUserNotEmpty() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = null;
        boolean            forceDelete;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);

        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXUser(id, forceDelete);
        xUserRest.deleteXUser(id, request);
        Mockito.verify(xUserMgr).deleteXUser(id, forceDelete);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test24searchXUsers() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "name", "User name", null)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "emailAddress", "Email Address", null)).thenReturn("");
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "userSource", "User Source")).thenReturn(1);
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "isVisible", "User Visibility")).thenReturn(1);
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "status", "User Status")).thenReturn(1);
        Mockito.when(searchUtil.extractStringList(request, testSearchCriteria, "userRoleList", "User Role List", "userRoleList", null, null)).thenReturn(new ArrayList<String>());
        Mockito.when(searchUtil.extractRoleString(request, testSearchCriteria, "userRole", "Role", null)).thenReturn("");

        List<VXUser> vXUsersList = new ArrayList<>();
        vXUsersList.add(vxUser);
        VXUserList testVXUserList = new VXUserList();
        testVXUserList.setVXUsers(vXUsersList);

        Mockito.when(xUserMgr.searchXUsers(testSearchCriteria)).thenReturn(testVXUserList);
        VXUserList gotVXUserList = xUserRest.searchXUsers(request, null, null);

        Mockito.verify(xUserMgr).searchXUsers(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());

        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "name", "User name", null);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "emailAddress", "Email Address", null);
        Mockito.verify(searchUtil).extractInt(request, testSearchCriteria, "userSource", "User Source");
        Mockito.verify(searchUtil).extractInt(request, testSearchCriteria, "isVisible", "User Visibility");
        Mockito.verify(searchUtil).extractInt(request, testSearchCriteria, "status", "User Status");
        Mockito.verify(searchUtil).extractStringList(request, testSearchCriteria, "userRoleList", "User Role List", "userRoleList", null, null);
        Mockito.verify(searchUtil).extractRoleString(request, testSearchCriteria, "userRole", "Role", null);
        Assertions.assertNotNull(gotVXUserList);
        Assertions.assertEquals(testVXUserList.getTotalCount(), gotVXUserList.getTotalCount());
        Assertions.assertEquals(testVXUserList.getClass(), gotVXUserList.getClass());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test25countXUsers() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        vXLong.setValue(1);

        Mockito.when(xUserMgr.getXUserSearchCount(testSearchCriteria)).thenReturn(vXLong);
        VXLong testvxLong = xUserRest.countXUsers(request);
        Mockito.verify(xUserMgr).getXUserSearchCount(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());

        Assertions.assertNotNull(testvxLong);
        Assertions.assertEquals(testvxLong.getValue(), vXLong.getValue());
        Assertions.assertEquals(testvxLong.getClass(), vXLong.getClass());
    }

    @Test
    public void test26getXGroupUser() {
        VXGroupUser testVXGroupUser = createVXGroupUser();

        Mockito.when(xUserMgr.getXGroupUser(id)).thenReturn(testVXGroupUser);
        VXGroupUser retVxGroupUser = xUserRest.getXGroupUser(id);

        Assertions.assertNotNull(retVxGroupUser);
        Assertions.assertEquals(testVXGroupUser.getClass(), retVxGroupUser.getClass());
        Assertions.assertEquals(testVXGroupUser.getId(), retVxGroupUser.getId());
        Mockito.verify(xUserMgr).getXGroupUser(id);
    }

    @Test
    public void test27createXGroupUser() {
        VXGroupUser testVXGroupUser = createVXGroupUser();

        Mockito.when(xUserMgr.createXGroupUser(testVXGroupUser)).thenReturn(testVXGroupUser);
        VXGroupUser retVxGroupUser = xUserRest.createXGroupUser(testVXGroupUser);

        Assertions.assertNotNull(retVxGroupUser);
        Assertions.assertEquals(testVXGroupUser.getClass(), retVxGroupUser.getClass());
        Assertions.assertEquals(testVXGroupUser.getId(), retVxGroupUser.getId());
        Mockito.verify(xUserMgr).createXGroupUser(testVXGroupUser);
    }

    @Test
    public void test28updateXGroupUser() {
        VXGroupUser testVXGroupUser = createVXGroupUser();

        Mockito.when(xUserMgr.updateXGroupUser(testVXGroupUser)).thenReturn(testVXGroupUser);
        VXGroupUser retVxGroupUser = xUserRest.updateXGroupUser(testVXGroupUser);

        Assertions.assertNotNull(retVxGroupUser);
        Assertions.assertEquals(testVXGroupUser.getClass(), retVxGroupUser.getClass());
        Assertions.assertEquals(testVXGroupUser.getId(), retVxGroupUser.getId());
        Mockito.verify(xUserMgr).updateXGroupUser(testVXGroupUser);
    }

    @Test
    public void test29deleteXGroupUser() {
        boolean force = true;

        Mockito.doNothing().when(xUserMgr).deleteXGroupUser(id, force);
        xUserRest.deleteXGroupUser(id, request);
        Mockito.verify(xUserMgr).deleteXGroupUser(id, force);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test30searchXGroupUsers() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        VXGroupUserList   testVXGroupUserList = new VXGroupUserList();
        VXGroupUser       vXGroupUser         = createVXGroupUser();
        List<VXGroupUser> vXGroupUsers        = new ArrayList<>();
        vXGroupUsers.add(vXGroupUser);
        testVXGroupUserList.setVXGroupUsers(vXGroupUsers);
        Mockito.when(xUserMgr.searchXGroupUsers(testSearchCriteria)).thenReturn(testVXGroupUserList);
        VXGroupUserList outputvXGroupList = xUserRest.searchXGroupUsers(request);

        Mockito.verify(xUserMgr).searchXGroupUsers(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());

        Assertions.assertNotNull(outputvXGroupList);
        Assertions.assertEquals(outputvXGroupList.getClass(), testVXGroupUserList.getClass());
        Assertions.assertEquals(outputvXGroupList.getResultSize(), testVXGroupUserList.getResultSize());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test31countXGroupUserst() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        vXLong.setValue(1);

        Mockito.when(xUserMgr.getXGroupUserSearchCount(testSearchCriteria)).thenReturn(vXLong);
        VXLong testvxLong = xUserRest.countXGroupUsers(request);
        Mockito.verify(xUserMgr).getXGroupUserSearchCount(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());

        Assertions.assertNotNull(testvxLong);
        Assertions.assertEquals(testvxLong.getValue(), vXLong.getValue());
        Assertions.assertEquals(testvxLong.getClass(), vXLong.getClass());
    }

    @Test
    public void test62getXUserByUserName() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        VXUser compareTestVxUser = createVXUser();

        Mockito.when(xUserMgr.getXUserByUserName("User1")).thenReturn(compareTestVxUser);
        VXUser retVXUser = xUserRest.getXUserByUserName(request, "User1");

        Assertions.assertNotNull(retVXUser);
        Assertions.assertEquals(compareTestVxUser.getClass(), retVXUser.getClass());
        Assertions.assertEquals(compareTestVxUser.getId(), retVXUser.getId());
        Mockito.verify(xUserMgr).getXUserByUserName("User1");
    }

    @Test
    public void test63getXGroupByGroupName() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        VXGroup compareTestVXGroup = createVXGroup();

        Mockito.when(xGroupService.getGroupByGroupName(compareTestVXGroup.getName())).thenReturn(compareTestVXGroup);

        VXGroup retVxGroup = xUserRest.getXGroupByGroupName(request, compareTestVXGroup.getName());

        Assertions.assertNotNull(retVxGroup);
        Assertions.assertEquals(compareTestVXGroup.getClass(), compareTestVXGroup.getClass());
        Assertions.assertEquals(compareTestVXGroup.getId(), compareTestVXGroup.getId());
        Mockito.verify(xGroupService).getGroupByGroupName(compareTestVXGroup.getName());
    }

    @Test
    public void test64deleteXUserByUserName() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "true";
        boolean            forceDelete        = false;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXUser testUser = createVXUser();
        Mockito.when(xUserService.getXUserByUserName(testUser.getName())).thenReturn(testUser);
        forceDelete = true;
        Mockito.doNothing().when(xUserMgr).deleteXUser(testUser.getId(), forceDelete);
        xUserRest.deleteXUserByUserName(testUser.getName(), request);
        Mockito.verify(xUserMgr).deleteXUser(testUser.getId(), forceDelete);
        Mockito.verify(xUserService).getXUserByUserName(testUser.getName());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test65deleteXUserByUserNametrue() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXUser testUser = createVXUser();
        Mockito.when(xUserService.getXUserByUserName(testUser.getName())).thenReturn(testUser);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXUser(testUser.getId(), forceDelete);
        xUserRest.deleteXUserByUserName(testUser.getName(), request);
        Mockito.verify(xUserMgr).deleteXUser(testUser.getId(), forceDelete);
        Mockito.verify(xUserService).getXUserByUserName(testUser.getName());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test66deleteXUserByUserNameNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = null;
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXUser testUser = createVXUser();
        Mockito.when(xUserService.getXUserByUserName(testUser.getName())).thenReturn(testUser);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXUser(testUser.getId(), forceDelete);
        xUserRest.deleteXUserByUserName(testUser.getName(), request);
        Mockito.verify(xUserMgr).deleteXUser(testUser.getId(), forceDelete);
        Mockito.verify(xUserService).getXUserByUserName(testUser.getName());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test67deleteXGroupByGroupName() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXGroup testVXGroup = createVXGroup();
        Mockito.when(xGroupService.getGroupByGroupName(testVXGroup.getName())).thenReturn(testVXGroup);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        xUserRest.deleteXGroupByGroupName(testVXGroup.getName(), request);
        Mockito.verify(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        Mockito.verify(xGroupService).getGroupByGroupName(testVXGroup.getName());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test68deleteXGroupByGroupNameNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = null;
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXGroup testVXGroup = createVXGroup();
        Mockito.when(xGroupService.getGroupByGroupName(testVXGroup.getName())).thenReturn(testVXGroup);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        xUserRest.deleteXGroupByGroupName(testVXGroup.getName(), request);
        Mockito.verify(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        Mockito.verify(xGroupService).getGroupByGroupName(testVXGroup.getName());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test69deleteXGroupByGroupNameflase() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "true";
        boolean            forceDelete        = false;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXGroup testVXGroup = createVXGroup();
        Mockito.when(xGroupService.getGroupByGroupName(testVXGroup.getName())).thenReturn(testVXGroup);
        forceDelete = true;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        xUserRest.deleteXGroupByGroupName(testVXGroup.getName(), request);
        Mockito.verify(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        Mockito.verify(xGroupService).getGroupByGroupName(testVXGroup.getName());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test70deleteXGroupAndXUser() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        VXGroup testVXGroup = createVXGroup();
        VXUser  testVXuser  = createVXUser();

        Mockito.doNothing().when(xUserMgr).deleteXGroupAndXUser(testVXGroup.getName(), testVXuser.getName());
        xUserRest.deleteXGroupAndXUser(testVXGroup.getName(), testVXuser.getName(), request);
        Mockito.verify(xUserMgr).deleteXGroupAndXUser(testVXGroup.getName(), testVXuser.getName());
    }

    @Test
    public void test71getXUserGroups() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        VXGroupList groupList = createXGroupList();
        Mockito.when(xUserMgr.getXUserGroups(id)).thenReturn(groupList);
        VXGroupList retVxGroupList = xUserRest.getXUserGroups(request, id);

        Assertions.assertNotNull(retVxGroupList);
        Assertions.assertEquals(groupList.getClass(), retVxGroupList.getClass());
        Assertions.assertEquals(groupList.getResultSize(), retVxGroupList.getResultSize());
        Mockito.verify(xUserMgr).getXUserGroups(id);
    }

    @Test
    public void test72getXGroupUsers() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        VXGroupList groupList = createXGroupList();
        Mockito.when(xUserMgr.getXUserGroups(id)).thenReturn(groupList);
        VXGroupList retVxGroupList = xUserRest.getXUserGroups(request, id);

        Assertions.assertNotNull(retVxGroupList);
        Assertions.assertEquals(groupList.getClass(), retVxGroupList.getClass());
        Assertions.assertEquals(groupList.getResultSize(), retVxGroupList.getResultSize());
        Mockito.verify(xUserMgr).getXUserGroups(id);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test73getXGroupUsers() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();
        testSearchCriteria.addParam("xGroupId", id);

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        VXUser       testVXUser     = createVXUser();
        VXUserList   testVXUserList = new VXUserList();
        List<VXUser> testVXUsers    = new ArrayList<>();
        testVXUsers.add(testVXUser);
        testVXUserList.setVXUsers(testVXUsers);
        testVXUserList.setStartIndex(1);
        testVXUserList.setTotalCount(1);
        Mockito.when(xUserMgr.getXGroupUsers(testSearchCriteria)).thenReturn(testVXUserList);
        VXUserList retVxGroupList = xUserRest.getXGroupUsers(request, id);

        Assertions.assertNotNull(retVxGroupList);
        Assertions.assertEquals(testVXUserList.getTotalCount(), retVxGroupList.getTotalCount());
        Assertions.assertEquals(testVXUserList.getStartIndex(), retVxGroupList.getStartIndex());
        Mockito.verify(xUserMgr).getXGroupUsers(testSearchCriteria);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test74getAuthSessions() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        Mockito.when(searchUtil.extractLong(request, testSearchCriteria, "id", "Auth Session Id")).thenReturn(1L);
        Mockito.when(searchUtil.extractLong(request, testSearchCriteria, "userId", "User Id")).thenReturn(1L);
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "authStatus", "Auth Status")).thenReturn(1);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "firstName", "User First Name", StringUtil.VALIDATION_NAME)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "lastName", "User Last Name", StringUtil.VALIDATION_NAME)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "requestUserAgent", "User Agent", StringUtil.VALIDATION_TEXT)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "requestIP", "Request IP Address", StringUtil.VALIDATION_IP_ADDRESS)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "loginId", "Login ID", StringUtil.VALIDATION_TEXT)).thenReturn("");

        VXAuthSessionList testVXAuthSessionList = new VXAuthSessionList();
        testVXAuthSessionList.setTotalCount(1);
        testVXAuthSessionList.setStartIndex(1);
        VXAuthSession       testVXAuthSession  = createVXAuthSession();
        List<VXAuthSession> testvXAuthSessions = new ArrayList<>();
        testvXAuthSessions.add(testVXAuthSession);

        testVXAuthSessionList.setVXAuthSessions(testvXAuthSessions);
        Mockito.when(sessionMgr.searchAuthSessions(testSearchCriteria)).thenReturn(testVXAuthSessionList);
        VXAuthSessionList outputvXGroupList = xUserRest.getAuthSessions(request);

        Mockito.verify(sessionMgr).searchAuthSessions(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());
        Mockito.verify(searchUtil).extractLong(request, testSearchCriteria, "id", "Auth Session Id");
        Mockito.verify(searchUtil).extractLong(request, testSearchCriteria, "userId", "User Id");
        Mockito.verify(searchUtil).extractInt(request, testSearchCriteria, "authStatus", "Auth Status");
        Mockito.verify(searchUtil).extractInt(request, testSearchCriteria, "authType", "Login Type");
        Mockito.verify(searchUtil).extractInt(request, testSearchCriteria, "deviceType", "Device Type");
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "firstName", "User First Name", StringUtil.VALIDATION_NAME);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "lastName", "User Last Name", StringUtil.VALIDATION_NAME);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "requestUserAgent", "User Agent", StringUtil.VALIDATION_TEXT);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "requestIP", "Request IP Address", StringUtil.VALIDATION_IP_ADDRESS);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "loginId", "Login ID", StringUtil.VALIDATION_TEXT);
        Mockito.verify(searchUtil).extractDate(request, testSearchCriteria, "startDate", "Start Date", null);
        Mockito.verify(searchUtil).extractDate(request, testSearchCriteria, "endDate", "End Date", null);
        Assertions.assertNotNull(outputvXGroupList);
        Assertions.assertEquals(outputvXGroupList.getStartIndex(), testVXAuthSessionList.getStartIndex());
        Assertions.assertEquals(outputvXGroupList.getTotalCount(), testVXAuthSessionList.getTotalCount());
    }

    @Test
    public void test75getAuthSession() {
        String        authSessionId     = "testauthSessionId";
        VXAuthSession testVXAuthSession = createVXAuthSession();

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getParameter("extSessionId")).thenReturn(authSessionId);
        Mockito.when(sessionMgr.getAuthSessionBySessionId(authSessionId)).thenReturn(testVXAuthSession);
        VXAuthSession retVXAuthSession = xUserRest.getAuthSession(request);
        Mockito.verify(sessionMgr).getAuthSessionBySessionId(authSessionId);
        Mockito.verify(request).getParameter("extSessionId");
        Assertions.assertEquals(testVXAuthSession.getId(), retVXAuthSession.getId());
        Assertions.assertEquals(testVXAuthSession.getClass(), retVXAuthSession.getClass());
        Assertions.assertNotNull(retVXAuthSession);
    }

    @Test
    public void test76createXModuleDefPermission() {
        VXModuleDef testVXModuleDef = createVXModuleDef();

        Mockito.doNothing().when(xUserMgr).checkAdminAccess();

        Mockito.when(xUserMgr.createXModuleDefPermission(testVXModuleDef)).thenReturn(testVXModuleDef);
        VXModuleDef retVxModuleDef = xUserRest.createXModuleDefPermission(testVXModuleDef);

        Assertions.assertNotNull(retVxModuleDef);
        Assertions.assertEquals(testVXModuleDef.getId(), retVxModuleDef.getId());
        Assertions.assertEquals(testVXModuleDef.getOwner(), retVxModuleDef.getOwner());
        Mockito.verify(xUserMgr).createXModuleDefPermission(testVXModuleDef);
        Mockito.verify(xUserMgr).checkAdminAccess();
    }

    @Test
    public void test77getXModuleDefPermission() {
        VXModuleDef testVXModuleDef = createVXModuleDef();
        Mockito.when(xUserMgr.getXModuleDefPermission(testVXModuleDef.getId())).thenReturn(testVXModuleDef);
        VXModuleDef retVxModuleDef = xUserRest.getXModuleDefPermission(testVXModuleDef.getId());

        Assertions.assertNotNull(retVxModuleDef);
        Assertions.assertEquals(testVXModuleDef.getId(), retVxModuleDef.getId());
        Assertions.assertEquals(testVXModuleDef.getOwner(), retVxModuleDef.getOwner());

        Mockito.verify(xUserMgr).getXModuleDefPermission(testVXModuleDef.getId());
    }

    @Test
    public void test78updateXModuleDefPermission() {
        VXModuleDef testVXModuleDef = createVXModuleDef();

        Mockito.doNothing().when(xUserMgr).checkAdminAccess();

        Mockito.when(xUserMgr.updateXModuleDefPermission(testVXModuleDef)).thenReturn(testVXModuleDef);
        VXModuleDef retVxModuleDef = xUserRest.updateXModuleDefPermission(testVXModuleDef);

        Assertions.assertNotNull(retVxModuleDef);
        Assertions.assertEquals(testVXModuleDef.getId(), retVxModuleDef.getId());
        Assertions.assertEquals(testVXModuleDef.getOwner(), retVxModuleDef.getOwner());

        Mockito.verify(xUserMgr).updateXModuleDefPermission(testVXModuleDef);
        Mockito.verify(xUserMgr).checkAdminAccess();
    }

    @Test
    public void test79deleteXModuleDefPermission() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        boolean forceDelete = true;
        Mockito.doNothing().when(xUserMgr).checkAdminAccess();
        Mockito.doNothing().when(xUserMgr).deleteXModuleDefPermission(id, forceDelete);
        xUserRest.deleteXModuleDefPermission(id, request);
        Mockito.verify(xUserMgr).deleteXModuleDefPermission(id, forceDelete);
        Mockito.verify(xUserMgr).checkAdminAccess();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test80searchXModuleDef() {
        VXModuleDefList   testVXModuleDefList = new VXModuleDefList();
        VXModuleDef       vXModuleDef         = createVXModuleDef();
        List<VXModuleDef> vxModuleDefs        = new ArrayList<>();
        vxModuleDefs.add(vXModuleDef);
        testVXModuleDefList.setvXModuleDef(vxModuleDefs);
        testVXModuleDefList.setTotalCount(1);
        testVXModuleDefList.setStartIndex(1);
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "module", "modulename", null)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "moduleDefList", "id", null)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "userName", "userName", null)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "groupName", "groupName", null)).thenReturn("");

        Mockito.when(xUserMgr.searchXModuleDef(testSearchCriteria)).thenReturn(testVXModuleDefList);
        VXModuleDefList outputVXModuleDefList = xUserRest.searchXModuleDef(request);
        Assertions.assertNotNull(outputVXModuleDefList);
        Assertions.assertEquals(outputVXModuleDefList.getTotalCount(), testVXModuleDefList.getTotalCount());
        Assertions.assertEquals(outputVXModuleDefList.getStartIndex(), testVXModuleDefList.getStartIndex());

        Mockito.verify(xUserMgr).searchXModuleDef(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "module", "modulename", null);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "moduleDefList", "id", null);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "userName", "userName", null);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "groupName", "groupName", null);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test81countXModuleDef() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        vXLong.setValue(1);

        Mockito.when(xUserMgr.getXModuleDefSearchCount(testSearchCriteria)).thenReturn(vXLong);
        VXLong testvxLong = xUserRest.countXModuleDef(request);
        Mockito.verify(xUserMgr).getXModuleDefSearchCount(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());

        Assertions.assertNotNull(testvxLong);
        Assertions.assertEquals(testvxLong.getValue(), vXLong.getValue());
        Assertions.assertEquals(testvxLong.getClass(), vXLong.getClass());
    }

    @Test
    public void test82createXUserPermission() {
        VXUserPermission testvXUserPermission = createVXUserPermission();

        Mockito.doNothing().when(xUserMgr).checkAdminAccess();
        Mockito.when(xUserMgr.createXUserPermission(testvXUserPermission)).thenReturn(testvXUserPermission);
        VXUserPermission retVXUserPermission = xUserRest.createXUserPermission(testvXUserPermission);
        Mockito.verify(xUserMgr).createXUserPermission(testvXUserPermission);
        Mockito.verify(xUserMgr).checkAdminAccess();
        Assertions.assertNotNull(retVXUserPermission);
        Assertions.assertEquals(retVXUserPermission.getId(), testvXUserPermission.getId());
        Assertions.assertEquals(retVXUserPermission.getUserName(), testvXUserPermission.getUserName());
    }

    @Test
    public void test83getXUserPermission() {
        VXUserPermission testVXUserPermission = createVXUserPermission();
        Mockito.when(xUserMgr.getXUserPermission(testVXUserPermission.getId())).thenReturn(testVXUserPermission);
        VXUserPermission retVXUserPermission = xUserRest.getXUserPermission(testVXUserPermission.getId());
        Mockito.verify(xUserMgr).getXUserPermission(id);
        Assertions.assertNotNull(retVXUserPermission);
        Assertions.assertEquals(retVXUserPermission.getId(), testVXUserPermission.getId());
        Assertions.assertEquals(retVXUserPermission.getUserName(), testVXUserPermission.getUserName());
    }

    @Test
    public void test84updateXUserPermission() {
        VXUserPermission testvXUserPermission = createVXUserPermission();
        Mockito.doNothing().when(xUserMgr).checkAdminAccess();
        Mockito.when(xUserMgr.updateXUserPermission(testvXUserPermission)).thenReturn(testvXUserPermission);
        VXUserPermission retVXUserPermission = xUserRest.updateXUserPermission(testvXUserPermission);
        Mockito.verify(xUserMgr).updateXUserPermission(testvXUserPermission);
        Mockito.verify(xUserMgr).checkAdminAccess();
        Assertions.assertNotNull(retVXUserPermission);
        Assertions.assertEquals(retVXUserPermission.getId(), testvXUserPermission.getId());
        Assertions.assertEquals(retVXUserPermission.getUserName(), testvXUserPermission.getUserName());
    }

    @Test
    public void test85deleteXUserPermission() {
        boolean forceDelete = true;

        Mockito.doNothing().when(xUserMgr).checkAdminAccess();

        Mockito.doNothing().when(xUserMgr).deleteXUserPermission(id, forceDelete);
        xUserRest.deleteXUserPermission(id, request);
        Mockito.verify(xUserMgr).deleteXUserPermission(id, forceDelete);
        Mockito.verify(xUserMgr).checkAdminAccess();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test86searchXUserPermission() {
        VXUserPermissionList testVXUserPermissionList = new VXUserPermissionList();
        testVXUserPermissionList.setTotalCount(1);
        testVXUserPermissionList.setStartIndex(1);
        VXUserPermission       testVXUserPermission  = createVXUserPermission();
        List<VXUserPermission> testVXUserPermissions = new ArrayList<>();
        testVXUserPermissions.add(testVXUserPermission);
        testVXUserPermissionList.setvXModuleDef(testVXUserPermissions);
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();
        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "id", "id", StringUtil.VALIDATION_NAME)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "userPermissionList", "userId", StringUtil.VALIDATION_NAME)).thenReturn("");

        Mockito.when(xUserMgr.searchXUserPermission(testSearchCriteria)).thenReturn(testVXUserPermissionList);
        VXUserPermissionList outputVXUserPermissionList = xUserRest.searchXUserPermission(request);
        Assertions.assertNotNull(outputVXUserPermissionList);
        Assertions.assertEquals(outputVXUserPermissionList.getStartIndex(), testVXUserPermissionList.getStartIndex());
        Assertions.assertEquals(outputVXUserPermissionList.getTotalCount(), testVXUserPermissionList.getTotalCount());

        Mockito.verify(xUserMgr).searchXUserPermission(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "id", "id", StringUtil.VALIDATION_NAME);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "userPermissionList", "userId", StringUtil.VALIDATION_NAME);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test87countXUserPermission() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        vXLong.setValue(1);

        Mockito.when(xUserMgr.getXUserPermissionSearchCount(testSearchCriteria)).thenReturn(vXLong);
        VXLong testvxLong = xUserRest.countXUserPermission(request);
        Mockito.verify(xUserMgr).getXUserPermissionSearchCount(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());

        Assertions.assertNotNull(testvxLong);
        Assertions.assertEquals(testvxLong.getValue(), vXLong.getValue());
        Assertions.assertEquals(testvxLong.getClass(), vXLong.getClass());
    }

    @Test
    public void test88createXGroupPermission() {
        VXGroupPermission testVXGroupPermission = createVXGroupPermission();

        Mockito.doNothing().when(xUserMgr).checkAdminAccess();
        Mockito.when(xUserMgr.createXGroupPermission(testVXGroupPermission)).thenReturn(testVXGroupPermission);
        VXGroupPermission retVXGroupPermission = xUserRest.createXGroupPermission(testVXGroupPermission);
        Mockito.verify(xUserMgr).createXGroupPermission(testVXGroupPermission);
        Mockito.verify(xUserMgr).checkAdminAccess();
        Assertions.assertNotNull(retVXGroupPermission);
        Assertions.assertEquals(retVXGroupPermission.getId(), testVXGroupPermission.getId());
        Assertions.assertEquals(retVXGroupPermission.getClass(), testVXGroupPermission.getClass());
    }

    @Test
    public void test89getXGroupPermission() {
        VXGroupPermission testVXGroupPermission = createVXGroupPermission();
        Mockito.when(xUserMgr.getXGroupPermission(testVXGroupPermission.getId())).thenReturn(testVXGroupPermission);
        VXGroupPermission retVXGroupPermission = xUserRest.getXGroupPermission(testVXGroupPermission.getId());
        Mockito.verify(xUserMgr).getXGroupPermission(testVXGroupPermission.getId());
        Assertions.assertNotNull(retVXGroupPermission);
        Assertions.assertEquals(retVXGroupPermission.getId(), testVXGroupPermission.getId());
        Assertions.assertEquals(retVXGroupPermission.getClass(), testVXGroupPermission.getClass());
    }

    @Test
    public void test90updateXGroupPermission() {
        VXGroupPermission testVXGroupPermission = createVXGroupPermission();
        Mockito.doNothing().when(xUserMgr).checkAdminAccess();
        Mockito.when(xUserMgr.updateXGroupPermission(testVXGroupPermission)).thenReturn(testVXGroupPermission);
        VXGroupPermission retVXGroupPermission = xUserRest.updateXGroupPermission(testVXGroupPermission.getId(), testVXGroupPermission);
        Mockito.verify(xUserMgr).updateXGroupPermission(testVXGroupPermission);
        Mockito.verify(xUserMgr).checkAdminAccess();
        Assertions.assertNotNull(retVXGroupPermission);
        Assertions.assertEquals(retVXGroupPermission.getId(), testVXGroupPermission.getId());
        Assertions.assertEquals(retVXGroupPermission.getClass(), testVXGroupPermission.getClass());
    }

    @Test
    public void test91deleteXGroupPermission() {
        boolean forceDelete = true;

        Mockito.doNothing().when(xUserMgr).checkAdminAccess();

        Mockito.doNothing().when(xUserMgr).deleteXGroupPermission(id, forceDelete);
        xUserRest.deleteXGroupPermission(id, request);
        Mockito.verify(xUserMgr).deleteXGroupPermission(id, forceDelete);
        Mockito.verify(xUserMgr).checkAdminAccess();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test92searchXGroupPermission() {
        VXGroupPermissionList testVXGroupPermissionList = new VXGroupPermissionList();
        testVXGroupPermissionList.setTotalCount(1);
        VXGroupPermission       testVXGroupPermission  = createVXGroupPermission();
        List<VXGroupPermission> testVXGroupPermissions = new ArrayList<>();
        testVXGroupPermissions.add(testVXGroupPermission);
        testVXGroupPermissionList.setvXGroupPermission(testVXGroupPermissions);
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();
        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "id", "id", StringUtil.VALIDATION_NAME)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "groupPermissionList", "groupId", StringUtil.VALIDATION_NAME)).thenReturn("");
        Mockito.when(xUserMgr.searchXGroupPermission(testSearchCriteria)).thenReturn(testVXGroupPermissionList);
        VXGroupPermissionList outputVXGroupPermissionList = xUserRest.searchXGroupPermission(request);
        Assertions.assertNotNull(outputVXGroupPermissionList);
        Assertions.assertEquals(outputVXGroupPermissionList.getClass(), testVXGroupPermissionList.getClass());
        Assertions.assertEquals(outputVXGroupPermissionList.getTotalCount(), testVXGroupPermissionList.getTotalCount());

        Mockito.verify(xUserMgr).searchXGroupPermission(testSearchCriteria);

        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "id", "id", StringUtil.VALIDATION_NAME);
        Mockito.verify(searchUtil).extractString(request, testSearchCriteria, "groupPermissionList", "groupId", StringUtil.VALIDATION_NAME);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test93countXGroupPermission() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        SearchCriteria     testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

        vXLong.setValue(1);

        Mockito.when(xUserMgr.getXGroupPermissionSearchCount(testSearchCriteria)).thenReturn(vXLong);
        VXLong testvxLong = xUserRest.countXGroupPermission(request);
        Mockito.verify(xUserMgr).getXGroupPermissionSearchCount(testSearchCriteria);
        Mockito.verify(searchUtil).extractCommonCriterias(Mockito.any(), Mockito.any());

        Assertions.assertNotNull(testvxLong);
        Assertions.assertEquals(testvxLong.getValue(), vXLong.getValue());
        Assertions.assertEquals(testvxLong.getClass(), vXLong.getClass());
    }

    @Test
    public void test94modifyUserActiveStatus() {
        HashMap<Long, Integer> statusMap = new HashMap<>();
        statusMap.put(id, 1);
        Mockito.doNothing().when(xUserMgr).modifyUserActiveStatus(statusMap);
        xUserRest.modifyUserActiveStatus(statusMap);
        Mockito.verify(xUserMgr).modifyUserActiveStatus(statusMap);
    }

    @Test
    public void test95setUserRolesByExternalID() {
        VXStringList testVXStringList = createVXStringList();
        Mockito.when(xUserMgr.setUserRolesByExternalID(id, testVXStringList.getVXStrings())).thenReturn(testVXStringList);
        VXStringList retVXStringList = xUserRest.setUserRolesByExternalID(id, testVXStringList);
        Mockito.verify(xUserMgr).setUserRolesByExternalID(id, testVXStringList.getVXStrings());

        Assertions.assertNotNull(retVXStringList);
        Assertions.assertEquals(testVXStringList.getTotalCount(), retVXStringList.getTotalCount());
        Assertions.assertEquals(testVXStringList.getClass(), retVXStringList.getClass());
    }

    @Test
    public void test96setUserRolesByName() {
        VXStringList testVXStringList = createVXStringList();
        Mockito.when(xUserMgr.setUserRolesByName("Admin", testVXStringList.getVXStrings())).thenReturn(testVXStringList);
        VXStringList retVXStringList = xUserRest.setUserRolesByName("Admin", testVXStringList);
        Mockito.verify(xUserMgr).setUserRolesByName("Admin", testVXStringList.getVXStrings());

        Assertions.assertNotNull(retVXStringList);
        Assertions.assertEquals(testVXStringList.getTotalCount(), retVXStringList.getTotalCount());
        Assertions.assertEquals(testVXStringList.getClass(), retVXStringList.getClass());
    }

    @Test
    public void test97getUserRolesByExternalID() {
        VXStringList testVXStringList = createVXStringList();

        Mockito.when(xUserMgr.getUserRolesByExternalID(id)).thenReturn(testVXStringList);
        VXStringList retVXStringList = xUserRest.getUserRolesByExternalID(id);
        Mockito.verify(xUserMgr).getUserRolesByExternalID(id);
        Assertions.assertNotNull(retVXStringList);
        Assertions.assertEquals(testVXStringList.getTotalCount(), retVXStringList.getTotalCount());
        Assertions.assertEquals(testVXStringList.getClass(), retVXStringList.getClass());
    }

    @Test
    public void test98getUserRolesByName() {
        VXStringList testVXStringList = createVXStringList();

        Mockito.when(xUserMgr.getUserRolesByName("Admin")).thenReturn(testVXStringList);
        VXStringList retVXStringList = xUserRest.getUserRolesByName("Admin");
        Mockito.verify(xUserMgr).getUserRolesByName("Admin");
        Assertions.assertNotNull(retVXStringList);
        Assertions.assertEquals(testVXStringList.getTotalCount(), retVXStringList.getTotalCount());
        Assertions.assertEquals(testVXStringList.getClass(), retVXStringList.getClass());
    }

    @Test
    public void test99deleteUsersByUserName() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "true";
        boolean            forceDelete        = false;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("User1");
        VXUser       testVXUser   = createVXUser();
        VXStringList vxStringList = createVXStringList();

        Mockito.when(xUserService.getXUserByUserName(testVXString.getValue())).thenReturn(testVXUser);
        forceDelete = true;
        Mockito.doNothing().when(xUserMgr).deleteXUser(testVXUser.getId(), forceDelete);
        xUserRest.deleteUsersByUserName(request, vxStringList);
        Mockito.verify(xUserMgr).deleteXUser(testVXUser.getId(), forceDelete);
        Mockito.verify(xUserService).getXUserByUserName(testVXString.getValue());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test100deleteUsersByUserNameNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("User1");
        VXUser       testVXUser   = createVXUser();
        VXStringList vxStringList = createVXStringList();

        Mockito.when(xUserService.getXUserByUserName(testVXString.getValue())).thenReturn(testVXUser);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXUser(testVXUser.getId(), forceDelete);
        xUserRest.deleteUsersByUserName(request, vxStringList);
        Mockito.verify(xUserMgr).deleteXUser(testVXUser.getId(), forceDelete);
        Mockito.verify(xUserService).getXUserByUserName(testVXString.getValue());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test101deleteUsersByUserNameNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = null;
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("User1");
        VXUser       testVXUser   = createVXUser();
        VXStringList vxStringList = createVXStringList();

        Mockito.when(xUserService.getXUserByUserName(testVXString.getValue())).thenReturn(testVXUser);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXUser(testVXUser.getId(), forceDelete);
        xUserRest.deleteUsersByUserName(request, vxStringList);
        Mockito.verify(xUserMgr).deleteXUser(testVXUser.getId(), forceDelete);
        Mockito.verify(xUserService).getXUserByUserName(testVXString.getValue());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test102deleteUsersByUserNameSetValueNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("User1");
        VXUser       testVXUser   = createVXUser();
        VXStringList vxStringList = createVXStringList();

        Mockito.when(xUserService.getXUserByUserName(testVXString.getValue())).thenReturn(testVXUser);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXUser(testVXUser.getId(), forceDelete);
        xUserRest.deleteUsersByUserName(request, vxStringList);
        Mockito.verify(xUserMgr).deleteXUser(testVXUser.getId(), forceDelete);
        Mockito.verify(xUserService).getXUserByUserName(testVXString.getValue());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test103deleteUsersByUserNameListNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("User1");
        xUserRest.deleteUsersByUserName(request, null);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test104deleteUsersByUserNameListGetListNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";

        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXStringList vxStringList = createVXStringList();
        vxStringList.setVXStrings(null);
        xUserRest.deleteUsersByUserName(request, vxStringList);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test105deleteUsersByUserNameNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "true";

        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue(null);

        VXStringList   vxStringList  = createVXStringList();
        List<VXString> testVXStrings = new ArrayList<>();
        testVXStrings.add(testVXString);
        vxStringList.setVXStrings(testVXStrings);
        xUserRest.deleteUsersByUserName(request, vxStringList);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test106deleteGroupsByGroupName() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "true";
        boolean            forceDelete        = false;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("testVXGroup");
        VXGroup      testVXGroup  = createVXGroup();
        VXStringList vxStringList = createVXStringListGroup();

        Mockito.when(xGroupService.getGroupByGroupName(testVXString.getValue())).thenReturn(testVXGroup);
        forceDelete = true;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        xUserRest.deleteGroupsByGroupName(request, vxStringList);
        Mockito.verify(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        Mockito.verify(xGroupService).getGroupByGroupName(testVXString.getValue());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test107GroupsByGroupNameNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("testVXGroup");
        VXGroup      testVXGroup  = createVXGroup();
        VXStringList vxStringList = createVXStringListGroup();

        Mockito.when(xGroupService.getGroupByGroupName(testVXString.getValue())).thenReturn(testVXGroup);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        xUserRest.deleteGroupsByGroupName(request, vxStringList);
        Mockito.verify(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        Mockito.verify(xGroupService).getGroupByGroupName(testVXString.getValue());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test108deleteGroupsByGroupNameNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = null;
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("testVXGroup");
        VXGroup      testVXGroup  = createVXGroup();
        VXStringList vxStringList = createVXStringListGroup();

        Mockito.when(xGroupService.getGroupByGroupName(testVXString.getValue())).thenReturn(testVXGroup);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        xUserRest.deleteGroupsByGroupName(request, vxStringList);
        Mockito.verify(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        Mockito.verify(xGroupService).getGroupByGroupName(testVXString.getValue());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test109deleteGroupsByGroupNameSetValueNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";
        boolean            forceDelete        = true;
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("testVXGroup");
        VXGroup      testVXGroup  = createVXGroup();
        VXStringList vxStringList = createVXStringListGroup();

        Mockito.when(xGroupService.getGroupByGroupName(testVXString.getValue())).thenReturn(testVXGroup);
        forceDelete = false;
        Mockito.doNothing().when(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        xUserRest.deleteGroupsByGroupName(request, vxStringList);
        Mockito.verify(xUserMgr).deleteXGroup(testVXGroup.getId(), forceDelete);
        Mockito.verify(xGroupService).getGroupByGroupName(testVXString.getValue());
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test110deleteGroupsByGroupNameListNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";
        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue("testVXGroup");
        xUserRest.deleteGroupsByGroupName(request, null);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test111deleteUsersByUserNameListGetListNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "false";

        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXStringList vxStringList = createVXStringList();
        vxStringList.setVXStrings(null);
        xUserRest.deleteGroupsByGroupName(request, vxStringList);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test112deleteUsersByUserNameNull() {
        HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
        String             testforceDeleteStr = "true";

        Mockito.when(request.getParameter("forceDelete")).thenReturn(testforceDeleteStr);
        VXString testVXString = new VXString();
        testVXString.setValue(null);

        VXStringList   vxStringList  = createVXStringListGroup();
        List<VXString> testVXStrings = new ArrayList<VXString>();
        testVXStrings.add(testVXString);
        vxStringList.setVXStrings(testVXStrings);
        xUserRest.deleteGroupsByGroupName(request, vxStringList);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @SuppressWarnings({"unchecked", "static-access"})
    @Test
    public void test113ErrorWhenRoleUserIsTryingToFetchAnotherUserDetails() {
        destroySession();
        Assertions.assertThrows(Throwable.class, () -> {
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

            VXUser       loggedInUser     = createVXUser();
            List<String> loggedInUserRole = new ArrayList<String>();
            loggedInUserRole.add(RangerConstants.ROLE_USER);
            loggedInUser.setId(8L);
            loggedInUser.setName("testuser");
            loggedInUser.setUserRoleList(loggedInUserRole);

            HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
            SearchCriteria     testSearchCriteria = createsearchCriteria();
            testSearchCriteria.addParam("name", "admin");

            Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

            Mockito.when(searchUtil.extractCommonCriterias(request, xUserService.sortFields)).thenReturn(testSearchCriteria);
            Mockito.when(searchUtil.extractString(request, testSearchCriteria, "emailAddress", "Email Address", null)).thenReturn("");
            Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "userSource", "User Source")).thenReturn(1);
            Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "isVisible", "User Visibility")).thenReturn(1);
            Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "status", "User Status")).thenReturn(1);
            Mockito.when(searchUtil.extractStringList(request, testSearchCriteria, "userRoleList", "User Role List", "userRoleList", null, null)).thenReturn(new ArrayList<String>());
            Mockito.when(searchUtil.extractRoleString(request, testSearchCriteria, "userRole", "Role", null)).thenReturn("");
            Mockito.when(xUserService.getXUserByUserName("testuser")).thenReturn(loggedInUser);
            Mockito.when(restErrorUtil.create403RESTException("Logged-In user is not allowed to access requested user data.")).thenThrow(new WebApplicationException());
            //thrown.expect(WebApplicationException.class);
            xUserRest.searchXUsers(request, null, null);
        });
    }

    @SuppressWarnings({"unchecked", "static-access"})
    @Test
    public void test114RoleUserWillGetOnlyHisOwnUserDetails() {
        destroySession();
        Assertions.assertThrows(Throwable.class, () -> {
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

            VXUser       loggedInUser     = createVXUser();
            List<String> loggedInUserRole = new ArrayList<String>();
            loggedInUserRole.add(RangerConstants.ROLE_USER);
            loggedInUser.setId(8L);
            loggedInUser.setName("testuser");
            loggedInUser.setUserRoleList(loggedInUserRole);

            VXUserList expecteUserList = new VXUserList();
            VXUser     expectedUser    = new VXUser();
            expectedUser.setId(8L);
            expectedUser.setName("testuser");
            List<VXUser> userList = new ArrayList<VXUser>();
            userList.add(expectedUser);
            expecteUserList.setVXUsers(userList);

            HttpServletRequest request            = Mockito.mock(HttpServletRequest.class);
            SearchCriteria     testSearchCriteria = createsearchCriteria();

            Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(testSearchCriteria);

            Mockito.when(searchUtil.extractCommonCriterias(request, xUserService.sortFields)).thenReturn(testSearchCriteria);
            Mockito.when(searchUtil.extractString(request, testSearchCriteria, "emailAddress", "Email Address", null)).thenReturn("");
            Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "userSource", "User Source")).thenReturn(1);
            Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "isVisible", "User Visibility")).thenReturn(1);
            Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "status", "User Status")).thenReturn(1);
            Mockito.when(searchUtil.extractStringList(request, testSearchCriteria, "userRoleList", "User Role List", "userRoleList", null, null)).thenReturn(new ArrayList<String>());
            Mockito.when(searchUtil.extractRoleString(request, testSearchCriteria, "userRole", "Role", null)).thenReturn("");
            Mockito.when(xUserService.getXUserByUserName("testuser")).thenReturn(loggedInUser);
            Mockito.when(xUserMgr.searchXUsers(testSearchCriteria)).thenReturn(expecteUserList);

            VXUserList gotVXUserList = xUserRest.searchXUsers(request, null, null);

            Assertions.assertEquals(gotVXUserList.getList().size(), 1);
            Assertions.assertEquals(gotVXUserList.getList().get(0).getId(), expectedUser.getId());
            Assertions.assertEquals(gotVXUserList.getList().get(0).getName(), expectedUser.getName());
        });
    }

    @Test
    public void test115updateXGroupPermissionWithInvalidPermissionId() {
        Assertions.assertThrows(WebApplicationException.class, () -> {
            VXGroupPermission testVXGroupPermission = createVXGroupPermission();
            Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean())).thenThrow(new WebApplicationException());
            //thrown.expect(WebApplicationException.class);
            VXGroupPermission retVXGroupPermission = xUserRest.updateXGroupPermission(-1L, testVXGroupPermission);
            Mockito.verify(xUserMgr).updateXGroupPermission(testVXGroupPermission);
            Mockito.verify(xUserMgr).checkAdminAccess();
            Assertions.assertNotNull(retVXGroupPermission);
            Assertions.assertEquals(retVXGroupPermission.getId(), testVXGroupPermission.getId());
            Assertions.assertEquals(retVXGroupPermission.getClass(), testVXGroupPermission.getClass());
        });
    }

    @Test
    public void test116updateXGroupPermissionWithPermissionIdIsNull() {
        VXGroupPermission testVXGroupPermission   = createVXGroupPermission();
        Long              testVXGroupPermissionId = testVXGroupPermission.getId();
        testVXGroupPermission.setId(null);
        Mockito.doNothing().when(xUserMgr).checkAdminAccess();
        Mockito.when(xUserMgr.updateXGroupPermission(testVXGroupPermission)).thenReturn(testVXGroupPermission);
        VXGroupPermission retVXGroupPermission = xUserRest.updateXGroupPermission(testVXGroupPermissionId, testVXGroupPermission);
        Mockito.verify(xUserMgr).updateXGroupPermission(testVXGroupPermission);
        Mockito.verify(xUserMgr).checkAdminAccess();
        Assertions.assertNotNull(retVXGroupPermission);
        Assertions.assertEquals(retVXGroupPermission.getId(), testVXGroupPermission.getId());
        Assertions.assertEquals(retVXGroupPermission.getClass(), testVXGroupPermission.getClass());
    }

    @Test
    public void test117forceDeleteExternalUsers() {
        List<Long> listOfIds = new ArrayList<>(Arrays.asList(1L, 10L, 100L, 1000L));

        Mockito.when(xUserService.searchXUsersForIds(Mockito.any(SearchCriteria.class))).thenReturn(listOfIds);
        Mockito.when(xUserMgr.forceDeleteExternalUsers(listOfIds)).thenReturn(4L);

        Assertions.assertEquals("4 users deleted successfully.", xUserRest.forceDeleteExternalUsers(request).getEntity());

        Mockito.verify(searchUtil).extractString(eq(request), Mockito.any(SearchCriteria.class), eq("name"), eq("User name"), eq(null));
        Mockito.verify(searchUtil).extractString(eq(request), Mockito.any(SearchCriteria.class), eq("emailAddress"), eq("Email Address"), eq(null));
        Mockito.verify(searchUtil).extractInt(eq(request), Mockito.any(SearchCriteria.class), eq("isVisible"), eq("User Visibility"));
        Mockito.verify(searchUtil).extractInt(eq(request), Mockito.any(SearchCriteria.class), eq("status"), eq("User Status"));
        Mockito.verify(searchUtil).extractString(eq(request), Mockito.any(SearchCriteria.class), eq("syncSource"), eq("Sync Source"), eq(null));
        Mockito.verify(searchUtil).extractRoleString(eq(request), Mockito.any(SearchCriteria.class), eq("userRole"), eq("Role"), eq(null));
    }

    @Test
    public void test118forceDeleteExternalGroups() {
        List<Long> listOfIds = new ArrayList<>(Arrays.asList(1L, 10L, 100L));

        Mockito.when(xGroupService.searchXGroupsForIds(Mockito.any(SearchCriteria.class))).thenReturn(listOfIds);
        Mockito.when(xUserMgr.forceDeleteExternalGroups(listOfIds)).thenReturn(3L);

        Assertions.assertEquals("3 groups deleted successfully.", xUserRest.forceDeleteExternalGroups(request).getEntity());

        Mockito.verify(searchUtil).extractString(eq(request), Mockito.any(SearchCriteria.class), eq("name"), eq("Group Name"), eq(null));
        Mockito.verify(searchUtil).extractInt(eq(request), Mockito.any(SearchCriteria.class), eq("isVisible"), eq("Group Visibility"));
        Mockito.verify(searchUtil).extractString(eq(request), Mockito.any(SearchCriteria.class), eq("syncSource"), eq("Sync Source"), eq(null));
    }

    @Test
    public void test119createExternalUser() {
        VXUser vxUser = createVXUser();
        vxUser.setName("externalUser");

        Mockito.when(xUserMgr.createExternalUser("externalUser")).thenReturn(vxUser);
        VXUser result = xUserRest.createExternalUser(vxUser);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("externalUser", result.getName());
        Mockito.verify(xUserMgr).createExternalUser("externalUser");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test120getUsersLookup() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchCriteria testSearchCriteria = createsearchCriteria();
        VXStringList expectedResult = createVXStringList();
        VXUserList vXUserList = new VXUserList();
        List<VXUser> users = Arrays.asList(createVXUser());
        vXUserList.setVXUsers(users);

        Mockito.when(searchUtil.extractCommonCriterias(request, xUserService.sortFields)).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "name", "User name", null)).thenReturn("");
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "isVisible", "User Visibility")).thenReturn(1);
        Mockito.when(xUserMgr.lookupXUsers(testSearchCriteria)).thenReturn(vXUserList);

        VXStringList result = xUserRest.getUsersLookup(request);

        Assertions.assertNotNull(result);
        Mockito.verify(xUserMgr).lookupXUsers(testSearchCriteria);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test121getGroupsLookup() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchCriteria testSearchCriteria = createsearchCriteria();
        VXGroupList vXGroupList = createXGroupList();

        Mockito.when(searchUtil.extractCommonCriterias(request, xGroupService.sortFields)).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "name", "group name", null)).thenReturn("");
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "isVisible", "Group Visibility")).thenReturn(1);
        Mockito.when(xUserMgr.lookupXGroups(testSearchCriteria)).thenReturn(vXGroupList);

        VXStringList result = xUserRest.getGroupsLookup(request);

        Assertions.assertNotNull(result);
        Mockito.verify(xUserMgr).lookupXGroups(testSearchCriteria);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test122getPrincipalsLookup() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchCriteria testSearchCriteria = createsearchCriteria();
        List<RangerPrincipal> expectedPrincipals = new ArrayList<>();

        Mockito.when(searchUtil.extractCommonCriterias(request, xGroupService.sortFields)).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "name", null, null)).thenReturn("");
        Mockito.when(xUserMgr.getRangerPrincipals(testSearchCriteria)).thenReturn(expectedPrincipals);

        List<RangerPrincipal> result = xUserRest.getPrincipalsLookup(request);

        Assertions.assertEquals(expectedPrincipals, result);
        Mockito.verify(xUserMgr).getRangerPrincipals(testSearchCriteria);
    }

    @Test
    public void test123getXGroupUsersByGroupName() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String groupName = "testGroup";
        VXGroupUserInfo expectedInfo = new VXGroupUserInfo();

        Mockito.when(xUserMgr.getXGroupUserFromMap(groupName)).thenReturn(expectedInfo);

        VXGroupUserInfo result = xUserRest.getXGroupUsersByGroupName(request, groupName);

        Assertions.assertEquals(expectedInfo, result);
        Mockito.verify(xUserMgr).getXGroupUserFromMap(groupName);
    }

    @Test
    public void test124deleteSingleUserByUserId() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Long userId = 1L;
        boolean forceDelete = false;

        Mockito.when(request.getParameter("forceDelete")).thenReturn("false");
        Mockito.doNothing().when(xUserMgr).deleteXUser(userId, forceDelete);

        xUserRest.deleteSingleUserByUserId(request, userId);

        Mockito.verify(xUserMgr).deleteXUser(userId, forceDelete);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test125deleteSingleGroupByGroupId() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Long groupId = 1L;
        boolean forceDelete = true;

        Mockito.when(request.getParameter("forceDelete")).thenReturn("true");
        Mockito.doNothing().when(xUserMgr).deleteXGroup(groupId, forceDelete);

        xUserRest.deleteSingleGroupByGroupId(request, groupId);

        Mockito.verify(xUserMgr).deleteXGroup(groupId, forceDelete);
        Mockito.verify(request).getParameter("forceDelete");
    }

    @Test
    public void test126deleteSingleUserByUserName() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String userName = "testUser";
        VXUser vxUser = createVXUser();
        boolean forceDelete = false;

        Mockito.when(request.getParameter("forceDelete")).thenReturn("false");
        Mockito.when(xUserService.getXUserByUserName(userName)).thenReturn(vxUser);
        Mockito.doNothing().when(xUserMgr).deleteXUser(vxUser.getId(), forceDelete);

        xUserRest.deleteSingleUserByUserName(request, userName);

        Mockito.verify(xUserMgr).deleteXUser(vxUser.getId(), forceDelete);
        Mockito.verify(xUserService).getXUserByUserName(userName);
    }

    @Test
    public void test127deleteSingleGroupByGroupName() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String groupName = "testGroup";
        VXGroup vxGroup = createVXGroup();
        boolean forceDelete = true;

        Mockito.when(request.getParameter("forceDelete")).thenReturn("true");
        Mockito.when(xGroupService.getGroupByGroupName(groupName.trim())).thenReturn(vxGroup);
        Mockito.doNothing().when(xUserMgr).deleteXGroup(vxGroup.getId(), forceDelete);

        xUserRest.deleteSingleGroupByGroupName(request, groupName);

        Mockito.verify(xUserMgr).deleteXGroup(vxGroup.getId(), forceDelete);
        Mockito.verify(xGroupService).getGroupByGroupName(groupName.trim());
    }

    @Test
    public void test128postUserGroupAuditInfo() {
        VXUgsyncAuditInfo vxUgsyncAuditInfo = new VXUgsyncAuditInfo();
        vxUgsyncAuditInfo.setUserName("testUser");

        Mockito.when(xUserMgr.postUserGroupAuditInfo(vxUgsyncAuditInfo)).thenReturn(vxUgsyncAuditInfo);

        VXUgsyncAuditInfo result = xUserRest.postUserGroupAuditInfo(vxUgsyncAuditInfo);

        Assertions.assertEquals(vxUgsyncAuditInfo, result);
        Mockito.verify(xUserMgr).postUserGroupAuditInfo(vxUgsyncAuditInfo);
    }

    @Test
    public void test129getAllGroupUsers() {
        Map<String, Set<String>> expectedMap = new HashMap<>();
        expectedMap.put("group1", new HashSet<>(Arrays.asList("user1", "user2")));

        Mockito.when(rangerDaoManager.getXXGroupUser()).thenReturn(xXGroupUserDao);
        Mockito.when(xXGroupUserDao.findUsersByGroupIds()).thenReturn(expectedMap);

        Map<String, Set<String>> result = xUserRest.getAllGroupUsers();

        Assertions.assertEquals(expectedMap, result);
        Mockito.verify(rangerDaoManager).getXXGroupUser();
        Mockito.verify(xXGroupUserDao).findUsersByGroupIds();
    }

    @Test
    public void test130addOrUpdateUsers() {
        VXUserList users = new VXUserList();
        List<VXUser> userList = Arrays.asList(createVXUser());
        users.setVXUsers(userList);

        Mockito.when(xUserMgr.createOrUpdateXUsers(users)).thenReturn(1);

        String result = xUserRest.addOrUpdateUsers(users);

        Assertions.assertEquals("1", result);
        Mockito.verify(xUserMgr).createOrUpdateXUsers(users);
    }

    @Test
    public void test131addOrUpdateGroups() {
        VXGroupList groups = createXGroupList();

        Mockito.when(xUserMgr.createOrUpdateXGroups(groups)).thenReturn(1);

        int result = xUserRest.addOrUpdateGroups(groups);

        Assertions.assertEquals(1, result);
        Mockito.verify(xUserMgr).createOrUpdateXGroups(groups);
    }

    @Test
    public void test132addOrUpdateGroupUsersList() {
        List<GroupUserInfo> groupUserInfoList = new ArrayList<>();
        GroupUserInfo groupUserInfo = new GroupUserInfo();
        groupUserInfo.setGroupName("testGroup");
        groupUserInfoList.add(groupUserInfo);

        Mockito.when(xUserMgr.createOrDeleteXGroupUserList(groupUserInfoList)).thenReturn(1);

        int result = xUserRest.addOrUpdateGroupUsersList(groupUserInfoList);

        Assertions.assertEquals(1, result);
        Mockito.verify(xUserMgr).createOrDeleteXGroupUserList(groupUserInfoList);
    }

    @Test
    public void test133setXUserRolesByName() {
        UsersGroupRoleAssignments ugRoleAssignments = new UsersGroupRoleAssignments();
        List<String> expectedRoles = Arrays.asList("ROLE_USER", "ROLE_ADMIN");

        Mockito.when(xUserMgr.updateUserRoleAssignments(ugRoleAssignments)).thenReturn(expectedRoles);

        List<String> result = xUserRest.setXUserRolesByName(ugRoleAssignments);

        Assertions.assertEquals(expectedRoles, result);
        Mockito.verify(xUserMgr).updateUserRoleAssignments(ugRoleAssignments);
    }

    @Test
    public void test134updateDeletedGroups() {
        Set<String> deletedGroups = new HashSet<>(Arrays.asList("group1", "group2"));

        Mockito.when(xUserMgr.updateDeletedGroups(deletedGroups)).thenReturn(2);

        int result = xUserRest.updateDeletedGroups(deletedGroups);

        Assertions.assertEquals(2, result);
        Mockito.verify(xUserMgr).updateDeletedGroups(deletedGroups);
    }

    @Test
    public void test135updateDeletedUsers() {
        Set<String> deletedUsers = new HashSet<>(Arrays.asList("user1", "user2"));

        Mockito.when(xUserMgr.updateDeletedUsers(deletedUsers)).thenReturn(2);

        int result = xUserRest.updateDeletedUsers(deletedUsers);

        Assertions.assertEquals(2, result);
        Mockito.verify(xUserMgr).updateDeletedUsers(deletedUsers);
    }

    @Test
    public void test136searchXModuleDefList() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchCriteria testSearchCriteria = createsearchCriteria();
        VXModulePermissionList expectedList = new VXModulePermissionList();

        Mockito.when(searchUtil.extractCommonCriterias(request, xModuleDefService.sortFields)).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "module", "modulename", null)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "moduleDefList", "id", null)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "userName", "userName", null)).thenReturn("");
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "groupName", "groupName", null)).thenReturn("");
        Mockito.when(xUserMgr.searchXModuleDefList(testSearchCriteria)).thenReturn(expectedList);

        VXModulePermissionList result = xUserRest.searchXModuleDefList(request);

        Assertions.assertEquals(expectedList, result);
        Mockito.verify(xUserMgr).searchXModuleDefList(testSearchCriteria);
    }

    @Test
    public void test137createXGroupUserError() {
        VXGroupUser testVXGroupUser = new VXGroupUser();
        testVXGroupUser.setName(null);
        testVXGroupUser.setUserId(null);

        Mockito.when(restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST,
                "Group name or UserId is empty or null", true)).thenReturn(new WebApplicationException());

        Assertions.assertThrows(WebApplicationException.class, () -> {
            xUserRest.createXGroupUser(testVXGroupUser);
        });
    }

    @Test
    public void test138updateXGroupUserError() {
        VXGroupUser testVXGroupUser = new VXGroupUser();
        testVXGroupUser.setName("");
        testVXGroupUser.setUserId(null);

        Mockito.when(restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST,
                "Group name or UserId is empty or null", true)).thenReturn(new WebApplicationException());

        Assertions.assertThrows(WebApplicationException.class, () -> {
            xUserRest.updateXGroupUser(testVXGroupUser);
        });
    }

    @Test
    public void test140getRangerUserStoreIfUpdatedServiceNotFound() throws Exception {
        String serviceName = "testService";
        Long lastKnownUserStoreVersion = -1L;
        Long lastActivationTime = 0L;
        String pluginId = "plugin1";
        String clusterName = "cluster1";
        String pluginCapabilities = "";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.doNothing().when(bizUtil).failUnauthenticatedDownloadIfNotAllowed();
        Mockito.when(serviceUtil.isValidService(serviceName, request)).thenReturn(true);
        Mockito.when(rangerDaoManager.getXXService()).thenReturn(xXServiceDao);
        Mockito.when(xXServiceDao.findByName(serviceName)).thenReturn(null);

        RangerUserStore result = xUserRest.getRangerUserStoreIfUpdated(serviceName, lastKnownUserStoreVersion,
                lastActivationTime, pluginId, clusterName, pluginCapabilities, request);

        Assertions.assertNull(result);
        Mockito.verify(rangerDaoManager).getXXService();
        Mockito.verify(xXServiceDao).findByName(serviceName);
    }

    @Test
    public void test142getSecureRangerUserStoreIfUpdatedNoParams() throws Exception {
        Long lastKnownUserStoreVersion = -1L;
        Long lastActivationTime = 0L;
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        UserSessionBase userSession = Mockito.mock(UserSessionBase.class);

        RangerSecurityContext context = new RangerSecurityContext();
        context.setUserSession(userSession);
        RangerContextHolder.setSecurityContext(context);

        Mockito.when(userSession.isUserAdmin()).thenReturn(false);
        Mockito.when(userSession.isKeyAdmin()).thenReturn(false);
        Mockito.when(userSession.getLoginId()).thenReturn("testuser");
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new WebApplicationException());
        Mockito.when(rangerDaoManager.getXXService()).thenReturn(xXServiceDao);
        Mockito.when(xXServiceDao.findByName(Mockito.anyString())).thenReturn(new XXService());
        XXServiceDefDao serviceDefDao = Mockito.mock(XXServiceDefDao.class);
        Mockito.when(rangerDaoManager.getXXServiceDef()).thenReturn(serviceDefDao);
        XXServiceDef xServiceDef = new XXServiceDef();
        xServiceDef.setImplclassname("not.kms.impl");
        Mockito.when(serviceDefDao.getById(Mockito.anyLong())).thenReturn(xServiceDef);
        Mockito.when(svcStore.getServiceByName(Mockito.anyString())).thenReturn(new RangerService());
        Mockito.when(bizUtil.isUserAllowed(Mockito.any(RangerService.class), Mockito.anyString())).thenReturn(false);

        WebApplicationException thrown = assertThrows(WebApplicationException.class, () -> {
            xUserRest.getSecureRangerUserStoreIfUpdated("service1", lastKnownUserStoreVersion, lastActivationTime, null, "", "", request);
        });

        Mockito.verify(restErrorUtil).createRESTException(Mockito.anyInt(), Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void test143addOrUpdateUsersWithNullList() {
        VXUserList users = null;

        Mockito.when(xUserMgr.createOrUpdateXUsers(users)).thenReturn(0);

        String result = xUserRest.addOrUpdateUsers(users);

        Assertions.assertEquals("0", result);
        Mockito.verify(xUserMgr).createOrUpdateXUsers(users);
    }

    @Test
    public void test144addOrUpdateGroupsWithNullList() {
        VXGroupList groups = null;

        Mockito.when(xUserMgr.createOrUpdateXGroups(groups)).thenReturn(0);

        int result = xUserRest.addOrUpdateGroups(groups);

        Assertions.assertEquals(0, result);
        Mockito.verify(xUserMgr).createOrUpdateXGroups(groups);
    }

    @Test
    public void test145addOrUpdateGroupUsersListWithNullList() {
        List<GroupUserInfo> groupUserInfoList = null;

        Mockito.when(xUserMgr.createOrDeleteXGroupUserList(groupUserInfoList)).thenReturn(0);

        int result = xUserRest.addOrUpdateGroupUsersList(groupUserInfoList);

        Assertions.assertEquals(0, result);
        Mockito.verify(xUserMgr).createOrDeleteXGroupUserList(groupUserInfoList);
    }

    @Test
    public void test146setXUserRolesByNameWithNullAssignments() {
        UsersGroupRoleAssignments ugRoleAssignments = null;
        List<String> expectedRoles = new ArrayList<>();

        Mockito.when(xUserMgr.updateUserRoleAssignments(ugRoleAssignments)).thenReturn(expectedRoles);

        List<String> result = xUserRest.setXUserRolesByName(ugRoleAssignments);

        Assertions.assertEquals(expectedRoles, result);
        Mockito.verify(xUserMgr).updateUserRoleAssignments(ugRoleAssignments);
    }

    @Test
    public void test147updateDeletedGroupsWithNullSet() {
        Set<String> deletedGroups = null;

        Mockito.when(xUserMgr.updateDeletedGroups(deletedGroups)).thenReturn(0);

        int result = xUserRest.updateDeletedGroups(deletedGroups);

        Assertions.assertEquals(0, result);
        Mockito.verify(xUserMgr).updateDeletedGroups(deletedGroups);
    }

    @Test
    public void test148updateDeletedUsersWithNullSet() {
        Set<String> deletedUsers = null;

        Mockito.when(xUserMgr.updateDeletedUsers(deletedUsers)).thenReturn(0);

        int result = xUserRest.updateDeletedUsers(deletedUsers);

        Assertions.assertEquals(0, result);
        Mockito.verify(xUserMgr).updateDeletedUsers(deletedUsers);
    }

    @Test
    public void test149createExternalUserWithNullName() {
        VXUser vxUser = new VXUser();
        vxUser.setName(null);

        Mockito.when(xUserMgr.createExternalUser(null)).thenReturn(vxUser);

        VXUser result = xUserRest.createExternalUser(vxUser);

        Assertions.assertNotNull(result);
        Mockito.verify(xUserMgr).createExternalUser(null);
    }

    @Test
    public void test150getAllGroupUsersWithEmptyResult() {
        Map<String, Set<String>> expectedMap = new HashMap<>();

        Mockito.when(rangerDaoManager.getXXGroupUser()).thenReturn(xXGroupUserDao);
        Mockito.when(xXGroupUserDao.findUsersByGroupIds()).thenReturn(expectedMap);

        Map<String, Set<String>> result = xUserRest.getAllGroupUsers();

        Assertions.assertEquals(expectedMap, result);
        Assertions.assertTrue(result.isEmpty());
        Mockito.verify(rangerDaoManager).getXXGroupUser();
        Mockito.verify(xXGroupUserDao).findUsersByGroupIds();
    }

    @Test
    public void test151postUserGroupAuditInfoWithNullInfo() {
        VXUgsyncAuditInfo vxUgsyncAuditInfo = null;

        Mockito.when(xUserMgr.postUserGroupAuditInfo(vxUgsyncAuditInfo)).thenReturn(vxUgsyncAuditInfo);

        VXUgsyncAuditInfo result = xUserRest.postUserGroupAuditInfo(vxUgsyncAuditInfo);

        Assertions.assertNull(result);
        Mockito.verify(xUserMgr).postUserGroupAuditInfo(vxUgsyncAuditInfo);
    }

    @Test
    public void test152deleteSingleUserByUserIdWithNullId() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Long userId = null;

        Mockito.when(request.getParameter("forceDelete")).thenReturn("false");

        xUserRest.deleteSingleUserByUserId(request, userId);

        Mockito.verify(request).getParameter("forceDelete");
        Mockito.verify(xUserMgr, Mockito.never()).deleteXUser(Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    public void test153deleteSingleGroupByGroupIdWithNullId() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Long groupId = null;

        Mockito.when(request.getParameter("forceDelete")).thenReturn("false");

        xUserRest.deleteSingleGroupByGroupId(request, groupId);

        Mockito.verify(request).getParameter("forceDelete");
        Mockito.verify(xUserMgr, Mockito.never()).deleteXGroup(Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    public void test154deleteSingleUserByUserNameWithEmptyName() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String userName = "";

        Mockito.when(request.getParameter("forceDelete")).thenReturn("false");

        xUserRest.deleteSingleUserByUserName(request, userName);

        Mockito.verify(request).getParameter("forceDelete");
        Mockito.verify(xUserService, Mockito.never()).getXUserByUserName(Mockito.anyString());
        Mockito.verify(xUserMgr, Mockito.never()).deleteXUser(Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    public void test155deleteSingleGroupByGroupNameWithEmptyName() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String groupName = "";

        Mockito.when(request.getParameter("forceDelete")).thenReturn("false");

        xUserRest.deleteSingleGroupByGroupName(request, groupName);

        Mockito.verify(request).getParameter("forceDelete");
        Mockito.verify(xGroupService, Mockito.never()).getGroupByGroupName(Mockito.anyString());
        Mockito.verify(xUserMgr, Mockito.never()).deleteXGroup(Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    public void test156getUsersLookupWithException() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchCriteria testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(request, xUserService.sortFields)).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "name", "User name", null)).thenReturn("");
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "isVisible", "User Visibility")).thenReturn(1);
        Mockito.when(xUserMgr.lookupXUsers(testSearchCriteria)).thenThrow(new RuntimeException("Database error"));
        Mockito.when(restErrorUtil.createRESTException("Database error")).thenReturn(new WebApplicationException());

        WebApplicationException thrown = assertThrows(WebApplicationException.class, () -> {
            xUserRest.getUsersLookup(request);
        });

        Mockito.verify(xUserMgr).lookupXUsers(testSearchCriteria);
        Mockito.verify(restErrorUtil).createRESTException("Database error");
    }

    @Test
    public void test157getGroupsLookupWithException() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchCriteria testSearchCriteria = createsearchCriteria();

        Mockito.when(searchUtil.extractCommonCriterias(request, xGroupService.sortFields)).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "name", "group name", null)).thenReturn("");
        Mockito.when(searchUtil.extractInt(request, testSearchCriteria, "isVisible", "Group Visibility")).thenReturn(1);
        Mockito.when(xUserMgr.lookupXGroups(testSearchCriteria)).thenThrow(new RuntimeException("Database error"));
        Mockito.when(restErrorUtil.createRESTException("Database error")).thenReturn(new WebApplicationException());

        WebApplicationException thrown = assertThrows(WebApplicationException.class, () -> {
            xUserRest.getGroupsLookup(request);
        });

        Mockito.verify(xUserMgr).lookupXGroups(testSearchCriteria);
        Mockito.verify(restErrorUtil).createRESTException("Database error");
    }

    @Test
    public void test158getPrincipalsLookupWithEmptyResult() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        SearchCriteria testSearchCriteria = createsearchCriteria();
        List<RangerPrincipal> expectedPrincipals = new ArrayList<>();

        Mockito.when(searchUtil.extractCommonCriterias(request, xGroupService.sortFields)).thenReturn(testSearchCriteria);
        Mockito.when(searchUtil.extractString(request, testSearchCriteria, "name", null, null)).thenReturn("");
        Mockito.when(xUserMgr.getRangerPrincipals(testSearchCriteria)).thenReturn(expectedPrincipals);

        List<RangerPrincipal> result = xUserRest.getPrincipalsLookup(request);

        Assertions.assertEquals(expectedPrincipals, result);
        Assertions.assertTrue(result.isEmpty());
        Mockito.verify(xUserMgr).getRangerPrincipals(testSearchCriteria);
    }

    @Test
    public void test159getXGroupUsersByGroupNameWithNullResult() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String groupName = "nonExistentGroup";

        Mockito.when(xUserMgr.getXGroupUserFromMap(groupName)).thenReturn(null);

        VXGroupUserInfo result = xUserRest.getXGroupUsersByGroupName(request, groupName);

        Assertions.assertNull(result);
        Mockito.verify(xUserMgr).getXGroupUserFromMap(groupName);
    }

    @Test
    public void test160getXGroupByGroupNameWithNullUser() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String groupName = "testGroup";
        VXGroup vXGroup = createVXGroup();
        vXGroup.setName(groupName);

        RangerSecurityContext context = new RangerSecurityContext();
        context.setUserSession(null);
        RangerContextHolder.setSecurityContext(context);

        Mockito.when(xGroupService.getGroupByGroupName(groupName)).thenReturn(vXGroup);

        VXGroup result = xUserRest.getXGroupByGroupName(request, groupName);

        Assertions.assertEquals(vXGroup, result);
        Mockito.verify(xGroupService).getGroupByGroupName(groupName);
    }

    @Test
    public void test161getXUserByUserNameWithNullResult() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        String userName = "nonExistentUser";

        Mockito.when(xUserMgr.getXUserByUserName(userName)).thenReturn(null);

        VXUser result = xUserRest.getXUserByUserName(request, userName);

        Assertions.assertNull(result);
        Mockito.verify(xUserMgr).getXUserByUserName(userName);
    }

    @Test
    public void test162modifyUserActiveStatusWithEmptyMap() {
        HashMap<Long, Integer> statusMap = new HashMap<>();

        Mockito.doNothing().when(xUserMgr).modifyUserActiveStatus(statusMap);

        xUserRest.modifyUserActiveStatus(statusMap);

        Mockito.verify(xUserMgr).modifyUserActiveStatus(statusMap);
    }

    @Test
    public void test163modifyGroupsVisibilityWithEmptyMap() {
        HashMap<Long, Integer> groupVisibilityMap = new HashMap<>();

        Mockito.doNothing().when(xUserMgr).modifyGroupsVisibility(groupVisibilityMap);

        xUserRest.modifyGroupsVisibility(groupVisibilityMap);

        Mockito.verify(xUserMgr).modifyGroupsVisibility(groupVisibilityMap);
    }

    @Test
    public void test164modifyUserVisibilityWithEmptyMap() {
        HashMap<Long, Integer> visibilityMap = new HashMap<>();

        Mockito.doNothing().when(xUserMgr).modifyUserVisibility(visibilityMap);

        xUserRest.modifyUserVisibility(visibilityMap);

        Mockito.verify(xUserMgr).modifyUserVisibility(visibilityMap);
    }

    @AfterEach
    public void destroySession() {
        RangerSecurityContext context = new RangerSecurityContext();
        context.setUserSession(null);
        RangerContextHolder.setSecurityContext(context);
    }

    private HashMap<Long, Integer> creategroupVisibilityMap() {
        HashMap<Long, Integer> groupVisibilityMap = new HashMap<>();
        groupVisibilityMap.put(id, 1);
        return groupVisibilityMap;
    }

    private SearchCriteria createsearchCriteria() {
        SearchCriteria testsearchCriteria = new SearchCriteria();
        testsearchCriteria.setStartIndex(0);
        testsearchCriteria.setMaxRows(Integer.MAX_VALUE);
        testsearchCriteria.setSortBy("id");
        testsearchCriteria.setSortType("asc");
        testsearchCriteria.setGetCount(true);
        testsearchCriteria.setOwnerId(null);
        testsearchCriteria.setGetChildren(false);
        testsearchCriteria.setDistinct(false);

        return testsearchCriteria;
    }

    private VXGroupList createXGroupList() {
        VXGroupList   testVXGroupList = new VXGroupList();
        VXGroup       vXGroup1        = createVXGroup();
        List<VXGroup> vXGroups        = new ArrayList<>();
        vXGroups.add(vXGroup1);
        testVXGroupList.setVXGroups(vXGroups);
        testVXGroupList.setStartIndex(0);
        testVXGroupList.setTotalCount(1);

        return testVXGroupList;
    }

    private VXUser createVXUser() {
        VXUser             testVXUser = new VXUser();
        Collection<String> c          = new ArrayList<>();
        testVXUser.setId(id);
        testVXUser.setCreateDate(new Date());
        testVXUser.setUpdateDate(new Date());
        testVXUser.setOwner("Admin");
        testVXUser.setUpdatedBy("Admin");
        testVXUser.setName("User1");
        testVXUser.setFirstName("FnameUser1");
        testVXUser.setLastName("LnameUser1");
        testVXUser.setPassword("User1");
        testVXUser.setGroupIdList(null);
        testVXUser.setGroupNameList(null);
        testVXUser.setStatus(1);
        testVXUser.setIsVisible(1);
        testVXUser.setUserSource(0);
        c.add("ROLE_USER");
        testVXUser.setUserRoleList(c);

        return testVXUser;
    }

    private VXGroupUser createVXGroupUser() {
        VXGroupUser testVXGroupUser = new VXGroupUser();
        testVXGroupUser.setId(id);
        testVXGroupUser.setCreateDate(new Date());
        testVXGroupUser.setUpdateDate(new Date());
        testVXGroupUser.setOwner("Admin");
        testVXGroupUser.setUpdatedBy("Admin");
        testVXGroupUser.setName("finance");
        testVXGroupUser.setParentGroupId(id);
        testVXGroupUser.setUserId(id);
        return testVXGroupUser;
    }

    private VXGroup createVXGroup() {
        VXGroup testVXGroup = new VXGroup();
        testVXGroup.setName("testVXGroup");
        testVXGroup.setCreateDate(new Date());
        testVXGroup.setUpdateDate(new Date());
        testVXGroup.setUpdatedBy("Admin");
        testVXGroup.setOwner("Admin");
        testVXGroup.setId(id);
        testVXGroup.setGroupType(1);
        testVXGroup.setCredStoreId(1L);
        testVXGroup.setGroupSource(1);
        testVXGroup.setIsVisible(1);
        return testVXGroup;
    }

    private VXAuthSession createVXAuthSession() {
        VXAuthSession testVXAuthSession = new VXAuthSession();
        testVXAuthSession.setAuthProvider(1);
        testVXAuthSession.setAuthStatus(1);
        testVXAuthSession.setAuthTime(new Date());
        testVXAuthSession.setCityName("Mumbai");
        testVXAuthSession.setCountryName("India");
        testVXAuthSession.setCreateDate(new Date());
        testVXAuthSession.setDeviceType(1);
        testVXAuthSession.setEmailAddress("email@EXAMPLE.COM");
        testVXAuthSession.setFamilyScreenName("testfamilyScreenName");
        testVXAuthSession.setFirstName("testAuthSessionName");
        testVXAuthSession.setId(id);
        testVXAuthSession.setLoginId("Admin");
        testVXAuthSession.setOwner("Admin");
        testVXAuthSession.setPublicScreenName("Admin");
        testVXAuthSession.setUpdatedBy("Admin");
        testVXAuthSession.setUpdateDate(new Date());
        testVXAuthSession.setUserId(id);
        testVXAuthSession.setStateName("Maharashtra");
        return testVXAuthSession;
    }

    private VXUserPermission createVXUserPermission() {
        VXUserPermission testVXUserPermission = new VXUserPermission();

        testVXUserPermission.setCreateDate(new Date());
        testVXUserPermission.setId(id);
        testVXUserPermission.setIsAllowed(1);
        testVXUserPermission.setModuleId(id);
        testVXUserPermission.setModuleName("testModule");
        testVXUserPermission.setOwner("Admin");
        testVXUserPermission.setUpdateDate(new Date());
        testVXUserPermission.setUpdatedBy("Admin");
        testVXUserPermission.setUserId(id);
        testVXUserPermission.setUserName("testVXUser");

        return testVXUserPermission;
    }

    private VXGroupPermission createVXGroupPermission() {
        VXGroupPermission testVXGroupPermission = new VXGroupPermission();

        testVXGroupPermission.setCreateDate(new Date());
        testVXGroupPermission.setGroupId(id);
        testVXGroupPermission.setGroupName("testVXGroup");
        testVXGroupPermission.setId(id);
        testVXGroupPermission.setIsAllowed(1);
        testVXGroupPermission.setModuleId(id);
        testVXGroupPermission.setModuleName("testModule");
        testVXGroupPermission.setOwner("Admin");
        testVXGroupPermission.setUpdateDate(new Date());
        testVXGroupPermission.setUpdatedBy("Admin");

        return testVXGroupPermission;
    }

    private VXModuleDef createVXModuleDef() {
        VXModuleDef testVXModuleDef = new VXModuleDef();
        testVXModuleDef.setAddedById(id);
        testVXModuleDef.setCreateDate(new Date());
        testVXModuleDef.setCreateTime(new Date());

        VXGroupPermission       testVXGroupPermission = createVXGroupPermission();
        List<VXGroupPermission> groupPermList         = new ArrayList<>();
        groupPermList.add(testVXGroupPermission);
        testVXModuleDef.setGroupPermList(groupPermList);

        testVXModuleDef.setId(id);
        testVXModuleDef.setModule("testModule");
        testVXModuleDef.setOwner("Admin");
        testVXModuleDef.setUpdateDate(new Date());
        testVXModuleDef.setUpdatedBy("Admin");
        testVXModuleDef.setUpdatedById(id);
        testVXModuleDef.setUpdateTime(new Date());
        testVXModuleDef.setUrl("testUrrl");

        List<VXUserPermission> userPermList         = new ArrayList<>();
        VXUserPermission       testVXUserPermission = createVXUserPermission();
        userPermList.add(testVXUserPermission);
        testVXModuleDef.setUserPermList(userPermList);

        return testVXModuleDef;
    }

    private VXStringList createVXStringList() {
        VXStringList testVXStringList = new VXStringList();
        VXString     testVXString     = new VXString();
        testVXString.setValue("User1");
        List<VXString> testVXStrings = new ArrayList<>();

        testVXStrings.add(testVXString);

        testVXStringList.setVXStrings(testVXStrings);
        testVXStringList.setResultSize(1);
        testVXStringList.setPageSize(1);
        testVXStringList.setSortBy("Id");
        testVXStringList.setStartIndex(1);
        testVXStringList.setTotalCount(1);
        return testVXStringList;
    }

    private VXStringList createVXStringListGroup() {
        VXStringList testVXStringList = new VXStringList();
        VXString     testVXString     = new VXString();
        testVXString.setValue("testVXGroup");
        List<VXString> testVXStrings = new ArrayList<>();

        testVXStrings.add(testVXString);

        testVXStringList.setVXStrings(testVXStrings);
        testVXStringList.setResultSize(1);
        testVXStringList.setPageSize(1);
        testVXStringList.setSortBy("Id");
        testVXStringList.setStartIndex(1);
        testVXStringList.setTotalCount(1);
        return testVXStringList;
    }

    @Test
    public void testCreateXGroupUserFromMap() {
        VXGroupUserInfo info = new VXGroupUserInfo();
        info.setId(5L);
        Mockito.when(xUserMgr.createXGroupUserFromMap(info)).thenReturn(info);
        VXGroupUserInfo ret = xUserRest.createXGroupUserFromMap(info);
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(5L, ret.getId());
        Mockito.verify(xUserMgr).createXGroupUserFromMap(info);
    }

    @Test
    public void testGetXPermMap_Success() {
        VXPermMap map = new VXPermMap();
        map.setId(11L);
        map.setResourceId(99L);
        Mockito.when(xUserMgr.getXPermMap(11L)).thenReturn(map);
        Mockito.when(xResourceService.readResource(99L)).thenReturn(vxResource);
        VXPermMap ret = xUserRest.getXPermMap(11L);
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(11L, ret.getId());
        Mockito.verify(xUserMgr).getXPermMap(11L);
        Mockito.verify(xResourceService).readResource(99L);
    }

    @Test
    public void testGetXPermMap_InvalidResource() {
        VXPermMap map = new VXPermMap();
        map.setId(12L);
        map.setResourceId(101L);
        Mockito.when(xUserMgr.getXPermMap(12L)).thenReturn(map);
        Mockito.when(xResourceService.readResource(101L)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class)))
                .thenThrow(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> xUserRest.getXPermMap(12L));
        Mockito.verify(xUserMgr).getXPermMap(12L);
        Mockito.verify(xResourceService).readResource(101L);
    }

    @Test
    public void testCreateXPermMap_Success() {
        VXPermMap map = new VXPermMap();
        map.setResourceId(200L);
        Mockito.when(xResourceService.readResource(200L)).thenReturn(vxResource);
        Mockito.when(xUserMgr.createXPermMap(map)).thenReturn(map);
        VXPermMap ret = xUserRest.createXPermMap(map);
        Assertions.assertNotNull(ret);
        Mockito.verify(xUserMgr).createXPermMap(map);
    }

    @Test
    public void testCreateXPermMap_InvalidResource() {
        VXPermMap map = new VXPermMap();
        map.setResourceId(201L);
        Mockito.when(xResourceService.readResource(201L)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class)))
                .thenThrow(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> xUserRest.createXPermMap(map));
        Mockito.verify(xResourceService).readResource(201L);
    }

    @Test
    public void testUpdateXPermMap_Success() {
        VXPermMap map = new VXPermMap();
        map.setId(15L);
        map.setResourceId(300L);
        Mockito.when(xResourceService.readResource(300L)).thenReturn(vxResource);
        Mockito.when(xUserMgr.updateXPermMap(map)).thenReturn(map);
        VXPermMap ret = xUserRest.updateXPermMap(map);
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(15L, ret.getId());
        Mockito.verify(xUserMgr).updateXPermMap(map);
    }

    @Test
    public void testUpdateXPermMap_InvalidResource() {
        VXPermMap map = new VXPermMap();
        map.setResourceId(301L);
        Mockito.when(xResourceService.readResource(301L)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenReturn(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> xUserRest.updateXPermMap(map));
        Mockito.verify(xResourceService).readResource(301L);
    }

    @Test
    public void testDeleteXPermMap() {
        Mockito.doNothing().when(xUserMgr).deleteXPermMap(21L, false);
        xUserRest.deleteXPermMap(21L, request);
        Mockito.verify(xUserMgr).deleteXPermMap(21L, false);
    }

    @Test
    public void testSearchXPermMaps_And_Count() {
        SearchCriteria criteria = createsearchCriteria();
        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(criteria);
        VXPermMapList list = new VXPermMapList();
        Mockito.when(xUserMgr.searchXPermMaps(criteria)).thenReturn(list);
        VXPermMapList retList = xUserRest.searchXPermMaps(request);
        Assertions.assertNotNull(retList);
        VXLong cntObj = new VXLong();
        cntObj.setValue(3);
        Mockito.when(xUserMgr.getXPermMapSearchCount(criteria)).thenReturn(cntObj);
        VXLong cnt = xUserRest.countXPermMaps(request);
        Assertions.assertEquals(3, cnt.getValue());
    }

    @Test
    public void testGetXAuditMap_Success() {
        VXAuditMap map = new VXAuditMap();
        map.setId(31L);
        map.setResourceId(400L);
        Mockito.when(xUserMgr.getXAuditMap(31L)).thenReturn(map);
        Mockito.when(xResourceService.readResource(400L)).thenReturn(vxResource);
        VXAuditMap ret = xUserRest.getXAuditMap(31L);
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(31L, ret.getId());
        Mockito.verify(xUserMgr).getXAuditMap(31L);
        Mockito.verify(xResourceService).readResource(400L);
    }

    @Test
    public void testGetXAuditMap_InvalidResource() {
        VXAuditMap map = new VXAuditMap();
        map.setId(32L);
        map.setResourceId(401L);
        Mockito.when(xUserMgr.getXAuditMap(32L)).thenReturn(map);
        Mockito.when(xResourceService.readResource(401L)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class)))
                .thenThrow(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> xUserRest.getXAuditMap(32L));
        Mockito.verify(xUserMgr).getXAuditMap(32L);
        Mockito.verify(xResourceService).readResource(401L);
    }

    @Test
    public void testCreateXAuditMap_Success() {
        VXAuditMap map = new VXAuditMap();
        map.setResourceId(500L);
        Mockito.when(xResourceService.readResource(500L)).thenReturn(vxResource);
        Mockito.when(xUserMgr.createXAuditMap(map)).thenReturn(map);
        VXAuditMap ret = xUserRest.createXAuditMap(map);
        Assertions.assertNotNull(ret);
        Mockito.verify(xUserMgr).createXAuditMap(map);
    }

    @Test
    public void testCreateXAuditMap_InvalidResource() {
        VXAuditMap map = new VXAuditMap();
        map.setResourceId(501L);
        Mockito.when(xResourceService.readResource(501L)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class)))
                .thenThrow(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> xUserRest.createXAuditMap(map));
        Mockito.verify(xResourceService).readResource(501L);
    }

    @Test
    public void testUpdateXAuditMap_Success() {
        VXAuditMap map = new VXAuditMap();
        map.setId(35L);
        map.setResourceId(600L);
        Mockito.when(xResourceService.readResource(600L)).thenReturn(vxResource);
        Mockito.when(xUserMgr.updateXAuditMap(map)).thenReturn(map);
        VXAuditMap ret = xUserRest.updateXAuditMap(map);
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(35L, ret.getId());
        Mockito.verify(xUserMgr).updateXAuditMap(map);
    }

    @Test
    public void testUpdateXAuditMap_InvalidResource() {
        VXAuditMap map = new VXAuditMap();
        map.setResourceId(601L);
        Mockito.when(xResourceService.readResource(601L)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class)))
                .thenThrow(new WebApplicationException());
        Assertions.assertThrows(WebApplicationException.class, () -> xUserRest.updateXAuditMap(map));
        Mockito.verify(xResourceService).readResource(601L);
    }

    @Test
    public void testDeleteXAuditMap() {
        Mockito.doNothing().when(xUserMgr).deleteXAuditMap(41L, false);
        xUserRest.deleteXAuditMap(41L, request);
        Mockito.verify(xUserMgr).deleteXAuditMap(41L, false);
    }

    @Test
    public void testSearchXAuditMaps_And_Count() {
        SearchCriteria criteria = createsearchCriteria();
        Mockito.when(searchUtil.extractCommonCriterias(Mockito.any(), Mockito.any())).thenReturn(criteria);
        VXAuditMapList list = new VXAuditMapList();
        Mockito.when(xUserMgr.searchXAuditMaps(criteria)).thenReturn(list);
        VXAuditMapList retList = xUserRest.searchXAuditMaps(request);
        Assertions.assertNotNull(retList);
        VXLong cntObj2 = new VXLong();
        cntObj2.setValue(7);
        Mockito.when(xUserMgr.getXAuditMapSearchCount(criteria)).thenReturn(cntObj2);
        VXLong cnt = xUserRest.countXAuditMaps(request);
        Assertions.assertEquals(7, cnt.getValue());
    }
}
