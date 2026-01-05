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

import org.apache.ranger.biz.UserMgr;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConfigUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXPortalUserDao;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.util.RangerRestUtil;
import org.apache.ranger.view.VXPasswordChange;
import org.apache.ranger.view.VXPortalUser;
import org.apache.ranger.view.VXPortalUserList;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXStringList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.WebApplicationException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestUserREST {
    @InjectMocks
    UserREST userREST = new UserREST();
    @Mock
    HttpServletRequest request;
    @Mock
    SearchUtil searchUtil;
    @Mock
    RangerConfigUtil configUtil;
    @Mock
    UserMgr userManager;
    @Mock
    RangerDaoManager daoManager;
    @Mock
    XUserMgr xUserMgr;
    @Mock
    RESTErrorUtil restErrorUtil;
    @Mock
    VXPortalUserList vXPUserExpList;
    @Mock
    RangerRestUtil msRestUtil;
    @Mock
    VXPortalUser vxPUserAct;
    @Mock
    VXPasswordChange changePassword;
    @Mock
    VXResponse responseExp;
    @Mock
    StringUtil stringUtil;
    Long   userId    = 10L;
    int    pageSize  = 100;
    String firstName = "abc";
    String lastName  = "xyz";
    String loginId   = "xyzId";
    String emailId   = "abc@Example.com";

    String oldPassword = "ranger123$";
    String newPassword = "rangerAdmin1234$";

    @Test
    public void test1SearchUsers() {
        SearchCriteria searchCriteria = new SearchCriteria();
        vXPUserExpList = new VXPortalUserList();
        vXPUserExpList.setPageSize(pageSize);
        List<Integer> status           = new ArrayList<>();
        String        publicScreenName = "nrp";
        List<String>  roles            = new ArrayList<>();

        Mockito.when(searchUtil.extractCommonCriterias(Matchers.eq(request), Matchers.anyListOf(SortField.class))).thenReturn(searchCriteria);
        Mockito.when(searchUtil.extractLong(request, searchCriteria, "userId", "User Id")).thenReturn(userId);
        Mockito.when(searchUtil.extractString(request, searchCriteria, "loginId", "Login Id", null)).thenReturn(loginId);
        Mockito.when(searchUtil.extractString(request, searchCriteria, "emailAddress", "Email Address", null)).thenReturn(emailId);
        Mockito.when(searchUtil.extractString(request, searchCriteria, "firstName", "First Name", StringUtil.VALIDATION_NAME)).thenReturn(firstName);
        Mockito.when(searchUtil.extractString(request, searchCriteria, "lastName", "Last Name", StringUtil.VALIDATION_NAME)).thenReturn(lastName);
        Mockito.when(searchUtil.extractEnum(request, searchCriteria, "status", "Status", "statusList", RangerConstants.ActivationStatus_MAX)).thenReturn(status);
        Mockito.when(searchUtil.extractString(request, searchCriteria, "publicScreenName", "Public Screen Name", StringUtil.VALIDATION_NAME)).thenReturn(publicScreenName);
        Mockito.when(searchUtil.extractStringList(request, searchCriteria, "role", "Role", "roleList", configUtil.getRoles(), StringUtil.VALIDATION_NAME)).thenReturn(roles);
        Mockito.when(userManager.searchUsers(searchCriteria)).thenReturn(vXPUserExpList);

        VXPortalUserList vXPUserListAct = userREST.searchUsers(request);

        Assertions.assertNotNull(vXPUserListAct);
        Assertions.assertEquals(vXPUserExpList, vXPUserListAct);
        Assertions.assertEquals(vXPUserExpList.getPageSize(), vXPUserListAct.getPageSize());

        Mockito.verify(searchUtil).extractCommonCriterias(Matchers.eq(request), Matchers.anyListOf(SortField.class));
        Mockito.verify(searchUtil).extractLong(request, searchCriteria, "userId", "User Id");
        Mockito.verify(searchUtil).extractString(request, searchCriteria, "loginId", "Login Id", null);
        Mockito.verify(searchUtil).extractString(request, searchCriteria, "emailAddress", "Email Address", null);
        Mockito.verify(searchUtil).extractString(request, searchCriteria, "firstName", "First Name", StringUtil.VALIDATION_NAME);
        Mockito.verify(searchUtil).extractString(request, searchCriteria, "lastName", "Last Name", StringUtil.VALIDATION_NAME);
        Mockito.verify(searchUtil).extractEnum(request, searchCriteria, "status", "Status", "statusList", RangerConstants.ActivationStatus_MAX);
        Mockito.verify(searchUtil).extractString(request, searchCriteria, "publicScreenName", "Public Screen Name", StringUtil.VALIDATION_NAME);
        Mockito.verify(searchUtil).extractStringList(request, searchCriteria, "role", "Role", "roleList", configUtil.getRoles(), StringUtil.VALIDATION_NAME);
        Mockito.verify(userManager).searchUsers(searchCriteria);
    }

    @Test
    public void test2GetUserProfileForUser() {
        VXPortalUser vxPUserExp = createVXPortalUser();

        Mockito.when(userManager.getUserProfile(userId)).thenReturn(vxPUserExp);

        VXPortalUser vxPUserAct = userREST.getUserProfileForUser(userId);

        Assertions.assertNotNull(vxPUserAct);
        Assertions.assertEquals(vxPUserExp, vxPUserAct);
        Assertions.assertEquals(vxPUserExp.getLoginId(), vxPUserAct.getLoginId());
        Assertions.assertEquals(vxPUserExp.getFirstName(), vxPUserAct.getFirstName());
        Assertions.assertEquals(vxPUserExp.getEmailAddress(), vxPUserAct.getEmailAddress());
        Assertions.assertEquals(vxPUserExp.getId(), vxPUserAct.getId());

        Mockito.verify(userManager).getUserProfile(userId);
    }

    @Test
    public void test3GetUserProfileForUser() {
        VXPortalUser vxPUserExp = new VXPortalUser();
        vxPUserExp = null;

        Mockito.when(userManager.getUserProfile(userId)).thenReturn(vxPUserExp);

        VXPortalUser vxPUserAct = userREST.getUserProfileForUser(userId);

        Assertions.assertEquals(vxPUserExp, vxPUserAct);

        Mockito.verify(userManager).getUserProfile(userId);
    }

    @Test
    public void test6Create() {
        VXPortalUser vxPUserExp = createVXPortalUser();

        Mockito.when(userManager.createUser(vxPUserExp)).thenReturn(vxPUserExp);

        VXPortalUser vxPUserAct = userREST.create(vxPUserExp, request);

        Assertions.assertNotNull(vxPUserAct);
        Assertions.assertEquals(vxPUserExp.getLoginId(), vxPUserAct.getLoginId());
        Assertions.assertEquals(vxPUserExp.getFirstName(), vxPUserAct.getFirstName());
        Assertions.assertEquals(vxPUserExp.getLastName(), vxPUserAct.getLastName());
        Assertions.assertEquals(vxPUserExp.getEmailAddress(), vxPUserAct.getEmailAddress());

        Mockito.verify(userManager).createUser(vxPUserExp);
    }

    @Test
    public void test7CreateDefaultAccountUser() {
        VXPortalUser vxPUserExp = new VXPortalUser();
        vxPUserExp = null;
        Mockito.when(userManager.createDefaultAccountUser(vxPUserExp)).thenReturn(vxPUserExp);

        VXPortalUser vxPUserAct = userREST.createDefaultAccountUser(vxPUserExp, request);

        Assertions.assertNull(vxPUserAct);

        Mockito.verify(userManager).createDefaultAccountUser(vxPUserExp);
    }

    @Test
    public void test8CreateDefaultAccountUser() {
        VXPortalUser vxPUserExp = createVXPortalUser();

        Mockito.when(userManager.createDefaultAccountUser(vxPUserExp)).thenReturn(vxPUserExp);
        Mockito.doNothing().when(xUserMgr).assignPermissionToUser(vxPUserExp, true);

        VXPortalUser vxPUserAct = userREST.createDefaultAccountUser(vxPUserExp, request);

        Assertions.assertNotNull(vxPUserAct);
        Assertions.assertEquals(vxPUserExp, vxPUserAct);
        Assertions.assertEquals(vxPUserExp.getLoginId(), vxPUserAct.getLoginId());
        Assertions.assertEquals(vxPUserExp.getFirstName(), vxPUserAct.getFirstName());
        Assertions.assertEquals(vxPUserExp.getLastName(), vxPUserAct.getLastName());
        Assertions.assertEquals(vxPUserExp.getEmailAddress(), vxPUserAct.getEmailAddress());

        Mockito.verify(userManager).createDefaultAccountUser(vxPUserExp);
        Mockito.verify(xUserMgr).assignPermissionToUser(vxPUserExp, true);
    }

    @Test
    public void test8Update() {
        VXPortalUser vxPUserExp = createVXPortalUser();
        vxPUserExp.setLoginId(loginId);
        XXPortalUser xxPUserExp = new XXPortalUser();
        xxPUserExp.setLoginId(loginId);
        XXPortalUserDao xxPortalUserDao = Mockito.mock(XXPortalUserDao.class);

        Mockito.when(daoManager.getXXPortalUser()).thenReturn(xxPortalUserDao);
        Mockito.when(xxPortalUserDao.getById(Mockito.anyLong())).thenReturn(xxPUserExp);
        Mockito.doNothing().when(userManager).checkAccess(xxPUserExp);
        Mockito.doNothing().when(msRestUtil).validateVUserProfileForUpdate(xxPUserExp, vxPUserExp);
        Mockito.when(userManager.updateUser(vxPUserExp)).thenReturn(xxPUserExp);
        Mockito.when(userManager.mapXXPortalUserVXPortalUser(xxPUserExp)).thenReturn(vxPUserExp);

        VXPortalUser vxPUserAct = userREST.update(vxPUserExp, request);

        Assertions.assertNotNull(vxPUserAct);
        Assertions.assertEquals(xxPUserExp.getLoginId(), vxPUserAct.getLoginId());
        Assertions.assertEquals(vxPUserExp.getId(), vxPUserAct.getId());
        Assertions.assertEquals(vxPUserExp.getFirstName(), vxPUserAct.getFirstName());

        Mockito.verify(daoManager).getXXPortalUser();
        Mockito.verify(xxPortalUserDao).getById(Mockito.anyLong());
        Mockito.verify(userManager).checkAccess(xxPUserExp);
        Mockito.verify(msRestUtil).validateVUserProfileForUpdate(xxPUserExp, vxPUserExp);
        Mockito.verify(userManager).updateUser(vxPUserExp);
        Mockito.verify(userManager).mapXXPortalUserVXPortalUser(xxPUserExp);
    }

    @Test
    public void test9Update() {
        VXPortalUser vxPUserExp = new VXPortalUser();
        XXPortalUser xxPUserExp = new XXPortalUser();
        xxPUserExp = null;
        XXPortalUserDao xxPortalUserDao = Mockito.mock(XXPortalUserDao.class);

        Mockito.when(daoManager.getXXPortalUser()).thenReturn(xxPortalUserDao);
        Mockito.doNothing().when(userManager).checkAccess(xxPUserExp);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(), Mockito.nullable(Long.class), Mockito.nullable(String.class), Mockito.anyString())).thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            userREST.update(vxPUserExp, request);
        });
    }

    @Test
    public void test10SetUserRoles() {
        Long         userId      = 10L;
        VXResponse   responseExp = new VXResponse();
        VXStringList roleList    = new VXStringList();
        Mockito.doNothing().when(userManager).checkAccess(userId);
        Mockito.doNothing().when(userManager).setUserRoles(userId, roleList.getVXStrings());

        VXResponse responseAct = userREST.setUserRoles(userId, roleList);

        Assertions.assertNotNull(responseAct);
        Assertions.assertEquals(responseExp.getStatusCode(), responseAct.getStatusCode());

        Mockito.verify(userManager).checkAccess(userId);
        Mockito.verify(userManager).setUserRoles(userId, roleList.getVXStrings());
    }

    @Test
    public void test11DeactivateUser() {
        VXPortalUser vxPUserExp = createVXPortalUser();
        XXPortalUser xxPUserExp = new XXPortalUser();
        xxPUserExp.setLoginId(loginId);
        xxPUserExp.setStatus(1);
        vxPUserExp.setStatus(5);

        XXPortalUserDao xxPortalUserDao = Mockito.mock(XXPortalUserDao.class);

        Mockito.when(daoManager.getXXPortalUser()).thenReturn(xxPortalUserDao);
        Mockito.when(xxPortalUserDao.getById(userId)).thenReturn(xxPUserExp);
        Mockito.when(userManager.deactivateUser(xxPUserExp)).thenReturn(vxPUserExp);

        VXPortalUser vxPUserAct = userREST.deactivateUser(userId);
        Assertions.assertNotNull(vxPUserAct);
        Assertions.assertEquals(xxPUserExp.getLoginId(), vxPUserAct.getLoginId());
        Assertions.assertEquals(vxPUserExp.getStatus(), vxPUserAct.getStatus());
        Assertions.assertEquals(vxPUserExp.getId(), vxPUserAct.getId());
        Assertions.assertEquals(vxPUserExp.getFirstName(), vxPUserAct.getFirstName());

        Mockito.verify(daoManager).getXXPortalUser();
        Mockito.verify(xxPortalUserDao).getById(userId);
        Mockito.verify(userManager).deactivateUser(xxPUserExp);
    }

    @Test
    public void test12DeactivateUser() {
        XXPortalUser xxPUserExp = new XXPortalUser();
        xxPUserExp = null;
        XXPortalUserDao xxPortalUserDao = Mockito.mock(XXPortalUserDao.class);

        Mockito.when(daoManager.getXXPortalUser()).thenReturn(xxPortalUserDao);
        Mockito.when(xxPortalUserDao.getById(userId)).thenReturn(xxPUserExp);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(), Mockito.nullable(Long.class), Mockito.nullable(String.class), Mockito.anyString())).thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            userREST.deactivateUser(userId);
        });
    }

    @Test
    public void test13GetUserProfile() {
        HttpSession  hs         = Mockito.mock(HttpSession.class);
        VXPortalUser vxPUserExp = createVXPortalUser();
        Mockito.when(userManager.getUserProfileByLoginId()).thenReturn(vxPUserExp);
        Mockito.when(request.getSession()).thenReturn(hs);
        Mockito.when(hs.getId()).thenReturn("id");

        VXPortalUser vxPUserAct = userREST.getUserProfile(request);

        Assertions.assertNotNull(vxPUserAct);
        Assertions.assertEquals(vxPUserExp, vxPUserAct);
        Assertions.assertEquals(vxPUserExp.getId(), vxPUserAct.getId());
        Assertions.assertEquals(vxPUserExp.getFirstName(), vxPUserAct.getFirstName());

        Mockito.verify(userManager).getUserProfileByLoginId();
    }

    @Test
    public void test16ChangePassword() {
        VXPasswordChange vxPasswordChange = createPasswordChange();
        XXPortalUserDao xxPortalUserDao = Mockito.mock(XXPortalUserDao.class);

        Mockito.when(daoManager.getXXPortalUser()).thenReturn(xxPortalUserDao);
        Mockito.when(restErrorUtil.createRESTException("serverMsg.userRestUser", MessageEnums.DATA_NOT_FOUND, null, null, vxPasswordChange.getLoginId())).thenThrow(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            userREST.changePassword(userId, vxPasswordChange);
        });
    }

    @Test
    public void test17ChangePassword() {
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(), Mockito.nullable(Long.class), Mockito.nullable(String.class), Mockito.nullable(String.class))).thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            userREST.changePassword(userId, changePassword);
        });
    }

    @Test
    public void test18ChangeEmailAddress() {
        VXPasswordChange changeEmail = createPasswordChange();
        XXPortalUserDao xxPortalUserDao = Mockito.mock(XXPortalUserDao.class);

        Mockito.when(daoManager.getXXPortalUser()).thenReturn(xxPortalUserDao);
        Mockito.when(restErrorUtil.createRESTException("serverMsg.userRestUser", MessageEnums.DATA_NOT_FOUND, null, null, changeEmail.getLoginId())).thenThrow(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            userREST.changeEmailAddress(userId, changeEmail);
        });
    }

    @Test
    public void test19ChangeEmailAddress() {
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any(), Mockito.nullable(Long.class), Mockito.nullable(String.class), Mockito.nullable(String.class))).thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            userREST.changeEmailAddress(userId, changePassword);
        });
    }

    private VXPortalUser createVXPortalUser() {
        VXPortalUser vxPUserExp = new VXPortalUser();
        vxPUserExp.setId(userId);
        vxPUserExp.setFirstName(firstName);
        vxPUserExp.setLastName(lastName);
        vxPUserExp.setEmailAddress(emailId);
        vxPUserExp.setLoginId(loginId);
        return vxPUserExp;
    }

    private VXPasswordChange createPasswordChange() {
        VXPasswordChange vxPasswordChange = new VXPasswordChange();
        vxPasswordChange.setId(userId);
        vxPasswordChange.setOldPassword(oldPassword);
        vxPasswordChange.setUpdPassword(newPassword);
        vxPasswordChange.setEmailAddress(emailId);
        vxPasswordChange.setLoginId(loginId);
        return vxPasswordChange;
    }
}
