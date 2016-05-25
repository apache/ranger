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
package org.apache.ranger.biz;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.ws.rs.WebApplicationException;

import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXGroupPermissionDao;
import org.apache.ranger.db.XXModuleDefDao;
import org.apache.ranger.db.XXPortalUserDao;
import org.apache.ranger.db.XXPortalUserRoleDao;
import org.apache.ranger.db.XXUserPermissionDao;
import org.apache.ranger.entity.XXGroupPermission;
import org.apache.ranger.entity.XXModuleDef;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXPortalUserRole;
import org.apache.ranger.entity.XXUserPermission;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.service.XGroupPermissionService;
import org.apache.ranger.service.XUserPermissionService;
import org.apache.ranger.view.VXGroupPermission;
import org.apache.ranger.view.VXPasswordChange;
import org.apache.ranger.view.VXPortalUser;
import org.apache.ranger.view.VXPortalUserList;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXUserPermission;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestUserMgr {

	private static Long userId = 1L;

	@InjectMocks
	UserMgr userMgr = new UserMgr();

	@Mock
	VXPortalUser VXPortalUser;

	@Mock
	RangerDaoManager daoManager;

	@Mock
	RESTErrorUtil restErrorUtil;

	@Mock
	ContextUtil contextUtil;

	@Mock
	StringUtil stringUtil;

	@Mock
	SearchUtil searchUtil;

	@Mock
	RangerBizUtil msBizUtil;

	@Mock
	XUserPermissionService xUserPermissionService;

	@Mock
	XGroupPermissionService xGroupPermissionService;

	@Mock
	SessionMgr sessionMgr;

	@Mock
	XUserMgr xUserMgr;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	public void setup() {
		RangerSecurityContext context = new RangerSecurityContext();
		context.setUserSession(new UserSessionBase());
		RangerContextHolder.setSecurityContext(context);
		UserSessionBase currentUserSession = ContextUtil
				.getCurrentUserSession();
		currentUserSession.setUserAdmin(true);
	}

	private VXPortalUser userProfile() {
		VXPortalUser userProfile = new VXPortalUser();
		userProfile.setEmailAddress("test@test.com");
		userProfile.setFirstName("user12");
		userProfile.setLastName("test12");
		userProfile.setLoginId("134");
		userProfile.setPassword("usertest12323");
		userProfile.setUserSource(123);
		userProfile.setPublicScreenName("user");
		userProfile.setId(userId);
		return userProfile;
	}

	@Test
	public void test11CreateUser() {
		setup();
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXPortalUserRoleDao roleDao = Mockito.mock(XXPortalUserRoleDao.class);

		VXPortalUser userProfile = userProfile();

		Collection<String> userRoleList = new ArrayList<String>();
		userRoleList.add("ROLE_USER");

		XXPortalUser user = new XXPortalUser();
		user.setEmailAddress(userProfile.getEmailAddress());
		user.setFirstName(userProfile.getFirstName());
		user.setLastName(userProfile.getLastName());
		user.setLoginId(userProfile.getLoginId());
		user.setPassword(userProfile.getPassword());
		user.setUserSource(userProfile.getUserSource());
		user.setPublicScreenName(userProfile.getPublicScreenName());
		user.setId(userProfile.getId());

		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(user.getId());
		XXPortalUserRole.setUserRole("ROLE_USER");
		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);

		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.create((XXPortalUser) Mockito.anyObject()))
				.thenReturn(user);
		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(roleDao);
		Mockito.when(roleDao.findByUserId(userId)).thenReturn(list);

		XXPortalUser dbxxPortalUser = userMgr.createUser(userProfile, 1,
				userRoleList);
		Assert.assertNotNull(dbxxPortalUser);
		userId = dbxxPortalUser.getId();

		Assert.assertEquals(userId, dbxxPortalUser.getId());
		Assert.assertEquals(userProfile.getFirstName(),
				dbxxPortalUser.getFirstName());
		Assert.assertEquals(userProfile.getFirstName(),
				dbxxPortalUser.getFirstName());
		Assert.assertEquals(userProfile.getLastName(),
				dbxxPortalUser.getLastName());
		Assert.assertEquals(userProfile.getLoginId(),
				dbxxPortalUser.getLoginId());
		Assert.assertEquals(userProfile.getEmailAddress(),
				dbxxPortalUser.getEmailAddress());
		Assert.assertEquals(userProfile.getPassword(),
				dbxxPortalUser.getPassword());

		Mockito.verify(daoManager).getXXPortalUser();
		Mockito.verify(daoManager).getXXPortalUserRole();
	}

	@Test
	public void test12CreateUser() {
		setup();
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXPortalUserRoleDao roleDao = Mockito.mock(XXPortalUserRoleDao.class);

		VXPortalUser userProfile = userProfile();

		XXPortalUser user = new XXPortalUser();
		user.setEmailAddress(userProfile.getEmailAddress());
		user.setFirstName(userProfile.getFirstName());
		user.setLastName(userProfile.getLastName());
		user.setLoginId(userProfile.getLoginId());
		user.setPassword(userProfile.getPassword());
		user.setUserSource(userProfile.getUserSource());
		user.setPublicScreenName(userProfile.getPublicScreenName());
		user.setId(userProfile.getId());

		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(user.getId());
		XXPortalUserRole.setUserRole("ROLE_USER");
		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);

		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.create((XXPortalUser) Mockito.anyObject()))
				.thenReturn(user);
		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(roleDao);
		Mockito.when(roleDao.findByUserId(userId)).thenReturn(list);

		XXPortalUser dbxxPortalUser = userMgr.createUser(userProfile, 1);
		userId = dbxxPortalUser.getId();

		Assert.assertNotNull(dbxxPortalUser);
		Assert.assertEquals(userId, dbxxPortalUser.getId());
		Assert.assertEquals(userProfile.getFirstName(),
				dbxxPortalUser.getFirstName());
		Assert.assertEquals(userProfile.getFirstName(),
				dbxxPortalUser.getFirstName());
		Assert.assertEquals(userProfile.getLastName(),
				dbxxPortalUser.getLastName());
		Assert.assertEquals(userProfile.getLoginId(),
				dbxxPortalUser.getLoginId());
		Assert.assertEquals(userProfile.getEmailAddress(),
				dbxxPortalUser.getEmailAddress());
		Assert.assertEquals(userProfile.getPassword(),
				dbxxPortalUser.getPassword());

		Mockito.verify(daoManager).getXXPortalUser();
		Mockito.verify(daoManager).getXXPortalUserRole();
	}

	@Test
	public void test15ChangePassword() {
		setup();
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		VXPortalUser userProfile = userProfile();

		VXPasswordChange pwdChange = new VXPasswordChange();
		pwdChange.setId(userProfile.getId());
		pwdChange.setLoginId(userProfile.getLoginId());
		pwdChange.setOldPassword(userProfile.getPassword());
		pwdChange.setEmailAddress(userProfile.getEmailAddress());
		pwdChange.setUpdPassword(userProfile.getPassword());

		XXPortalUser user = new XXPortalUser();

		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.findByLoginId(Mockito.anyString())).thenReturn(
				user);
		Mockito.when(
				stringUtil.equals(Mockito.anyString(), Mockito.anyString()))
				.thenReturn(true);

		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.getById(Mockito.anyLong())).thenReturn(user);
		Mockito.when(
				stringUtil.validatePassword(Mockito.anyString(),
						new String[] { Mockito.anyString() })).thenReturn(true);

		VXResponse dbVXResponse = userMgr.changePassword(pwdChange);
		Assert.assertNotNull(dbVXResponse);
		Assert.assertEquals(userProfile.getStatus(),
				dbVXResponse.getStatusCode());

		Mockito.verify(stringUtil).equals(Mockito.anyString(),
				Mockito.anyString());
		Mockito.verify(stringUtil).validatePassword(Mockito.anyString(),
				new String[] { Mockito.anyString() });
	}

	@Test
	public void test16ChangeEmailAddress() {
		setup();
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXPortalUserRoleDao roleDao = Mockito.mock(XXPortalUserRoleDao.class);
		XXUserPermissionDao xUserPermissionDao = Mockito.mock(XXUserPermissionDao.class);
		XXGroupPermissionDao xGroupPermissionDao = Mockito.mock(XXGroupPermissionDao.class);
		XXModuleDefDao xModuleDefDao = Mockito.mock(XXModuleDefDao.class);
		XXModuleDef xModuleDef = Mockito.mock(XXModuleDef.class);
		VXPortalUser userProfile = userProfile();

		XXPortalUser user = new XXPortalUser();
		user.setEmailAddress(userProfile.getEmailAddress());
		user.setFirstName(userProfile.getFirstName());
		user.setLastName(userProfile.getLastName());
		user.setLoginId(userProfile.getLoginId());
		String encryptedPwd = userMgr.encrypt(userProfile.getLoginId(),userProfile.getPassword());
		user.setPassword(encryptedPwd);
		user.setUserSource(userProfile.getUserSource());
		user.setPublicScreenName(userProfile.getPublicScreenName());
		user.setId(userProfile.getId());

		VXPasswordChange changeEmail = new VXPasswordChange();
		changeEmail.setEmailAddress("testuser@test.com");
		changeEmail.setId(user.getId());
		changeEmail.setLoginId(user.getLoginId());
		changeEmail.setOldPassword(userProfile.getPassword());

		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(userId);
		XXPortalUserRole.setUserRole("ROLE_USER");
		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);

		List<XXUserPermission> xUserPermissionsList = new ArrayList<XXUserPermission>();
		XXUserPermission xUserPermissionObj = new XXUserPermission();
		xUserPermissionObj.setAddedByUserId(userId);
		xUserPermissionObj.setCreateTime(new Date());
		xUserPermissionObj.setId(userId);
		xUserPermissionObj.setIsAllowed(1);
		xUserPermissionObj.setModuleId(1L);
		xUserPermissionObj.setUpdatedByUserId(userId);
		xUserPermissionObj.setUpdateTime(new Date());
		xUserPermissionObj.setUserId(userId);
		xUserPermissionsList.add(xUserPermissionObj);

		List<XXGroupPermission> xGroupPermissionList = new ArrayList<XXGroupPermission>();
		XXGroupPermission xGroupPermissionObj = new XXGroupPermission();
		xGroupPermissionObj.setAddedByUserId(userId);
		xGroupPermissionObj.setCreateTime(new Date());
		xGroupPermissionObj.setId(userId);
		xGroupPermissionObj.setIsAllowed(1);
		xGroupPermissionObj.setModuleId(1L);
		xGroupPermissionObj.setUpdatedByUserId(userId);
		xGroupPermissionObj.setUpdateTime(new Date());
		xGroupPermissionObj.setGroupId(userId);
		xGroupPermissionList.add(xGroupPermissionObj);

		VXUserPermission userPermission = new VXUserPermission();
		userPermission.setId(1L);
		userPermission.setIsAllowed(1);
		userPermission.setModuleId(1L);
		userPermission.setUserId(userId);
		userPermission.setUserName("xyz");
		userPermission.setOwner("admin");

		VXGroupPermission groupPermission = new VXGroupPermission();
		groupPermission.setId(1L);
		groupPermission.setIsAllowed(1);
		groupPermission.setModuleId(1L);
		groupPermission.setGroupId(userId);
		groupPermission.setGroupName("xyz");
		groupPermission.setOwner("admin");

		Mockito.when(stringUtil.validateEmail(Mockito.anyString())).thenReturn(true);
		Mockito.when(stringUtil.equals(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
		Mockito.when(stringUtil.normalizeEmail(Mockito.anyString())).thenReturn(changeEmail.getEmailAddress());
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(roleDao);
		Mockito.when(userDao.update(user)).thenReturn(user);
		Mockito.when(roleDao.findByParentId(Mockito.anyLong())).thenReturn(list);
		Mockito.when(daoManager.getXXUserPermission()).thenReturn(xUserPermissionDao);
		Mockito.when(daoManager.getXXGroupPermission()).thenReturn(xGroupPermissionDao);
		Mockito.when(xUserPermissionDao.findByUserPermissionIdAndIsAllowed(userProfile.getId())).thenReturn(xUserPermissionsList);
		Mockito.when(xGroupPermissionDao.findbyVXPortalUserId(userProfile.getId())).thenReturn(xGroupPermissionList);
		Mockito.when(xGroupPermissionService.populateViewBean(xGroupPermissionObj)).thenReturn(groupPermission);
		Mockito.when(xUserPermissionService.populateViewBean(xUserPermissionObj)).thenReturn(userPermission);
		Mockito.when(daoManager.getXXModuleDef()).thenReturn(xModuleDefDao);
		Mockito.when(xModuleDefDao.findByModuleId(Mockito.anyLong())).thenReturn(xModuleDef);

		VXPortalUser dbVXPortalUser = userMgr.changeEmailAddress(user,
				changeEmail);
		Assert.assertNotNull(dbVXPortalUser);
		Assert.assertEquals(userId, dbVXPortalUser.getId());
		Assert.assertEquals(userProfile.getLastName(),
				dbVXPortalUser.getLastName());
		Assert.assertEquals(changeEmail.getLoginId(),
				dbVXPortalUser.getLoginId());
		Assert.assertEquals(changeEmail.getEmailAddress(),
				dbVXPortalUser.getEmailAddress());
	}

	@Test
	public void test21CreateUser() {
		setup();
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXPortalUserRoleDao roleDao = Mockito.mock(XXPortalUserRoleDao.class);
		XXUserPermissionDao xUserPermissionDao = Mockito
				.mock(XXUserPermissionDao.class);
		XXGroupPermissionDao xGroupPermissionDao = Mockito
				.mock(XXGroupPermissionDao.class);

		XXPortalUser user = new XXPortalUser();
		VXPortalUser userProfile = userProfile();

		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(userId);
		XXPortalUserRole.setUserRole("ROLE_USER");
		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);

		List<XXUserPermission> xUserPermissionsList = new ArrayList<XXUserPermission>();
		XXUserPermission xUserPermissionObj = new XXUserPermission();
		xUserPermissionObj.setAddedByUserId(userId);
		xUserPermissionObj.setCreateTime(new Date());
		xUserPermissionObj.setId(userId);
		xUserPermissionObj.setIsAllowed(1);
		xUserPermissionObj.setModuleId(1L);
		xUserPermissionObj.setUpdatedByUserId(userId);
		xUserPermissionObj.setUpdateTime(new Date());
		xUserPermissionObj.setUserId(userId);
		xUserPermissionsList.add(xUserPermissionObj);

		List<XXGroupPermission> xGroupPermissionList = new ArrayList<XXGroupPermission>();
		XXGroupPermission xGroupPermissionObj = new XXGroupPermission();
		xGroupPermissionObj.setAddedByUserId(userId);
		xGroupPermissionObj.setCreateTime(new Date());
		xGroupPermissionObj.setId(userId);
		xGroupPermissionObj.setIsAllowed(1);
		xGroupPermissionObj.setModuleId(1L);
		xGroupPermissionObj.setUpdatedByUserId(userId);
		xGroupPermissionObj.setUpdateTime(new Date());
		xGroupPermissionObj.setGroupId(userId);
		xGroupPermissionList.add(xGroupPermissionObj);

		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.create((XXPortalUser) Mockito.anyObject()))
				.thenReturn(user);
		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(roleDao);
		Mockito.when(roleDao.findByUserId(Mockito.anyLong())).thenReturn(list);

		Mockito.when(daoManager.getXXUserPermission()).thenReturn(
				xUserPermissionDao);
		Mockito.when(
				xUserPermissionDao
						.findByUserPermissionIdAndIsAllowed(userProfile.getId()))
				.thenReturn(xUserPermissionsList);

		Mockito.when(daoManager.getXXGroupPermission()).thenReturn(
				xGroupPermissionDao);
		Mockito.when(
				xGroupPermissionDao.findbyVXPortalUserId(userProfile.getId()))
				.thenReturn(xGroupPermissionList);
		Mockito.when(stringUtil.validateEmail(Mockito.anyString())).thenReturn(true);
		VXPortalUser dbVXPortalUser = userMgr.createUser(userProfile);
		Assert.assertNotNull(dbVXPortalUser);
		Assert.assertEquals(user.getId(), dbVXPortalUser.getId());
		Assert.assertEquals(user.getFirstName(), dbVXPortalUser.getFirstName());
		Assert.assertEquals(user.getFirstName(), dbVXPortalUser.getFirstName());
		Assert.assertEquals(user.getLastName(), dbVXPortalUser.getLastName());
		Assert.assertEquals(user.getLoginId(), dbVXPortalUser.getLoginId());
		Assert.assertEquals(user.getEmailAddress(),
				dbVXPortalUser.getEmailAddress());
		Assert.assertEquals(user.getPassword(), dbVXPortalUser.getPassword());

		Mockito.verify(daoManager).getXXPortalUser();
		Mockito.verify(daoManager).getXXUserPermission();
		Mockito.verify(daoManager).getXXGroupPermission();
	}

	@Test
	public void test22CreateDefaultAccountUser() {
		setup();
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXPortalUserRoleDao roleDao = Mockito.mock(XXPortalUserRoleDao.class);
		VXPortalUser userProfile = userProfile();
		XXPortalUser user = new XXPortalUser();

		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(userId);
		XXPortalUserRole.setUserRole("ROLE_USER");

		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);

		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.findByLoginId(Mockito.anyString())).thenReturn(
				user);
		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(roleDao);
		Mockito.when(roleDao.findByParentId(Mockito.anyLong()))
				.thenReturn(list);
		Mockito.when(stringUtil.validateEmail(Mockito.anyString())).thenReturn(true);
		VXPortalUser dbVXPortalUser = userMgr
				.createDefaultAccountUser(userProfile);
		Assert.assertNotNull(dbVXPortalUser);
		Assert.assertEquals(user.getId(), dbVXPortalUser.getId());
		Assert.assertEquals(user.getFirstName(), dbVXPortalUser.getFirstName());
		Assert.assertEquals(user.getFirstName(), dbVXPortalUser.getFirstName());
		Assert.assertEquals(user.getLastName(), dbVXPortalUser.getLastName());
		Assert.assertEquals(user.getLoginId(), dbVXPortalUser.getLoginId());
		Assert.assertEquals(user.getEmailAddress(),
				dbVXPortalUser.getEmailAddress());
		Assert.assertEquals(user.getPassword(), dbVXPortalUser.getPassword());

		Mockito.verify(daoManager).getXXPortalUser();
		Mockito.verify(daoManager).getXXPortalUserRole();
	}

	@Test
	public void test23IsUserInRole() {
		XXPortalUserRoleDao roleDao = Mockito.mock(XXPortalUserRoleDao.class);

		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(userId);
		XXPortalUserRole.setUserRole("ROLE_USER");

		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);

		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(roleDao);
		Mockito.when(roleDao.findByRoleUserId(userId, "ROLE_USER")).thenReturn(
				XXPortalUserRole);

		boolean isValue = userMgr.isUserInRole(userId, "ROLE_USER");
		Assert.assertTrue(isValue);

		Mockito.verify(daoManager).getXXPortalUserRole();
	}

	@Test
	public void test24UpdateUserWithPass() {
		setup();
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);

		VXPortalUser userProfile = userProfile();
		XXPortalUser user = new XXPortalUser();

		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.getById(userProfile.getId())).thenReturn(user);

		Mockito.when(
				restErrorUtil.createRESTException(
						"Please provide valid email address.",
						MessageEnums.INVALID_INPUT_DATA)).thenThrow(
				new WebApplicationException());
		thrown.expect(WebApplicationException.class);

		XXPortalUser dbXXPortalUser = userMgr.updateUserWithPass(userProfile);
		Assert.assertNotNull(dbXXPortalUser);
		Assert.assertEquals(userId, dbXXPortalUser.getId());
		Assert.assertEquals(userProfile.getFirstName(),
				dbXXPortalUser.getFirstName());
		Assert.assertEquals(userProfile.getFirstName(),
				dbXXPortalUser.getFirstName());
		Assert.assertEquals(userProfile.getLastName(),
				dbXXPortalUser.getLastName());
		Assert.assertEquals(userProfile.getLoginId(),
				dbXXPortalUser.getLoginId());
		Assert.assertEquals(userProfile.getEmailAddress(),
				dbXXPortalUser.getEmailAddress());
		Assert.assertEquals(userProfile.getPassword(),
				dbXXPortalUser.getPassword());

		Mockito.verify(restErrorUtil).createRESTException(
				"Please provide valid email address.",
				MessageEnums.INVALID_INPUT_DATA);
	}

	@Test
	public void test25searchUsers() {
		Query query = Mockito.mock(Query.class);
		EntityManager entityManager = Mockito.mock(EntityManager.class);
		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.setDistinct(true);
		searchCriteria.setGetChildren(true);
		searchCriteria.setGetCount(true);
		searchCriteria.setMaxRows(12);
		searchCriteria.setOwnerId(userId);
		searchCriteria.setStartIndex(1);
		searchCriteria.setSortBy("asc");
		Long count = 1l;
		Mockito.when(daoManager.getEntityManager()).thenReturn(entityManager);
		Mockito.when(entityManager.createQuery(Mockito.anyString()))
				.thenReturn(query);
		Mockito.when(query.getSingleResult()).thenReturn(count);

		VXPortalUserList dbVXPortalUserList = userMgr
				.searchUsers(searchCriteria);

		Assert.assertNotNull(dbVXPortalUserList);
		Mockito.verify(query).getSingleResult();
	}

	@Test
	public void test26FindByEmailAddress() {
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);

		XXPortalUser user = new XXPortalUser();

		String emailId = "jeet786sonkar@gmail.com";
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.findByEmailAddress(emailId)).thenReturn(user);

		XXPortalUser dbXXPortalUser = userMgr.findByEmailAddress(emailId);
		Assert.assertNotNull(dbXXPortalUser);
		Assert.assertNotEquals(emailId, dbXXPortalUser.getEmailAddress());

		Mockito.verify(daoManager).getXXPortalUser();
	}

	@Test
	public void test27GetRolesForUser() {
		XXPortalUserRoleDao roleDao = Mockito.mock(XXPortalUserRoleDao.class);
		VXPortalUser userProfile = userProfile();

		XXPortalUser user = new XXPortalUser();
		user.setEmailAddress(userProfile.getEmailAddress());
		user.setFirstName(userProfile.getFirstName());
		user.setLastName(userProfile.getLastName());
		user.setLoginId(userProfile.getLoginId());
		user.setPassword(userProfile.getPassword());
		user.setUserSource(userProfile.getUserSource());
		user.setPublicScreenName(userProfile.getPublicScreenName());
		user.setId(userProfile.getId());

		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(user.getId());
		XXPortalUserRole.setUserRole("ROLE_USER");
		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);

		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(roleDao);
		Mockito.when(roleDao.findByUserId(userId)).thenReturn(list);

		Collection<String> stringReturn = userMgr.getRolesForUser(user);
		Assert.assertNotNull(stringReturn);

		Mockito.verify(daoManager).getXXPortalUserRole();
	}

	@Test
	public void test28DeleteUserRole() {
		setup();
		XXPortalUserRoleDao roleDao = Mockito.mock(XXPortalUserRoleDao.class);

		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		String userRole = "ROLE_USER";
		XXPortalUser user = new XXPortalUser();
		XXPortalUserRole.setId(user.getId());
		XXPortalUserRole.setUserRole("ROLE_USER");
		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);

		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(roleDao);
		Mockito.when(roleDao.findByUserId(userId)).thenReturn(list);

		boolean deleteValue = userMgr.deleteUserRole(userId, userRole);
		Assert.assertTrue(deleteValue);
	}

	@Test
	public void test29DeactivateUser() {
		setup();
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXPortalUserRoleDao roleDao = Mockito.mock(XXPortalUserRoleDao.class);
		XXUserPermissionDao xUserPermissionDao = Mockito
				.mock(XXUserPermissionDao.class);
		XXGroupPermissionDao xGroupPermissionDao = Mockito
				.mock(XXGroupPermissionDao.class);
		VXGroupPermission vXGroupPermission = Mockito
				.mock(VXGroupPermission.class);
		XXModuleDefDao xModuleDefDao = Mockito.mock(XXModuleDefDao.class);
		XXModuleDef xModuleDef = Mockito.mock(XXModuleDef.class);
		VXUserPermission vXUserPermission = Mockito
				.mock(VXUserPermission.class);

		VXPortalUser userProfile = userProfile();

		XXPortalUser user = new XXPortalUser();
		user.setEmailAddress(userProfile.getEmailAddress());
		user.setFirstName(userProfile.getFirstName());
		user.setLastName(userProfile.getLastName());
		user.setLoginId(userProfile.getLoginId());
		user.setPassword(userProfile.getPassword());
		user.setUserSource(userProfile.getUserSource());
		user.setPublicScreenName(userProfile.getPublicScreenName());
		user.setId(userProfile.getId());

		List<XXUserPermission> xUserPermissionsList = new ArrayList<XXUserPermission>();
		XXUserPermission xUserPermissionObj = new XXUserPermission();
		xUserPermissionObj.setAddedByUserId(userId);
		xUserPermissionObj.setCreateTime(new Date());
		xUserPermissionObj.setId(userId);
		xUserPermissionObj.setIsAllowed(1);
		xUserPermissionObj.setModuleId(1L);
		xUserPermissionObj.setUpdatedByUserId(userId);
		xUserPermissionObj.setUpdateTime(new Date());
		xUserPermissionObj.setUserId(userId);
		xUserPermissionsList.add(xUserPermissionObj);

		List<XXGroupPermission> xGroupPermissionList = new ArrayList<XXGroupPermission>();
		XXGroupPermission xGroupPermissionObj = new XXGroupPermission();
		xGroupPermissionObj.setAddedByUserId(userId);
		xGroupPermissionObj.setCreateTime(new Date());
		xGroupPermissionObj.setId(userId);
		xGroupPermissionObj.setIsAllowed(1);
		xGroupPermissionObj.setModuleId(1L);
		xGroupPermissionObj.setUpdatedByUserId(userId);
		xGroupPermissionObj.setUpdateTime(new Date());
		xGroupPermissionObj.setGroupId(userId);
		xGroupPermissionList.add(xGroupPermissionObj);

		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(userId);
		XXPortalUserRole.setUserRole("ROLE_USER");

		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.update(user)).thenReturn(user);

		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(roleDao);
		Mockito.when(roleDao.findByParentId(Mockito.anyLong()))
				.thenReturn(list);

		Mockito.when(daoManager.getXXUserPermission()).thenReturn(
				xUserPermissionDao);
		Mockito.when(
				xUserPermissionDao
						.findByUserPermissionIdAndIsAllowed(userProfile.getId()))
				.thenReturn(xUserPermissionsList);

		Mockito.when(daoManager.getXXGroupPermission()).thenReturn(
				xGroupPermissionDao);
		Mockito.when(
				xGroupPermissionDao.findbyVXPortalUserId(userProfile.getId()))
				.thenReturn(xGroupPermissionList);

		Mockito.when(
				xGroupPermissionService.populateViewBean(xGroupPermissionObj))
				.thenReturn(vXGroupPermission);

		Mockito.when(daoManager.getXXModuleDef()).thenReturn(xModuleDefDao);
		Mockito.when(xModuleDefDao.findByModuleId(Mockito.anyLong()))
				.thenReturn(xModuleDef);

		Mockito.when(
				xUserPermissionService.populateViewBean(xUserPermissionObj))
				.thenReturn(vXUserPermission);

		VXPortalUser dbVXPortalUser = userMgr.deactivateUser(user);
		Assert.assertNotNull(dbVXPortalUser);
		Assert.assertEquals(user.getId(), dbVXPortalUser.getId());
		Assert.assertEquals(user.getFirstName(), dbVXPortalUser.getFirstName());
		Assert.assertEquals(user.getFirstName(), dbVXPortalUser.getFirstName());
		Assert.assertEquals(user.getLastName(), dbVXPortalUser.getLastName());
		Assert.assertEquals(user.getLoginId(), dbVXPortalUser.getLoginId());

		Mockito.verify(daoManager).getXXPortalUser();
		Mockito.verify(daoManager).getXXUserPermission();
		Mockito.verify(daoManager).getXXGroupPermission();
		Mockito.verify(xUserPermissionService).populateViewBean(
				xUserPermissionObj);
		Mockito.verify(xGroupPermissionService).populateViewBean(
				xGroupPermissionObj);
	}

	@Test
	public void test30checkAccess() {
		setup();
		XXPortalUserDao xPortalUserDao = Mockito.mock(XXPortalUserDao.class);
		XXPortalUser xPortalUser = Mockito.mock(XXPortalUser.class);
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(xPortalUserDao);
		Mockito.when(xPortalUserDao.getById(userId)).thenReturn(xPortalUser);

		userMgr.checkAccess(userId);
		Mockito.verify(daoManager).getXXPortalUser();
	}

	@Test
	public void test31getUserProfile() {
		setup();
		XXPortalUserDao xPortalUserDao = Mockito.mock(XXPortalUserDao.class);
		XXPortalUser xPortalUser = Mockito.mock(XXPortalUser.class);
		XXUserPermissionDao xUserPermissionDao = Mockito
				.mock(XXUserPermissionDao.class);
		XXGroupPermissionDao xGroupPermissionDao = Mockito
				.mock(XXGroupPermissionDao.class);

		XXPortalUserRoleDao xPortalUserRoleDao = Mockito
				.mock(XXPortalUserRoleDao.class);

		List<XXPortalUserRole> xPortalUserRoleList = new ArrayList<XXPortalUserRole>();
		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(userId);
		XXPortalUserRole.setUserRole("ROLE_USER");
		xPortalUserRoleList.add(XXPortalUserRole);

		List<XXUserPermission> xUserPermissionsList = new ArrayList<XXUserPermission>();
		XXUserPermission xUserPermissionObj = new XXUserPermission();
		xUserPermissionObj.setAddedByUserId(userId);
		xUserPermissionObj.setCreateTime(new Date());
		xUserPermissionObj.setId(userId);
		xUserPermissionObj.setIsAllowed(1);
		xUserPermissionObj.setModuleId(1L);
		xUserPermissionObj.setUpdatedByUserId(userId);
		xUserPermissionObj.setUpdateTime(new Date());
		xUserPermissionObj.setUserId(userId);
		xUserPermissionsList.add(xUserPermissionObj);

		List<XXGroupPermission> xGroupPermissionList = new ArrayList<XXGroupPermission>();
		XXGroupPermission xGroupPermissionObj = new XXGroupPermission();
		xGroupPermissionObj.setAddedByUserId(userId);
		xGroupPermissionObj.setCreateTime(new Date());
		xGroupPermissionObj.setId(userId);
		xGroupPermissionObj.setIsAllowed(1);
		xGroupPermissionObj.setModuleId(1L);
		xGroupPermissionObj.setUpdatedByUserId(userId);
		xGroupPermissionObj.setUpdateTime(new Date());
		xGroupPermissionObj.setGroupId(userId);
		xGroupPermissionList.add(xGroupPermissionObj);
		VXPortalUser userProfile = userProfile();

		Mockito.when(daoManager.getXXPortalUser()).thenReturn(xPortalUserDao);
		Mockito.when(xPortalUserDao.getById(userId)).thenReturn(xPortalUser);
		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(
				xPortalUserRoleDao);
		Mockito.when(xPortalUserRoleDao.findByParentId(userId)).thenReturn(
				xPortalUserRoleList);
		Mockito.when(daoManager.getXXUserPermission()).thenReturn(
				xUserPermissionDao);
		Mockito.when(
				xUserPermissionDao
						.findByUserPermissionIdAndIsAllowed(userProfile.getId()))
				.thenReturn(xUserPermissionsList);

		Mockito.when(daoManager.getXXGroupPermission()).thenReturn(
				xGroupPermissionDao);
		Mockito.when(
				xGroupPermissionDao.findbyVXPortalUserId(userProfile.getId()))
				.thenReturn(xGroupPermissionList);
		VXPortalUser dbVXPortalUser = userMgr.getUserProfile(userId);
		Assert.assertNotNull(dbVXPortalUser);

		Mockito.verify(daoManager).getXXPortalUser();
		Mockito.verify(daoManager).getXXUserPermission();
		Mockito.verify(daoManager).getXXUserPermission();
		Mockito.verify(daoManager).getXXGroupPermission();
	}

	@Test
	public void test32getUserProfileByLoginId() {
		setup();
		XXPortalUserDao xPortalUserDao = Mockito.mock(XXPortalUserDao.class);
		XXPortalUser xPortalUser = Mockito.mock(XXPortalUser.class);
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(xPortalUserDao);
		Mockito.when(xPortalUserDao.findByLoginId("1L"))
				.thenReturn(xPortalUser);

		VXPortalUser dbVXPortalUser = userMgr.getUserProfileByLoginId();
		Assert.assertNull(dbVXPortalUser);

		Mockito.verify(daoManager).getXXPortalUser();
	}

	@Test
	public void test33setUserRoles() {
		setup();
		XXPortalUserRoleDao xPortalUserRoleDao = Mockito
				.mock(XXPortalUserRoleDao.class);
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXUserPermissionDao xUserPermissionDao = Mockito
				.mock(XXUserPermissionDao.class);
		XXGroupPermissionDao xGroupPermissionDao = Mockito
				.mock(XXGroupPermissionDao.class);
		XXModuleDefDao xModuleDefDao = Mockito.mock(XXModuleDefDao.class);

		VXPortalUser userProfile = userProfile();
		XXPortalUser user = new XXPortalUser();
		user.setEmailAddress(userProfile.getEmailAddress());
		user.setFirstName(userProfile.getFirstName());
		user.setLastName(userProfile.getLastName());
		user.setLoginId(userProfile.getLoginId());
		user.setPassword(userProfile.getPassword());
		user.setUserSource(userProfile.getUserSource());
		user.setPublicScreenName(userProfile.getPublicScreenName());
		user.setId(userProfile.getId());

		List<VXString> vStringRolesList = new ArrayList<VXString>();
		VXString vXStringObj = new VXString();
		vXStringObj.setValue("ROLE_USER");
		vStringRolesList.add(vXStringObj);

		List<XXPortalUserRole> xPortalUserRoleList = new ArrayList<XXPortalUserRole>();
		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(userId);
		XXPortalUserRole.setUserId(userId);
		XXPortalUserRole.setUserRole("ROLE_USER");
		xPortalUserRoleList.add(XXPortalUserRole);

		List<XXUserPermission> xUserPermissionsList = new ArrayList<XXUserPermission>();
		XXUserPermission xUserPermissionObj = new XXUserPermission();
		xUserPermissionObj.setAddedByUserId(userId);
		xUserPermissionObj.setCreateTime(new Date());
		xUserPermissionObj.setId(userId);
		xUserPermissionObj.setIsAllowed(1);
		xUserPermissionObj.setModuleId(1L);
		xUserPermissionObj.setUpdatedByUserId(userId);
		xUserPermissionObj.setUpdateTime(new Date());
		xUserPermissionObj.setUserId(userId);
		xUserPermissionsList.add(xUserPermissionObj);

		List<XXGroupPermission> xGroupPermissionList = new ArrayList<XXGroupPermission>();
		XXGroupPermission xGroupPermissionObj = new XXGroupPermission();
		xGroupPermissionObj.setAddedByUserId(userId);
		xGroupPermissionObj.setCreateTime(new Date());
		xGroupPermissionObj.setId(userId);
		xGroupPermissionObj.setIsAllowed(1);
		xGroupPermissionObj.setModuleId(1L);
		xGroupPermissionObj.setUpdatedByUserId(userId);
		xGroupPermissionObj.setUpdateTime(new Date());
		xGroupPermissionObj.setGroupId(userId);
		xGroupPermissionList.add(xGroupPermissionObj);

		List<VXGroupPermission> groupPermList = new ArrayList<VXGroupPermission>();
		VXGroupPermission groupPermission = new VXGroupPermission();
		groupPermission.setId(1L);
		groupPermission.setIsAllowed(1);
		groupPermission.setModuleId(1L);
		groupPermission.setGroupId(userId);
		groupPermission.setGroupName("xyz");
		groupPermission.setOwner("admin");
		groupPermList.add(groupPermission);

		XXModuleDef xModuleDef = new XXModuleDef();
		xModuleDef.setUpdatedByUserId(userId);
		xModuleDef.setAddedByUserId(userId);
		xModuleDef.setCreateTime(new Date());
		xModuleDef.setId(userId);
		xModuleDef.setModule("Policy manager");
		xModuleDef.setUpdateTime(new Date());
		xModuleDef.setUrl("/policy manager");

		VXUserPermission userPermission = new VXUserPermission();
		userPermission.setId(1L);
		userPermission.setIsAllowed(1);
		userPermission.setModuleId(1L);
		userPermission.setUserId(userId);
		userPermission.setUserName("xyz");
		userPermission.setOwner("admin");

		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(
				xPortalUserRoleDao);
		Mockito.when(xPortalUserRoleDao.findByUserId(userId)).thenReturn(
				xPortalUserRoleList);
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.getById(userId)).thenReturn(user);
		Mockito.when(daoManager.getXXUserPermission()).thenReturn(
				xUserPermissionDao);
		Mockito.when(
				xUserPermissionDao
						.findByUserPermissionIdAndIsAllowed(userProfile.getId()))
				.thenReturn(xUserPermissionsList);
		Mockito.when(daoManager.getXXGroupPermission()).thenReturn(
				xGroupPermissionDao);
		Mockito.when(
				xGroupPermissionDao.findbyVXPortalUserId(userProfile.getId()))
				.thenReturn(xGroupPermissionList);
		Mockito.when(
				xGroupPermissionService.populateViewBean(xGroupPermissionObj))
				.thenReturn(groupPermission);
		Mockito.when(daoManager.getXXModuleDef()).thenReturn(xModuleDefDao);
		Mockito.when(xModuleDefDao.findByModuleId(Mockito.anyLong()))
				.thenReturn(xModuleDef);
		Mockito.when(
				xUserPermissionService.populateViewBean(xUserPermissionObj))
				.thenReturn(userPermission);
		Mockito.when(daoManager.getXXModuleDef()).thenReturn(xModuleDefDao);
		Mockito.when(xModuleDefDao.findByModuleId(Mockito.anyLong()))
				.thenReturn(xModuleDef);

		userMgr.checkAccess(userId);
		userMgr.setUserRoles(userId, vStringRolesList);

		Mockito.verify(daoManager).getXXUserPermission();
		Mockito.verify(daoManager).getXXGroupPermission();
		Mockito.verify(xGroupPermissionService).populateViewBean(
				xGroupPermissionObj);
		Mockito.verify(xUserPermissionService).populateViewBean(
				xUserPermissionObj);
	}

	@Test
	public void test19updateRoles() {
		//setup();
		Collection<String> rolesList = new ArrayList<String>();
		rolesList.add("ROLE_USER");
		rolesList.add("ROLE_ADMIN");
		XXPortalUserRole XXPortalUserRole = new XXPortalUserRole();
		XXPortalUserRole.setId(userId);
		XXPortalUserRole.setUserRole("ROLE_USER");
		List<XXPortalUserRole> list = new ArrayList<XXPortalUserRole>();
		list.add(XXPortalUserRole);
		XXPortalUserRoleDao userDao = Mockito.mock(XXPortalUserRoleDao.class);
		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(userDao);
		Mockito.when(userDao.findByUserId(userId)).thenReturn(list);
		boolean isFound = userMgr.updateRoles(userId, rolesList);
		Assert.assertFalse(isFound);
	}

	@Test
	public void test20UpdateUserWithPass() {
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		VXPortalUser userProfile = userProfile();
		String userName = userProfile.getFirstName();
		String userPassword = userProfile.getPassword();
		XXPortalUser user = new XXPortalUser();
		user.setEmailAddress(userProfile.getEmailAddress());
		user.setFirstName(userProfile.getFirstName());
		user.setLastName(userProfile.getLastName());
		user.setLoginId(userProfile.getLoginId());
		user.setPassword(userProfile.getPassword());
		user.setUserSource(userProfile.getUserSource());
		user.setPublicScreenName(userProfile.getPublicScreenName());
		user.setId(userProfile.getId());
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.findByLoginId(Mockito.anyString())).thenReturn(
				user);
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.update(user)).thenReturn(user);
		XXPortalUser dbXXPortalUser = userMgr.updatePasswordInSHA256(userName,
				userPassword);
		Assert.assertNotNull(dbXXPortalUser);
	 }

}
