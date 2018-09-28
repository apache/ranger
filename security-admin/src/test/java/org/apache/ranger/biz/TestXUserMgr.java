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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAuditMapDao;
import org.apache.ranger.db.XXAuthSessionDao;
import org.apache.ranger.db.XXGroupDao;
import org.apache.ranger.db.XXGroupGroupDao;
import org.apache.ranger.db.XXGroupPermissionDao;
import org.apache.ranger.db.XXGroupUserDao;
import org.apache.ranger.db.XXModuleDefDao;
import org.apache.ranger.db.XXPermMapDao;
import org.apache.ranger.db.XXPolicyDao;
import org.apache.ranger.db.XXPortalUserDao;
import org.apache.ranger.db.XXPortalUserRoleDao;
import org.apache.ranger.db.XXUserDao;
import org.apache.ranger.db.XXUserPermissionDao;
import org.apache.ranger.entity.XXAuthSession;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXGroupGroup;
import org.apache.ranger.entity.XXGroupPermission;
import org.apache.ranger.entity.XXGroupUser;
import org.apache.ranger.entity.XXModuleDef;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXPortalUserRole;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.entity.XXUserPermission;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.XAuditMapService;
import org.apache.ranger.service.XGroupPermissionService;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XGroupUserService;
import org.apache.ranger.service.XModuleDefService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XPortalUserService;
import org.apache.ranger.service.XUserPermissionService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXAuditMapList;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXGroupList;
import org.apache.ranger.view.VXGroupPermission;
import org.apache.ranger.view.VXGroupUser;
import org.apache.ranger.view.VXGroupUserList;
import org.apache.ranger.view.VXModuleDef;
import org.apache.ranger.view.VXPermMapList;
import org.apache.ranger.view.VXPortalUser;
import org.apache.ranger.view.VXStringList;
import org.apache.ranger.view.VXUser;
import org.apache.ranger.view.VXUserGroupInfo;
import org.apache.ranger.view.VXUserList;
import org.apache.ranger.view.VXUserPermission;
import org.apache.ranger.view.VXString;
import org.junit.Assert;
import org.junit.FixMethodOrder;
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
public class TestXUserMgr {

	private static Long userId = 8L;

	private static Integer emptyValue;

	@InjectMocks
	XUserMgr xUserMgr = new XUserMgr();

	@Mock
	XGroupService xGroupService;

	@Mock
	RangerDaoManager daoManager;

	@Mock
	RESTErrorUtil restErrorUtil;

	@Mock
	XGroupUserService xGroupUserService;

	@Mock
	StringUtil stringUtil;

	@Mock
	RangerBizUtil msBizUtil;

	@Mock
	UserMgr userMgr;

	@Mock
	XUserService xUserService;

	@Mock
	XModuleDefService xModuleDefService;

	@Mock
	XUserPermissionService xUserPermissionService;

	@Mock
	XGroupPermissionService xGroupPermissionService;

	@Mock
	ContextUtil contextUtil;

	@Mock
	RangerSecurityContext rangerSecurityContext;

	@Mock
	XPortalUserService xPortalUserService;
	
	@Mock
	SessionMgr sessionMgr;

	@Mock
	XPermMapService xPermMapService;

	@Mock
	XAuditMapService xAuditMapService;

	@Mock
	RangerPolicyService policyService;

	@Mock
	ServiceDBStore svcStore;
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	public void setup() {
		RangerSecurityContext context = new RangerSecurityContext();
		context.setUserSession(new UserSessionBase());
		RangerContextHolder.setSecurityContext(context);
		UserSessionBase currentUserSession = ContextUtil
				.getCurrentUserSession();
		currentUserSession.setUserAdmin(true);
                XXPortalUser gjUser = new XXPortalUser();
                gjUser.setLoginId("test");
                gjUser.setId(1L);
                currentUserSession.setXXPortalUser(gjUser);
	}

	private VXUser vxUser() {
		Collection<String> userRoleList = new ArrayList<String>();
		userRoleList.add("ROLE_USER");
		Collection<String> groupNameList = new ArrayList<String>();
		groupNameList.add("Grp2");
		VXUser vxUser = new VXUser();
		vxUser.setId(userId);
		vxUser.setDescription("group test working");
		vxUser.setName("grouptest");
		vxUser.setUserRoleList(userRoleList);
		vxUser.setGroupNameList(groupNameList);
                vxUser.setPassword("usertest123");
		return vxUser;
	}

	private VXModuleDef vXModuleDef() {
		VXUserPermission userPermission = vXUserPermission();
		List<VXUserPermission> userPermList = new ArrayList<VXUserPermission>();
		userPermList.add(userPermission);

		VXGroupPermission groupPermission = vXGroupPermission();
		List<VXGroupPermission> groupPermList = new ArrayList<VXGroupPermission>();
		groupPermList.add(groupPermission);

		VXModuleDef vxModuleDef = new VXModuleDef();
		vxModuleDef.setAddedById(userId);
		vxModuleDef.setCreateDate(new Date());
		vxModuleDef.setCreateTime(new Date());
		vxModuleDef.setId(userId);
		vxModuleDef.setModule("Policy manager");
		vxModuleDef.setOwner("admin");
		vxModuleDef.setUpdateDate(new Date());
		vxModuleDef.setUpdatedBy("admin");
		vxModuleDef.setUpdatedById(userId);
		vxModuleDef.setUpdateTime(new Date());
		vxModuleDef.setUrl("/policy manager");
		vxModuleDef.setUserPermList(userPermList);
		vxModuleDef.setGroupPermList(groupPermList);

		return vxModuleDef;
	}

	private VXUserPermission vXUserPermission() {
		VXUserPermission userPermission = new VXUserPermission();
		userPermission.setId(1L);
		userPermission.setIsAllowed(1);
		userPermission.setModuleId(1L);
		userPermission.setUserId(userId);
		userPermission.setUserName("xyz");
		userPermission.setOwner("admin");

		return userPermission;
	}

	private VXGroupPermission vXGroupPermission() {
		VXGroupPermission groupPermission = new VXGroupPermission();
		groupPermission.setId(1L);
		groupPermission.setIsAllowed(1);
		groupPermission.setModuleId(1L);
		groupPermission.setGroupId(userId);
		groupPermission.setGroupName("xyz");
		groupPermission.setOwner("admin");

		return groupPermission;
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

	private RangerPolicy rangerPolicy() {
		List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicyItemAccess>();
		List<String> users = new ArrayList<String>();
		List<String> groups = new ArrayList<String>();
		List<RangerPolicyItemCondition> conditions = new ArrayList<RangerPolicyItemCondition>();
		List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicyItem>();
		RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.setAccesses(accesses);
		rangerPolicyItem.setConditions(conditions);
		rangerPolicyItem.setGroups(groups);
		rangerPolicyItem.setUsers(users);
		rangerPolicyItem.setDelegateAdmin(false);

		policyItems.add(rangerPolicyItem);

		Map<String, RangerPolicyResource> policyResource = new HashMap<String, RangerPolicyResource>();
		RangerPolicyResource rangerPolicyResource = new RangerPolicyResource();
		rangerPolicyResource.setIsExcludes(true);
		rangerPolicyResource.setIsRecursive(true);
		rangerPolicyResource.setValue("1");
		rangerPolicyResource.setValues(users);
		RangerPolicy policy = new RangerPolicy();
		policy.setId(userId);
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
	@Test
	public void test11CreateXUser() {
		setup();
		VXUser vxUser = vxUser();
		Collection<String> userRoleList = new ArrayList<String>();
		userRoleList.add("test");
		vxUser.setUserRoleList(userRoleList);

		ArrayList<String> userRoleListVXPortaUser = new ArrayList<String>();

		VXPortalUser vXPortalUser = new VXPortalUser();
		vXPortalUser.setUserRoleList(userRoleListVXPortaUser);

		Mockito.when(xUserService.createResource(vxUser)).thenReturn(vxUser);
		XXModuleDefDao value = Mockito.mock(XXModuleDefDao.class);
		Mockito.when(daoManager.getXXModuleDef()).thenReturn(value);

		Mockito.when(
				userMgr.createDefaultAccountUser((VXPortalUser) Mockito
						.anyObject())).thenReturn(vXPortalUser);

		VXUser dbUser = xUserMgr.createXUser(vxUser);
		Assert.assertNotNull(dbUser);
		userId = dbUser.getId();
		Assert.assertEquals(userId, dbUser.getId());
		Assert.assertEquals(dbUser.getDescription(), vxUser.getDescription());
		Assert.assertEquals(dbUser.getName(), vxUser.getName());
		Assert.assertEquals(dbUser.getUserRoleList(), vxUser.getUserRoleList());
		Assert.assertEquals(dbUser.getGroupNameList(),
				vxUser.getGroupNameList());

		Mockito.verify(xUserService).createResource(vxUser);

		Mockito.when(xUserService.readResourceWithOutLogin(userId)).thenReturn(
				vxUser);
		VXUser dbvxUser = xUserMgr.getXUser(userId);

		Mockito.verify(userMgr).createDefaultAccountUser(
				(VXPortalUser) Mockito.anyObject());
		Mockito.verify(daoManager).getXXModuleDef();
		Assert.assertNotNull(dbvxUser);
		Assert.assertEquals(userId, dbvxUser.getId());
		Assert.assertEquals(dbvxUser.getDescription(), vxUser.getDescription());
		Assert.assertEquals(dbvxUser.getName(), vxUser.getName());
		Assert.assertEquals(dbvxUser.getUserRoleList(),
				vxUser.getUserRoleList());
		Assert.assertEquals(dbvxUser.getGroupNameList(),
				vxUser.getGroupNameList());
		Mockito.verify(xUserService).readResourceWithOutLogin(userId);
	}

	@Test
	public void test12UpdateXUser() {
		setup();
		VXUser vxUser = vxUser();
		vxUser.setUserSource(RangerCommonEnums.USER_APP);
		vxUser.setName("name");
		Mockito.when(xUserService.updateResource(vxUser)).thenReturn(vxUser);
		VXPortalUser vXPortalUser = new VXPortalUser();
		Mockito.when(userMgr.getUserProfileByLoginId(vxUser.getName())).thenReturn(vXPortalUser);

		VXUser dbvxUser = xUserMgr.updateXUser(vxUser);
		Assert.assertNotNull(dbvxUser);
		Assert.assertEquals(dbvxUser.getId(), vxUser.getId());
		Assert.assertEquals(dbvxUser.getDescription(), vxUser.getDescription());
		Assert.assertEquals(dbvxUser.getName(), vxUser.getName());
		Mockito.verify(xUserService).updateResource(vxUser);
	}

	@Test
	public void test13ModifyUserVisibilitySetOne() {
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		XXUser xxUser = Mockito.mock(XXUser.class);
		VXUser vxUser = vxUser();

		Mockito.when(xUserService.updateResource(vxUser)).thenReturn(vxUser);
		HashMap<Long, Integer> visibilityMap = new HashMap<Long, Integer>();
		Integer value = 1;
		visibilityMap.put(userId, value);

		Mockito.when(daoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.getById(userId)).thenReturn(xxUser);
		Mockito.when(xUserService.populateViewBean(xxUser)).thenReturn(vxUser);

		xUserMgr.modifyUserVisibility(visibilityMap);
		Assert.assertEquals(value, vxUser.getIsVisible());
		Assert.assertEquals(userId, vxUser.getId());
		Mockito.verify(xUserService).updateResource(vxUser);
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(xUserService).populateViewBean(xxUser);
	}

	@Test
	public void test14ModifyUserVisibilitySetZero() {
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		XXUser xxUser = Mockito.mock(XXUser.class);
		VXUser vxUser = vxUser();

		Mockito.when(xUserService.updateResource(vxUser)).thenReturn(vxUser);
		HashMap<Long, Integer> visibilityMap = new HashMap<Long, Integer>();
		Integer value = 0;
		visibilityMap.put(userId, value);

		Mockito.when(daoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.getById(userId)).thenReturn(xxUser);
		Mockito.when(xUserService.populateViewBean(xxUser)).thenReturn(vxUser);

		xUserMgr.modifyUserVisibility(visibilityMap);
		Assert.assertEquals(value, vxUser.getIsVisible());
		Assert.assertEquals(userId, vxUser.getId());
		Mockito.verify(xUserService).updateResource(vxUser);
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(xUserService).populateViewBean(xxUser);
	}

	@Test
	public void test15ModifyUserVisibilitySetEmpty() {
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		XXUser xxUser = Mockito.mock(XXUser.class);
		VXUser vxUser = vxUser();

		Mockito.when(xUserService.updateResource(vxUser)).thenReturn(vxUser);
		HashMap<Long, Integer> visibilityMap = new HashMap<Long, Integer>();
		visibilityMap.put(userId, emptyValue);

		Mockito.when(daoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.getById(userId)).thenReturn(xxUser);
		Mockito.when(xUserService.populateViewBean(xxUser)).thenReturn(vxUser);

		xUserMgr.modifyUserVisibility(visibilityMap);
		Assert.assertEquals(emptyValue, vxUser.getIsVisible());
		Assert.assertEquals(userId, vxUser.getId());
		Mockito.verify(xUserService).updateResource(vxUser);
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(xUserService).populateViewBean(xxUser);
	}

	@Test
	public void test16CreateXGroup() {
		setup();
		VXGroup vXGroup = new VXGroup();
		vXGroup.setId(userId);
		vXGroup.setDescription("group test");
		vXGroup.setName("grouptest");

		Mockito.when(xGroupService.createResource(vXGroup)).thenReturn(vXGroup);

		VXGroup dbXGroup = xUserMgr.createXGroup(vXGroup);
		Assert.assertNotNull(dbXGroup);
		userId = dbXGroup.getId();
		Assert.assertEquals(userId, dbXGroup.getId());
		Assert.assertEquals(vXGroup.getDescription(), dbXGroup.getDescription());
		Assert.assertEquals(vXGroup.getName(), dbXGroup.getName());
		Mockito.verify(xGroupService).createResource(vXGroup);

		Mockito.when(xGroupService.readResourceWithOutLogin(userId))
				.thenReturn(vXGroup);
		VXGroup dbxGroup = xUserMgr.getXGroup(userId);
		Assert.assertNotNull(dbXGroup);
		Assert.assertEquals(userId, dbxGroup.getId());
		Assert.assertEquals(dbXGroup.getDescription(),
				dbxGroup.getDescription());
		Assert.assertEquals(dbXGroup.getName(), dbxGroup.getName());
		Mockito.verify(xGroupService).readResourceWithOutLogin(userId);
	}

	@Test
	public void test17UpdateXGroup() {
		XXGroupDao xxGroupDao = Mockito.mock(XXGroupDao.class);
		XXGroupUserDao xxGroupUserDao = Mockito.mock(XXGroupUserDao.class);
		List<XXGroupUser> grpUsers =new ArrayList<XXGroupUser>();
		setup();
		VXGroup vXGroup = new VXGroup();
		vXGroup.setId(userId);
		vXGroup.setDescription("group test");
		vXGroup.setName("grouptest");

		XXGroup xxGroup = new XXGroup();
		xxGroup.setName("grouptest");
		Mockito.when(daoManager.getXXGroup()).thenReturn(xxGroupDao);
		Mockito.when(xxGroupDao.getById(vXGroup.getId())).thenReturn(xxGroup);
		Mockito.when(xGroupService.updateResource(vXGroup)).thenReturn(vXGroup);
		Mockito.when(daoManager.getXXGroupUser()).thenReturn(xxGroupUserDao);
		Mockito.when(xxGroupUserDao.findByGroupId(vXGroup.getId())).thenReturn(grpUsers);
		VXGroup dbvxGroup = xUserMgr.updateXGroup(vXGroup);
		Assert.assertNotNull(dbvxGroup);
		userId = dbvxGroup.getId();
		Assert.assertEquals(userId, dbvxGroup.getId());
		Assert.assertEquals(vXGroup.getDescription(),
				dbvxGroup.getDescription());
		Assert.assertEquals(vXGroup.getName(), dbvxGroup.getName());
		Mockito.verify(daoManager).getXXGroup();
		Mockito.verify(daoManager).getXXGroupUser();
		Mockito.verify(xGroupService).updateResource(vXGroup);
		Mockito.verify(xxGroupUserDao).findByGroupId(vXGroup.getId());
	}

	@Test
	public void test18ModifyGroupsVisibilitySetOne() {
		XXGroupDao xxGroupDao = Mockito.mock(XXGroupDao.class);
		VXGroup vXGroup = new VXGroup();
		vXGroup.setId(userId);
		vXGroup.setDescription("group test");
		vXGroup.setName("grouptest");

		XXGroup xxGroup = new XXGroup();
		HashMap<Long, Integer> groupVisibilityMap = new HashMap<Long, Integer>();
		Integer value = 1;
		groupVisibilityMap.put(userId, value);

		Mockito.when(daoManager.getXXGroup()).thenReturn(xxGroupDao);
		Mockito.when(xxGroupDao.getById(vXGroup.getId())).thenReturn(xxGroup);
		Mockito.when(xGroupService.populateViewBean(xxGroup)).thenReturn(
				vXGroup);
		Mockito.when(xGroupService.updateResource(vXGroup)).thenReturn(vXGroup);

		xUserMgr.modifyGroupsVisibility(groupVisibilityMap);
		Assert.assertEquals(value, vXGroup.getIsVisible());
		Assert.assertEquals(userId, vXGroup.getId());
		Mockito.verify(daoManager).getXXGroup();
		Mockito.verify(xGroupService).populateViewBean(xxGroup);
		Mockito.verify(xGroupService).updateResource(vXGroup);
	}

	@Test
	public void test19ModifyGroupsVisibilitySetZero() {
		XXGroupDao xxGroupDao = Mockito.mock(XXGroupDao.class);
		VXGroup vXGroup = new VXGroup();
		vXGroup.setId(userId);
		vXGroup.setDescription("group test");
		vXGroup.setName("grouptest");

		XXGroup xxGroup = new XXGroup();
		HashMap<Long, Integer> groupVisibilityMap = new HashMap<Long, Integer>();
		Integer value = 0;
		groupVisibilityMap.put(userId, value);

		Mockito.when(daoManager.getXXGroup()).thenReturn(xxGroupDao);
		Mockito.when(xxGroupDao.getById(vXGroup.getId())).thenReturn(xxGroup);
		Mockito.when(xGroupService.populateViewBean(xxGroup)).thenReturn(
				vXGroup);
		Mockito.when(xGroupService.updateResource(vXGroup)).thenReturn(vXGroup);

		xUserMgr.modifyGroupsVisibility(groupVisibilityMap);
		Assert.assertEquals(value, vXGroup.getIsVisible());
		Assert.assertEquals(userId, vXGroup.getId());
		Mockito.verify(daoManager).getXXGroup();
		Mockito.verify(xGroupService).populateViewBean(xxGroup);
		Mockito.verify(xGroupService).updateResource(vXGroup);
	}

	@Test
	public void test20ModifyGroupsVisibilitySetEmpty() {
		XXGroupDao xxGroupDao = Mockito.mock(XXGroupDao.class);
		VXGroup vXGroup = new VXGroup();
		vXGroup.setId(userId);
		vXGroup.setDescription("group test");
		vXGroup.setName("grouptest");

		XXGroup xxGroup = new XXGroup();
		HashMap<Long, Integer> groupVisibilityMap = new HashMap<Long, Integer>();
		groupVisibilityMap.put(userId, emptyValue);

		Mockito.when(daoManager.getXXGroup()).thenReturn(xxGroupDao);
		Mockito.when(xxGroupDao.getById(vXGroup.getId())).thenReturn(xxGroup);
		Mockito.when(xGroupService.populateViewBean(xxGroup)).thenReturn(
				vXGroup);
		Mockito.when(xGroupService.updateResource(vXGroup)).thenReturn(vXGroup);

		xUserMgr.modifyGroupsVisibility(groupVisibilityMap);
		Assert.assertEquals(emptyValue, vXGroup.getIsVisible());
		Assert.assertEquals(userId, vXGroup.getId());
		Mockito.verify(daoManager).getXXGroup();
		Mockito.verify(xGroupService).populateViewBean(xxGroup);
		Mockito.verify(xGroupService).updateResource(vXGroup);
	}

	@Test
	public void test21createXGroupUser() {
		setup();
		VXGroupUser vxGroupUser = new VXGroupUser();
		vxGroupUser.setId(userId);
		vxGroupUser.setName("group user test");
		vxGroupUser.setOwner("Admin");
		vxGroupUser.setUserId(userId);
		vxGroupUser.setUpdatedBy("User");

		Mockito.when(
				xGroupUserService.createXGroupUserWithOutLogin(vxGroupUser))
				.thenReturn(vxGroupUser);

		VXGroupUser dbVXGroupUser = xUserMgr.createXGroupUser(vxGroupUser);
		Assert.assertNotNull(dbVXGroupUser);
		userId = dbVXGroupUser.getId();
		Assert.assertEquals(userId, dbVXGroupUser.getId());
		Assert.assertEquals(dbVXGroupUser.getOwner(), vxGroupUser.getOwner());
		Assert.assertEquals(dbVXGroupUser.getName(), vxGroupUser.getName());
		Assert.assertEquals(dbVXGroupUser.getUserId(), vxGroupUser.getUserId());
		Assert.assertEquals(dbVXGroupUser.getUpdatedBy(),
				vxGroupUser.getUpdatedBy());
		Mockito.verify(xGroupUserService).createXGroupUserWithOutLogin(
				vxGroupUser);

		Mockito.when(xGroupUserService.readResourceWithOutLogin(userId))
				.thenReturn(vxGroupUser);
		VXGroup vXGroup = new VXGroup();
		vXGroup.setId(userId);
		vXGroup.setDescription("group test");
		vXGroup.setName("grouptest");

		VXGroupUser dbvxGroupUser = xUserMgr.getXGroupUser(userId);
		Assert.assertNotNull(dbvxGroupUser);
		userId = dbvxGroupUser.getId();
		Assert.assertEquals(userId, dbvxGroupUser.getId());
		Assert.assertEquals(dbvxGroupUser.getOwner(), vxGroupUser.getOwner());
		Assert.assertEquals(dbvxGroupUser.getName(), vxGroupUser.getName());
		Assert.assertEquals(dbvxGroupUser.getUserId(), vxGroupUser.getUserId());
		Assert.assertEquals(dbvxGroupUser.getUpdatedBy(),
				vxGroupUser.getUpdatedBy());
		Mockito.verify(xGroupUserService).readResourceWithOutLogin(userId);
	}

	@Test
	public void test22GetXUserGroups() {
		VXGroupList dbVXGroupList = xUserMgr.getXUserGroups(userId);
		Assert.assertNotNull(dbVXGroupList);
	}

	@Test
	public void test23GetXGroupUsers() {
		VXUserList dbVXUserList = xUserMgr.getXGroupUsers(userId);
		VXGroup vXGroup = new VXGroup();
		vXGroup.setId(userId);
		vXGroup.setDescription("group test");
		vXGroup.setName("grouptest");
		Assert.assertNotNull(dbVXUserList);
	}

	@Test
	public void test24GetXUserByUserName() {
		VXUser vxUser = vxUser();
		String userName = "test";

		Mockito.when(xUserService.getXUserByUserName(userName)).thenReturn(
				vxUser);
                XXModuleDefDao xxModuleDefDao = Mockito.mock(XXModuleDefDao.class);
                Mockito.when(daoManager.getXXModuleDef()).thenReturn(xxModuleDefDao);
		VXUser dbVXUser = xUserMgr.getXUserByUserName(userName);
		Assert.assertNotNull(dbVXUser);
		userId = dbVXUser.getId();
		Assert.assertEquals(userId, dbVXUser.getId());
		Assert.assertEquals(dbVXUser.getName(), vxUser.getName());
		Assert.assertEquals(dbVXUser.getOwner(), vxUser.getOwner());
                Mockito.verify(xUserService, Mockito.atLeast(2)).getXUserByUserName(
                                userName);
	}

	@Test
	public void test25CreateXUserWithOutLogin() {
		setup();
		VXUser vxUser = vxUser();

		Mockito.when(xUserService.createXUserWithOutLogin(vxUser)).thenReturn(
				vxUser);

		VXUser dbUser = xUserMgr.createXUserWithOutLogin(vxUser);
		Assert.assertNotNull(dbUser);
		userId = dbUser.getId();
		Assert.assertEquals(userId, dbUser.getId());
		Assert.assertEquals(dbUser.getDescription(), vxUser.getDescription());
		Assert.assertEquals(dbUser.getName(), vxUser.getName());
		Assert.assertEquals(dbUser.getUserRoleList(), vxUser.getUserRoleList());
		Assert.assertEquals(dbUser.getGroupNameList(),
				vxUser.getGroupNameList());
		Mockito.verify(xUserService).createXUserWithOutLogin(vxUser);
	}

	@Test
	public void test26CreateXGroupWithoutLogin() {
		setup();
		VXGroup vXGroup = new VXGroup();
		vXGroup.setId(userId);
		vXGroup.setDescription("group test");
		vXGroup.setName("grouptest");

		Mockito.when(xGroupService.createXGroupWithOutLogin(vXGroup))
				.thenReturn(vXGroup);

		VXGroup dbVXGroup = xUserMgr.createXGroupWithoutLogin(vXGroup);
		Assert.assertNotNull(dbVXGroup);
		userId = dbVXGroup.getId();
		Assert.assertEquals(userId, dbVXGroup.getId());
		Assert.assertEquals(vXGroup.getDescription(),
				dbVXGroup.getDescription());
		Assert.assertEquals(vXGroup.getName(), dbVXGroup.getName());
		Mockito.verify(xGroupService).createXGroupWithOutLogin(vXGroup);
	}

	@Test
	public void test27DeleteXGroup() {
		setup();
		boolean force = true;
		VXGroup vXGroup = new VXGroup();
		vXGroup.setId(userId);
		vXGroup.setDescription("group test");
		vXGroup.setName("grouptest");
		// XXGroup
		XXGroupDao xXGroupDao = Mockito.mock(XXGroupDao.class);
		XXGroup xXGroup = new XXGroup();
		Mockito.when(daoManager.getXXGroup()).thenReturn(xXGroupDao);
		Mockito.when(xXGroupDao.getById(vXGroup.getId())).thenReturn(xXGroup);
		Mockito.when(xGroupService.populateViewBean(xXGroup)).thenReturn(vXGroup);
		// VXGroupUser
		VXGroupUserList vxGroupUserList = new VXGroupUserList();
		XXGroupUserDao xGroupUserDao = Mockito.mock(XXGroupUserDao.class);
		VXGroupUser vxGroupUser = new VXGroupUser();
		vxGroupUser.setId(userId);
		vxGroupUser.setName("group user test");
		vxGroupUser.setOwner("Admin");
		vxGroupUser.setUserId(userId);
		vxGroupUser.setUpdatedBy("User");
		Mockito.when(xGroupUserService.searchXGroupUsers((SearchCriteria) Mockito.anyObject()))
				.thenReturn(vxGroupUserList);
		Mockito.when(daoManager.getXXGroupUser()).thenReturn(xGroupUserDao);
		// VXPermMap
		VXPermMapList vXPermMapList = new VXPermMapList();
		XXPermMapDao xXPermMapDao = Mockito.mock(XXPermMapDao.class);
		Mockito.when(xPermMapService.searchXPermMaps((SearchCriteria) Mockito.anyObject())).thenReturn(vXPermMapList);
		Mockito.when(daoManager.getXXPermMap()).thenReturn(xXPermMapDao);
		// VXAuditMap
		VXAuditMapList vXAuditMapList = new VXAuditMapList();
		XXAuditMapDao xXAuditMapDao = Mockito.mock(XXAuditMapDao.class);
		Mockito.when(xAuditMapService.searchXAuditMaps((SearchCriteria) Mockito.anyObject()))
				.thenReturn(vXAuditMapList);
		Mockito.when(daoManager.getXXAuditMap()).thenReturn(xXAuditMapDao);
		//XXGroupGroup
		XXGroupGroupDao xXGroupGroupDao = Mockito.mock(XXGroupGroupDao.class);
		List<XXGroupGroup> xXGroupGroups = new ArrayList<XXGroupGroup>();
		Mockito.when(daoManager.getXXGroupGroup()).thenReturn(xXGroupGroupDao);
		Mockito.when(xXGroupGroupDao.findByGroupId(userId)).thenReturn(xXGroupGroups);
		//update XXGroupPermission
		XXGroupPermissionDao xXGroupPermissionDao= Mockito.mock(XXGroupPermissionDao.class);
		Mockito.when(daoManager.getXXGroupPermission()).thenReturn(xXGroupPermissionDao);
		List<XXGroupPermission> xXGroupPermissions=new ArrayList<XXGroupPermission>();
		Mockito.when(xXGroupPermissionDao.findByGroupId(vXGroup.getId())).thenReturn(xXGroupPermissions);
		//update XXPolicyItemUserPerm
		XXPolicyDao xXPolicyDao = Mockito.mock(XXPolicyDao.class);
		List<XXPolicy> xXPolicyList = new ArrayList<XXPolicy>();
		XXPolicy xXPolicy = Mockito.mock(XXPolicy.class);
		RangerPolicy rangerPolicy = rangerPolicy();
		Mockito.when(daoManager.getXXPolicy()).thenReturn(xXPolicyDao);
		Mockito.when(xXPolicyDao.findByGroupId(userId)).thenReturn(xXPolicyList);
		Mockito.when(policyService.getPopulatedViewObject(xXPolicy)).thenReturn(rangerPolicy);
		xUserMgr.deleteXGroup(vXGroup.getId(), force);
		Mockito.verify(xGroupUserService).searchXGroupUsers((SearchCriteria) Mockito.anyObject());
	}

	@Test
	public void test28DeleteXUser() {
		setup();
		boolean force = true;
		VXUser vXUser = vxUser();
		// XXUser
		XXUser xXUser = new XXUser();
		XXUserDao xXUserDao = Mockito.mock(XXUserDao.class);
		Mockito.when(daoManager.getXXUser()).thenReturn(xXUserDao);
		Mockito.when(xXUserDao.getById(vXUser.getId())).thenReturn(xXUser);
		Mockito.when(xUserService.populateViewBean(xXUser)).thenReturn(vXUser);
		// VXGroupUser
		VXGroupUserList vxGroupUserList = new VXGroupUserList();
		XXGroupUserDao xGroupUserDao = Mockito.mock(XXGroupUserDao.class);
		VXGroupUser vxGroupUser = new VXGroupUser();
		vxGroupUser.setId(userId);
		vxGroupUser.setName("group user test");
		vxGroupUser.setOwner("Admin");
		vxGroupUser.setUserId(vXUser.getId());
		vxGroupUser.setUpdatedBy("User");
		Mockito.when(xGroupUserService.searchXGroupUsers((SearchCriteria) Mockito.anyObject()))
				.thenReturn(vxGroupUserList);
		Mockito.when(daoManager.getXXGroupUser()).thenReturn(xGroupUserDao);
		// VXPermMap
		VXPermMapList vXPermMapList = new VXPermMapList();
		XXPermMapDao xXPermMapDao = Mockito.mock(XXPermMapDao.class);
		Mockito.when(xPermMapService.searchXPermMaps((SearchCriteria) Mockito.anyObject())).thenReturn(vXPermMapList);
		Mockito.when(daoManager.getXXPermMap()).thenReturn(xXPermMapDao);
		// VXAuditMap
		VXAuditMapList vXAuditMapList = new VXAuditMapList();
		XXAuditMapDao xXAuditMapDao = Mockito.mock(XXAuditMapDao.class);
		Mockito.when(xAuditMapService.searchXAuditMaps((SearchCriteria) Mockito.anyObject()))
				.thenReturn(vXAuditMapList);
		Mockito.when(daoManager.getXXAuditMap()).thenReturn(xXAuditMapDao);
		//XXPortalUser
		VXPortalUser vXPortalUser = userProfile();
		XXPortalUser xXPortalUser = new XXPortalUser();
		XXPortalUserDao xXPortalUserDao = Mockito.mock(XXPortalUserDao.class);
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(xXPortalUserDao);
		Mockito.when(xXPortalUserDao.findByLoginId(vXUser.getName().trim())).thenReturn(xXPortalUser);
		Mockito.when(xPortalUserService.populateViewBean(xXPortalUser)).thenReturn(vXPortalUser);

		XXAuthSessionDao xXAuthSessionDao= Mockito.mock(XXAuthSessionDao.class);
		XXUserPermissionDao xXUserPermissionDao= Mockito.mock(XXUserPermissionDao.class);
		XXPortalUserRoleDao xXPortalUserRoleDao= Mockito.mock(XXPortalUserRoleDao.class);
		Mockito.when(daoManager.getXXAuthSession()).thenReturn(xXAuthSessionDao);
		Mockito.when(daoManager.getXXUserPermission()).thenReturn(xXUserPermissionDao);
		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(xXPortalUserRoleDao);
		List<XXAuthSession> xXAuthSessions=new ArrayList<XXAuthSession>();
		List<XXUserPermission> xXUserPermissions=new ArrayList<XXUserPermission>();
		List<XXPortalUserRole> xXPortalUserRoles=new ArrayList<XXPortalUserRole>();
		Mockito.when(xXAuthSessionDao.getAuthSessionByUserId(vXPortalUser.getId())).thenReturn(xXAuthSessions);
		Mockito.when(xXUserPermissionDao.findByUserPermissionId(vXPortalUser.getId())).thenReturn(xXUserPermissions);
		Mockito.when(xXPortalUserRoleDao.findByUserId(vXPortalUser.getId())).thenReturn(xXPortalUserRoles);
		//update XXPolicyItemUserPerm
		XXPolicyDao xXPolicyDao = Mockito.mock(XXPolicyDao.class);
		List<XXPolicy> xXPolicyList = new ArrayList<XXPolicy>();
		XXPolicy xXPolicy = Mockito.mock(XXPolicy.class);
		RangerPolicy rangerPolicy = rangerPolicy();
		Mockito.when(daoManager.getXXPolicy()).thenReturn(xXPolicyDao);
		Mockito.when(xXPolicyDao.findByUserId(vXUser.getId())).thenReturn(xXPolicyList);
		Mockito.when(policyService.getPopulatedViewObject(xXPolicy)).thenReturn(rangerPolicy);
		xUserMgr.deleteXUser(vXUser.getId(), force);
		Mockito.verify(xGroupUserService).searchXGroupUsers((SearchCriteria) Mockito.anyObject());
	}

	@Test
	public void test29deleteXGroupAndXUser() {
		setup();
		VXUser vxUser = vxUser();
		VXGroup vxGroup = new VXGroup();
		VXGroupUserList vxGroupUserList = new VXGroupUserList();

		String groupName = "Grp2";
		String userName = "test";

		Mockito.when(xGroupService.getGroupByGroupName(Mockito.anyString()))
				.thenReturn(vxGroup);
		Mockito.when(xUserService.getXUserByUserName(Mockito.anyString()))
				.thenReturn(vxUser);
		Mockito.when(
				xGroupUserService.searchXGroupUsers((SearchCriteria) Mockito
						.anyObject())).thenReturn(vxGroupUserList);

		xUserMgr.deleteXGroupAndXUser(groupName, userName);
		Mockito.verify(xGroupService).getGroupByGroupName(Mockito.anyString());
		Mockito.verify(xUserService).getXUserByUserName(Mockito.anyString());
		Mockito.verify(xGroupUserService).searchXGroupUsers(
				(SearchCriteria) Mockito.anyObject());
	}

	@Test
	public void test30CreateVXUserGroupInfo() {
		setup();
		VXUserGroupInfo vXUserGroupInfo = new VXUserGroupInfo();
		VXUser vXUser = new VXUser();
		vXUser.setName("user1");
		vXUser.setDescription("testuser1 -added for unit testing");
                vXUser.setPassword("usertest123");
		List<VXGroupUser> vXGroupUserList = new ArrayList<VXGroupUser>();
		List<VXGroup> vXGroupList = new ArrayList<VXGroup>();

		final VXGroup vXGroup1 = new VXGroup();
		vXGroup1.setName("users");
		vXGroup1.setDescription("users -added for unit testing");
		vXGroupList.add(vXGroup1);

		VXGroupUser vXGroupUser1 = new VXGroupUser();
		vXGroupUser1.setName("users");
		vXGroupUserList.add(vXGroupUser1);

		final VXGroup vXGroup2 = new VXGroup();
		vXGroup2.setName("user1");
		vXGroup2.setDescription("user1 -added for unit testing");
		vXGroupList.add(vXGroup2);

		VXGroupUser vXGroupUser2 = new VXGroupUser();
		vXGroupUser2.setName("user1");
		vXGroupUserList.add(vXGroupUser2);

		vXUserGroupInfo.setXuserInfo(vXUser);
		vXUserGroupInfo.setXgroupInfo(vXGroupList);

		Mockito.when(xUserService.createXUserWithOutLogin(vXUser)).thenReturn(
				vXUser);
		Mockito.when(xGroupService.createXGroupWithOutLogin(vXGroup1))
				.thenReturn(vXGroup1);
		Mockito.when(xGroupService.createXGroupWithOutLogin(vXGroup2))
				.thenReturn(vXGroup2);
		Mockito.when(
				xGroupUserService.createXGroupUserWithOutLogin(vXGroupUser1))
				.thenReturn(vXGroupUser1);
		Mockito.when(
				xGroupUserService.createXGroupUserWithOutLogin(vXGroupUser2))
				.thenReturn(vXGroupUser2);
                XXPortalUserDao portalUser = Mockito.mock(XXPortalUserDao.class);
                Mockito.when(daoManager.getXXPortalUser()).thenReturn(portalUser);
                XXPortalUser user = new XXPortalUser();
                user.setId(1L);
                user.setUserSource(RangerCommonEnums.USER_APP);
                Mockito.when(portalUser.findByLoginId(vXUser.getName())).thenReturn(
                                user);
                XXPortalUserRoleDao userDao = Mockito.mock(XXPortalUserRoleDao.class);
                Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(userDao);
                List<String> lstRole = new ArrayList<String>();
                lstRole.add(RangerConstants.ROLE_SYS_ADMIN);
                Mockito.when(
                                userDao.findXPortalUserRolebyXPortalUserId(Mockito.anyLong()))
                                .thenReturn(lstRole);

		VXUserGroupInfo vxUserGroupTest = xUserMgr
				.createXUserGroupFromMap(vXUserGroupInfo);
		Assert.assertEquals("user1", vxUserGroupTest.getXuserInfo().getName());
		List<VXGroup> result = vxUserGroupTest.getXgroupInfo();
		List<VXGroup> expected = new ArrayList<VXGroup>();
		expected.add(vXGroup1);
		expected.add(vXGroup2);
		Assert.assertTrue(result.containsAll(expected));
        Mockito.verify(portalUser).findByLoginId(vXUser.getName());
        Mockito.verify(userDao).findXPortalUserRolebyXPortalUserId(Mockito.anyLong());
	}

	// Module permission
	@Test
	public void test31createXModuleDefPermission() {
		VXModuleDef vXModuleDef = vXModuleDef();

		Mockito.when(xModuleDefService.createResource(vXModuleDef)).thenReturn(
				vXModuleDef);
		XXModuleDefDao obj = Mockito.mock(XXModuleDefDao.class);
		Mockito.when(daoManager.getXXModuleDef()).thenReturn(obj);

		VXModuleDef dbMuduleDef = xUserMgr
				.createXModuleDefPermission(vXModuleDef);
		Assert.assertNotNull(dbMuduleDef);
		Assert.assertEquals(dbMuduleDef, vXModuleDef);
		Assert.assertEquals(dbMuduleDef.getId(), vXModuleDef.getId());
		Assert.assertEquals(dbMuduleDef.getOwner(), vXModuleDef.getOwner());
		Assert.assertEquals(dbMuduleDef.getUpdatedBy(),
				vXModuleDef.getUpdatedBy());
		Assert.assertEquals(dbMuduleDef.getUrl(), vXModuleDef.getUrl());
		Assert.assertEquals(dbMuduleDef.getAddedById(),
				vXModuleDef.getAddedById());
		Assert.assertEquals(dbMuduleDef.getCreateDate(),
				vXModuleDef.getCreateDate());
		Assert.assertEquals(dbMuduleDef.getCreateTime(),
				vXModuleDef.getCreateTime());
		Assert.assertEquals(dbMuduleDef.getUserPermList(),
				vXModuleDef.getUserPermList());
		Assert.assertEquals(dbMuduleDef.getGroupPermList(),
				vXModuleDef.getGroupPermList());
		Mockito.verify(xModuleDefService).createResource(vXModuleDef);
	}

	@Test
	public void test32getXModuleDefPermission() {
		VXModuleDef vXModuleDef = vXModuleDef();

		Mockito.when(xModuleDefService.readResource(1L))
				.thenReturn(vXModuleDef);

		VXModuleDef dbMuduleDef = xUserMgr.getXModuleDefPermission(1L);
		Assert.assertNotNull(dbMuduleDef);
		Assert.assertEquals(dbMuduleDef, vXModuleDef);
		Assert.assertEquals(dbMuduleDef.getId(), vXModuleDef.getId());
		Assert.assertEquals(dbMuduleDef.getOwner(), vXModuleDef.getOwner());
		Assert.assertEquals(dbMuduleDef.getUpdatedBy(),
				vXModuleDef.getUpdatedBy());
		Assert.assertEquals(dbMuduleDef.getUrl(), vXModuleDef.getUrl());
		Assert.assertEquals(dbMuduleDef.getAddedById(),
				vXModuleDef.getAddedById());
		Assert.assertEquals(dbMuduleDef.getCreateDate(),
				vXModuleDef.getCreateDate());
		Assert.assertEquals(dbMuduleDef.getCreateTime(),
				vXModuleDef.getCreateTime());
		Assert.assertEquals(dbMuduleDef.getUserPermList(),
				vXModuleDef.getUserPermList());
		Assert.assertEquals(dbMuduleDef.getGroupPermList(),
				vXModuleDef.getGroupPermList());

		Mockito.verify(xModuleDefService).readResource(1L);
	}

	@Test
	public void test33updateXModuleDefPermission() {
		XXModuleDefDao xModuleDefDao = Mockito.mock(XXModuleDefDao.class);
		XXModuleDef xModuleDef = Mockito.mock(XXModuleDef.class);

		XXUserPermissionDao xUserPermissionDao = Mockito
				.mock(XXUserPermissionDao.class);
		XXUserPermission xUserPermission = Mockito.mock(XXUserPermission.class);

		XXGroupPermissionDao xGroupPermissionDao = Mockito
				.mock(XXGroupPermissionDao.class);
		XXGroupPermission xGroupPermission = Mockito
				.mock(XXGroupPermission.class);

		VXUserPermission vXUserPermission = vXUserPermission();
		VXGroupPermission vXGroupPermission = vXGroupPermission();
		VXModuleDef vXModuleDef = vXModuleDef();

		Mockito.when(xModuleDefService.updateResource(vXModuleDef)).thenReturn(
				vXModuleDef);
		Mockito.when(xGroupPermissionService.updateResource(vXGroupPermission))
				.thenReturn(vXGroupPermission);
		Mockito.when(xGroupPermissionService.createResource(vXGroupPermission))
				.thenReturn(vXGroupPermission);
		Mockito.when(xUserPermissionService.updateResource(vXUserPermission))
				.thenReturn(vXUserPermission);
		Mockito.when(xUserPermissionService.createResource(vXUserPermission))
				.thenReturn(vXUserPermission);

		Mockito.when(daoManager.getXXModuleDef()).thenReturn(xModuleDefDao);
		Mockito.when(xModuleDefDao.getById(userId)).thenReturn(xModuleDef);
		Mockito.when(xModuleDefService.populateViewBean(xModuleDef))
				.thenReturn(vXModuleDef);

		Mockito.when(daoManager.getXXUserPermission()).thenReturn(
				xUserPermissionDao);
		Mockito.when(xUserPermissionDao.getById(userId)).thenReturn(
				xUserPermission);
		Mockito.when(xUserPermissionService.populateViewBean(xUserPermission))
				.thenReturn(vXUserPermission);

		Mockito.when(daoManager.getXXGroupPermission()).thenReturn(
				xGroupPermissionDao);
		Mockito.when(xGroupPermissionDao.getById(userId)).thenReturn(
				xGroupPermission);
		Mockito.when(xGroupPermissionService.populateViewBean(xGroupPermission))
				.thenReturn(vXGroupPermission);
		XXGroupUserDao xGrpUserDao = Mockito.mock(XXGroupUserDao.class);
		Mockito.when(daoManager.getXXGroupUser()).thenReturn(xGrpUserDao);
		
		UserSessionBase userSession = Mockito.mock(UserSessionBase.class);
		Set<UserSessionBase> userSessions = new HashSet<UserSessionBase>();
		userSessions.add(userSession);

		Mockito.when(xGroupPermissionService.createResource((VXGroupPermission) Mockito.anyObject())).thenReturn(vXGroupPermission);
		Mockito.when(xUserPermissionService.createResource((VXUserPermission) Mockito.anyObject())).thenReturn(vXUserPermission);
		Mockito.when(sessionMgr.getActiveUserSessionsForPortalUserId(userId)).thenReturn(userSessions);
		
		VXModuleDef dbMuduleDef = xUserMgr
				.updateXModuleDefPermission(vXModuleDef);
		Assert.assertEquals(dbMuduleDef, vXModuleDef);
		Assert.assertNotNull(dbMuduleDef);
		Assert.assertEquals(dbMuduleDef, vXModuleDef);
		Assert.assertEquals(dbMuduleDef.getId(), vXModuleDef.getId());
		Assert.assertEquals(dbMuduleDef.getOwner(), vXModuleDef.getOwner());
		Assert.assertEquals(dbMuduleDef.getUpdatedBy(),
				vXModuleDef.getUpdatedBy());
		Assert.assertEquals(dbMuduleDef.getUrl(), vXModuleDef.getUrl());
		Assert.assertEquals(dbMuduleDef.getAddedById(),
				vXModuleDef.getAddedById());
		Assert.assertEquals(dbMuduleDef.getCreateDate(),
				vXModuleDef.getCreateDate());
		Assert.assertEquals(dbMuduleDef.getCreateTime(),
				vXModuleDef.getCreateTime());
		Assert.assertEquals(dbMuduleDef.getUserPermList(),
				vXModuleDef.getUserPermList());
		Assert.assertEquals(dbMuduleDef.getGroupPermList(),
				vXModuleDef.getGroupPermList());

		Mockito.verify(xModuleDefService).updateResource(vXModuleDef);
		Mockito.verify(daoManager).getXXModuleDef();
		Mockito.verify(xModuleDefService).populateViewBean(xModuleDef);
		Mockito.verify(daoManager).getXXUserPermission();
		Mockito.verify(daoManager).getXXGroupPermission();
	}

	@Test
	public void test34deleteXModuleDefPermission() {
		Long moduleId=Long.valueOf(1);
		XXUserPermissionDao xUserPermissionDao = Mockito.mock(XXUserPermissionDao.class);
		XXGroupPermissionDao xGroupPermissionDao = Mockito.mock(XXGroupPermissionDao.class);
		Mockito.when(daoManager.getXXUserPermission()).thenReturn(xUserPermissionDao);
		Mockito.when(daoManager.getXXGroupPermission()).thenReturn(xGroupPermissionDao);
		Mockito.doNothing().when(xUserPermissionDao).deleteByModuleId(moduleId);
		Mockito.doNothing().when(xGroupPermissionDao).deleteByModuleId(moduleId);
		Mockito.when(xModuleDefService.deleteResource(1L)).thenReturn(true);
		xUserMgr.deleteXModuleDefPermission(1L, true);
		Mockito.verify(xModuleDefService).deleteResource(1L);
	}

	@Test
	public void test35createXUserPermission() {
		VXUserPermission vXUserPermission = vXUserPermission();

		Mockito.when(xUserPermissionService.createResource(vXUserPermission))
				.thenReturn(vXUserPermission);

		VXUserPermission dbUserPermission = xUserMgr
				.createXUserPermission(vXUserPermission);
		Assert.assertNotNull(dbUserPermission);
		Assert.assertEquals(dbUserPermission, vXUserPermission);
		Assert.assertEquals(dbUserPermission.getId(), vXUserPermission.getId());
		Assert.assertEquals(dbUserPermission.getOwner(),
				vXUserPermission.getOwner());
		Assert.assertEquals(dbUserPermission.getUpdatedBy(),
				vXUserPermission.getUpdatedBy());
		Assert.assertEquals(dbUserPermission.getUserName(),
				vXUserPermission.getUserName());
		Assert.assertEquals(dbUserPermission.getCreateDate(),
				vXUserPermission.getCreateDate());
		Assert.assertEquals(dbUserPermission.getIsAllowed(),
				vXUserPermission.getIsAllowed());
		Assert.assertEquals(dbUserPermission.getModuleId(),
				vXUserPermission.getModuleId());
		Assert.assertEquals(dbUserPermission.getUpdateDate(),
				vXUserPermission.getUpdateDate());
		Assert.assertEquals(dbUserPermission.getUserId(),
				vXUserPermission.getUserId());

		Mockito.verify(xUserPermissionService).createResource(vXUserPermission);
	}

	@Test
	public void test36getXUserPermission() {
		VXUserPermission vXUserPermission = vXUserPermission();

		Mockito.when(xUserPermissionService.readResource(1L)).thenReturn(
				vXUserPermission);

		VXUserPermission dbUserPermission = xUserMgr.getXUserPermission(1L);
		Assert.assertNotNull(dbUserPermission);
		Assert.assertEquals(dbUserPermission, vXUserPermission);
		Assert.assertEquals(dbUserPermission.getId(), vXUserPermission.getId());
		Assert.assertEquals(dbUserPermission.getOwner(),
				vXUserPermission.getOwner());
		Assert.assertEquals(dbUserPermission.getUpdatedBy(),
				vXUserPermission.getUpdatedBy());
		Assert.assertEquals(dbUserPermission.getUserName(),
				vXUserPermission.getUserName());
		Assert.assertEquals(dbUserPermission.getCreateDate(),
				vXUserPermission.getCreateDate());
		Assert.assertEquals(dbUserPermission.getIsAllowed(),
				vXUserPermission.getIsAllowed());
		Assert.assertEquals(dbUserPermission.getModuleId(),
				vXUserPermission.getModuleId());
		Assert.assertEquals(dbUserPermission.getUpdateDate(),
				vXUserPermission.getUpdateDate());
		Assert.assertEquals(dbUserPermission.getUserId(),
				vXUserPermission.getUserId());

		Mockito.verify(xUserPermissionService).readResource(1L);
	}

	@Test
	public void test37updateXUserPermission() {
		VXUserPermission vXUserPermission = vXUserPermission();

		Mockito.when(xUserPermissionService.updateResource(vXUserPermission))
				.thenReturn(vXUserPermission);

		VXUserPermission dbUserPermission = xUserMgr
				.updateXUserPermission(vXUserPermission);
		Assert.assertNotNull(dbUserPermission);
		Assert.assertEquals(dbUserPermission, vXUserPermission);
		Assert.assertEquals(dbUserPermission.getId(), vXUserPermission.getId());
		Assert.assertEquals(dbUserPermission.getOwner(),
				vXUserPermission.getOwner());
		Assert.assertEquals(dbUserPermission.getUpdatedBy(),
				vXUserPermission.getUpdatedBy());
		Assert.assertEquals(dbUserPermission.getUserName(),
				vXUserPermission.getUserName());
		Assert.assertEquals(dbUserPermission.getCreateDate(),
				vXUserPermission.getCreateDate());
		Assert.assertEquals(dbUserPermission.getIsAllowed(),
				vXUserPermission.getIsAllowed());
		Assert.assertEquals(dbUserPermission.getModuleId(),
				vXUserPermission.getModuleId());
		Assert.assertEquals(dbUserPermission.getUpdateDate(),
				vXUserPermission.getUpdateDate());
		Assert.assertEquals(dbUserPermission.getUserId(),
				vXUserPermission.getUserId());

		Mockito.verify(xUserPermissionService).updateResource(vXUserPermission);
	}

	@Test
	public void test38deleteXUserPermission() {

		Mockito.when(xUserPermissionService.deleteResource(1L))
				.thenReturn(true);
		XXUserPermission xUserPerm = Mockito.mock(XXUserPermission.class);
		XXUserPermissionDao xUserPermDao = Mockito.mock(XXUserPermissionDao.class);
		Mockito.when(daoManager.getXXUserPermission()).thenReturn(xUserPermDao);
		Mockito.when(daoManager.getXXUserPermission().getById(1L)).thenReturn(xUserPerm);
		xUserMgr.deleteXUserPermission(1L, true);
		Mockito.verify(xUserPermissionService).deleteResource(1L);
	}

	@Test
	public void test39createXGroupPermission() {
		VXGroupPermission vXGroupPermission = vXGroupPermission();

		XXGroupUserDao xGrpUserDao = Mockito.mock(XXGroupUserDao.class);
		Mockito.when(daoManager.getXXGroupUser()).thenReturn(xGrpUserDao);
		
		Mockito.when(xGroupPermissionService.createResource(vXGroupPermission)).thenReturn(vXGroupPermission);
		
		VXGroupPermission dbGroupPermission = xUserMgr
				.createXGroupPermission(vXGroupPermission);
		Assert.assertNotNull(dbGroupPermission);
		Assert.assertEquals(dbGroupPermission, vXGroupPermission);
		Assert.assertEquals(dbGroupPermission.getId(),
				vXGroupPermission.getId());
		Assert.assertEquals(dbGroupPermission.getGroupName(),
				vXGroupPermission.getGroupName());
		Assert.assertEquals(dbGroupPermission.getOwner(),
				vXGroupPermission.getOwner());
		Assert.assertEquals(dbGroupPermission.getUpdatedBy(),
				vXGroupPermission.getUpdatedBy());
		Assert.assertEquals(dbGroupPermission.getCreateDate(),
				vXGroupPermission.getCreateDate());
		Assert.assertEquals(dbGroupPermission.getGroupId(),
				vXGroupPermission.getGroupId());
		Assert.assertEquals(dbGroupPermission.getIsAllowed(),
				vXGroupPermission.getIsAllowed());
		Assert.assertEquals(dbGroupPermission.getModuleId(),
				vXGroupPermission.getModuleId());
		Assert.assertEquals(dbGroupPermission.getUpdateDate(),
				vXGroupPermission.getUpdateDate());

		Mockito.verify(xGroupPermissionService).createResource(
				vXGroupPermission);
	}

	@Test
	public void test40getXGroupPermission() {
		VXGroupPermission vXGroupPermission = vXGroupPermission();

		Mockito.when(xGroupPermissionService.readResource(1L)).thenReturn(
				vXGroupPermission);

		VXGroupPermission dbGroupPermission = xUserMgr.getXGroupPermission(1L);
		Assert.assertNotNull(dbGroupPermission);
		Assert.assertEquals(dbGroupPermission, vXGroupPermission);
		Assert.assertEquals(dbGroupPermission.getId(),
				vXGroupPermission.getId());
		Assert.assertEquals(dbGroupPermission.getGroupName(),
				vXGroupPermission.getGroupName());
		Assert.assertEquals(dbGroupPermission.getOwner(),
				vXGroupPermission.getOwner());
		Assert.assertEquals(dbGroupPermission.getUpdatedBy(),
				vXGroupPermission.getUpdatedBy());
		Assert.assertEquals(dbGroupPermission.getCreateDate(),
				vXGroupPermission.getCreateDate());
		Assert.assertEquals(dbGroupPermission.getGroupId(),
				vXGroupPermission.getGroupId());
		Assert.assertEquals(dbGroupPermission.getIsAllowed(),
				vXGroupPermission.getIsAllowed());
		Assert.assertEquals(dbGroupPermission.getModuleId(),
				vXGroupPermission.getModuleId());
		Assert.assertEquals(dbGroupPermission.getUpdateDate(),
				vXGroupPermission.getUpdateDate());

		Mockito.verify(xGroupPermissionService).readResource(1L);
	}

	@Test
	public void test41updateXGroupPermission() {
		VXGroupPermission vXGroupPermission = vXGroupPermission();

		XXGroupUserDao xGrpUserDao = Mockito.mock(XXGroupUserDao.class);
		Mockito.when(daoManager.getXXGroupUser()).thenReturn(xGrpUserDao);
		Mockito.when(xGroupPermissionService.updateResource(vXGroupPermission)).thenReturn(vXGroupPermission);

		VXGroupPermission dbGroupPermission = xUserMgr
				.updateXGroupPermission(vXGroupPermission);
		Assert.assertNotNull(dbGroupPermission);
		Assert.assertEquals(dbGroupPermission, vXGroupPermission);
		Assert.assertEquals(dbGroupPermission.getId(),
				vXGroupPermission.getId());
		Assert.assertEquals(dbGroupPermission.getGroupName(),
				vXGroupPermission.getGroupName());
		Assert.assertEquals(dbGroupPermission.getOwner(),
				vXGroupPermission.getOwner());
		Assert.assertEquals(dbGroupPermission.getUpdatedBy(),
				vXGroupPermission.getUpdatedBy());
		Assert.assertEquals(dbGroupPermission.getCreateDate(),
				vXGroupPermission.getCreateDate());
		Assert.assertEquals(dbGroupPermission.getGroupId(),
				vXGroupPermission.getGroupId());
		Assert.assertEquals(dbGroupPermission.getIsAllowed(),
				vXGroupPermission.getIsAllowed());
		Assert.assertEquals(dbGroupPermission.getModuleId(),
				vXGroupPermission.getModuleId());
		Assert.assertEquals(dbGroupPermission.getUpdateDate(),
				vXGroupPermission.getUpdateDate());

		Mockito.verify(xGroupPermissionService).updateResource(
				vXGroupPermission);
	}

	@Test
	public void test42deleteXGroupPermission() {

		XXGroupPermissionDao xGrpPermDao = Mockito.mock(XXGroupPermissionDao.class);
		XXGroupPermission xGrpPerm = Mockito.mock(XXGroupPermission.class);

		Mockito.when(daoManager.getXXGroupPermission()).thenReturn(xGrpPermDao);
		Mockito.when(daoManager.getXXGroupPermission().getById(1L)).thenReturn(xGrpPerm);

		XXGroupUserDao xGrpUserDao = Mockito.mock(XXGroupUserDao.class);
		Mockito.when(daoManager.getXXGroupUser()).thenReturn(xGrpUserDao);
		
		Mockito.when(xGroupPermissionService.deleteResource(1L)).thenReturn(true);
		xUserMgr.deleteXGroupPermission(1L, true);
		Mockito.verify(xGroupPermissionService).deleteResource(1L);
	}

	/*@Test
	public void test43checkPermissionRoleByGivenUrls() {
		XXModuleDefDao value = Mockito.mock(XXModuleDefDao.class);
		XXPortalUserRoleDao xPortalUserRoleDao = Mockito
				.mock(XXPortalUserRoleDao.class);

		List<String> lsvalue = new ArrayList<String>();
		List<XXPortalUserRole> xPortalUserRolesList = new ArrayList<XXPortalUserRole>();
		XXPortalUserRole xPortalUserRole = new XXPortalUserRole();
		xPortalUserRole.setAddedByUserId(userId);
		xPortalUserRole.setCreateTime(new Date());
		xPortalUserRole.setId(userId);
		xPortalUserRole.setStatus(0);
		xPortalUserRole.setUpdatedByUserId(userId);
		xPortalUserRole.setUserId(userId);
		xPortalUserRole.setUserRole("admin");
		xPortalUserRolesList.add(xPortalUserRole);
		Mockito.when(daoManager.getXXModuleDef()).thenReturn(value);
		Mockito.when(value.findModuleURLOfPemittedModules(null)).thenReturn(
				lsvalue);
		Mockito.when(daoManager.getXXPortalUserRole()).thenReturn(
				xPortalUserRoleDao);
		Mockito.when(xPortalUserRoleDao.findByUserId(null)).thenReturn(
				xPortalUserRolesList);
		String enteredURL = "";
		String method = "";
		xUserMgr.checkPermissionRoleByGivenUrls(enteredURL, method);
		Mockito.verify(daoManager).getXXModuleDef();
		Mockito.verify(value).findModuleURLOfPemittedModules(null);
		Mockito.verify(daoManager).getXXPortalUserRole();
		Mockito.verify(xPortalUserRoleDao).findByUserId(null);
	}*/
	
	@Test
	public void test44getGroupsForUser() {
		VXUser vxUser = vxUser();
		String userName = "test";
		Mockito.when(xUserService.getXUserByUserName(userName)).thenReturn(
				vxUser);
                XXModuleDefDao modDef = Mockito.mock(XXModuleDefDao.class);
                Mockito.when(daoManager.getXXModuleDef()).thenReturn(modDef);
                List<String> lstModule = new ArrayList<String>();
                lstModule.add(RangerConstants.MODULE_USER_GROUPS);
                Mockito.when(
                                modDef.findAccessibleModulesByUserId(Mockito.anyLong(),
                                                Mockito.anyLong())).thenReturn(lstModule);
		Set<String> list = xUserMgr.getGroupsForUser(userName);
		Assert.assertNotNull(list);
                Mockito.verify(xUserService, Mockito.atLeast(2)).getXUserByUserName(
                                userName);
                Mockito.verify(modDef).findAccessibleModulesByUserId(Mockito.anyLong(),
                                Mockito.anyLong());
	}

	@Test
	public void test45setUserRolesByExternalID() {
		setup();
		XXPortalUserRoleDao xPortalUserRoleDao = Mockito
				.mock(XXPortalUserRoleDao.class);
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXUserPermissionDao xUserPermissionDao = Mockito
				.mock(XXUserPermissionDao.class);
		XXGroupPermissionDao xGroupPermissionDao = Mockito
				.mock(XXGroupPermissionDao.class);
		XXModuleDefDao xModuleDefDao = Mockito.mock(XXModuleDefDao.class);

		VXUser vXUser = vxUser();
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
		Mockito.when(xUserMgr.getXUser(userId)).thenReturn(vXUser);
		Mockito.when(userMgr.getUserProfileByLoginId(vXUser.getName()))
				.thenReturn(userProfile);
		VXStringList vXStringList = xUserMgr.setUserRolesByExternalID(userId,
				vStringRolesList);
		Assert.assertNotNull(vXStringList);
	}

	@Test
	public void test46setUserRolesByName() {
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
		Mockito.when(userMgr.getUserProfileByLoginId(userProfile.getLoginId()))
				.thenReturn(userProfile);
		VXStringList vXStringList = xUserMgr.setUserRolesByName(
				userProfile.getLoginId(), vStringRolesList);
		Assert.assertNotNull(vXStringList);
	}

	@Test
	public void test47getUserRolesByExternalID() {
		setup();
		XXPortalUserRoleDao xPortalUserRoleDao = Mockito
				.mock(XXPortalUserRoleDao.class);
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXUserPermissionDao xUserPermissionDao = Mockito
				.mock(XXUserPermissionDao.class);
		XXGroupPermissionDao xGroupPermissionDao = Mockito
				.mock(XXGroupPermissionDao.class);
		XXModuleDefDao xModuleDefDao = Mockito.mock(XXModuleDefDao.class);

		VXUser vXUser = vxUser();
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
		Mockito.when(xUserMgr.getXUser(userId)).thenReturn(vXUser);
		Mockito.when(userMgr.getUserProfileByLoginId(vXUser.getName()))
				.thenReturn(userProfile);
		VXStringList vXStringList = xUserMgr.getUserRolesByExternalID(userId);
		Assert.assertNotNull(vXStringList);
	}

	@Test
	public void test48getUserRolesByName() {
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
		Collection<String> userRoleList = new ArrayList<String>();
		userRoleList.add("ROLE_USER");
		userProfile.setUserRoleList(userRoleList);

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
		Mockito.when(userMgr.getUserProfileByLoginId(userProfile.getLoginId()))
				.thenReturn(userProfile);
		VXStringList vXStringList = xUserMgr.getUserRolesByName(userProfile
				.getLoginId());
		Assert.assertNotNull(vXStringList);
	}
}
