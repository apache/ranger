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
package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAccessTypeDefDao;
import org.apache.ranger.db.XXGroupDao;
import org.apache.ranger.db.XXPolicyConditionDefDao;
import org.apache.ranger.db.XXPolicyItemAccessDao;
import org.apache.ranger.db.XXPolicyItemConditionDao;
import org.apache.ranger.db.XXPolicyItemDao;
import org.apache.ranger.db.XXPolicyResourceDao;
import org.apache.ranger.db.XXPolicyResourceMapDao;
import org.apache.ranger.db.XXPortalUserDao;
import org.apache.ranger.db.XXResourceDefDao;
import org.apache.ranger.db.XXServiceConfigMapDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.db.XXUserDao;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPolicyItem;
import org.apache.ranger.entity.XXPolicyItemAccess;
import org.apache.ranger.entity.XXPolicyItemCondition;
import org.apache.ranger.entity.XXPolicyResource;
import org.apache.ranger.entity.XXPolicyResourceMap;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
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
public class TestRangerPolicyService {

	private static Long Id = 8L;

	@InjectMocks
	RangerPolicyService policyService = new RangerPolicyService();

	@Mock
	RangerDaoManager daoManager;

	@Mock
	RangerServiceService svcService;

	@Mock
	JSONUtil jsonUtil;

	@Mock
	RangerServiceDefService serviceDefService;

	@Mock
	StringUtil stringUtil;

	@Mock
	XUserService xUserService;

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

	private XXPolicy policy() {
		XXPolicy xxPolicy = new XXPolicy();
		xxPolicy.setId(Id);
		xxPolicy.setName("HDFS_1-1-20150316062453");
		xxPolicy.setAddedByUserId(Id);
		xxPolicy.setCreateTime(new Date());
		xxPolicy.setDescription("test");
		xxPolicy.setIsAuditEnabled(false);
		xxPolicy.setIsEnabled(false);
		xxPolicy.setService(1L);
		xxPolicy.setUpdatedByUserId(Id);
		xxPolicy.setUpdateTime(new Date());
		return xxPolicy;
	}

	private XXService xService() {
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
		xService.setVersion(1L);

		return xService;
	}

	@Test
	public void test1ValidateForCreate() {
		RangerPolicy rangerPolicy = rangerPolicy();
		policyService.validateForCreate(rangerPolicy);
		Assert.assertNotNull(rangerPolicy);
	}

	@Test
	public void test2ValidateForUpdate() {
		RangerPolicy rangerPolicy = rangerPolicy();
		XXPolicy policy = policy();
		policyService.validateForUpdate(rangerPolicy, policy);

		Assert.assertNotNull(rangerPolicy);
	}

	@Test
	public void test3PopulateViewBean() {
		XXServiceConfigMapDao xServiceConfigMapDao = Mockito
				.mock(XXServiceConfigMapDao.class);
		XXPortalUserDao xPortalUserDao = Mockito.mock(XXPortalUserDao.class);
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		XXResourceDefDao xResourceDefDao = Mockito.mock(XXResourceDefDao.class);
		XXPolicyResourceDao xPolicyResourceDao = Mockito
				.mock(XXPolicyResourceDao.class);
		XXPolicyResourceMapDao xPolicyResourceMapDao = Mockito
				.mock(XXPolicyResourceMapDao.class);
		XXPolicyItemDao xPolicyItemDao = Mockito.mock(XXPolicyItemDao.class);
		XXPolicyItemAccessDao xPolicyItemAccessDao = Mockito
				.mock(XXPolicyItemAccessDao.class);
		XXAccessTypeDefDao xAccessTypeDefDao = Mockito
				.mock(XXAccessTypeDefDao.class);
		XXAccessTypeDef xAccessTypeDef = Mockito.mock(XXAccessTypeDef.class);
		XXPolicyConditionDefDao xPolicyConditionDefDao = Mockito
				.mock(XXPolicyConditionDefDao.class);
		XXPolicyItemConditionDao xPolicyItemConditionDao = Mockito
				.mock(XXPolicyItemConditionDao.class);

		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXGroupDao xGroupDao = Mockito.mock(XXGroupDao.class);

		XXPolicy policy = policy();

		XXService xService = xService();
		String name = "fdfdfds";

		List<XXServiceConfigMap> svcConfigMapList = new ArrayList<XXServiceConfigMap>();
		XXServiceConfigMap xConfMap = new XXServiceConfigMap();
		xConfMap.setAddedByUserId(null);
		xConfMap.setConfigkey(name);
		xConfMap.setConfigvalue(name);
		xConfMap.setCreateTime(new Date());
		xConfMap.setServiceId(null);

		xConfMap.setUpdatedByUserId(null);
		xConfMap.setUpdateTime(new Date());
		svcConfigMapList.add(xConfMap);

		XXPortalUser tUser = new XXPortalUser();
		tUser.setAddedByUserId(Id);
		tUser.setCreateTime(new Date());
		tUser.setEmailAddress("test@gmail.com");
		tUser.setFirstName(name);
		tUser.setId(Id);
		tUser.setLastName(name);

		XXServiceDef xServiceDef = new XXServiceDef();
		xServiceDef.setAddedByUserId(Id);
		xServiceDef.setCreateTime(new Date());
		xServiceDef.setDescription("test");
		xServiceDef.setGuid("1427365526516_835_0");
		xServiceDef.setId(Id);

		List<XXResourceDef> resDefList = new ArrayList<XXResourceDef>();
		XXResourceDef resourceDef = new XXResourceDef();
		resourceDef.setAddedByUserId(Id);
		resourceDef.setCreateTime(new Date());
		resourceDef.setDefid(Id);
		resourceDef.setDescription("test");
		resourceDef.setId(Id);
		resDefList.add(resourceDef);

		XXPolicyResource policyResource = new XXPolicyResource();
		policyResource.setId(Id);
		policyResource.setCreateTime(new Date());
		policyResource.setAddedByUserId(Id);
		policyResource.setIsExcludes(false);
		policyResource.setIsRecursive(false);
		policyResource.setPolicyId(Id);
		policyResource.setResDefId(Id);
		policyResource.setUpdatedByUserId(Id);
		policyResource.setUpdateTime(new Date());

		List<XXPolicyResourceMap> policyResourceMapList = new ArrayList<XXPolicyResourceMap>();
		XXPolicyResourceMap policyResourceMap = new XXPolicyResourceMap();
		policyResourceMap.setAddedByUserId(Id);
		policyResourceMap.setCreateTime(new Date());
		policyResourceMap.setId(Id);
		policyResourceMap.setOrder(1);
		policyResourceMap.setResourceId(Id);
		policyResourceMap.setUpdatedByUserId(Id);
		policyResourceMap.setUpdateTime(new Date());
		policyResourceMap.setValue("1L");
		policyResourceMapList.add(policyResourceMap);

		List<XXPolicyItem> xPolicyItemList = new ArrayList<XXPolicyItem>();
		XXPolicyItem xPolicyItem = new XXPolicyItem();
		xPolicyItem.setDelegateAdmin(false);
		xPolicyItem.setAddedByUserId(null);
		xPolicyItem.setCreateTime(new Date());
		xPolicyItem.setGUID(null);
		xPolicyItem.setId(Id);
		xPolicyItem.setOrder(null);
		xPolicyItem.setPolicyId(Id);
		xPolicyItem.setUpdatedByUserId(null);
		xPolicyItem.setUpdateTime(new Date());
		xPolicyItemList.add(xPolicyItem);

		List<XXPolicyItemAccess> policyItemAccessList = new ArrayList<XXPolicyItemAccess>();
		XXPolicyItemAccess policyItemAccess = new XXPolicyItemAccess();
		policyItemAccess.setAddedByUserId(Id);
		policyItemAccess.setCreateTime(new Date());
		policyItemAccess.setPolicyitemid(Id);
		policyItemAccess.setId(Id);
		policyItemAccess.setOrder(1);
		policyItemAccess.setUpdatedByUserId(Id);
		policyItemAccess.setUpdateTime(new Date());
		policyItemAccessList.add(policyItemAccess);

		List<XXPolicyConditionDef> xConditionDefList = new ArrayList<XXPolicyConditionDef>();
		XXPolicyConditionDef policyConditionDefObj = new XXPolicyConditionDef();
		policyConditionDefObj.setAddedByUserId(Id);
		policyConditionDefObj.setCreateTime(new Date());
		policyConditionDefObj.setDefid(Id);
		policyConditionDefObj.setDescription("policy conditio");
		policyConditionDefObj.setId(Id);
		policyConditionDefObj.setName(name);
		policyConditionDefObj.setOrder(1);
		policyConditionDefObj.setLabel("label");
		xConditionDefList.add(policyConditionDefObj);

		List<XXPolicyItemCondition> policyItemConditionList = new ArrayList<XXPolicyItemCondition>();
		XXPolicyItemCondition policyItemCondition = new XXPolicyItemCondition();
		policyItemCondition.setAddedByUserId(Id);
		policyItemCondition.setCreateTime(new Date());
		policyItemCondition.setType(1L);
		policyItemCondition.setId(Id);
		policyItemCondition.setOrder(1);
		policyItemCondition.setPolicyItemId(Id);
		policyItemCondition.setUpdatedByUserId(Id);
		policyItemCondition.setUpdateTime(new Date());
		policyItemConditionList.add(policyItemCondition);

		List<String> usersList = new ArrayList<String>();
		List<String> groupsList = new ArrayList<String>();
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(xPortalUserDao);
		Mockito.when(xPortalUserDao.getById(Id)).thenReturn(tUser);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(
				xServiceDef);

		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);
		Mockito.when(xServiceConfigMapDao.findByServiceId(xService.getId()))
				.thenReturn(svcConfigMapList);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(policy.getService())).thenReturn(
				xService);

		Mockito.when(daoManager.getXXResourceDef()).thenReturn(xResourceDefDao);
		Mockito.when(xResourceDefDao.findByPolicyId(policy.getId()))
				.thenReturn(resDefList);

		Mockito.when(daoManager.getXXPolicyResource()).thenReturn(
				xPolicyResourceDao);
		Mockito.when(
				xPolicyResourceDao.findByResDefIdAndPolicyId(
						resourceDef.getId(), policy.getId())).thenReturn(
				policyResource);

		Mockito.when(daoManager.getXXPolicyResourceMap()).thenReturn(
				xPolicyResourceMapDao);
		Mockito.when(
				xPolicyResourceMapDao.findByPolicyResId(policyResource.getId()))
				.thenReturn(policyResourceMapList);

		Mockito.when(daoManager.getXXPolicyItem()).thenReturn(xPolicyItemDao);
		Mockito.when(xPolicyItemDao.findByPolicyId(policy.getId())).thenReturn(
				xPolicyItemList);

		Mockito.when(daoManager.getXXPolicyItemAccess()).thenReturn(
				xPolicyItemAccessDao);
		Mockito.when(
				xPolicyItemAccessDao.findByPolicyItemId(policyItemAccess
						.getId())).thenReturn(policyItemAccessList);

		Mockito.when(daoManager.getXXAccessTypeDef()).thenReturn(
				xAccessTypeDefDao);
		Mockito.when(xAccessTypeDefDao.getById(policyItemAccess.getType()))
				.thenReturn(xAccessTypeDef);

		Mockito.when(daoManager.getXXPolicyConditionDef()).thenReturn(
				xPolicyConditionDefDao);
		Mockito.when(
				xPolicyConditionDefDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(xConditionDefList);

		Mockito.when(daoManager.getXXPolicyItemCondition()).thenReturn(
				xPolicyItemConditionDao);
		Mockito.when(
				xPolicyItemConditionDao.findByPolicyItemAndDefId(
						xPolicyItem.getId(), policyConditionDefObj.getId()))
				.thenReturn(policyItemConditionList);

		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(usersList);

		Mockito.when(daoManager.getXXGroup()).thenReturn(xGroupDao);
		Mockito.when(xGroupDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(groupsList);

		RangerPolicy dbRangerPolicy = policyService.populateViewBean(policy);
		Assert.assertNotNull(dbRangerPolicy);
		Assert.assertEquals(dbRangerPolicy.getId(), policy.getId());
		Assert.assertEquals(dbRangerPolicy.getName(), policy.getName());

		Assert.assertEquals(dbRangerPolicy.getDescription(),
				policy.getDescription());
		Assert.assertEquals(dbRangerPolicy.getGuid(), policy.getGuid());

		Assert.assertEquals(dbRangerPolicy.getVersion(), policy.getVersion());
		Assert.assertEquals(dbRangerPolicy.getIsAuditEnabled(),
				policy.getIsAuditEnabled());

		Mockito.verify(daoManager).getXXService();
		Mockito.verify(daoManager).getXXResourceDef();
		Mockito.verify(daoManager).getXXPolicyResource();
		Mockito.verify(daoManager).getXXPolicyResourceMap();
		Mockito.verify(daoManager).getXXPolicyItem();
		Mockito.verify(daoManager).getXXPolicyItemAccess();
		Mockito.verify(daoManager).getXXAccessTypeDef();
		Mockito.verify(daoManager).getXXPolicyConditionDef();
		Mockito.verify(daoManager).getXXPolicyItemCondition();
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(daoManager).getXXGroup();
	}

	@Test
	public void test4GetPolicyItemListForXXPolicy() {

		XXPolicyItemDao xPolicyItemDao = Mockito.mock(XXPolicyItemDao.class);
		XXPolicyItemAccessDao xPolicyItemAccessDao = Mockito
				.mock(XXPolicyItemAccessDao.class);
		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXGroupDao xGroupDao = Mockito.mock(XXGroupDao.class);
		XXAccessTypeDefDao xAccessTypeDefDao = Mockito
				.mock(XXAccessTypeDefDao.class);
		XXAccessTypeDef xAccessTypeDef = Mockito.mock(XXAccessTypeDef.class);
		XXPolicyConditionDefDao xPolicyConditionDefDao = Mockito
				.mock(XXPolicyConditionDefDao.class);
		XXPolicyItemConditionDao xPolicyItemConditionDao = Mockito
				.mock(XXPolicyItemConditionDao.class);

		XXPolicy policy = policy();
		String name = "fdfdfds";

		List<XXPolicyItem> xPolicyItemList = new ArrayList<XXPolicyItem>();
		XXPolicyItem xPolicyItem = new XXPolicyItem();
		xPolicyItem.setDelegateAdmin(false);
		xPolicyItem.setAddedByUserId(null);
		xPolicyItem.setCreateTime(new Date());
		xPolicyItem.setGUID(null);
		xPolicyItem.setId(Id);
		xPolicyItem.setOrder(null);
		xPolicyItem.setPolicyId(Id);
		xPolicyItem.setUpdatedByUserId(null);
		xPolicyItem.setUpdateTime(new Date());
		xPolicyItemList.add(xPolicyItem);

		List<XXPolicyItemAccess> policyItemAccessList = new ArrayList<XXPolicyItemAccess>();
		XXPolicyItemAccess policyItemAccess = new XXPolicyItemAccess();
		policyItemAccess.setAddedByUserId(Id);
		policyItemAccess.setCreateTime(new Date());
		policyItemAccess.setPolicyitemid(Id);
		policyItemAccess.setId(Id);
		policyItemAccess.setOrder(1);
		policyItemAccess.setUpdatedByUserId(Id);
		policyItemAccess.setUpdateTime(new Date());
		policyItemAccessList.add(policyItemAccess);

		List<XXResourceDef> resDefList = new ArrayList<XXResourceDef>();
		XXResourceDef resourceDef = new XXResourceDef();
		resourceDef.setAddedByUserId(Id);
		resourceDef.setCreateTime(new Date());
		resourceDef.setDefid(Id);
		resourceDef.setDescription("test");
		resourceDef.setId(Id);
		resDefList.add(resourceDef);

		XXPolicyResource policyResource = new XXPolicyResource();
		policyResource.setId(Id);
		policyResource.setCreateTime(new Date());
		policyResource.setAddedByUserId(Id);
		policyResource.setIsExcludes(false);
		policyResource.setIsRecursive(false);
		policyResource.setPolicyId(Id);
		policyResource.setResDefId(Id);
		policyResource.setUpdatedByUserId(Id);
		policyResource.setUpdateTime(new Date());

		List<XXPolicyResourceMap> policyResourceMapList = new ArrayList<XXPolicyResourceMap>();
		XXPolicyResourceMap policyResourceMap = new XXPolicyResourceMap();
		policyResourceMap.setAddedByUserId(Id);
		policyResourceMap.setCreateTime(new Date());
		policyResourceMap.setId(Id);
		policyResourceMap.setOrder(1);
		policyResourceMap.setResourceId(Id);
		policyResourceMap.setUpdatedByUserId(Id);
		policyResourceMap.setUpdateTime(new Date());
		policyResourceMap.setValue("1L");
		policyResourceMapList.add(policyResourceMap);

		List<XXPolicyConditionDef> xConditionDefList = new ArrayList<XXPolicyConditionDef>();
		XXPolicyConditionDef policyConditionDefObj = new XXPolicyConditionDef();
		policyConditionDefObj.setAddedByUserId(Id);
		policyConditionDefObj.setCreateTime(new Date());
		policyConditionDefObj.setDefid(Id);
		policyConditionDefObj.setDescription("policy conditio");
		policyConditionDefObj.setId(Id);
		policyConditionDefObj.setName(name);
		policyConditionDefObj.setOrder(1);
		policyConditionDefObj.setLabel("label");
		xConditionDefList.add(policyConditionDefObj);

		List<XXPolicyItemCondition> policyItemConditionList = new ArrayList<XXPolicyItemCondition>();
		XXPolicyItemCondition policyItemCondition = new XXPolicyItemCondition();
		policyItemCondition.setAddedByUserId(Id);
		policyItemCondition.setCreateTime(new Date());
		policyItemCondition.setType(1L);
		policyItemCondition.setId(Id);
		policyItemCondition.setOrder(1);
		policyItemCondition.setPolicyItemId(Id);
		policyItemCondition.setUpdatedByUserId(Id);
		policyItemCondition.setUpdateTime(new Date());
		policyItemConditionList.add(policyItemCondition);

		List<String> usersList = new ArrayList<String>();
		List<String> groupsList = new ArrayList<String>();

		Mockito.when(daoManager.getXXPolicyItem()).thenReturn(xPolicyItemDao);
		Mockito.when(xPolicyItemDao.findByPolicyId(policy.getId())).thenReturn(
				xPolicyItemList);

		Mockito.when(daoManager.getXXPolicyItemAccess()).thenReturn(
				xPolicyItemAccessDao);
		Mockito.when(xPolicyItemAccessDao.findByPolicyItemId(policy.getId()))
				.thenReturn(policyItemAccessList);

		Mockito.when(daoManager.getXXAccessTypeDef()).thenReturn(
				xAccessTypeDefDao);
		Mockito.when(xAccessTypeDefDao.getById(policyItemAccess.getType()))
				.thenReturn(xAccessTypeDef);

		Mockito.when(daoManager.getXXPolicyConditionDef()).thenReturn(
				xPolicyConditionDefDao);
		Mockito.when(
				xPolicyConditionDefDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(xConditionDefList);

		Mockito.when(daoManager.getXXPolicyItemCondition()).thenReturn(
				xPolicyItemConditionDao);
		Mockito.when(
				xPolicyItemConditionDao.findByPolicyItemAndDefId(
						xPolicyItem.getId(), policyConditionDefObj.getId()))
				.thenReturn(policyItemConditionList);

		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(usersList);

		Mockito.when(daoManager.getXXGroup()).thenReturn(xGroupDao);
		Mockito.when(xGroupDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(groupsList);

		List<RangerPolicyItem> dbRangerPolicyItem = policyService
				.getPolicyItemListForXXPolicy(policy);
		Assert.assertNotNull(dbRangerPolicyItem);

		Mockito.verify(daoManager).getXXPolicyItemAccess();
		Mockito.verify(daoManager).getXXAccessTypeDef();
		Mockito.verify(daoManager).getXXPolicyConditionDef();
		Mockito.verify(daoManager).getXXPolicyItemCondition();
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(daoManager).getXXGroup();
	}

	@Test
	public void test5PopulateXXToRangerPolicyItem() {
		String name = "fdfdfds";

		XXPolicyItemAccessDao xPolicyItemAccessDao = Mockito
				.mock(XXPolicyItemAccessDao.class);
		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXGroupDao xGroupDao = Mockito.mock(XXGroupDao.class);
		XXAccessTypeDefDao xAccessTypeDefDao = Mockito
				.mock(XXAccessTypeDefDao.class);
		XXAccessTypeDef xAccessTypeDef = Mockito.mock(XXAccessTypeDef.class);
		XXPolicyConditionDefDao xPolicyConditionDefDao = Mockito
				.mock(XXPolicyConditionDefDao.class);
		XXPolicyItemConditionDao xPolicyItemConditionDao = Mockito
				.mock(XXPolicyItemConditionDao.class);

		List<XXPolicyItem> xPolicyItemList = new ArrayList<XXPolicyItem>();
		XXPolicyItem xPolicyItem = new XXPolicyItem();
		xPolicyItem.setDelegateAdmin(false);
		xPolicyItem.setAddedByUserId(null);
		xPolicyItem.setCreateTime(new Date());
		xPolicyItem.setGUID(null);
		xPolicyItem.setId(Id);
		xPolicyItem.setOrder(null);
		xPolicyItem.setPolicyId(Id);
		xPolicyItem.setUpdatedByUserId(null);
		xPolicyItem.setUpdateTime(new Date());
		xPolicyItemList.add(xPolicyItem);

		List<XXPolicyItemAccess> policyItemAccessList = new ArrayList<XXPolicyItemAccess>();
		XXPolicyItemAccess policyItemAccess = new XXPolicyItemAccess();
		policyItemAccess.setAddedByUserId(Id);
		policyItemAccess.setCreateTime(new Date());
		policyItemAccess.setPolicyitemid(Id);
		policyItemAccess.setId(Id);
		policyItemAccess.setOrder(1);
		policyItemAccess.setUpdatedByUserId(Id);
		policyItemAccess.setUpdateTime(new Date());
		policyItemAccessList.add(policyItemAccess);

		List<XXPolicyConditionDef> xConditionDefList = new ArrayList<XXPolicyConditionDef>();
		XXPolicyConditionDef policyConditionDefObj = new XXPolicyConditionDef();
		policyConditionDefObj.setAddedByUserId(Id);
		policyConditionDefObj.setCreateTime(new Date());
		policyConditionDefObj.setDefid(Id);
		policyConditionDefObj.setDescription("policy conditio");
		policyConditionDefObj.setId(Id);
		policyConditionDefObj.setName(name);
		policyConditionDefObj.setOrder(1);
		policyConditionDefObj.setLabel("label");
		xConditionDefList.add(policyConditionDefObj);

		List<XXPolicyItemCondition> policyItemConditionList = new ArrayList<XXPolicyItemCondition>();
		XXPolicyItemCondition policyItemCondition = new XXPolicyItemCondition();
		policyItemCondition.setAddedByUserId(Id);
		policyItemCondition.setCreateTime(new Date());
		policyItemCondition.setType(1L);
		policyItemCondition.setId(Id);
		policyItemCondition.setOrder(1);
		policyItemCondition.setPolicyItemId(Id);
		policyItemCondition.setUpdatedByUserId(Id);
		policyItemCondition.setUpdateTime(new Date());
		policyItemConditionList.add(policyItemCondition);

		List<String> usersList = new ArrayList<String>();
		List<String> groupsList = new ArrayList<String>();

		Mockito.when(daoManager.getXXPolicyItemAccess()).thenReturn(
				xPolicyItemAccessDao);
		Mockito.when(xPolicyItemAccessDao.findByPolicyItemId(Id)).thenReturn(
				policyItemAccessList);

		Mockito.when(daoManager.getXXAccessTypeDef()).thenReturn(
				xAccessTypeDefDao);
		Mockito.when(xAccessTypeDefDao.getById(policyItemAccess.getType()))
				.thenReturn(xAccessTypeDef);

		Mockito.when(daoManager.getXXPolicyConditionDef()).thenReturn(
				xPolicyConditionDefDao);
		Mockito.when(
				xPolicyConditionDefDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(xConditionDefList);

		Mockito.when(daoManager.getXXPolicyItemCondition()).thenReturn(
				xPolicyItemConditionDao);
		Mockito.when(
				xPolicyItemConditionDao.findByPolicyItemAndDefId(
						xPolicyItem.getId(), policyConditionDefObj.getId()))
				.thenReturn(policyItemConditionList);

		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(usersList);

		Mockito.when(daoManager.getXXGroup()).thenReturn(xGroupDao);
		Mockito.when(xGroupDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(groupsList);

		RangerPolicyItem dbRangerPolicyItem = policyService
				.populateXXToRangerPolicyItem(xPolicyItem);
		Assert.assertNotNull(dbRangerPolicyItem);

		Mockito.verify(daoManager).getXXPolicyItemAccess();
		Mockito.verify(daoManager).getXXAccessTypeDef();
		Mockito.verify(daoManager).getXXPolicyConditionDef();
		Mockito.verify(daoManager).getXXPolicyItemCondition();
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(daoManager).getXXGroup();
	}

	@Test
	public void test6GetResourcesForXXPolicy() {

		XXResourceDefDao xResourceDefDao = Mockito.mock(XXResourceDefDao.class);
		XXPolicyResourceDao xPolicyResourceDao = Mockito
				.mock(XXPolicyResourceDao.class);
		XXPolicyResourceMapDao xPolicyResourceMapDao = Mockito
				.mock(XXPolicyResourceMapDao.class);

		XXPolicy policy = policy();

		List<XXResourceDef> resDefList = new ArrayList<XXResourceDef>();
		XXResourceDef resourceDef = new XXResourceDef();
		resourceDef.setAddedByUserId(Id);
		resourceDef.setCreateTime(new Date());
		resourceDef.setDefid(Id);
		resourceDef.setDescription("test");
		resourceDef.setId(Id);
		resDefList.add(resourceDef);

		XXPolicyResource policyResource = new XXPolicyResource();
		policyResource.setId(Id);
		policyResource.setCreateTime(new Date());
		policyResource.setAddedByUserId(Id);
		policyResource.setIsExcludes(false);
		policyResource.setIsRecursive(false);
		policyResource.setPolicyId(Id);
		policyResource.setResDefId(Id);
		policyResource.setUpdatedByUserId(Id);
		policyResource.setUpdateTime(new Date());

		List<XXPolicyResourceMap> policyResourceMapList = new ArrayList<XXPolicyResourceMap>();
		XXPolicyResourceMap policyResourceMap = new XXPolicyResourceMap();
		policyResourceMap.setAddedByUserId(Id);
		policyResourceMap.setCreateTime(new Date());
		policyResourceMap.setId(Id);
		policyResourceMap.setOrder(1);
		policyResourceMap.setResourceId(Id);
		policyResourceMap.setUpdatedByUserId(Id);
		policyResourceMap.setUpdateTime(new Date());
		policyResourceMap.setValue("1L");
		policyResourceMapList.add(policyResourceMap);

		Mockito.when(daoManager.getXXResourceDef()).thenReturn(xResourceDefDao);
		Mockito.when(xResourceDefDao.findByPolicyId(policy.getId()))
				.thenReturn(resDefList);

		Mockito.when(daoManager.getXXPolicyResource()).thenReturn(
				xPolicyResourceDao);
		Mockito.when(
				xPolicyResourceDao.findByResDefIdAndPolicyId(
						resourceDef.getId(), policy.getId())).thenReturn(
				policyResource);

		Mockito.when(daoManager.getXXPolicyResourceMap()).thenReturn(
				xPolicyResourceMapDao);
		Mockito.when(
				xPolicyResourceMapDao.findByPolicyResId(policyResource.getId()))
				.thenReturn(policyResourceMapList);

		Map<String, RangerPolicyResource> dbListMap = policyService
				.getResourcesForXXPolicy(policy);
		Assert.assertNotNull(dbListMap);

		Mockito.verify(daoManager).getXXResourceDef();
		Mockito.verify(daoManager).getXXPolicyResource();
		Mockito.verify(daoManager).getXXPolicyResourceMap();
	}

	@Test
	public void test7GetPopulatedViewObject() {
		XXPortalUserDao xPortalUserDao = Mockito.mock(XXPortalUserDao.class);
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceConfigMapDao xServiceConfigMapDao = Mockito
				.mock(XXServiceConfigMapDao.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		XXResourceDefDao xResourceDefDao = Mockito.mock(XXResourceDefDao.class);
		XXPolicyResourceDao xPolicyResourceDao = Mockito
				.mock(XXPolicyResourceDao.class);
		XXPolicyResourceMapDao xPolicyResourceMapDao = Mockito
				.mock(XXPolicyResourceMapDao.class);
		XXPolicyItemDao xPolicyItemDao = Mockito.mock(XXPolicyItemDao.class);
		XXPolicyItemAccessDao xPolicyItemAccessDao = Mockito
				.mock(XXPolicyItemAccessDao.class);
		XXAccessTypeDefDao xAccessTypeDefDao = Mockito
				.mock(XXAccessTypeDefDao.class);
		XXAccessTypeDef xAccessTypeDef = Mockito.mock(XXAccessTypeDef.class);
		XXPolicyConditionDefDao xPolicyConditionDefDao = Mockito
				.mock(XXPolicyConditionDefDao.class);
		XXPolicyItemConditionDao xPolicyItemConditionDao = Mockito
				.mock(XXPolicyItemConditionDao.class);
		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXGroupDao xGroupDao = Mockito.mock(XXGroupDao.class);

		XXPolicy policy = policy();
		XXService xService = xService();
		String name = "fdfdfds";

		XXPortalUser tUser = new XXPortalUser();
		tUser.setAddedByUserId(Id);
		tUser.setCreateTime(new Date());
		tUser.setEmailAddress("test@gmail.com");
		tUser.setFirstName(name);
		tUser.setId(Id);
		tUser.setLastName(name);

		XXServiceDef xServiceDef = new XXServiceDef();
		xServiceDef.setAddedByUserId(Id);
		xServiceDef.setCreateTime(new Date());
		xServiceDef.setDescription("test");
		xServiceDef.setGuid("1427365526516_835_0");
		xServiceDef.setId(Id);

		List<XXServiceConfigMap> srcConfigMapList = new ArrayList<XXServiceConfigMap>();
		XXServiceConfigMap xConfMap = new XXServiceConfigMap();
		xConfMap.setAddedByUserId(null);
		xConfMap.setConfigkey(name);
		xConfMap.setConfigvalue(name);
		xConfMap.setCreateTime(new Date());
		xConfMap.setServiceId(null);
		xConfMap.setUpdatedByUserId(null);
		xConfMap.setUpdateTime(new Date());
		srcConfigMapList.add(xConfMap);

		List<XXResourceDef> resDefList = new ArrayList<XXResourceDef>();
		XXResourceDef resourceDef = new XXResourceDef();
		resourceDef.setAddedByUserId(Id);
		resourceDef.setCreateTime(new Date());
		resourceDef.setDefid(Id);
		resourceDef.setDescription("test");
		resourceDef.setId(Id);
		resDefList.add(resourceDef);

		XXPolicyResource policyResource = new XXPolicyResource();
		policyResource.setId(Id);
		policyResource.setCreateTime(new Date());
		policyResource.setAddedByUserId(Id);
		policyResource.setIsExcludes(false);
		policyResource.setIsRecursive(false);
		policyResource.setPolicyId(Id);
		policyResource.setResDefId(Id);
		policyResource.setUpdatedByUserId(Id);
		policyResource.setUpdateTime(new Date());

		List<XXPolicyResourceMap> policyResourceMapList = new ArrayList<XXPolicyResourceMap>();
		XXPolicyResourceMap policyResourceMap = new XXPolicyResourceMap();
		policyResourceMap.setAddedByUserId(Id);
		policyResourceMap.setCreateTime(new Date());
		policyResourceMap.setId(Id);
		policyResourceMap.setOrder(1);
		policyResourceMap.setResourceId(Id);
		policyResourceMap.setUpdatedByUserId(Id);
		policyResourceMap.setUpdateTime(new Date());
		policyResourceMap.setValue("1L");
		policyResourceMapList.add(policyResourceMap);

		List<XXPolicyItem> xPolicyItemList = new ArrayList<XXPolicyItem>();
		XXPolicyItem xPolicyItem = new XXPolicyItem();
		xPolicyItem.setDelegateAdmin(false);
		xPolicyItem.setAddedByUserId(null);
		xPolicyItem.setCreateTime(new Date());
		xPolicyItem.setGUID(null);
		xPolicyItem.setId(Id);
		xPolicyItem.setOrder(null);
		xPolicyItem.setPolicyId(Id);
		xPolicyItem.setUpdatedByUserId(null);
		xPolicyItem.setUpdateTime(new Date());
		xPolicyItemList.add(xPolicyItem);

		List<XXPolicyItemAccess> policyItemAccessList = new ArrayList<XXPolicyItemAccess>();
		XXPolicyItemAccess policyItemAccess = new XXPolicyItemAccess();
		policyItemAccess.setAddedByUserId(Id);
		policyItemAccess.setCreateTime(new Date());
		policyItemAccess.setPolicyitemid(Id);
		policyItemAccess.setId(Id);
		policyItemAccess.setOrder(1);
		policyItemAccess.setUpdatedByUserId(Id);
		policyItemAccess.setUpdateTime(new Date());
		policyItemAccessList.add(policyItemAccess);

		List<XXPolicyConditionDef> xConditionDefList = new ArrayList<XXPolicyConditionDef>();
		XXPolicyConditionDef policyConditionDefObj = new XXPolicyConditionDef();
		policyConditionDefObj.setAddedByUserId(Id);
		policyConditionDefObj.setCreateTime(new Date());
		policyConditionDefObj.setDefid(Id);
		policyConditionDefObj.setDescription("policy conditio");
		policyConditionDefObj.setId(Id);
		policyConditionDefObj.setName(name);
		policyConditionDefObj.setOrder(1);
		policyConditionDefObj.setLabel("label");
		xConditionDefList.add(policyConditionDefObj);

		List<XXPolicyItemCondition> policyItemConditionList = new ArrayList<XXPolicyItemCondition>();
		XXPolicyItemCondition policyItemCondition = new XXPolicyItemCondition();
		policyItemCondition.setAddedByUserId(Id);
		policyItemCondition.setCreateTime(new Date());
		policyItemCondition.setType(1L);
		policyItemCondition.setId(Id);
		policyItemCondition.setOrder(1);
		policyItemCondition.setPolicyItemId(Id);
		policyItemCondition.setUpdatedByUserId(Id);
		policyItemCondition.setUpdateTime(new Date());
		policyItemConditionList.add(policyItemCondition);

		List<String> usersList = new ArrayList<String>();
		List<String> groupsList = new ArrayList<String>();

		Mockito.when(daoManager.getXXPortalUser()).thenReturn(xPortalUserDao);
		Mockito.when(xPortalUserDao.getById(Id)).thenReturn(tUser);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(
				xServiceDef);

		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);
		Mockito.when(xServiceConfigMapDao.findByServiceId(xService.getId()))
				.thenReturn(srcConfigMapList);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(policy.getService())).thenReturn(
				xService);

		Mockito.when(daoManager.getXXResourceDef()).thenReturn(xResourceDefDao);
		Mockito.when(xResourceDefDao.findByPolicyId(policy.getId()))
				.thenReturn(resDefList);

		Mockito.when(daoManager.getXXPolicyResource()).thenReturn(
				xPolicyResourceDao);
		Mockito.when(
				xPolicyResourceDao.findByResDefIdAndPolicyId(
						resourceDef.getId(), policy.getId())).thenReturn(
				policyResource);

		Mockito.when(daoManager.getXXPolicyResourceMap()).thenReturn(
				xPolicyResourceMapDao);
		Mockito.when(
				xPolicyResourceMapDao.findByPolicyResId(policyResource.getId()))
				.thenReturn(policyResourceMapList);

		Mockito.when(daoManager.getXXPolicyItem()).thenReturn(xPolicyItemDao);
		Mockito.when(xPolicyItemDao.findByPolicyId(policy.getId())).thenReturn(
				xPolicyItemList);

		Mockito.when(daoManager.getXXPolicyItemAccess()).thenReturn(
				xPolicyItemAccessDao);
		Mockito.when(xPolicyItemAccessDao.findByPolicyItemId(policy.getId()))
				.thenReturn(policyItemAccessList);

		Mockito.when(daoManager.getXXAccessTypeDef()).thenReturn(
				xAccessTypeDefDao);
		Mockito.when(xAccessTypeDefDao.getById(policyItemAccess.getType()))
				.thenReturn(xAccessTypeDef);

		Mockito.when(daoManager.getXXPolicyConditionDef()).thenReturn(
				xPolicyConditionDefDao);
		Mockito.when(
				xPolicyConditionDefDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(xConditionDefList);

		Mockito.when(daoManager.getXXPolicyItemCondition()).thenReturn(
				xPolicyItemConditionDao);
		Mockito.when(
				xPolicyItemConditionDao.findByPolicyItemAndDefId(
						xPolicyItem.getId(), policyConditionDefObj.getId()))
				.thenReturn(policyItemConditionList);

		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(usersList);

		Mockito.when(daoManager.getXXGroup()).thenReturn(xGroupDao);
		Mockito.when(xGroupDao.findByPolicyItemId(xPolicyItem.getId()))
				.thenReturn(groupsList);

		RangerPolicy dbRangerPolicy = policyService
				.getPopulatedViewObject(policy);
		Assert.assertNotNull(dbRangerPolicy);
		Assert.assertEquals(dbRangerPolicy.getId(), policy.getId());
		Assert.assertEquals(dbRangerPolicy.getName(), policy.getName());
		Assert.assertEquals(dbRangerPolicy.getDescription(),
				policy.getDescription());
		Assert.assertEquals(dbRangerPolicy.getGuid(), policy.getGuid());
		Assert.assertEquals(dbRangerPolicy.getVersion(), policy.getVersion());
		Assert.assertEquals(dbRangerPolicy.getIsAuditEnabled(),
				policy.getIsAuditEnabled());
		Mockito.verify(daoManager).getXXPolicyItemAccess();
		Mockito.verify(daoManager).getXXAccessTypeDef();
		Mockito.verify(daoManager).getXXPolicyConditionDef();
		Mockito.verify(daoManager).getXXPolicyItemCondition();
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(daoManager).getXXGroup();
	}

	@Test
	public void test8getTransactionLog() {
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		RangerPolicy rangerPolicy = rangerPolicy();
		XXService xService = xService();

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(rangerPolicy.getService()))
				.thenReturn(xService);

		List<XXTrxLog> dbXXTrxLogList = policyService.getTransactionLog(
				rangerPolicy, 1);
		Assert.assertNotNull(dbXXTrxLogList);
	}

}
