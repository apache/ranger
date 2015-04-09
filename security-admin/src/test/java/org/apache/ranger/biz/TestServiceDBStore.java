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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAccessTypeDefDao;
import org.apache.ranger.db.XXAccessTypeDefGrantsDao;
import org.apache.ranger.db.XXContextEnricherDefDao;
import org.apache.ranger.db.XXDataHistDao;
import org.apache.ranger.db.XXEnumDefDao;
import org.apache.ranger.db.XXEnumElementDefDao;
import org.apache.ranger.db.XXPolicyConditionDefDao;
import org.apache.ranger.db.XXPolicyDao;
import org.apache.ranger.db.XXPolicyItemAccessDao;
import org.apache.ranger.db.XXPolicyItemConditionDao;
import org.apache.ranger.db.XXPolicyItemDao;
import org.apache.ranger.db.XXPolicyItemGroupPermDao;
import org.apache.ranger.db.XXPolicyItemUserPermDao;
import org.apache.ranger.db.XXPolicyResourceDao;
import org.apache.ranger.db.XXPolicyResourceMapDao;
import org.apache.ranger.db.XXResourceDefDao;
import org.apache.ranger.db.XXServiceConfigDefDao;
import org.apache.ranger.db.XXServiceConfigMapDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.db.XXUserDao;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXAccessTypeDefGrants;
import org.apache.ranger.entity.XXContextEnricherDef;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXDataHist;
import org.apache.ranger.entity.XXEnumDef;
import org.apache.ranger.entity.XXEnumElementDef;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPolicyItem;
import org.apache.ranger.entity.XXPolicyItemAccess;
import org.apache.ranger.entity.XXPolicyItemCondition;
import org.apache.ranger.entity.XXPolicyItemGroupPerm;
import org.apache.ranger.entity.XXPolicyItemUserPerm;
import org.apache.ranger.entity.XXPolicyResource;
import org.apache.ranger.entity.XXPolicyResourceMap;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigDef;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXUser;
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
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerDataHistService;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.service.RangerServiceWithAssignedIdService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.RangerPolicyList;
import org.apache.ranger.view.RangerServiceDefList;
import org.apache.ranger.view.RangerServiceList;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXUser;
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
public class TestServiceDBStore {

	private static Long Id = 8L;

	@InjectMocks
	ServiceDBStore serviceDBStore = new ServiceDBStore();

	@Mock
	RangerDaoManager daoManager;

	@Mock
	RangerServiceService svcService;

	@Mock
	RangerDataHistService dataHistService;

	@Mock
	RangerServiceDefService serviceDefService;

	@Mock
	RangerPolicyService policyService;

	@Mock
	StringUtil stringUtil;

	@Mock
	XUserService xUserService;

	@Mock
	XUserMgr xUserMgr;

	@Mock
	RangerAuditFields<XXDBBase> rangerAuditFields;

	@Mock
	ContextUtil contextUtil;

	@Mock
	RangerBizUtil bizUtil;

	@Mock
	RangerServiceWithAssignedIdService svcServiceWithAssignedId;
	
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

	private RangerServiceDef rangerServiceDef() {
		List<RangerServiceConfigDef> configs = new ArrayList<RangerServiceConfigDef>();
		List<RangerResourceDef> resources = new ArrayList<RangerResourceDef>();
		List<RangerAccessTypeDef> accessTypes = new ArrayList<RangerAccessTypeDef>();
		List<RangerPolicyConditionDef> policyConditions = new ArrayList<RangerPolicyConditionDef>();
		List<RangerContextEnricherDef> contextEnrichers = new ArrayList<RangerContextEnricherDef>();
		List<RangerEnumDef> enums = new ArrayList<RangerEnumDef>();

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

	private RangerService rangerService() {
		Map<String, String> configs = new HashMap<String, String>();
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
		rangerService.setPolicyUpdateTime(new Date());
		rangerService.setType("1");
		rangerService.setUpdatedBy("Admin");
		rangerService.setUpdateTime(new Date());

		return rangerService;
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

	@Test
	public void test11createServiceDef() throws Exception {

		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXResourceDefDao xResourceDefDao = Mockito.mock(XXResourceDefDao.class);
		XXServiceConfigDefDao xServiceConfigDefDao = Mockito
				.mock(XXServiceConfigDefDao.class);
		XXAccessTypeDefDao xAccessTypeDefDao = Mockito
				.mock(XXAccessTypeDefDao.class);
		XXAccessTypeDefGrantsDao xAccessTypeDefGrantsDao = Mockito
				.mock(XXAccessTypeDefGrantsDao.class);
		XXPolicyConditionDefDao xPolicyConditionDefDao = Mockito
				.mock(XXPolicyConditionDefDao.class);
		XXContextEnricherDefDao xContextEnricherDefDao = Mockito
				.mock(XXContextEnricherDefDao.class);
		XXEnumDefDao xEnumDefDao = Mockito.mock(XXEnumDefDao.class);
		XXEnumElementDefDao xEnumElementDefDao = Mockito
				.mock(XXEnumElementDefDao.class);

		XXServiceDef xServiceDef = Mockito.mock(XXServiceDef.class);
		XXResourceDef xResourceDef = Mockito.mock(XXResourceDef.class);
		XXServiceConfigDef xServiceConfigDef = Mockito
				.mock(XXServiceConfigDef.class);
		XXPolicyConditionDef xPolicyConditionDef = Mockito
				.mock(XXPolicyConditionDef.class);
		XXContextEnricherDef xContextEnricherDef = Mockito
				.mock(XXContextEnricherDef.class);
		XXEnumDef xEnumDef = Mockito.mock(XXEnumDef.class);
		XXAccessTypeDef xAccessTypeDef = Mockito.mock(XXAccessTypeDef.class);
		XXEnumElementDef xEnumElementDef = Mockito.mock(XXEnumElementDef.class);
		XXAccessTypeDefGrants xAccessTypeDefGrants = Mockito
				.mock(XXAccessTypeDefGrants.class);

		RangerServiceConfigDef rangerServiceConfigDef = Mockito
				.mock(RangerServiceConfigDef.class);
		RangerResourceDef rangerResourceDef = Mockito
				.mock(RangerResourceDef.class);
		RangerAccessTypeDef rangerAccessTypeDef = Mockito
				.mock(RangerAccessTypeDef.class);
		RangerPolicyConditionDef rangerPolicyConditionDef = Mockito
				.mock(RangerPolicyConditionDef.class);
		RangerContextEnricherDef rangerContextEnricherDef = Mockito
				.mock(RangerContextEnricherDef.class);
		RangerEnumDef rangerEnumDef = Mockito.mock(RangerEnumDef.class);
		RangerEnumElementDef rangerEnumElementDef = Mockito
				.mock(RangerEnumElementDef.class);

		RangerServiceDef serviceDef = new RangerServiceDef();
		Mockito.when(serviceDefService.create(serviceDef)).thenReturn(
				serviceDef);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.findByName("HDFS_1")).thenReturn(
				xServiceDef);
		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(null)).thenReturn(xServiceDef);

		Mockito.when(daoManager.getXXServiceConfigDef()).thenReturn(
				xServiceConfigDefDao);
		Mockito.when(
				serviceDefService.populateRangerServiceConfigDefToXX(
						rangerServiceConfigDef, xServiceConfigDef, xServiceDef , 1))
				.thenReturn(xServiceConfigDef);
		Mockito.when(xServiceConfigDefDao.create(xServiceConfigDef))
				.thenReturn(xServiceConfigDef);

		Mockito.when(daoManager.getXXResourceDef()).thenReturn(xResourceDefDao);
		Mockito.when(
				serviceDefService.populateRangerResourceDefToXX(
						rangerResourceDef, xResourceDef, xServiceDef, 1))
				.thenReturn(xResourceDef);
		Mockito.when(xResourceDefDao.create(xResourceDef)).thenReturn(
				xResourceDef);

		Mockito.when(daoManager.getXXAccessTypeDef()).thenReturn(
				xAccessTypeDefDao);
		Mockito.when(
				serviceDefService.populateRangerAccessTypeDefToXX(
						rangerAccessTypeDef, xAccessTypeDef, xServiceDef, 1))
				.thenReturn(xAccessTypeDef);
		Mockito.when(xAccessTypeDefDao.create(xAccessTypeDef)).thenReturn(
				xAccessTypeDef);

		Mockito.when(daoManager.getXXAccessTypeDefGrants()).thenReturn(
				xAccessTypeDefGrantsDao);
		Mockito.when(xAccessTypeDefGrantsDao.create(xAccessTypeDefGrants))
				.thenReturn(xAccessTypeDefGrants);

		Mockito.when(daoManager.getXXPolicyConditionDef()).thenReturn(
				xPolicyConditionDefDao);
		Mockito.when(
				serviceDefService.populateRangerPolicyConditionDefToXX(
						rangerPolicyConditionDef, xPolicyConditionDef,
						xServiceDef, 1)).thenReturn(xPolicyConditionDef);
		Mockito.when(xPolicyConditionDefDao.create(xPolicyConditionDef))
				.thenReturn(xPolicyConditionDef);

		Mockito.when(daoManager.getXXContextEnricherDef()).thenReturn(
				xContextEnricherDefDao);
		Mockito.when(
				serviceDefService.populateRangerContextEnricherDefToXX(
						rangerContextEnricherDef, xContextEnricherDef,
						xServiceDef, 1)).thenReturn(xContextEnricherDef);
		Mockito.when(xContextEnricherDefDao.create(xContextEnricherDef))
				.thenReturn(xContextEnricherDef);

		Mockito.when(daoManager.getXXEnumDef()).thenReturn(xEnumDefDao);
		Mockito.when(
				serviceDefService.populateRangerEnumDefToXX(rangerEnumDef,
						xEnumDef, xServiceDef ,1)).thenReturn(xEnumDef);
		Mockito.when(xEnumDefDao.create(xEnumDef)).thenReturn(xEnumDef);

		Mockito.when(daoManager.getXXEnumElementDef()).thenReturn(
				xEnumElementDefDao);
		Mockito.when(
				serviceDefService.populateRangerEnumElementDefToXX(
						rangerEnumElementDef, xEnumElementDef, xEnumDef, 1))
				.thenReturn(xEnumElementDef);
		Mockito.when(xEnumElementDefDao.create(xEnumElementDef)).thenReturn(
				xEnumElementDef);

		Mockito.when(serviceDefService.getPopulatedViewObject(xServiceDef))
				.thenReturn(serviceDef);

		RangerServiceDef dbServiceDef = serviceDBStore
				.createServiceDef(serviceDef);
		Assert.assertNotNull(dbServiceDef);
		Assert.assertEquals(dbServiceDef, serviceDef);
		Assert.assertEquals(dbServiceDef.getId(), serviceDef.getId());
		Assert.assertEquals(dbServiceDef.getCreatedBy(),
				serviceDef.getCreatedBy());
		Assert.assertEquals(dbServiceDef.getDescription(),
				serviceDef.getDescription());
		Assert.assertEquals(dbServiceDef.getGuid(), serviceDef.getGuid());
		Assert.assertEquals(dbServiceDef.getImplClass(),
				serviceDef.getImplClass());
		Assert.assertEquals(dbServiceDef.getLabel(), serviceDef.getLabel());
		Assert.assertEquals(dbServiceDef.getName(), serviceDef.getName());
		Assert.assertEquals(dbServiceDef.getRbKeyDescription(),
				serviceDef.getRbKeyDescription());
		Assert.assertEquals(dbServiceDef.getRbKeyLabel(), serviceDef.getLabel());
		Assert.assertEquals(dbServiceDef.getConfigs(), serviceDef.getConfigs());
		Assert.assertEquals(dbServiceDef.getVersion(), serviceDef.getVersion());
		Assert.assertEquals(dbServiceDef.getResources(),
				serviceDef.getResources());
		Mockito.verify(serviceDefService).getPopulatedViewObject(xServiceDef);
		Mockito.verify(serviceDefService).create(serviceDef);
		Mockito.verify(daoManager).getXXServiceConfigDef();
		Mockito.verify(daoManager).getXXEnumDef();
		Mockito.verify(daoManager).getXXAccessTypeDef();
	}

	@Test
	public void test12updateServiceDef() throws Exception {
		RangerServiceDef serviceDef = rangerServiceDef();
		RangerServiceDef dbServiceDef = serviceDBStore
				.updateServiceDef(serviceDef);
		Assert.assertNull(dbServiceDef);
	}

	@Test
	public void test13deleteServiceDef() throws Exception {
		serviceDBStore.deleteServiceDef(Id);
	}

	@Test
	public void test14getServiceDef() throws Exception {
		RangerServiceDef rangerServiceDef = rangerServiceDef();
		Mockito.when(serviceDefService.read(Id)).thenReturn(rangerServiceDef);
		RangerServiceDef dbRangerServiceDef = serviceDBStore.getServiceDef(Id);
		Assert.assertNotNull(dbRangerServiceDef);
		Assert.assertEquals(dbRangerServiceDef, rangerServiceDef);
		Assert.assertEquals(dbRangerServiceDef.getId(), rangerServiceDef.getId());
		Assert.assertEquals(dbRangerServiceDef.getCreatedBy(),
				rangerServiceDef.getCreatedBy());
		Assert.assertEquals(dbRangerServiceDef.getDescription(),
				rangerServiceDef.getDescription());
		Assert.assertEquals(dbRangerServiceDef.getGuid(), rangerServiceDef.getGuid());
		Assert.assertEquals(dbRangerServiceDef.getImplClass(),
				rangerServiceDef.getImplClass());
		Assert.assertEquals(dbRangerServiceDef.getLabel(), rangerServiceDef.getLabel());
		Assert.assertEquals(dbRangerServiceDef.getName(), rangerServiceDef.getName());
		Assert.assertEquals(dbRangerServiceDef.getRbKeyDescription(),
				rangerServiceDef.getRbKeyDescription());
		Assert.assertEquals(dbRangerServiceDef.getConfigs(), rangerServiceDef.getConfigs());
		Assert.assertEquals(dbRangerServiceDef.getVersion(), rangerServiceDef.getVersion());
		Assert.assertEquals(dbRangerServiceDef.getResources(),
				rangerServiceDef.getResources());
		Mockito.verify(serviceDefService).read(Id);
	}
	
	@Test
	public void test15getServiceDefByName() throws Exception {
		String name = "fdfdfds";

		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDef xServiceDef = Mockito.mock(XXServiceDef.class);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.findByName(name)).thenReturn(xServiceDef);

		RangerServiceDef dbServiceDef = serviceDBStore
				.getServiceDefByName(name);
		Assert.assertNull(dbServiceDef);
		Mockito.verify(daoManager).getXXServiceDef();
	}

	@Test
	public void test16getServiceDefByNameNotNull() throws Exception {
		String name = "fdfdfds";

		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDef xServiceDef = Mockito.mock(XXServiceDef.class);

		RangerServiceDef serviceDef = new RangerServiceDef();
		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.findByName(name)).thenReturn(xServiceDef);
		Mockito.when(serviceDefService.getPopulatedViewObject(xServiceDef))
				.thenReturn(serviceDef);

		RangerServiceDef dbServiceDef = serviceDBStore
				.getServiceDefByName(name);
		Assert.assertNotNull(dbServiceDef);
		Mockito.verify(daoManager).getXXServiceDef();
	}

	@Test
	public void test17getServiceDefs() throws Exception {
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		List<RangerServiceDef> serviceDefsList = new ArrayList<RangerServiceDef>();
		RangerServiceDef serviceDef = rangerServiceDef();
		serviceDefsList.add(serviceDef);
		RangerServiceDefList serviceDefList = new RangerServiceDefList();
		serviceDefList.setPageSize(0);
		serviceDefList.setResultSize(1);
		serviceDefList.setSortBy("asc");
		serviceDefList.setSortType("1");
		serviceDefList.setStartIndex(0);
		serviceDefList.setTotalCount(10);
		serviceDefList.setServiceDefs(serviceDefsList);
		Mockito.when(serviceDefService.searchRangerServiceDefs(filter))
				.thenReturn(serviceDefList);

		List<RangerServiceDef> dbServiceDef = serviceDBStore
				.getServiceDefs(filter);
		Assert.assertNotNull(dbServiceDef);
		Assert.assertEquals(dbServiceDef, serviceDefsList);
		Assert.assertEquals(dbServiceDef.get(0), serviceDefsList.get(0));
		Mockito.verify(serviceDefService).searchRangerServiceDefs(filter);
	}

	@Test
	public void test18getPaginatedServiceDefs() throws Exception {
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");

		List<RangerServiceDef> serviceDefsList = new ArrayList<RangerServiceDef>();
		RangerServiceDef serviceDef = rangerServiceDef();
		serviceDefsList.add(serviceDef);
		RangerServiceDefList serviceDefList = new RangerServiceDefList();
		serviceDefList.setPageSize(0);
		serviceDefList.setResultSize(1);
		serviceDefList.setSortBy("asc");
		serviceDefList.setSortType("1");
		serviceDefList.setStartIndex(0);
		serviceDefList.setTotalCount(10);
		serviceDefList.setServiceDefs(serviceDefsList);
		Mockito.when(serviceDefService.searchRangerServiceDefs(filter))
				.thenReturn(serviceDefList);

		RangerServiceDefList dbServiceDefList = serviceDBStore
				.getPaginatedServiceDefs(filter);
		Assert.assertNotNull(dbServiceDefList);
		Assert.assertEquals(dbServiceDefList, serviceDefList);
		Assert.assertEquals(dbServiceDefList.getList(),
				serviceDefList.getList());
		Assert.assertEquals(dbServiceDefList.getServiceDefs(),
				serviceDefList.getServiceDefs());
		Mockito.verify(serviceDefService).searchRangerServiceDefs(filter);
	}

	/*@Test
	public void test19createService() throws Exception {
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		XXServiceConfigMapDao xServiceConfigMapDao = Mockito
				.mock(XXServiceConfigMapDao.class);
		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXServiceConfigDefDao xServiceConfigDefDao = Mockito
				.mock(XXServiceConfigDefDao.class);
		XXService xService = Mockito.mock(XXService.class);
		XXUser xUser = Mockito.mock(XXUser.class);

		RangerService rangerService = rangerService();
		VXUser vXUser = null;
		String userName = "admin";

		List<XXServiceConfigDef> svcConfDefList = new ArrayList<XXServiceConfigDef>();
		XXServiceConfigDef serviceConfigDefObj = new XXServiceConfigDef();
		serviceConfigDefObj.setId(Id);
		serviceConfigDefObj.setType("1");
		svcConfDefList.add(serviceConfigDefObj);
		Mockito.when(daoManager.getXXServiceConfigDef()).thenReturn(
				xServiceConfigDefDao);
		Mockito.when(xServiceConfigDefDao.findByServiceDefName(userName))
				.thenReturn(svcConfDefList);

		Mockito.when(svcService.create(rangerService))
				.thenReturn(rangerService);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(rangerService.getId())).thenReturn(
				xService);
		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);

		Mockito.when(stringUtil.getValidUserName(userName))
				.thenReturn(userName);
		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByUserName(userName)).thenReturn(xUser);

		Mockito.when(xUserService.populateViewBean(xUser)).thenReturn(vXUser);
		Mockito.when(xUserMgr.createXUser(vXUser)).thenReturn(vXUser);

		XXServiceConfigMap xConfMap = new XXServiceConfigMap();
		Mockito.when(rangerAuditFields.populateAuditFields(xConfMap, xService))
				.thenReturn(xService);

		Mockito.when(svcService.getPopulatedViewObject(xService)).thenReturn(
				rangerService);

		serviceDBStore.setPopulateExistingBaseFields(true);

		Mockito.when(
				rangerAuditFields.populateAuditFields(
						Mockito.isA(XXServiceConfigMap.class),
						Mockito.isA(XXService.class))).thenReturn(xConfMap);

		RangerService dbRangerService = serviceDBStore
				.createService(rangerService);
		serviceDBStore.setPopulateExistingBaseFields(false);
		Assert.assertNotNull(dbRangerService);
		Mockito.verify(daoManager).getXXService();
		Mockito.verify(daoManager).getXXServiceConfigMap();
	}*/

	@Test
	public void test20updateService() throws Exception {
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		XXService xService = Mockito.mock(XXService.class);
		XXServiceConfigMapDao xServiceConfigMapDao = Mockito
				.mock(XXServiceConfigMapDao.class);
		XXServiceConfigDefDao xServiceConfigDefDao = Mockito
				.mock(XXServiceConfigDefDao.class);
		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXUser xUser = Mockito.mock(XXUser.class);

		VXUser vXUser = null;
		RangerService rangerService = rangerService();
		String name = "fdfdfds";

		List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
		XXTrxLog xTrxLogObj = new XXTrxLog();
		xTrxLogObj.setAction("create");
		xTrxLogObj.setAddedByUserId(Id);
		xTrxLogObj.setAttributeName("User Role");
		xTrxLogObj.setCreateTime(new Date());
		xTrxLogObj.setId(Id);
		xTrxLogObj.setNewValue("admin");
		xTrxLogObj.setObjectClassType(0);
		xTrxLogObj.setObjectId(1L);
		xTrxLogObj.setParentObjectClassType(0);
		xTrxLogObj.setParentObjectId(Id);
		trxLogList.add(xTrxLogObj);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(Id)).thenReturn(xService);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(name)).thenReturn(xService);

		List<XXServiceConfigDef> xServiceConfigDefList = new ArrayList<XXServiceConfigDef>();
		XXServiceConfigDef serviceConfigDefObj = new XXServiceConfigDef();
		serviceConfigDefObj.setId(Id);
		xServiceConfigDefList.add(serviceConfigDefObj);
		Mockito.when(daoManager.getXXServiceConfigDef()).thenReturn(
				xServiceConfigDefDao);
		Mockito.when(xServiceConfigDefDao.findByServiceDefName(name))
				.thenReturn(xServiceConfigDefList);

		Mockito.when(svcService.getTransactionLog(rangerService, xService, 0))
				.thenReturn(trxLogList);

		Mockito.when(svcService.update(rangerService))
				.thenReturn(rangerService);
		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(Id)).thenReturn(xService);

		List<XXServiceConfigMap> xConfMapList = new ArrayList<XXServiceConfigMap>();
		XXServiceConfigMap xConfMap = new XXServiceConfigMap();
		xConfMap.setAddedByUserId(null);
		xConfMap.setConfigkey(name);
		xConfMap.setConfigvalue(name);
		xConfMap.setCreateTime(new Date());
		xConfMap.setServiceId(null);

		xConfMap.setUpdatedByUserId(null);
		xConfMap.setUpdateTime(new Date());
		xConfMapList.add(xConfMap);

		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);
		Mockito.when(xServiceConfigMapDao.findByServiceId(Id)).thenReturn(
				xConfMapList);
		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);
		Mockito.when(xServiceConfigMapDao.remove(xConfMap)).thenReturn(true);

		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);
		Mockito.when(stringUtil.getValidUserName(name)).thenReturn(name);
		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByUserName(name)).thenReturn(xUser);

		Mockito.when(xUserService.populateViewBean(xUser)).thenReturn(vXUser);
		Mockito.when(xUserMgr.createXUser(vXUser)).thenReturn(vXUser);

		Mockito.when(
				(XXServiceConfigMap) rangerAuditFields.populateAuditFields(
						xConfMap, xService)).thenReturn(xConfMap);
		Mockito.when(
				rangerAuditFields.populateAuditFields(
						Mockito.isA(XXServiceConfigMap.class),
						Mockito.isA(XXService.class))).thenReturn(xConfMap);

		Mockito.when(svcService.getPopulatedViewObject(xService)).thenReturn(
				rangerService);

		RangerService dbRangerService = serviceDBStore
				.updateService(rangerService);
		Assert.assertNotNull(dbRangerService);
		Assert.assertEquals(dbRangerService, rangerService);
		Assert.assertEquals(dbRangerService.getId(), rangerService.getId());
		Assert.assertEquals(dbRangerService.getName(), rangerService.getName());
		Assert.assertEquals(dbRangerService.getCreatedBy(),
				rangerService.getCreatedBy());
		Assert.assertEquals(dbRangerService.getDescription(),
				rangerService.getDescription());
		Assert.assertEquals(dbRangerService.getType(), rangerService.getType());
		Assert.assertEquals(dbRangerService.getVersion(),
				rangerService.getVersion());
		Mockito.verify(daoManager).getXXUser();
	}

	@Test
	public void test21deleteService() throws Exception {
		XXPolicyDao xPolicyDao = Mockito.mock(XXPolicyDao.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		XXService xService = Mockito.mock(XXService.class);
		XXPolicyItemDao xPolicyItemDao = Mockito.mock(XXPolicyItemDao.class);
		XXPolicyItemConditionDao xPolicyItemConditionDao = Mockito
				.mock(XXPolicyItemConditionDao.class);
		XXPolicyItemGroupPermDao xPolicyItemGroupPermDao = Mockito
				.mock(XXPolicyItemGroupPermDao.class);
		XXPolicyItemUserPermDao xPolicyItemUserPermDao = Mockito
				.mock(XXPolicyItemUserPermDao.class);
		XXPolicyItemAccessDao xPolicyItemAccessDao = Mockito
				.mock(XXPolicyItemAccessDao.class);
		XXPolicyResourceDao xPolicyResourceDao = Mockito
				.mock(XXPolicyResourceDao.class);
		XXPolicyResourceMapDao xPolicyResourceMapDao = Mockito
				.mock(XXPolicyResourceMapDao.class);
		XXServiceConfigDefDao xServiceConfigDefDao = Mockito
				.mock(XXServiceConfigDefDao.class);
		XXServiceConfigMapDao xServiceConfigMapDao = Mockito
				.mock(XXServiceConfigMapDao.class);
		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXUser xUser = Mockito.mock(XXUser.class);

		RangerService rangerService = rangerService();
		RangerPolicy rangerPolicy = rangerPolicy();
		String name = "HDFS_1-1-20150316062453";

		List<XXPolicy> policiesList = new ArrayList<XXPolicy>();
		XXPolicy policy = new XXPolicy();
		policy.setAddedByUserId(Id);
		policy.setCreateTime(new Date());
		policy.setDescription("polcy test");
		policy.setGuid("");
		policy.setId(rangerService.getId());
		policy.setIsAuditEnabled(true);
		policy.setName("HDFS_1-1-20150316062453");
		policy.setService(rangerService.getId());
		policiesList.add(policy);

		List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
		XXTrxLog xTrxLogObj = new XXTrxLog();
		xTrxLogObj.setAction("delete");
		xTrxLogObj.setAddedByUserId(Id);
		xTrxLogObj.setAttributeName("User Role");
		xTrxLogObj.setCreateTime(new Date());
		xTrxLogObj.setId(Id);
		xTrxLogObj.setNewValue("admin");
		xTrxLogObj.setObjectClassType(0);
		xTrxLogObj.setObjectId(1L);
		xTrxLogObj.setParentObjectClassType(0);
		xTrxLogObj.setParentObjectId(Id);
		trxLogList.add(xTrxLogObj);

		List<XXPolicyItem> policyItemList = new ArrayList<XXPolicyItem>();
		XXPolicyItem policyItem = new XXPolicyItem();
		policyItem.setAddedByUserId(Id);
		policyItem.setCreateTime(new Date());
		policyItem.setDelegateAdmin(false);
		policyItem.setId(Id);
		policyItem.setOrder(1);
		policyItem.setPolicyId(Id);
		policyItem.setUpdatedByUserId(Id);
		policyItem.setUpdateTime(new Date());
		policyItemList.add(policyItem);

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

		List<XXPolicyItemGroupPerm> policyItemGroupPermList = new ArrayList<XXPolicyItemGroupPerm>();
		XXPolicyItemGroupPerm policyItemGroupPerm = new XXPolicyItemGroupPerm();
		policyItemGroupPerm.setAddedByUserId(Id);
		policyItemGroupPerm.setCreateTime(new Date());
		policyItemGroupPerm.setGroupId(Id);

		List<XXServiceConfigMap> xConfMapList = new ArrayList<XXServiceConfigMap>();
		XXServiceConfigMap xConfMap = new XXServiceConfigMap();
		xConfMap.setAddedByUserId(null);
		xConfMap.setConfigkey(name);
		xConfMap.setConfigvalue(name);
		xConfMap.setCreateTime(new Date());
		xConfMap.setServiceId(null);
		xConfMap.setId(Id);
		xConfMap.setUpdatedByUserId(null);
		xConfMap.setUpdateTime(new Date());
		xConfMapList.add(xConfMap);
		policyItemGroupPerm.setId(Id);
		policyItemGroupPerm.setOrder(1);
		policyItemGroupPerm.setPolicyItemId(Id);
		policyItemGroupPerm.setUpdatedByUserId(Id);
		policyItemGroupPerm.setUpdateTime(new Date());
		policyItemGroupPermList.add(policyItemGroupPerm);

		List<XXPolicyItemUserPerm> policyItemUserPermList = new ArrayList<XXPolicyItemUserPerm>();
		XXPolicyItemUserPerm policyItemUserPerm = new XXPolicyItemUserPerm();
		policyItemUserPerm.setAddedByUserId(Id);
		policyItemUserPerm.setCreateTime(new Date());
		policyItemUserPerm.setPolicyItemId(Id);
		policyItemUserPerm.setId(Id);
		policyItemUserPerm.setOrder(1);
		policyItemUserPerm.setUpdatedByUserId(Id);
		policyItemUserPerm.setUpdateTime(new Date());
		policyItemUserPermList.add(policyItemUserPerm);

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

		List<XXPolicyResource> policyResourceList = new ArrayList<XXPolicyResource>();
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
		policyResourceList.add(policyResource);

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

		List<XXServiceConfigDef> xServiceConfigDefList = new ArrayList<XXServiceConfigDef>();
		XXServiceConfigDef serviceConfigDefObj = new XXServiceConfigDef();
		serviceConfigDefObj.setId(Id);
		xServiceConfigDefList.add(serviceConfigDefObj);

		Mockito.when(daoManager.getXXPolicy()).thenReturn(xPolicyDao);
		Mockito.when(xPolicyDao.findByServiceId(rangerService.getId()))
				.thenReturn(policiesList);
		Mockito.when(svcService.delete(rangerService)).thenReturn(true);

		Mockito.when(svcService.getTransactionLog(rangerService, 3))
				.thenReturn(trxLogList);

		Mockito.when(svcService.read(Id)).thenReturn(rangerService);
		Mockito.when(policyService.read(Id)).thenReturn(rangerPolicy);
		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(name)).thenReturn(xService);
		Mockito.when(svcService.getPopulatedViewObject(xService)).thenReturn(
				rangerService);

		Mockito.when(daoManager.getXXPolicyItem()).thenReturn(xPolicyItemDao);
		Mockito.when(xPolicyItemDao.findByPolicyId(policyItem.getId()))
				.thenReturn(policyItemList);

		Mockito.when(daoManager.getXXPolicyItemCondition()).thenReturn(
				xPolicyItemConditionDao);
		Mockito.when(
				xPolicyItemConditionDao.findByPolicyItemId(policyItemCondition
						.getId())).thenReturn(policyItemConditionList);

		Mockito.when(daoManager.getXXPolicyItemGroupPerm()).thenReturn(
				xPolicyItemGroupPermDao);
		Mockito.when(
				xPolicyItemGroupPermDao.findByPolicyItemId(policyItem.getId()))
				.thenReturn(policyItemGroupPermList);

		Mockito.when(daoManager.getXXPolicyItemUserPerm()).thenReturn(
				xPolicyItemUserPermDao);
		Mockito.when(xPolicyItemUserPermDao.findByPolicyItemId(Id)).thenReturn(
				policyItemUserPermList);

		Mockito.when(daoManager.getXXPolicyItemAccess()).thenReturn(
				xPolicyItemAccessDao);
		Mockito.when(
				xPolicyItemAccessDao.findByPolicyItemId(policyItemAccess
						.getId())).thenReturn(policyItemAccessList);

		Mockito.when(daoManager.getXXPolicyResource()).thenReturn(
				xPolicyResourceDao);
		Mockito.when(xPolicyResourceDao.findByPolicyId(policyResource.getId()))
				.thenReturn(policyResourceList);

		Mockito.when(daoManager.getXXPolicyResourceMap()).thenReturn(
				xPolicyResourceMapDao);
		Mockito.when(
				xPolicyResourceMapDao.findByPolicyResId(policyResourceMap
						.getId())).thenReturn(policyResourceMapList);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(Id)).thenReturn(xService);

		Mockito.when(daoManager.getXXServiceConfigDef()).thenReturn(
				xServiceConfigDefDao);
		Mockito.when(
				xServiceConfigDefDao.findByServiceDefName(rangerService
						.getType())).thenReturn(xServiceConfigDefList);

		Mockito.when(svcService.update(rangerService))
				.thenReturn(rangerService);
		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(rangerService.getId())).thenReturn(
				xService);

		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);
		Mockito.when(
				xServiceConfigMapDao.findByServiceId(rangerService.getId()))
				.thenReturn(xConfMapList);

		Mockito.when(
				rangerAuditFields.populateAuditFields(
						Mockito.isA(XXServiceConfigMap.class),
						Mockito.isA(XXService.class))).thenReturn(xConfMap);
		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByUserName(name)).thenReturn(xUser);
		serviceDBStore.deleteService(Id);
		Mockito.verify(svcService).update(rangerService);
		Mockito.verify(daoManager).getXXUser();
	}

	@Test
	public void test22getService() throws Exception {
		RangerService rangerService = rangerService();
		Mockito.when(svcService.read(Id)).thenReturn(rangerService);

		RangerService dbRangerService = serviceDBStore.getService(Id);
		Assert.assertNotNull(dbRangerService);
		Assert.assertEquals(dbRangerService, rangerService);
		Assert.assertEquals(dbRangerService.getCreatedBy(),
				rangerService.getCreatedBy());
		Assert.assertEquals(dbRangerService.getDescription(),
				rangerService.getDescription());
		Assert.assertEquals(dbRangerService.getGuid(), rangerService.getGuid());
		Assert.assertEquals(dbRangerService.getName(), rangerService.getName());
		Assert.assertEquals(dbRangerService.getType(), rangerService.getType());
		Assert.assertEquals(dbRangerService.getUpdatedBy(),
				rangerService.getUpdatedBy());
		Assert.assertEquals(dbRangerService.getConfigs(),
				rangerService.getConfigs());
		Assert.assertEquals(dbRangerService.getCreateTime(),
				rangerService.getCreateTime());
		Assert.assertEquals(dbRangerService.getId(), rangerService.getId());
		Assert.assertEquals(dbRangerService.getPolicyVersion(),
				rangerService.getPolicyVersion());
		Assert.assertEquals(dbRangerService.getVersion(),
				rangerService.getVersion());
		Assert.assertEquals(dbRangerService.getPolicyUpdateTime(),
				rangerService.getPolicyUpdateTime());

		Mockito.verify(svcService).read(Id);

	}

	@Test
	public void test23getServiceByName() throws Exception {
		XXService xService = Mockito.mock(XXService.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);

		RangerService rangerService = rangerService();
		String name = rangerService.getName();

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(name)).thenReturn(xService);
		Mockito.when(svcService.getPopulatedViewObject(xService)).thenReturn(
				rangerService);

		RangerService dbRangerService = serviceDBStore.getServiceByName(name);
		Assert.assertNotNull(dbRangerService);
		Assert.assertEquals(dbRangerService, rangerService);
		Assert.assertEquals(dbRangerService.getName(), rangerService.getName());
		Mockito.verify(daoManager).getXXService();
		Mockito.verify(svcService).getPopulatedViewObject(xService);
	}

	@Test
	public void test24getServices() throws Exception {
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");

		List<RangerService> serviceList = new ArrayList<RangerService>();
		RangerService rangerService = rangerService();
		serviceList.add(rangerService);

		RangerServiceList serviceListObj = new RangerServiceList();
		serviceListObj.setPageSize(0);
		serviceListObj.setResultSize(1);
		serviceListObj.setSortBy("asc");
		serviceListObj.setSortType("1");
		serviceListObj.setStartIndex(0);
		serviceListObj.setTotalCount(10);
		serviceListObj.setServices(serviceList);

		Mockito.when(svcService.searchRangerServices(filter)).thenReturn(
				serviceListObj);
		List<RangerService> dbRangerService = serviceDBStore
				.getServices(filter);
		Assert.assertNotNull(dbRangerService);
		Assert.assertEquals(dbRangerService, serviceList);
		Mockito.verify(svcService).searchRangerServices(filter);
	}

	@Test
	public void test25getPaginatedServiceDefs() throws Exception {
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");

		List<RangerService> serviceList = new ArrayList<RangerService>();
		RangerService rangerService = rangerService();
		serviceList.add(rangerService);

		RangerServiceList serviceListObj = new RangerServiceList();
		serviceListObj.setPageSize(0);
		serviceListObj.setResultSize(1);
		serviceListObj.setSortBy("asc");
		serviceListObj.setSortType("1");
		serviceListObj.setStartIndex(0);
		serviceListObj.setTotalCount(10);
		serviceListObj.setServices(serviceList);

		Mockito.when(svcService.searchRangerServices(filter)).thenReturn(
				serviceListObj);

		RangerServiceList dbServiceList = serviceDBStore
				.getPaginatedServices(filter);
		Assert.assertNotNull(dbServiceList);
		Assert.assertEquals(dbServiceList, serviceListObj);
		Assert.assertEquals(dbServiceList.getList(), serviceListObj.getList());
		Assert.assertEquals(dbServiceList.getServices(),
				serviceListObj.getServices());

		Mockito.verify(svcService).searchRangerServices(filter);
	}

	@Test
	public void tess26createPolicy() throws Exception {

		XXServiceDef xServiceDef = Mockito.mock(XXServiceDef.class);
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXPolicy xPolicy = Mockito.mock(XXPolicy.class);
		XXPolicyDao xPolicyDao = Mockito.mock(XXPolicyDao.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		XXService xService = Mockito.mock(XXService.class);
		XXPolicyItemDao xPolicyItemDao = Mockito.mock(XXPolicyItemDao.class);
		XXServiceConfigDefDao xServiceConfigDefDao = Mockito
				.mock(XXServiceConfigDefDao.class);
		XXServiceConfigMapDao xServiceConfigMapDao = Mockito
				.mock(XXServiceConfigMapDao.class);

		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXUser xUser = Mockito.mock(XXUser.class);

		Map<String, String> configs = new HashMap<String, String>();
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
		rangerService.setPolicyUpdateTime(new Date());
		rangerService.setType("1");
		rangerService.setUpdatedBy("Admin");

		String policyName = "HDFS_1-1-20150316062345";
		String name = "HDFS_1-1-20150316062453";

		List<RangerPolicyItemAccess> accessesList = new ArrayList<RangerPolicyItemAccess>();
		RangerPolicyItemAccess policyItemAccess = new RangerPolicyItemAccess();
		policyItemAccess.setIsAllowed(true);
		policyItemAccess.setType("1");
		List<String> usersList = new ArrayList<String>();
		List<String> groupsList = new ArrayList<String>();
		List<RangerPolicyItemCondition> conditionsList = new ArrayList<RangerPolicyItemCondition>();
		RangerPolicyItemCondition policyItemCondition = new RangerPolicyItemCondition();
		policyItemCondition.setType("1");
		policyItemCondition.setValues(usersList);
		conditionsList.add(policyItemCondition);

		List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicy.RangerPolicyItem>();
		RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.setDelegateAdmin(false);
		rangerPolicyItem.setAccesses(accessesList);
		rangerPolicyItem.setConditions(conditionsList);
		rangerPolicyItem.setGroups(groupsList);
		rangerPolicyItem.setUsers(usersList);
		policyItems.add(rangerPolicyItem);

		List<RangerPolicyItem> policyItemsSet = new ArrayList<RangerPolicy.RangerPolicyItem>();
		RangerPolicyItem paramPolicyItem = new RangerPolicyItem(accessesList,
				usersList, groupsList, conditionsList, false);
		paramPolicyItem.setDelegateAdmin(false);
		paramPolicyItem.setAccesses(accessesList);
		paramPolicyItem.setConditions(conditionsList);
		paramPolicyItem.setGroups(groupsList);
		rangerPolicyItem.setUsers(usersList);
		policyItemsSet.add(paramPolicyItem);

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

		XXPolicy xxPolicy = new XXPolicy();
		xxPolicy.setId(Id);
		xxPolicy.setName(name);
		xxPolicy.setAddedByUserId(Id);
		xxPolicy.setCreateTime(new Date());
		xxPolicy.setDescription("test");
		xxPolicy.setIsAuditEnabled(false);
		xxPolicy.setIsEnabled(false);
		xxPolicy.setService(1L);
		xxPolicy.setUpdatedByUserId(Id);
		xxPolicy.setUpdateTime(new Date());

		List<XXServiceConfigDef> xServiceConfigDefList = new ArrayList<XXServiceConfigDef>();
		XXServiceConfigDef serviceConfigDefObj = new XXServiceConfigDef();
		serviceConfigDefObj.setId(Id);
		xServiceConfigDefList.add(serviceConfigDefObj);

		List<XXServiceConfigMap> xConfMapList = new ArrayList<XXServiceConfigMap>();
		XXServiceConfigMap xConfMap = new XXServiceConfigMap();
		xConfMap.setAddedByUserId(null);
		xConfMap.setConfigkey(name);
		xConfMap.setConfigvalue(name);
		xConfMap.setCreateTime(new Date());
		xConfMap.setServiceId(null);
		xConfMap.setId(Id);
		xConfMap.setUpdatedByUserId(null);
		xConfMap.setUpdateTime(new Date());
		xConfMapList.add(xConfMap);

		RangerPolicy rangerPolicy = rangerPolicy();

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(name)).thenReturn(xService);
		Mockito.when(svcService.getPopulatedViewObject(xService)).thenReturn(
				rangerService);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.findByName(rangerService.getType()))
				.thenReturn(xServiceDef);

		Mockito.when(daoManager.getXXPolicy()).thenReturn(xPolicyDao);
		Mockito.when(
				xPolicyDao.findByNameAndServiceId(policyName,
						rangerService.getId())).thenReturn(xPolicy);

		Mockito.when(policyService.create(rangerPolicy)).thenReturn(
				rangerPolicy);

		Mockito.when(daoManager.getXXPolicy()).thenReturn(xPolicyDao);
		Mockito.when(xPolicyDao.getById(Id)).thenReturn(xPolicy);

		Mockito.when(
				rangerAuditFields.populateAuditFields(
						Mockito.isA(XXPolicyItem.class),
						Mockito.isA(XXPolicy.class))).thenReturn(xPolicyItem);
		Mockito.when(daoManager.getXXPolicyItem()).thenReturn(xPolicyItemDao);
		Mockito.when(xPolicyItemDao.create(xPolicyItem))
				.thenReturn(xPolicyItem);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(Id)).thenReturn(xService);

		Mockito.when(daoManager.getXXServiceConfigDef()).thenReturn(
				xServiceConfigDefDao);
		Mockito.when(xServiceConfigDefDao.findByServiceDefName(name))
				.thenReturn(xServiceConfigDefList);

		Mockito.when(svcService.update(rangerService))
				.thenReturn(rangerService);
		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(Id)).thenReturn(xService);

		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);
		Mockito.when(xServiceConfigMapDao.findByServiceId(Id)).thenReturn(
				xConfMapList);

		Mockito.when(
				rangerAuditFields.populateAuditFields(
						Mockito.isA(XXServiceConfigMap.class),
						Mockito.isA(XXService.class))).thenReturn(xConfMap);
		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByUserName(name)).thenReturn(xUser);

		RangerPolicy dbRangerPolicy = serviceDBStore.createPolicy(rangerPolicy);
		Assert.assertNull(dbRangerPolicy);
		Assert.assertEquals(Id, rangerPolicy.getId());
		Mockito.verify(daoManager).getXXServiceDef();
		Mockito.verify(policyService).create(rangerPolicy);
		Mockito.verify(rangerAuditFields).populateAuditFields(
				Mockito.isA(XXPolicyItem.class), Mockito.isA(XXPolicy.class));
		Mockito.verify(daoManager).getXXPolicyItem();
		Mockito.verify(daoManager).getXXServiceConfigDef();
		Mockito.verify(svcService).update(rangerService);
		Mockito.verify(daoManager).getXXUser();
	}

	@Test
	public void tess27getPolicy() throws Exception {
		RangerPolicy rangerPolicy = rangerPolicy();
		Mockito.when(policyService.read(Id)).thenReturn(rangerPolicy);
		RangerPolicy dbRangerPolicy = serviceDBStore.getPolicy(Id);
		Assert.assertNotNull(dbRangerPolicy);
		Assert.assertEquals(dbRangerPolicy, rangerPolicy);
		Assert.assertEquals(dbRangerPolicy.getId(), rangerPolicy.getId());
		Assert.assertEquals(dbRangerPolicy.getName(), rangerPolicy.getName());
		Assert.assertEquals(dbRangerPolicy.getCreatedBy(),
				rangerPolicy.getCreatedBy());
		Assert.assertEquals(dbRangerPolicy.getDescription(),
				rangerPolicy.getDescription());
		Assert.assertEquals(dbRangerPolicy.getGuid(), rangerPolicy.getGuid());
		Assert.assertEquals(dbRangerPolicy.getService(),
				rangerPolicy.getService());
		Assert.assertEquals(dbRangerPolicy.getUpdatedBy(),
				rangerPolicy.getUpdatedBy());
		Assert.assertEquals(dbRangerPolicy.getCreateTime(),
				rangerPolicy.getCreateTime());
		Assert.assertEquals(dbRangerPolicy.getIsAuditEnabled(),
				rangerPolicy.getIsAuditEnabled());
		Assert.assertEquals(dbRangerPolicy.getIsEnabled(),
				rangerPolicy.getIsEnabled());
		Assert.assertEquals(dbRangerPolicy.getPolicyItems(),
				rangerPolicy.getPolicyItems());
		Assert.assertEquals(dbRangerPolicy.getVersion(),
				rangerPolicy.getVersion());
		Mockito.verify(policyService).read(Id);
		
	}

	@Test
	public void tess28updatePolicy() throws Exception {

		XXPolicyDao xPolicyDao = Mockito.mock(XXPolicyDao.class);
		XXPolicy xPolicy = Mockito.mock(XXPolicy.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		XXService xService = Mockito.mock(XXService.class);
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDef xServiceDef = Mockito.mock(XXServiceDef.class);
		XXPolicyResourceDao xPolicyResourceDao = Mockito
				.mock(XXPolicyResourceDao.class);
		XXPolicyResourceMapDao xPolicyResourceMapDao = Mockito
				.mock(XXPolicyResourceMapDao.class);
		XXPolicyItemDao xPolicyItemDao = Mockito.mock(XXPolicyItemDao.class);
		XXPolicyItem xPolicyItem = Mockito.mock(XXPolicyItem.class);
		XXServiceConfigDefDao xServiceConfigDefDao = Mockito
				.mock(XXServiceConfigDefDao.class);
		XXServiceConfigMapDao xServiceConfigMapDao = Mockito
				.mock(XXServiceConfigMapDao.class);
		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXUser xUser = Mockito.mock(XXUser.class);

		RangerService rangerService = rangerService();

		RangerPolicy rangerPolicy = rangerPolicy();
		String name = "HDFS_1-1-20150316062453";

		List<XXPolicyResource> policyResourceList = new ArrayList<XXPolicyResource>();
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
		policyResourceList.add(policyResource);

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

		List<XXServiceConfigDef> xServiceConfigDefList = new ArrayList<XXServiceConfigDef>();
		XXServiceConfigDef serviceConfigDefObj = new XXServiceConfigDef();
		serviceConfigDefObj.setId(Id);
		xServiceConfigDefList.add(serviceConfigDefObj);

		List<XXServiceConfigMap> xConfMapList = new ArrayList<XXServiceConfigMap>();
		XXServiceConfigMap xConfMap = new XXServiceConfigMap();
		xConfMap.setAddedByUserId(null);
		xConfMap.setConfigkey(name);
		xConfMap.setConfigvalue(name);
		xConfMap.setCreateTime(new Date());
		xConfMap.setServiceId(null);
		xConfMap.setId(Id);
		xConfMap.setUpdatedByUserId(null);
		xConfMap.setUpdateTime(new Date());
		xConfMapList.add(xConfMap);

		Mockito.when(daoManager.getXXPolicy()).thenReturn(xPolicyDao);
		Mockito.when(xPolicyDao.getById(Id)).thenReturn(xPolicy);
		Mockito.when(policyService.getPopulatedViewObject(xPolicy)).thenReturn(
				rangerPolicy);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(name)).thenReturn(xService);
		Mockito.when(svcService.getPopulatedViewObject(xService)).thenReturn(
				rangerService);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.findByName(rangerService.getType()))
				.thenReturn(xServiceDef);

		Mockito.when(policyService.update(rangerPolicy)).thenReturn(
				rangerPolicy);
		Mockito.when(daoManager.getXXPolicy()).thenReturn(xPolicyDao);
		Mockito.when(xPolicyDao.getById(rangerPolicy.getId())).thenReturn(
				xPolicy);

		Mockito.when(daoManager.getXXPolicyResource()).thenReturn(
				xPolicyResourceDao);
		Mockito.when(xPolicyResourceDao.findByPolicyId(rangerPolicy.getId()))
				.thenReturn(policyResourceList);

		Mockito.when(daoManager.getXXPolicyResourceMap()).thenReturn(
				xPolicyResourceMapDao);
		Mockito.when(
				xPolicyResourceMapDao.findByPolicyResId(policyResourceMap
						.getId())).thenReturn(policyResourceMapList);

		Mockito.when(daoManager.getXXPolicyItem()).thenReturn(xPolicyItemDao);

		Mockito.when(
				rangerAuditFields.populateAuditFields(
						Mockito.isA(XXPolicyItem.class),
						Mockito.isA(XXPolicy.class))).thenReturn(xPolicyItem);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(rangerService.getId())).thenReturn(
				xService);

		Mockito.when(daoManager.getXXServiceConfigDef()).thenReturn(
				xServiceConfigDefDao);
		Mockito.when(xServiceConfigDefDao.findByServiceDefName(name))
				.thenReturn(xServiceConfigDefList);

		Mockito.when(svcService.update(rangerService))
				.thenReturn(rangerService);
		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(rangerService.getId())).thenReturn(
				xService);

		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);
		Mockito.when(
				xServiceConfigMapDao.findByServiceId(rangerService.getId()))
				.thenReturn(xConfMapList);

		Mockito.when(
				rangerAuditFields.populateAuditFields(
						Mockito.isA(XXServiceConfigMap.class),
						Mockito.isA(XXService.class))).thenReturn(xConfMap);
		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByUserName(name)).thenReturn(xUser);

		RangerPolicy dbRangerPolicy = serviceDBStore.updatePolicy(rangerPolicy);
		Assert.assertNotNull(dbRangerPolicy);
		Assert.assertEquals(dbRangerPolicy, rangerPolicy);
		Assert.assertEquals(dbRangerPolicy.getId(), rangerPolicy.getId());
		Assert.assertEquals(dbRangerPolicy.getCreatedBy(),
				rangerPolicy.getCreatedBy());
		Assert.assertEquals(dbRangerPolicy.getDescription(),
				rangerPolicy.getDescription());
		Assert.assertEquals(dbRangerPolicy.getName(), rangerPolicy.getName());
		Assert.assertEquals(dbRangerPolicy.getGuid(), rangerPolicy.getGuid());
		Assert.assertEquals(dbRangerPolicy.getService(),
				rangerPolicy.getService());
		Assert.assertEquals(dbRangerPolicy.getIsEnabled(),
				rangerPolicy.getIsEnabled());
		Assert.assertEquals(dbRangerPolicy.getVersion(),
				rangerPolicy.getVersion());

		Mockito.verify(rangerAuditFields).populateAuditFields(
				Mockito.isA(XXPolicyItem.class), Mockito.isA(XXPolicy.class));
		Mockito.verify(daoManager).getXXServiceConfigDef();
		Mockito.verify(svcService).update(rangerService);
		Mockito.verify(daoManager).getXXUser();
	}

	@Test
	public void tess29deletePolicy() throws Exception {

		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		XXService xService = Mockito.mock(XXService.class);
		XXPolicyItemDao xPolicyItemDao = Mockito.mock(XXPolicyItemDao.class);
		XXPolicyItemConditionDao xPolicyItemConditionDao = Mockito
				.mock(XXPolicyItemConditionDao.class);
		XXPolicyItemGroupPermDao xPolicyItemGroupPermDao = Mockito
				.mock(XXPolicyItemGroupPermDao.class);
		XXPolicyItemUserPermDao xPolicyItemUserPermDao = Mockito
				.mock(XXPolicyItemUserPermDao.class);
		XXPolicyItemAccessDao xPolicyItemAccessDao = Mockito
				.mock(XXPolicyItemAccessDao.class);
		XXPolicyResourceDao xPolicyResourceDao = Mockito
				.mock(XXPolicyResourceDao.class);
		XXPolicyResourceMapDao xPolicyResourceMapDao = Mockito
				.mock(XXPolicyResourceMapDao.class);
		XXServiceConfigDefDao xServiceConfigDefDao = Mockito
				.mock(XXServiceConfigDefDao.class);
		XXServiceConfigMapDao xServiceConfigMapDao = Mockito
				.mock(XXServiceConfigMapDao.class);
		XXUserDao xUserDao = Mockito.mock(XXUserDao.class);
		XXUser xUser = Mockito.mock(XXUser.class);

		RangerService rangerService = rangerService();
		RangerPolicy rangerPolicy = rangerPolicy();
		String name = "HDFS_1-1-20150316062453";

		List<XXPolicyItem> policyItemList = new ArrayList<XXPolicyItem>();
		XXPolicyItem policyItem = new XXPolicyItem();
		policyItem.setAddedByUserId(Id);
		policyItem.setCreateTime(new Date());
		policyItem.setDelegateAdmin(false);
		policyItem.setId(Id);
		policyItem.setOrder(1);
		policyItem.setPolicyId(Id);
		policyItem.setUpdatedByUserId(Id);
		policyItem.setUpdateTime(new Date());
		policyItemList.add(policyItem);

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

		List<XXPolicyItemGroupPerm> policyItemGroupPermList = new ArrayList<XXPolicyItemGroupPerm>();
		XXPolicyItemGroupPerm policyItemGroupPerm = new XXPolicyItemGroupPerm();
		policyItemGroupPerm.setAddedByUserId(Id);
		policyItemGroupPerm.setCreateTime(new Date());
		policyItemGroupPerm.setGroupId(Id);

		List<XXServiceConfigMap> xConfMapList = new ArrayList<XXServiceConfigMap>();
		XXServiceConfigMap xConfMap = new XXServiceConfigMap();
		xConfMap.setAddedByUserId(null);
		xConfMap.setConfigkey(name);
		xConfMap.setConfigvalue(name);
		xConfMap.setCreateTime(new Date());
		xConfMap.setServiceId(null);
		xConfMap.setId(Id);
		xConfMap.setUpdatedByUserId(null);
		xConfMap.setUpdateTime(new Date());
		xConfMapList.add(xConfMap);
		policyItemGroupPerm.setId(Id);
		policyItemGroupPerm.setOrder(1);
		policyItemGroupPerm.setPolicyItemId(Id);
		policyItemGroupPerm.setUpdatedByUserId(Id);
		policyItemGroupPerm.setUpdateTime(new Date());
		policyItemGroupPermList.add(policyItemGroupPerm);

		List<XXPolicyItemUserPerm> policyItemUserPermList = new ArrayList<XXPolicyItemUserPerm>();
		XXPolicyItemUserPerm policyItemUserPerm = new XXPolicyItemUserPerm();
		policyItemUserPerm.setAddedByUserId(Id);
		policyItemUserPerm.setCreateTime(new Date());
		policyItemUserPerm.setPolicyItemId(Id);
		policyItemUserPerm.setId(Id);
		policyItemUserPerm.setOrder(1);
		policyItemUserPerm.setUpdatedByUserId(Id);
		policyItemUserPerm.setUpdateTime(new Date());
		policyItemUserPermList.add(policyItemUserPerm);

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

		List<XXPolicyResource> policyResourceList = new ArrayList<XXPolicyResource>();
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
		policyResourceList.add(policyResource);

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

		List<XXServiceConfigDef> xServiceConfigDefList = new ArrayList<XXServiceConfigDef>();
		XXServiceConfigDef serviceConfigDefObj = new XXServiceConfigDef();
		serviceConfigDefObj.setId(Id);
		xServiceConfigDefList.add(serviceConfigDefObj);

		Mockito.when(policyService.read(rangerPolicy.getId())).thenReturn(
				rangerPolicy);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(name)).thenReturn(xService);
		Mockito.when(svcService.getPopulatedViewObject(xService)).thenReturn(
				rangerService);

		Mockito.when(daoManager.getXXPolicyItem()).thenReturn(xPolicyItemDao);
		Mockito.when(xPolicyItemDao.findByPolicyId(policyItem.getId()))
				.thenReturn(policyItemList);

		Mockito.when(daoManager.getXXPolicyItemCondition()).thenReturn(
				xPolicyItemConditionDao);
		Mockito.when(
				xPolicyItemConditionDao.findByPolicyItemId(policyItemCondition
						.getId())).thenReturn(policyItemConditionList);

		Mockito.when(daoManager.getXXPolicyItemGroupPerm()).thenReturn(
				xPolicyItemGroupPermDao);
		Mockito.when(
				xPolicyItemGroupPermDao.findByPolicyItemId(policyItem.getId()))
				.thenReturn(policyItemGroupPermList);

		Mockito.when(daoManager.getXXPolicyItemUserPerm()).thenReturn(
				xPolicyItemUserPermDao);
		Mockito.when(xPolicyItemUserPermDao.findByPolicyItemId(Id)).thenReturn(
				policyItemUserPermList);

		Mockito.when(daoManager.getXXPolicyItemAccess()).thenReturn(
				xPolicyItemAccessDao);
		Mockito.when(
				xPolicyItemAccessDao.findByPolicyItemId(policyItemAccess
						.getId())).thenReturn(policyItemAccessList);

		Mockito.when(daoManager.getXXPolicyResource()).thenReturn(
				xPolicyResourceDao);
		Mockito.when(xPolicyResourceDao.findByPolicyId(policyResource.getId()))
				.thenReturn(policyResourceList);

		Mockito.when(daoManager.getXXPolicyResourceMap()).thenReturn(
				xPolicyResourceMapDao);
		Mockito.when(
				xPolicyResourceMapDao.findByPolicyResId(policyResourceMap
						.getId())).thenReturn(policyResourceMapList);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(Id)).thenReturn(xService);

		Mockito.when(daoManager.getXXServiceConfigDef()).thenReturn(
				xServiceConfigDefDao);
		Mockito.when(
				xServiceConfigDefDao.findByServiceDefName(rangerService
						.getType())).thenReturn(xServiceConfigDefList);

		Mockito.when(svcService.update(rangerService))
				.thenReturn(rangerService);
		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(rangerService.getId())).thenReturn(
				xService);

		Mockito.when(daoManager.getXXServiceConfigMap()).thenReturn(
				xServiceConfigMapDao);
		Mockito.when(
				xServiceConfigMapDao.findByServiceId(rangerService.getId()))
				.thenReturn(xConfMapList);

		Mockito.when(
				rangerAuditFields.populateAuditFields(
						Mockito.isA(XXServiceConfigMap.class),
						Mockito.isA(XXService.class))).thenReturn(xConfMap);
		Mockito.when(daoManager.getXXUser()).thenReturn(xUserDao);
		Mockito.when(xUserDao.findByUserName(name)).thenReturn(xUser);

		serviceDBStore.deletePolicy(Id);
		Mockito.verify(svcService).update(rangerService);
		Mockito.verify(daoManager).getXXUser();
	}

	@Test
	public void test30getPolicies() throws Exception {
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");

		List<RangerPolicy> rangerPolicyLists = new ArrayList<RangerPolicy>();
		RangerPolicy rangerPolicy = rangerPolicy();
		rangerPolicyLists.add(rangerPolicy);

		RangerPolicyList policyListObj = new RangerPolicyList();
		policyListObj.setPageSize(0);
		policyListObj.setResultSize(1);
		policyListObj.setSortBy("asc");
		policyListObj.setSortType("1");
		policyListObj.setStartIndex(0);
		policyListObj.setTotalCount(10);

		Mockito.when(policyService.searchRangerPolicies(filter)).thenReturn(
				policyListObj);
		List<RangerPolicy> dbRangerPolicy = serviceDBStore.getPolicies(filter);
		Assert.assertNotNull(dbRangerPolicy);
		Mockito.verify(policyService).searchRangerPolicies(filter);
	}

	@Test
	public void test31getPaginatedPolicies() throws Exception {
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");

		RangerPolicyList policyListObj = new RangerPolicyList();
		policyListObj.setPageSize(0);
		policyListObj.setResultSize(1);
		policyListObj.setSortBy("asc");
		policyListObj.setSortType("1");
		policyListObj.setStartIndex(0);
		policyListObj.setTotalCount(10);

		Mockito.when(policyService.searchRangerPolicies(filter)).thenReturn(
				policyListObj);

		RangerPolicyList dbRangerPolicyList = serviceDBStore
				.getPaginatedPolicies(filter);
		Assert.assertNotNull(dbRangerPolicyList);
		Mockito.verify(policyService).searchRangerPolicies(filter);
	}

	@Test
	public void test32getServicePolicies() throws Exception {
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");

		RangerService rangerService = rangerService();
		Mockito.when(svcService.read(Id)).thenReturn(rangerService);

		List<RangerPolicy> dbRangerPolicy = serviceDBStore.getServicePolicies(
				Id, filter);
		Assert.assertNotNull(dbRangerPolicy);
		Mockito.verify(svcService).read(Id);
	}

	@Test
	public void test33getServicePoliciesIfUpdated() throws Exception {
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		XXService xService = Mockito.mock(XXService.class);
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDef xServiceDef = Mockito.mock(XXServiceDef.class);
		RangerServiceDef rangerServiceDef = Mockito
				.mock(RangerServiceDef.class);

		RangerService rangerService = rangerService();
		String serviceName = "HDFS_1";

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(serviceName)).thenReturn(xService);
		Mockito.when(svcService.getPopulatedViewObject(xService)).thenReturn(
				rangerService);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.findByName(rangerService.getType()))
				.thenReturn(xServiceDef);
		Mockito.when(serviceDefService.getPopulatedViewObject(xServiceDef))
				.thenReturn(rangerServiceDef);

		ServicePolicies dbServicePolicies = serviceDBStore
				.getServicePoliciesIfUpdated(serviceName, Id);
		Assert.assertNotNull(dbServicePolicies);
		Assert.assertEquals(dbServicePolicies.getServiceName(), serviceName);
	}

	@Test
	public void test34getPolicyFromEventTime() {
		XXDataHistDao xDataHistDao = Mockito.mock(XXDataHistDao.class);
		XXDataHist xDataHist = Mockito.mock(XXDataHist.class);

		String eventTime = "2015-03-16 06:24:54";
		Mockito.when(daoManager.getXXDataHist()).thenReturn(xDataHistDao);
		Mockito.when(
				xDataHistDao.findObjByEventTimeClassTypeAndId(eventTime, 1020, Id))
				.thenReturn(xDataHist);
		
		RangerPolicy dbRangerPolicy = serviceDBStore.getPolicyFromEventTime(eventTime, Id);
		Assert.assertNull(dbRangerPolicy);
		Mockito.verify(daoManager).getXXDataHist();
	}

	@Test
	public void test35getPopulateExistingBaseFields() {
		Boolean isFound = serviceDBStore.getPopulateExistingBaseFields();
		Assert.assertFalse(isFound);
	}

	@Test
	public void test36getPaginatedServicePolicies() throws Exception {
		String serviceName = "HDFS_1";
		RangerPolicyList policyList = new RangerPolicyList();
		policyList.setPageSize(0);
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");

		Mockito.when(policyService.searchRangerPolicies(filter)).thenReturn(
				policyList);

		RangerPolicyList dbRangerPolicyList = serviceDBStore
				.getPaginatedServicePolicies(serviceName, filter);
		Assert.assertNotNull(dbRangerPolicyList);
		Mockito.verify(policyService).searchRangerPolicies(filter);
	}

	@Test
	public void test37getPaginatedServicePolicies() throws Exception {

		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		RangerService rangerService = rangerService();
		Mockito.when(svcService.read(rangerService.getId())).thenReturn(
				rangerService);
		RangerPolicyList dbRangerPolicyList = serviceDBStore
				.getPaginatedServicePolicies(rangerService.getId(), filter);
		Assert.assertNull(dbRangerPolicyList);
		Mockito.verify(svcService).read(rangerService.getId());
	}
	
	@Test
	public void test38getPolicyVersionList() throws Exception {
		XXDataHistDao xDataHistDao = Mockito.mock(XXDataHistDao.class);
		List<Integer> versionList = new ArrayList<Integer>();
		versionList.add(1);
		versionList.add(2);
		Mockito.when(daoManager.getXXDataHist()).thenReturn(xDataHistDao);
		Mockito.when(xDataHistDao.getVersionListOfObject(Id, 1020))
				.thenReturn(versionList);
		
		VXString dbVXString = serviceDBStore.getPolicyVersionList(Id);
		Assert.assertNotNull(dbVXString);
		Mockito.verify(daoManager).getXXDataHist();
	}
	
	@Test
	public void test39getPolicyForVersionNumber() throws Exception {
		XXDataHistDao xDataHistDao = Mockito.mock(XXDataHistDao.class);
		XXDataHist xDataHist = Mockito.mock(XXDataHist.class);
		Mockito.when(daoManager.getXXDataHist()).thenReturn(xDataHistDao);
		Mockito.when(xDataHistDao.findObjectByVersionNumber(Id, 1020,1))
				.thenReturn(xDataHist);
		RangerPolicy dbRangerPolicy = serviceDBStore.getPolicyForVersionNumber(Id, 1);
		Assert.assertNull(dbRangerPolicy);
		Mockito.verify(daoManager).getXXDataHist();
	}
}
