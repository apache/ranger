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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.ServiceMgr;
import org.apache.ranger.biz.TagDBStore;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.model.validation.RangerPolicyValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceValidator;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerDataHistService;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.RangerPolicyList;
import org.apache.ranger.view.RangerServiceDefList;
import org.apache.ranger.view.RangerServiceList;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXString;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestServiceREST {

	private static Long Id = 8L;

	@InjectMocks
	ServiceREST serviceREST = new ServiceREST();

	@Mock
	RangerValidatorFactory validatorFactory;

	@Mock
	RangerDaoManager daoManager;

	@Mock
	ServiceDBStore svcStore;

	@Mock
	TagDBStore tagStore;

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
	XUserMgr userMgr;

	@Mock
	RangerAuditFields<XXDBBase> rangerAuditFields;

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

	RangerPolicy rangerPolicy() {
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

	private XXServiceDef serviceDef() {
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

		return xService;
	}

	@Test
	public void test1createServiceDef() throws Exception {
		RangerServiceDef rangerServiceDef = rangerServiceDef();

		Mockito.when(validatorFactory.getServiceDefValidator(svcStore))
				.thenReturn(serviceDefValidator);

		Mockito.when(
				svcStore.createServiceDef((RangerServiceDef) Mockito
						.anyObject())).thenReturn(rangerServiceDef);

		RangerServiceDef dbRangerServiceDef = serviceREST
				.createServiceDef(rangerServiceDef);
		Assert.assertNotNull(dbRangerServiceDef);
		Assert.assertEquals(dbRangerServiceDef, rangerServiceDef);
		Assert.assertEquals(dbRangerServiceDef.getId(),
				rangerServiceDef.getId());
		Assert.assertEquals(dbRangerServiceDef.getName(),
				rangerServiceDef.getName());
		Assert.assertEquals(dbRangerServiceDef.getImplClass(),
				rangerServiceDef.getImplClass());
		Assert.assertEquals(dbRangerServiceDef.getLabel(),
				rangerServiceDef.getLabel());
		Assert.assertEquals(dbRangerServiceDef.getDescription(),
				rangerServiceDef.getDescription());
		Assert.assertEquals(dbRangerServiceDef.getRbKeyDescription(),
				rangerServiceDef.getRbKeyDescription());
		Assert.assertEquals(dbRangerServiceDef.getUpdatedBy(),
				rangerServiceDef.getUpdatedBy());
		Assert.assertEquals(dbRangerServiceDef.getUpdateTime(),
				rangerServiceDef.getUpdateTime());
		Assert.assertEquals(dbRangerServiceDef.getVersion(),
				rangerServiceDef.getVersion());
		Assert.assertEquals(dbRangerServiceDef.getConfigs(),
				rangerServiceDef.getConfigs());

		Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
		Mockito.verify(svcStore).createServiceDef(rangerServiceDef);
	}

	@Test
	public void test2updateServiceDef() throws Exception {
		RangerServiceDef rangerServiceDef = rangerServiceDef();

		Mockito.when(validatorFactory.getServiceDefValidator(svcStore))
				.thenReturn(serviceDefValidator);

		Mockito.when(
				svcStore.updateServiceDef((RangerServiceDef) Mockito
						.anyObject())).thenReturn(rangerServiceDef);

		RangerServiceDef dbRangerServiceDef = serviceREST
				.updateServiceDef(rangerServiceDef);
		Assert.assertNotNull(dbRangerServiceDef);
		Assert.assertEquals(dbRangerServiceDef, rangerServiceDef);
		Assert.assertEquals(dbRangerServiceDef.getId(),
				rangerServiceDef.getId());
		Assert.assertEquals(dbRangerServiceDef.getName(),
				rangerServiceDef.getName());
		Assert.assertEquals(dbRangerServiceDef.getImplClass(),
				rangerServiceDef.getImplClass());
		Assert.assertEquals(dbRangerServiceDef.getLabel(),
				rangerServiceDef.getLabel());
		Assert.assertEquals(dbRangerServiceDef.getDescription(),
				rangerServiceDef.getDescription());
		Assert.assertEquals(dbRangerServiceDef.getRbKeyDescription(),
				rangerServiceDef.getRbKeyDescription());
		Assert.assertEquals(dbRangerServiceDef.getUpdatedBy(),
				rangerServiceDef.getUpdatedBy());
		Assert.assertEquals(dbRangerServiceDef.getUpdateTime(),
				rangerServiceDef.getUpdateTime());
		Assert.assertEquals(dbRangerServiceDef.getVersion(),
				rangerServiceDef.getVersion());
		Assert.assertEquals(dbRangerServiceDef.getConfigs(),
				rangerServiceDef.getConfigs());

		Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
		Mockito.verify(svcStore).updateServiceDef(rangerServiceDef);
	}

	@Test
	public void test3deleteServiceDef() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		RangerServiceDef rangerServiceDef = rangerServiceDef();
		XXServiceDef xServiceDef = serviceDef();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		Mockito.when(validatorFactory.getServiceDefValidator(svcStore))
				.thenReturn(serviceDefValidator);
		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(Id)).thenReturn(xServiceDef);

		serviceREST.deleteServiceDef(rangerServiceDef.getId(), request);
		Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
		Mockito.verify(daoManager).getXXServiceDef();
	}

	@Test
	public void test4getServiceDefById() throws Exception {
		RangerServiceDef rangerServiceDef = rangerServiceDef();
		XXServiceDef xServiceDef = serviceDef();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(Id)).thenReturn(xServiceDef);
		Mockito.when(!bizUtil.hasAccess(xServiceDef, null)).thenReturn(true);
		Mockito.when(svcStore.getServiceDef(rangerServiceDef.getId()))
				.thenReturn(rangerServiceDef);
		RangerServiceDef dbRangerServiceDef = serviceREST
				.getServiceDef(rangerServiceDef.getId());
		Assert.assertNotNull(dbRangerServiceDef);
		Assert.assertEquals(dbRangerServiceDef.getId(),
				rangerServiceDef.getId());
		Mockito.verify(svcStore).getServiceDef(rangerServiceDef.getId());
		Mockito.verify(daoManager).getXXServiceDef();
		Mockito.verify(bizUtil).hasAccess(xServiceDef, null);
	}

	@Test
	public void test5getServiceDefByName() throws Exception {
		RangerServiceDef rangerServiceDef = rangerServiceDef();
		XXServiceDef xServiceDef = serviceDef();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.findByName(xServiceDef.getName()))
				.thenReturn(xServiceDef);
		Mockito.when(!bizUtil.hasAccess(xServiceDef, null)).thenReturn(true);
		Mockito.when(svcStore.getServiceDefByName(rangerServiceDef.getName()))
				.thenReturn(rangerServiceDef);
		RangerServiceDef dbRangerServiceDef = serviceREST
				.getServiceDefByName(rangerServiceDef.getName());
		Assert.assertNotNull(dbRangerServiceDef);
		Assert.assertEquals(dbRangerServiceDef.getName(),
				rangerServiceDef.getName());
		Mockito.verify(svcStore)
				.getServiceDefByName(rangerServiceDef.getName());
		Mockito.verify(daoManager).getXXServiceDef();
	}

	@Test
	public void test6createService() throws Exception {

		RangerService rangerService = rangerService();
		XXServiceDef xServiceDef = serviceDef();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		Mockito.when(validatorFactory.getServiceValidator(svcStore))
				.thenReturn(serviceValidator);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.findByName(rangerService.getType()))
				.thenReturn(xServiceDef);

		Mockito.when(
				svcStore.createService((RangerService) Mockito.anyObject()))
				.thenReturn(rangerService);

		RangerService dbRangerService = serviceREST
				.createService(rangerService);
		Assert.assertNotNull(dbRangerService);
		Assert.assertEquals(rangerService, dbRangerService);
		Assert.assertEquals(rangerService.getId(), dbRangerService.getId());
		Assert.assertEquals(rangerService.getConfigs(),
				dbRangerService.getConfigs());
		Assert.assertEquals(rangerService.getDescription(),
				dbRangerService.getDescription());
		Assert.assertEquals(rangerService.getGuid(), dbRangerService.getGuid());
		Assert.assertEquals(rangerService.getName(), dbRangerService.getName());
		Assert.assertEquals(rangerService.getPolicyVersion(),
				dbRangerService.getPolicyVersion());
		Assert.assertEquals(rangerService.getType(), dbRangerService.getType());
		Assert.assertEquals(rangerService.getVersion(),
				dbRangerService.getVersion());
		Assert.assertEquals(rangerService.getCreateTime(),
				dbRangerService.getCreateTime());
		Assert.assertEquals(rangerService.getUpdateTime(),
				dbRangerService.getUpdateTime());
		Assert.assertEquals(rangerService.getUpdatedBy(),
				dbRangerService.getUpdatedBy());

		Mockito.verify(validatorFactory).getServiceValidator(svcStore);
		Mockito.verify(svcStore).createService(rangerService);
	}

	@Test
	public void test7getServiceDefs() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		Mockito.when(
				searchUtil.getSearchFilter(request,
						serviceDefService.sortFields)).thenReturn(filter);

		List<RangerServiceDef> serviceDefsList = new ArrayList<RangerServiceDef>();
		RangerServiceDef serviceDef = rangerServiceDef();
		serviceDefsList.add(serviceDef);
		PList<RangerServiceDef> serviceDefList = new PList<RangerServiceDef>();
		serviceDefList.setPageSize(0);
		serviceDefList.setResultSize(1);
		serviceDefList.setSortBy("asc");
		serviceDefList.setSortType("1");
		serviceDefList.setStartIndex(0);
		serviceDefList.setTotalCount(10);
		serviceDefList.setList(serviceDefsList);
		Mockito.when(svcStore.getPaginatedServiceDefs(filter)).thenReturn(
				serviceDefList);
		Mockito.when(serviceDefService.searchRangerServiceDefs(filter))
				.thenReturn(new RangerServiceDefList(serviceDefsList));
		RangerServiceDefList dbRangerServiceDef = serviceREST
				.getServiceDefs(request);
		Assert.assertNotNull(dbRangerServiceDef);
		Mockito.verify(searchUtil).getSearchFilter(request,
				serviceDefService.sortFields);
		Mockito.verify(svcStore).getPaginatedServiceDefs(filter);
	}

	@Test
	public void test8updateServiceDef() throws Exception {

		RangerService rangerService = rangerService();
		XXServiceDef xServiceDef = serviceDef();
        HttpServletRequest request = null;
		Map<String, Object> options = null;

		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		Mockito.when(validatorFactory.getServiceValidator(svcStore))
				.thenReturn(serviceValidator);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.findByName(rangerService.getType()))
				.thenReturn(xServiceDef);

		Mockito.when(

				svcStore.updateService((RangerService) Mockito.any(), (Map<String, Object>) Mockito.any()))
				.thenReturn(rangerService);

		RangerService dbRangerService = serviceREST
				.updateService(rangerService, request);
		Assert.assertNotNull(dbRangerService);
		Assert.assertNotNull(dbRangerService);
		Assert.assertEquals(rangerService, dbRangerService);
		Assert.assertEquals(rangerService.getId(), dbRangerService.getId());
		Assert.assertEquals(rangerService.getConfigs(),
				dbRangerService.getConfigs());
		Assert.assertEquals(rangerService.getDescription(),
				dbRangerService.getDescription());
		Assert.assertEquals(rangerService.getGuid(), dbRangerService.getGuid());
		Assert.assertEquals(rangerService.getName(), dbRangerService.getName());
		Assert.assertEquals(rangerService.getPolicyVersion(),
				dbRangerService.getPolicyVersion());
		Assert.assertEquals(rangerService.getType(), dbRangerService.getType());
		Assert.assertEquals(rangerService.getVersion(),
				dbRangerService.getVersion());
		Assert.assertEquals(rangerService.getCreateTime(),
				dbRangerService.getCreateTime());
		Assert.assertEquals(rangerService.getUpdateTime(),
				dbRangerService.getUpdateTime());
		Assert.assertEquals(rangerService.getUpdatedBy(),
				dbRangerService.getUpdatedBy());
		Mockito.verify(validatorFactory).getServiceValidator(svcStore);
		Mockito.verify(daoManager).getXXServiceDef();
		Mockito.verify(svcStore).updateService(rangerService, options);
	}

	@Test
	public void test9deleteService() throws Exception {

		RangerService rangerService = rangerService();
		XXServiceDef xServiceDef = serviceDef();
		XXService xService = xService();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		Mockito.when(validatorFactory.getServiceValidator(svcStore))
				.thenReturn(serviceValidator);

		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.getById(Id)).thenReturn(xService);
		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(
				xServiceDef);

		serviceREST.deleteService(rangerService.getId());

		Mockito.verify(validatorFactory).getServiceValidator(svcStore);
		Mockito.verify(daoManager).getXXService();
		Mockito.verify(daoManager).getXXServiceDef();
	}

	@Test
	public void test10getServiceById() throws Exception {
		RangerService rangerService = rangerService();
		Mockito.when(svcStore.getService(rangerService.getId())).thenReturn(
				rangerService);
		RangerService dbRangerService = serviceREST.getService(rangerService
				.getId());
		Assert.assertNotNull(dbRangerService);
		Assert.assertEquals(dbRangerService.getId(), dbRangerService.getId());
		Mockito.verify(svcStore).getService(dbRangerService.getId());
	}

	@Test
	public void test11getServiceByName() throws Exception {
		RangerService rangerService = rangerService();
		Mockito.when(svcStore.getServiceByName(rangerService.getName()))
				.thenReturn(rangerService);
		RangerService dbRangerService = serviceREST
				.getServiceByName(rangerService.getName());
		Assert.assertNotNull(dbRangerService);
		Assert.assertEquals(dbRangerService.getName(),
				dbRangerService.getName());
		Mockito.verify(svcStore).getServiceByName(dbRangerService.getName());
	}

	@Test
	public void test12deleteServiceDef() {
		RangerServiceDef rangerServiceDef = rangerServiceDef();
		XXServiceDef xServiceDef = serviceDef();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		Mockito.when(validatorFactory.getServiceDefValidator(svcStore))
				.thenReturn(serviceDefValidator);

		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(Id)).thenReturn(xServiceDef);
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		serviceREST.deleteServiceDef(rangerServiceDef.getId(), request);
		Mockito.verify(validatorFactory).getServiceDefValidator(svcStore);
		Mockito.verify(daoManager).getXXServiceDef();
	}

	@Test
	public void test13lookupResource() throws Exception {
		String serviceName = "HDFS_1";
		ResourceLookupContext context = new ResourceLookupContext();
		context.setResourceName(serviceName);
		context.setUserInput("HDFS");
		List<String> list = serviceREST.lookupResource(serviceName, context);
		Assert.assertNotNull(list);
	}

	@Test
	public void test14grantAccess() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		String serviceName = "HDFS_1";
		GrantRevokeRequest grantRequestObj = new GrantRevokeRequest();
		grantRequestObj.setAccessTypes(null);
		grantRequestObj.setDelegateAdmin(true);
		grantRequestObj.setEnableAudit(true);
		grantRequestObj.setGrantor("read");
		grantRequestObj.setIsRecursive(true);

		Mockito.when(validatorFactory.getServiceDefValidator(svcStore))
				.thenReturn(serviceDefValidator);
		Mockito.when(
				serviceUtil.isValidateHttpsAuthentication(serviceName, request))
				.thenReturn(false);
		RESTResponse restResponse = serviceREST.grantAccess(serviceName,
				grantRequestObj, request);
		Assert.assertNotNull(restResponse);
		Mockito.verify(serviceUtil).isValidateHttpsAuthentication(serviceName,
				request);
	}

	@Test
	public void test15revokeAccess() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		String serviceName = "HDFS_1";
		Set<String> userList = new HashSet<String>();
		userList.add("user1");
		userList.add("user2");
		userList.add("user3");
		Set<String> groupList = new HashSet<String>();
		groupList.add("group1");
		groupList.add("group2");
		groupList.add("group3");
		GrantRevokeRequest revokeRequest = new GrantRevokeRequest();
		revokeRequest.setDelegateAdmin(true);
		revokeRequest.setEnableAudit(true);
		revokeRequest.setGrantor("read");
		revokeRequest.setGroups(groupList);
		revokeRequest.setUsers(userList);

		Mockito.when(validatorFactory.getServiceDefValidator(svcStore))
				.thenReturn(serviceDefValidator);

		RESTResponse restResponse = serviceREST.revokeAccess(serviceName,
				revokeRequest, request);
		Assert.assertNotNull(restResponse);
	}

	@Test
	public void test16createPolicyFalse() throws Exception {
		RangerPolicy rangerPolicy = rangerPolicy();
		RangerServiceDef rangerServiceDef = rangerServiceDef();

		List<RangerPolicy> policies = new ArrayList<RangerPolicy>();
		RangerPolicy rangPolicy = new RangerPolicy();
		policies.add(rangPolicy);

		String userName = "admin";
		Set<String> userGroupsList = new HashSet<String>();
		userGroupsList.add("group1");
		userGroupsList.add("group2");

		ServicePolicies servicePolicies = new ServicePolicies();
		servicePolicies.setServiceId(Id);
		servicePolicies.setServiceName("Hdfs_1");
		servicePolicies.setPolicyVersion(1L);
		servicePolicies.setPolicyUpdateTime(new Date());
		servicePolicies.setServiceDef(rangerServiceDef);
		servicePolicies.setPolicies(policies);

		List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();
		RangerAccessTypeDef rangerAccessTypeDefObj = new RangerAccessTypeDef();
		rangerAccessTypeDefObj.setLabel("Read");
		rangerAccessTypeDefObj.setName("read");
		rangerAccessTypeDefObj.setRbKeyLabel(null);
		rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
		XXServiceDef xServiceDef = serviceDef();
		XXService xService = xService();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);

		Mockito.when(
				svcStore.getServicePoliciesIfUpdated(Mockito.anyString(),
						Mockito.anyLong())).thenReturn(servicePolicies);
		Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(
				policyValidator);
		Mockito.when(bizUtil.isAdmin()).thenReturn(true);
		Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
		Mockito.when(userMgr.getGroupsForUser(userName)).thenReturn(
				userGroupsList);
		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(
				xService);
		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(
				xServiceDef);
		Mockito.when(svcStore.createPolicy((RangerPolicy) Mockito.anyObject()))
				.thenReturn(rangPolicy);

		RangerPolicy dbRangerPolicy = serviceREST.createPolicy(rangerPolicy,null);
		Assert.assertNotNull(dbRangerPolicy);
		Mockito.verify(bizUtil, Mockito.times(2)).isAdmin();
		Mockito.verify(validatorFactory).getPolicyValidator(svcStore);

		Mockito.verify(daoManager).getXXService();
		Mockito.verify(daoManager).getXXServiceDef();
	}

	@Test
	public void test17updatePolicyFalse() throws Exception {
		RangerPolicy rangerPolicy = rangerPolicy();
		String userName = "admin";

		Set<String> userGroupsList = new HashSet<String>();
		userGroupsList.add("group1");
		userGroupsList.add("group2");

		List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();
		RangerAccessTypeDef rangerAccessTypeDefObj = new RangerAccessTypeDef();
		rangerAccessTypeDefObj.setLabel("Read");
		rangerAccessTypeDefObj.setName("read");
		rangerAccessTypeDefObj.setRbKeyLabel(null);
		rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
		XXServiceDef xServiceDef = serviceDef();
		XXService xService = xService();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);

		Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(
				policyValidator);
		Mockito.when(bizUtil.isAdmin()).thenReturn(true);
		Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
		Mockito.when(userMgr.getGroupsForUser(userName)).thenReturn(
				userGroupsList);
		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
		RangerPolicy dbRangerPolicy = serviceREST.updatePolicy(rangerPolicy);
		Assert.assertNull(dbRangerPolicy);
		Mockito.verify(validatorFactory).getPolicyValidator(svcStore);
	}

	@Test
	public void test18deletePolicyFalse() throws Exception {
		RangerPolicy rangerPolicy = rangerPolicy();

		Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(
				policyValidator);
		String userName = "admin";

		Set<String> userGroupsList = new HashSet<String>();
		userGroupsList.add("group1");
		userGroupsList.add("group2");

		List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();
		RangerAccessTypeDef rangerAccessTypeDefObj = new RangerAccessTypeDef();
		rangerAccessTypeDefObj.setLabel("Read");
		rangerAccessTypeDefObj.setName("read");
		rangerAccessTypeDefObj.setRbKeyLabel(null);
		rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
		XXServiceDef xServiceDef = serviceDef();
		XXService xService = xService();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(
				policyValidator);
		Mockito.when(bizUtil.isAdmin()).thenReturn(true);
		Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
		Mockito.when(userMgr.getGroupsForUser(userName)).thenReturn(userGroupsList);
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
		Mockito.when(svcStore.getPolicy(rangerPolicy.getId())).thenReturn(
				rangerPolicy);
		String userName = "admin";

		Set<String> userGroupsList = new HashSet<String>();
		userGroupsList.add("group1");
		userGroupsList.add("group2");

		List<RangerAccessTypeDef> rangerAccessTypeDefList = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();
		RangerAccessTypeDef rangerAccessTypeDefObj = new RangerAccessTypeDef();
		rangerAccessTypeDefObj.setLabel("Read");
		rangerAccessTypeDefObj.setName("read");
		rangerAccessTypeDefObj.setRbKeyLabel(null);
		rangerAccessTypeDefList.add(rangerAccessTypeDefObj);
		XXServiceDef xServiceDef = serviceDef();
		XXService xService = xService();
		XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
		XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
		Mockito.when(validatorFactory.getPolicyValidator(svcStore)).thenReturn(
				policyValidator);
		Mockito.when(bizUtil.isAdmin()).thenReturn(true);
		Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
		Mockito.when(userMgr.getGroupsForUser(userName)).thenReturn(
				userGroupsList);
		Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
		Mockito.when(xServiceDao.findByName(Mockito.anyString())).thenReturn(xService);
		Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
		Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
		RangerPolicy dbRangerPolicy = serviceREST.getPolicy(rangerPolicy
				.getId());
		Assert.assertNotNull(dbRangerPolicy);
		Assert.assertEquals(dbRangerPolicy.getId(), rangerPolicy.getId());
		Mockito.verify(svcStore).getPolicy(rangerPolicy.getId());
	}

	@Test
	public void test20getPolicies() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		Mockito.when(
				searchUtil.getSearchFilter(request, policyService.sortFields))
				.thenReturn(filter);
		RangerPolicyList dbRangerPolicy = serviceREST.getPolicies(request);
		Assert.assertNotNull(dbRangerPolicy);
		Assert.assertEquals(dbRangerPolicy.getListSize(), 0);
		Mockito.verify(searchUtil).getSearchFilter(request,
				policyService.sortFields);
	}

	@Test
	public void test21countPolicies() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

		PList<RangerPolicy> ret  = Mockito.mock(PList.class);
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		Mockito.when(
				searchUtil.getSearchFilter(request, policyService.sortFields))
				.thenReturn(filter);

		Mockito.when(svcStore.getPaginatedPolicies(filter)).thenReturn(ret);

		Long data = serviceREST.countPolicies(request);
		Assert.assertNotNull(data);
		Mockito.verify(searchUtil).getSearchFilter(request,
				policyService.sortFields);
	}

	@Test
	public void test22getServicePoliciesById() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		RangerPolicy rangerPolicy = rangerPolicy();

		PList<RangerPolicy> ret = Mockito.mock(PList.class);
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		Mockito.when(
				searchUtil.getSearchFilter(request, policyService.sortFields))
				.thenReturn(filter);

		Mockito.when(svcStore.getPaginatedServicePolicies(Id, filter))
				.thenReturn(ret);

		RangerPolicyList dbRangerPolicy = serviceREST.getServicePolicies(
				rangerPolicy.getId(), request);
		Assert.assertNotNull(dbRangerPolicy);
		Mockito.verify(searchUtil).getSearchFilter(request,
				policyService.sortFields);
		Mockito.verify(svcStore).getServicePolicies(Id, filter);
	}

	@Test
	public void test23getServicePoliciesByName() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		RangerPolicy rangerPolicy = rangerPolicy();

		List<RangerPolicy> ret  = Mockito.mock(List.class);
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		Mockito.when(
				searchUtil.getSearchFilter(request, policyService.sortFields))
				.thenReturn(filter);

		Mockito.when(
				svcStore.getServicePolicies(rangerPolicy.getName(),
						filter)).thenReturn(ret);

		RangerPolicyList dbRangerPolicy = serviceREST.getServicePoliciesByName(
				rangerPolicy.getName(), request);
		Assert.assertNotNull(dbRangerPolicy);
	}

	@Test
	public void test24getServicePoliciesIfUpdated() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		String serviceName = "HDFS_1";
		Long lastKnownVersion = 1L;
		String pluginId = "1";

		Mockito.doNothing().when(assetMgr).createPluginInfo(serviceName, pluginId, null, 1, 1L, lastKnownVersion, 1, 0);

		ServicePolicies dbServicePolicies = serviceREST
				.getServicePoliciesIfUpdated(serviceName, lastKnownVersion, 0L,
						pluginId, "", request);
		Assert.assertNull(dbServicePolicies);
	}

	@Test
	public void test25getPolicies() throws Exception {
		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		Mockito.when(svcStore.getPolicies(filter)).thenReturn(ret);

		List<RangerPolicy> dbRangerPolicyList = serviceREST.getPolicies(filter);
		Assert.assertNotNull(dbRangerPolicyList);
		Mockito.verify(svcStore).getPolicies(filter);

	}

	@Test
	public void test26getServices() throws Exception {
		List<RangerService> ret = new ArrayList<RangerService>();
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		Mockito.when(svcStore.getServices(filter)).thenReturn(ret);

		List<RangerService> dbRangerService = serviceREST.getServices(filter);
		Assert.assertNotNull(dbRangerService);
		Mockito.verify(svcStore).getServices(filter);
	}

	@Test
	public void test30getPolicyFromEventTime() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

		String strdt = new Date().toString();
		String userName="Admin";
		Set<String> userGroupsList = new HashSet<String>();
		userGroupsList.add("group1");
		userGroupsList.add("group2");
		Mockito.when(request.getParameter("eventTime")).thenReturn(strdt);
		Mockito.when(request.getParameter("policyId")).thenReturn("1");
		RangerPolicy policy=new RangerPolicy();
		Map<String, RangerPolicyResource> resources=new HashMap<String, RangerPolicy.RangerPolicyResource>();
		policy.setService("services");
		policy.setResources(resources);
		Mockito.when(svcStore.getPolicyFromEventTime(strdt, 1l)).thenReturn(policy);
		Mockito.when(bizUtil.isAdmin()).thenReturn(false);
		Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(userName);
		Mockito.when(userMgr.getGroupsForUser(userName)).thenReturn(
				userGroupsList);

		Mockito.when(restErrorUtil.createRESTException(Matchers.anyInt(), Matchers.anyString(), Matchers.anyBoolean()))
				.thenThrow(new WebApplicationException());
		thrown.expect(WebApplicationException.class);
	
		RangerPolicy dbRangerPolicy = serviceREST
				.getPolicyFromEventTime(request);
		Assert.assertNull(dbRangerPolicy);
		Mockito.verify(request).getParameter("eventTime");
		Mockito.verify(request).getParameter("policyId");
	}

	@Test
	public void test31getServices() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		RangerServiceList dbRangerService = serviceREST.getServices(request);
		Assert.assertNull(dbRangerService);
	}

	@Test
	public void test32getPolicyVersionList() throws Exception {
		VXString vXString = new VXString();
		vXString.setValue("1");
		Mockito.when(svcStore.getPolicyVersionList(Id)).thenReturn(vXString);

		VXString dbVXString = serviceREST.getPolicyVersionList(Id);
		Assert.assertNotNull(dbVXString);
		Mockito.verify(svcStore).getPolicyVersionList(Id);
	}

	@Test
	public void test33getPolicyForVersionNumber() throws Exception {
		RangerPolicy rangerPolicy = rangerPolicy();
		Mockito.when(svcStore.getPolicyForVersionNumber(Id, 1)).thenReturn(
				rangerPolicy);
		RangerPolicy dbRangerPolicy = serviceREST.getPolicyForVersionNumber(Id,
				1);
		Assert.assertNotNull(dbRangerPolicy);
		Mockito.verify(svcStore).getPolicyForVersionNumber(Id, 1);
	}

	@Test
	public void test34countServices() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		PList<RangerService> ret  = Mockito.mock(PList.class);
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, "policyName");
		filter.setParam(SearchFilter.SERVICE_NAME, "serviceName");
		Mockito.when(
				searchUtil.getSearchFilter(request, policyService.sortFields))
				.thenReturn(filter);

		Mockito.when(svcStore.getPaginatedServices(filter)).thenReturn(ret);
		Long data = serviceREST.countServices(request);
		Assert.assertNotNull(data);
		Mockito.verify(searchUtil).getSearchFilter(request,
				policyService.sortFields);
		Mockito.verify(svcStore).getPaginatedServices(filter);
	}

	@Test
	public void test35validateConfig() throws Exception {
		RangerService rangerService = rangerService();
		Mockito.when(serviceMgr.validateConfig(rangerService, svcStore))
				.thenReturn(vXResponse);
		VXResponse dbVXResponse = serviceREST.validateConfig(rangerService);
		Assert.assertNotNull(dbVXResponse);
		Mockito.verify(serviceMgr).validateConfig(rangerService, svcStore);
	}

	@Test
	public void test40applyPolicy() {
		RangerPolicy existingPolicy = rangerPolicy();
		RangerPolicy appliedPolicy = rangerPolicy();

                List<RangerPolicyItem> policyItem = new ArrayList<RangerPolicyItem>();
                existingPolicy.setPolicyItems(policyItem );
		appliedPolicy.setPolicyItems(null);

		Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();
		RangerPolicyResource rangerPolicyResource = new RangerPolicyResource("/tmp");
		rangerPolicyResource.setIsExcludes(true);
		rangerPolicyResource.setIsRecursive(true);
		policyResources.put("path", rangerPolicyResource);

		existingPolicy.setResources(policyResources);
		appliedPolicy.setResources(policyResources);

		RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("read", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("write", true));
		rangerPolicyItem.getGroups().add("group1");
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(true);

		existingPolicy.getPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getGroups().add("group1");
		rangerPolicyItem.getGroups().add("public");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.getUsers().add("finance");
		rangerPolicyItem.setDelegateAdmin(false);

		appliedPolicy.getPolicyItems().add(rangerPolicyItem);

		String existingPolicyStr = existingPolicy.toString();
		System.out.println("existingPolicy=" + existingPolicyStr);

		ServiceRESTUtil.processApplyPolicy(existingPolicy, appliedPolicy);

		String resultPolicyStr = existingPolicy.toString();
		System.out.println("resultPolicy=" + resultPolicyStr);

		assert(true);
	}

	@Test
	public void test41applyPolicy() {
		RangerPolicy existingPolicy = rangerPolicy();
		RangerPolicy appliedPolicy = rangerPolicy();

                List<RangerPolicyItem> policyItem = new ArrayList<RangerPolicyItem>();
                existingPolicy.setPolicyItems(policyItem);
		appliedPolicy.setPolicyItems(null);

		Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();
		RangerPolicyResource rangerPolicyResource = new RangerPolicyResource("/tmp");
		rangerPolicyResource.setIsExcludes(true);
		rangerPolicyResource.setIsRecursive(true);
		policyResources.put("path", rangerPolicyResource);

		existingPolicy.setResources(policyResources);
		appliedPolicy.setResources(policyResources);

		RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("read", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("write", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("lock", true));
		rangerPolicyItem.getGroups().add("group1");
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(true);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("read", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("write", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("lock", true));
		rangerPolicyItem.getGroups().add("group3");
		rangerPolicyItem.getUsers().add("user3");
		rangerPolicyItem.setDelegateAdmin(true);

		existingPolicy.getPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("lock", true));
		rangerPolicyItem.getGroups().add("group1");
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(false);

		existingPolicy.getAllowExceptions().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(false);

		existingPolicy.getDenyPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("index", true));
		rangerPolicyItem.getGroups().add("public");
		rangerPolicyItem.getUsers().add("user");
		rangerPolicyItem.setDelegateAdmin(false);

		existingPolicy.getDenyPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("index", true));

		rangerPolicyItem.getGroups().add("group1");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.setDelegateAdmin(false);

		appliedPolicy.getPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));

		rangerPolicyItem.getGroups().add("public");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.setDelegateAdmin(false);

		appliedPolicy.getDenyPolicyItems().add(rangerPolicyItem);

		String existingPolicyStr = existingPolicy.toString();
		System.out.println("existingPolicy=" + existingPolicyStr);

		ServiceRESTUtil.processApplyPolicy(existingPolicy, appliedPolicy);

		String resultPolicyStr = existingPolicy.toString();
		System.out.println("resultPolicy=" + resultPolicyStr);

		assert(true);
	}

	@Test
	public void test42grant() {
		RangerPolicy existingPolicy = rangerPolicy();
                List<RangerPolicyItem> policyItem = new ArrayList<RangerPolicyItem>();
                existingPolicy.setPolicyItems(policyItem );

		Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();
		RangerPolicyResource rangerPolicyResource = new RangerPolicyResource("/tmp");
		rangerPolicyResource.setIsExcludes(true);
		rangerPolicyResource.setIsRecursive(true);
		policyResources.put("path", rangerPolicyResource);

		existingPolicy.setResources(policyResources);

		RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("read", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("write", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("lock", true));
		rangerPolicyItem.getGroups().add("group1");
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(true);

		existingPolicy.getPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("read", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("write", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("lock", true));
		rangerPolicyItem.getGroups().add("group3");
		rangerPolicyItem.getUsers().add("user3");
		rangerPolicyItem.setDelegateAdmin(true);

		existingPolicy.getPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("lock", true));
		rangerPolicyItem.getGroups().add("group1");
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(false);

		existingPolicy.getAllowExceptions().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(false);

		existingPolicy.getDenyPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("index", true));
		rangerPolicyItem.getGroups().add("public");
		rangerPolicyItem.getUsers().add("user");
		rangerPolicyItem.setDelegateAdmin(false);

		existingPolicy.getDenyPolicyItems().add(rangerPolicyItem);

		GrantRevokeRequest grantRequestObj = new GrantRevokeRequest();
		Map<String, String> resource = new HashMap<String, String>();
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
		System.out.println("existingPolicy=" + existingPolicyStr);

		ServiceRESTUtil.processGrantRequest(existingPolicy, grantRequestObj);

		String resultPolicyStr = existingPolicy.toString();
		System.out.println("resultPolicy=" + resultPolicyStr);

		assert(true);
	}

	@Test
	public void test43revoke() {
		RangerPolicy existingPolicy = rangerPolicy();

                List<RangerPolicyItem> policyItem = new ArrayList<RangerPolicyItem>();
                existingPolicy.setPolicyItems(policyItem );

		Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();
		RangerPolicyResource rangerPolicyResource = new RangerPolicyResource("/tmp");
		rangerPolicyResource.setIsExcludes(true);
		rangerPolicyResource.setIsRecursive(true);
		policyResources.put("path", rangerPolicyResource);

		existingPolicy.setResources(policyResources);

		RangerPolicyItem rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("read", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("write", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("lock", true));
		rangerPolicyItem.getGroups().add("group1");
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(true);

		existingPolicy.getPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("read", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("write", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("lock", true));
		rangerPolicyItem.getGroups().add("group3");
		rangerPolicyItem.getUsers().add("user3");
		rangerPolicyItem.setDelegateAdmin(true);

		existingPolicy.getPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("lock", true));
		rangerPolicyItem.getGroups().add("group1");
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user1");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(false);

		existingPolicy.getAllowExceptions().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("delete", true));
		rangerPolicyItem.getGroups().add("group2");
		rangerPolicyItem.getUsers().add("user2");
		rangerPolicyItem.setDelegateAdmin(false);

		existingPolicy.getDenyPolicyItems().add(rangerPolicyItem);

		rangerPolicyItem = new RangerPolicyItem();
		rangerPolicyItem.getAccesses().add(new RangerPolicyItemAccess("index", true));
		rangerPolicyItem.getGroups().add("public");
		rangerPolicyItem.getUsers().add("user");
		rangerPolicyItem.setDelegateAdmin(false);

		existingPolicy.getDenyPolicyItems().add(rangerPolicyItem);

		GrantRevokeRequest revokeRequestObj = new GrantRevokeRequest();
		Map<String, String> resource = new HashMap<String, String>();
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

		assert(true);
	}
}
