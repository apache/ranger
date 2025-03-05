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

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
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
import org.mockito.junit.MockitoJUnitRunner;

import javax.ws.rs.WebApplicationException;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestRangerPolicyServiceBase {
    private static final Long              Id     = 8L;
    @Rule
    public               ExpectedException thrown = ExpectedException.none();
    @InjectMocks
    RangerPolicyService policyService = new RangerPolicyService();
    @Mock
    RangerDaoManager daoManager;
    @Mock
    RESTErrorUtil restErrorUtil;
    @Mock
    ContextUtil contextUtil;
    @Mock
    RangerBizUtil rangerBizUtil;
    @Mock
    RangerSearchUtil searchUtil;

    public void setup() {
        RangerSecurityContext context = new RangerSecurityContext();
        context.setUserSession(new UserSessionBase());
        RangerContextHolder.setSecurityContext(context);
        UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
        currentUserSession.setUserAdmin(true);
    }

    @Test
    public void test1mapViewToEntityBean() {
        XXServiceDao    xServiceDao       = Mockito.mock(XXServiceDao.class);
        XXService       xService          = Mockito.mock(XXService.class);
        XXServiceDefDao xServiceDefDao    = Mockito.mock(XXServiceDefDao.class);
        XXServiceDef    xServiceDef       = Mockito.mock(XXServiceDef.class);
        RangerPolicy    rangerPolicy      = rangerPolicy();
        XXPolicy        policy            = policy();
        int             operationContext = 0;

        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(rangerPolicy.getService())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);

        XXPolicy dbPolicy = policyService.mapViewToEntityBean(rangerPolicy, policy, operationContext);
        Assert.assertNotNull(dbPolicy);
        Assert.assertEquals(dbPolicy.getId(), policy.getId());
        Assert.assertEquals(dbPolicy.getGuid(), policy.getGuid());
        Assert.assertEquals(dbPolicy.getName(), policy.getName());
        Assert.assertEquals(dbPolicy.getAddedByUserId(), policy.getAddedByUserId());
        Assert.assertEquals(dbPolicy.getIsEnabled(), policy.getIsEnabled());
        Assert.assertEquals(dbPolicy.getVersion(), policy.getVersion());
        Assert.assertEquals(dbPolicy.getDescription(), policy.getDescription());

        Mockito.verify(daoManager).getXXService();
    }

    @Test
    public void test2mapViewToEntityBeanNullValue() {
        XXServiceDao xServiceDao       = Mockito.mock(XXServiceDao.class);
        RangerPolicy rangerPolicy      = rangerPolicy();
        XXPolicy     policy            = policy();
        int          operationContext  = 0;

        Mockito.when(restErrorUtil.createRESTException("No corresponding service found for policyName: " + rangerPolicy.getName() + "Service Not Found : " + rangerPolicy.getName(), MessageEnums.INVALID_INPUT_DATA)).thenThrow(new WebApplicationException());

        thrown.expect(WebApplicationException.class);

        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.findByName(rangerPolicy.getService())).thenReturn(null);

        XXPolicy dbPolicy = policyService.mapViewToEntityBean(rangerPolicy, policy, operationContext);
        Assert.assertNotNull(dbPolicy);
        Assert.assertEquals(dbPolicy.getId(), policy.getId());
        Assert.assertEquals(dbPolicy.getGuid(), policy.getGuid());
        Assert.assertEquals(dbPolicy.getName(), policy.getName());
        Assert.assertEquals(dbPolicy.getAddedByUserId(), policy.getAddedByUserId());
        Assert.assertEquals(dbPolicy.getIsEnabled(), policy.getIsEnabled());
        Assert.assertEquals(dbPolicy.getVersion(), policy.getVersion());
        Assert.assertEquals(dbPolicy.getDescription(), policy.getDescription());

        Mockito.verify(daoManager).getXXService();
    }

    @Test
    public void test3mapEntityToViewBean() {
        XXServiceDao    xServiceDao    = Mockito.mock(XXServiceDao.class);
        XXService       xService       = Mockito.mock(XXService.class);
        XXServiceDefDao xServiceDefDao = Mockito.mock(XXServiceDefDao.class);
        XXServiceDef    xServiceDef    = Mockito.mock(XXServiceDef.class);
        RangerPolicy    rangerPolicy   = rangerPolicy();
        XXPolicy        policy         = policy();

        Mockito.when(daoManager.getXXService()).thenReturn(xServiceDao);
        Mockito.when(xServiceDao.getById(policy.getService())).thenReturn(xService);
        Mockito.when(daoManager.getXXServiceDef()).thenReturn(xServiceDefDao);
        Mockito.when(xServiceDefDao.getById(xService.getType())).thenReturn(xServiceDef);
        RangerPolicy dbRangerPolicy = policyService.mapEntityToViewBean(rangerPolicy, policy);

        Assert.assertNotNull(dbRangerPolicy);
        Assert.assertEquals(dbRangerPolicy.getId(), rangerPolicy.getId());
        Assert.assertEquals(dbRangerPolicy.getGuid(), rangerPolicy.getGuid());
        Assert.assertEquals(dbRangerPolicy.getName(), rangerPolicy.getName());
        Assert.assertEquals(dbRangerPolicy.getIsEnabled(), rangerPolicy.getIsEnabled());
        Assert.assertEquals(dbRangerPolicy.getVersion(), rangerPolicy.getVersion());
        Assert.assertEquals(dbRangerPolicy.getDescription(), rangerPolicy.getDescription());

        Mockito.verify(daoManager).getXXService();
    }

    private RangerPolicy rangerPolicy() {
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
        policy.setZoneName("");

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
        xxPolicy.setZoneId(1L);
        return xxPolicy;
    }
}
