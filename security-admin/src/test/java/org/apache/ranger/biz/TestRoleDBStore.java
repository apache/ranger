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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.ws.rs.WebApplicationException;

import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXGlobalStateDao;
import org.apache.ranger.db.XXPolicyRefRoleDao;
import org.apache.ranger.db.XXRoleDao;
import org.apache.ranger.db.XXRoleRefRoleDao;
import org.apache.ranger.db.XXSecurityZoneRefRoleDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXRole;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerRole.RoleMember;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.service.RangerRoleService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.RangerRoleList;
import org.apache.ranger.view.VXPortalUser;
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
public class TestRoleDBStore {
    private static final Long   id              = 8L;
    private static final Long   userId          = 8L;
    private static final Long   roleId          = 9L;
    private static final String keyAdminLoginID = "keyadmin";
    private static final String userLoginID     = "testuser";
    private static final String roleName        = "test-role";

    @InjectMocks
    RoleDBStore roleDBStore = new RoleDBStore();

    @Mock
    RangerBizUtil bizUtil;

    @Mock
    RangerDaoManager daoMgr;

    @Mock
    RESTErrorUtil restErrorUtil;

    @Mock
    RangerTransactionSynchronizationAdapter transactionSynchronizationAdapter;

    @Mock
    ServiceDBStore svcStore;

    @Mock
    RangerAdminConfig config;

    @Mock
    RangerRoleService roleService;

    @Mock
    XUserService xUserService;

    @Mock
    RoleRefUpdater roleRefUpdater;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRoleExistsByRoleName() throws Exception {
        XXRoleDao xxRoleDao = Mockito.mock(XXRoleDao.class);
        XXRole    xxRole    = getTestRole();

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleName(xxRole.getName())).thenReturn(xxRole);

        Assert.assertTrue(roleDBStore.roleExists(roleName));
        Assert.assertFalse(roleDBStore.roleExists(roleName + "non-existing"));
    }

    @Test
    public void testRoleExistsByRoleId() throws Exception {
        XXRoleDao xxRoleDao = Mockito.mock(XXRoleDao.class);
        XXRole    xxRole    = getTestRole();

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleId(xxRole.getId())).thenReturn(xxRole);

        Assert.assertTrue(roleDBStore.roleExists(roleId));
        Assert.assertFalse(roleDBStore.roleExists(roleId + 100L));
    }

    @Test
    public void testGetRoleByRoleId() throws Exception {
        RangerRole rangerRole = getRangerRole();

        Mockito.when(roleService.read(rangerRole.getId())).thenReturn(rangerRole);

        RangerRole rangerRoleInDB = roleDBStore.getRole(rangerRole.getId());

        Assert.assertNotNull(rangerRoleInDB);
        Assert.assertEquals(rangerRole.getId(), rangerRoleInDB.getId());
    }


    @Test
    public void testGetRolesBySearchFilter() throws Exception {
        RangerRole     rangerRole     = getRangerRole();
        RangerRoleList rangerRoleList = new RangerRoleList(Collections.singletonList(rangerRole));
        XXRole         xxRole         = getTestRole();
        List<XXRole>   xxRoles        = Collections.singletonList(xxRole);
        SearchFilter   searchFilter   = new SearchFilter();

        Mockito.when(roleService.searchResources(searchFilter, roleService.searchFields, roleService.sortFields , rangerRoleList)).thenReturn(xxRoles);
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);

        RangerRoleList rangerRoleListInDB = roleDBStore.getRoles(searchFilter, rangerRoleList);

        Assert.assertNotNull(rangerRoleListInDB);
        Assert.assertEquals(1, rangerRoleListInDB.getList().size());
    }

    @Test
    public void testGetRolesForUser_WithoutUserSession() throws Exception {
        RangerRole     rangerRole     = getRangerRole();
        RangerRoleList rangerRoleList = new RangerRoleList(Collections.singletonList(rangerRole));
        XXRole         xxRole         = getTestRole();
        List<XXRole>   xxRoles        = Collections.singletonList(xxRole);
        SearchFilter   searchFilter   = new SearchFilter();

        Mockito.when(roleService.searchResources(searchFilter, roleService.searchFields, roleService.sortFields , rangerRoleList)).thenReturn(xxRoles);
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);

        RangerContextHolder.setSecurityContext(null);

        RangerRoleList rangerRoleListInDB = roleDBStore.getRolesForUser(searchFilter, rangerRoleList);

        Assert.assertNotNull(rangerRoleListInDB);
        Assert.assertEquals(1, rangerRoleListInDB.getList().size());
    }

    @Test
    public void testGetRolesForUser_WithUserSession() throws Exception {
        RangerRole     rangerRole     = getRangerRole();
        RangerRoleList rangerRoleList = new RangerRoleList(Collections.singletonList(rangerRole));
        XXRole         xxRole         = getTestRole();
        List<XXRole>   xxRoles        = Collections.singletonList(xxRole);
        XXPortalUser   userKeyAdmin   = new XXPortalUser() {{ setId(getUserProfile().getId()); setLoginId(keyAdminLoginID); }};
        VXUser         vxUserKeyAdmin = new VXUser() {{ setId(userKeyAdmin.getId()); }};
        SearchFilter   searchFilter   = new SearchFilter();
        XXRoleDao      xxRoleDao      = Mockito.mock(XXRoleDao.class);

        Mockito.when(xUserService.getXUserByUserName(userKeyAdmin.getLoginId())).thenReturn(vxUserKeyAdmin);
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);
        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByUserId(userKeyAdmin.getId())).thenReturn(xxRoles);

        RangerSecurityContext context = new RangerSecurityContext() {{ setUserSession(new UserSessionBase());}};

        RangerContextHolder.setSecurityContext(context);

        UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();

        currentUserSession.setXXPortalUser(userKeyAdmin);
        currentUserSession.setKeyAdmin(true);
        currentUserSession.setUserRoleList(Collections.singletonList((RangerConstants.ROLE_USER)));

        RangerRoleList rangerRoleListInDB = roleDBStore.getRolesForUser(searchFilter, rangerRoleList);

        Assert.assertNotNull(rangerRoleListInDB);
        Assert.assertEquals(1, rangerRoleListInDB.getList().size());
    }

    @Test
    public void testGetRolesByServiceId() {
        XXService    xxService  = getXXService();
        XXRoleDao    xxRoleDao  = Mockito.mock(XXRoleDao.class);
        XXRole       xxRole     = getTestRole();
        RangerRole   rangerRole = getRangerRole();
        List<XXRole> xxRoles    = Collections.singletonList(xxRole);

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.getAll()).thenReturn(xxRoles);

        XXServiceDefDao xxServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(daoMgr.getXXServiceDef()).thenReturn(xxServiceDefDao);
        Mockito.when(xxServiceDefDao.findServiceDefTypeByServiceId(xxService.getId())).thenReturn("test");
        Mockito.when(config.get("ranger.admin.service.types.for.returning.all.roles", "solr")).thenReturn("test,test1");
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);
        Mockito.when(xxRoleDao.getAll()).thenReturn(xxRoles);

        List<RangerRole> rangerRoleListInDB = roleDBStore.getRoles(xxService.getId());

        Assert.assertNotNull(rangerRoleListInDB);
        Assert.assertEquals(1, rangerRoleListInDB.size());
    }

    @Test
    public void testGetRolesByService() {
        XXService       xxService       = getXXService();
        XXRole          xxRole          = getTestRole();
        RangerRole      rangerRole      = getRangerRole();
        List<XXRole>    xxRoles         = Collections.singletonList(xxRole);
        XXRoleDao       xxRoleDao       = Mockito.mock(XXRoleDao.class);
        XXServiceDefDao xxServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.getAll()).thenReturn(xxRoles);
        Mockito.when(daoMgr.getXXServiceDef()).thenReturn(xxServiceDefDao);
        Mockito.when(xxServiceDefDao.findServiceDefTypeByServiceId(xxService.getId())).thenReturn("test");
        Mockito.when(config.get("ranger.admin.service.types.for.returning.all.roles", "solr")).thenReturn("test,test1");
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);

        List<RangerRole> rangerRoleListInDB = roleDBStore.getRoles(xxService);

        Assert.assertNotNull(rangerRoleListInDB);
        Assert.assertEquals(1, rangerRoleListInDB.size());
    }

    @Test
    public void testGetRoleByRoleName() throws Exception {
        XXRoleDao  xxRoleDao  = Mockito.mock(XXRoleDao.class);
        XXRole     xxRole     = getTestRole();
        RangerRole rangerRole = getRangerRole();

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleName(roleName)).thenReturn(xxRole);
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);

        RangerRole rangerRoleInDB = roleDBStore.getRole(roleName);

        Assert.assertNotNull(rangerRoleInDB);

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenThrow(new WebApplicationException());
        thrown.expect(WebApplicationException.class);

        roleDBStore.getRole(roleName + "-non-existing");
    }

    @Test
    public void testGetRoleNames() throws Exception {
        List<String> roleNames    = Collections.singletonList(roleName);
        XXRoleDao    xxRoleDao    = Mockito.mock(XXRoleDao.class);
        SearchFilter searchFilter = new SearchFilter();

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.getAllNames()).thenReturn(roleNames);

        List<String> roleNamesInDB = roleDBStore.getRoleNames(searchFilter);

        Assert.assertNotNull(roleNamesInDB);
    }

    @Test
    public void testGetRoles() throws Exception {
        XXRoleDao    xxRoleDao    = Mockito.mock(XXRoleDao.class);
        XXRole       xxRole       = getTestRole();
        List<XXRole> xxRoles      = Collections.singletonList(xxRole);
        SearchFilter searchFilter = new SearchFilter();
        RangerRole   rangerRole   = getRangerRole();

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.getAll()).thenReturn(xxRoles);
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);

        List<RangerRole>  rangerRolesInDB = roleDBStore.getRoles(searchFilter);

        Assert.assertNotNull(rangerRolesInDB);
    }

    @Test
    public void testGetRoleVersion() {
        XXService        xxService        = getXXService();
        XXGlobalStateDao xxGlobalStateDao = Mockito.mock(XXGlobalStateDao.class);

        Mockito.when(daoMgr.getXXGlobalState()).thenReturn(xxGlobalStateDao);
        Mockito.when(xxGlobalStateDao.getAppDataVersion("RangerRole")).thenReturn(1L);

        Long  roleVersion = roleDBStore.getRoleVersion(xxService.getName());

        Assert.assertNotNull(roleVersion);
    }

    @Test
    public void testGetRolesByLastKnownVersion() throws Exception {
        XXRoleDao        xxRoleDao        = Mockito.mock(XXRoleDao.class);
        XXGlobalStateDao xxGlobalStateDao = Mockito.mock(XXGlobalStateDao.class);
        XXRole           xxRole           = getTestRole();
        List<XXRole>     xxRoles          = Collections.singletonList(xxRole);
        XXService        xxService        = getXXService();
        RangerRole       rangerRole       = getRangerRole();

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.getAll()).thenReturn(xxRoles);
        Mockito.when(daoMgr.getXXGlobalState()).thenReturn(xxGlobalStateDao);
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);
        Mockito.when(xxGlobalStateDao.getAppDataVersion("RangerRole")).thenReturn(2L);

        RangerRoles rangerRolesInCache = roleDBStore.getRoles(xxService.getName(), 1L);

        Assert.assertNotNull(rangerRolesInCache);
    }

    @Test
    public void testDeleteRoleByInValidRoleName() throws Exception {
        XXRoleDao xxRoleDao = Mockito.mock(XXRoleDao.class);

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleName(roleName)).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenThrow(new WebApplicationException());
        thrown.expect(WebApplicationException.class);

        roleDBStore.deleteRole(roleName);
    }

    @Test
    public void testDeleteRoleByValidRoleName() throws Exception {
        XXRoleDao                xxRoleDao          = Mockito.mock(XXRoleDao.class);
        XXPolicyRefRoleDao       xxPolicyRefRoleDao = Mockito.mock(XXPolicyRefRoleDao.class);
        XXRoleRefRoleDao         xxRoleRefRoleDao   = Mockito.mock(XXRoleRefRoleDao.class);
        XXSecurityZoneRefRoleDao xxSzRefRoleDao     = Mockito.mock(XXSecurityZoneRefRoleDao.class);
        XXRole                   xxRole             = getTestRole();
        RangerRole               rangerRole         = getRangerRole();

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(daoMgr.getXXPolicyRefRole()).thenReturn(xxPolicyRefRoleDao);
        Mockito.when(daoMgr.getXXSecurityZoneRefRole()).thenReturn(xxSzRefRoleDao);
        Mockito.when(xxPolicyRefRoleDao.findRoleRefPolicyCount(roleName)).thenReturn(0L);
        Mockito.when(daoMgr.getXXRoleRefRole()).thenReturn(xxRoleRefRoleDao);
        Mockito.when(xxRoleRefRoleDao.findRoleRefRoleCount(roleName)).thenReturn(0L);
        Mockito.when(xxSzRefRoleDao.findRoleRefZoneCount(roleName)).thenReturn(0L);
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);
        Mockito.when(xxRoleDao.findByRoleName(roleName)).thenReturn(xxRole);
        Mockito.doNothing().when(transactionSynchronizationAdapter).executeOnTransactionCommit(Mockito.any());
        Mockito.when(roleRefUpdater.cleanupRefTables(Mockito.any())).thenReturn(true);
        Mockito.doNothing().when(svcStore).updateServiceAuditConfig(Mockito.anyString(), Mockito.any());
        Mockito.when(roleService.delete(Mockito.any())).thenReturn(true);
        Mockito.doNothing().when(roleService).createTransactionLog( Mockito.any(),  Mockito.any(), Mockito.anyInt());

        roleDBStore.deleteRole(roleName);
    }

    @Test
    public void testCreateRoleWhenTheRoleExists() throws Exception {
        XXRoleDao  xxRoleDao  = Mockito.mock(XXRoleDao.class);
        XXRole     xxRole     = getTestRole();
        RangerRole rangerRole = getRangerRole();

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleName(roleName)).thenReturn(xxRole);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString(), Mockito.any())).thenThrow(new WebApplicationException());
        thrown.expect(WebApplicationException.class);

        roleDBStore.createRole(rangerRole, true);
    }

    @Test
    public void testCreateRole() throws Exception {
        RangerRole rangerRole = getRangerRole();
        XXRole     xxRole     = getTestRole();
        XXRoleDao  xxRoleDao  = Mockito.mock(XXRoleDao.class);

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleName(rangerRole.getName())).thenReturn(null).thenReturn(xxRole);
        Mockito.when(roleService.create(rangerRole)).thenReturn(rangerRole);
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);
        Mockito.doNothing().when(transactionSynchronizationAdapter).executeOnTransactionCommit(Mockito.any());
        Mockito.doNothing().when(roleRefUpdater).createNewRoleMappingForRefTable(Mockito.any(), Mockito.anyBoolean());
        Mockito.doNothing().when(roleService).createTransactionLog( Mockito.any(), Mockito.any(), Mockito.anyInt());

        roleDBStore.createRole(rangerRole, true);
    }

    @Test
    public void testUpdateRoleWhenTheRoleNotExists() throws Exception {
        RangerRole rangerRole = getRangerRole();
        XXRoleDao  xxRoleDao  = Mockito.mock(XXRoleDao.class);

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleId(rangerRole.getId())).thenReturn(null);
        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenThrow(new WebApplicationException());
        thrown.expect(WebApplicationException.class);

        roleDBStore.updateRole(rangerRole, true);
    }

    @Test
    public void testUpdateRole() throws Exception {
        RangerRole rangerRole = getRangerRole();
        XXRole     xxRole     = getTestRole();
        XXRoleDao  xxRoleDao  = Mockito.mock(XXRoleDao.class);

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleId(rangerRole.getId())).thenReturn(xxRole);
        Mockito.doNothing().when(transactionSynchronizationAdapter).executeOnTransactionCommit(Mockito.any());
        Mockito.when(roleService.update(rangerRole)).thenReturn(rangerRole);
        Mockito.doNothing().when(roleRefUpdater).createNewRoleMappingForRefTable(Mockito.any(), Mockito.anyBoolean());
        Mockito.doNothing().when(roleService).updatePolicyVersions(rangerRole.getId());
        Mockito.doNothing().when(roleService).createTransactionLog( Mockito.any(),  Mockito.any(), Mockito.anyInt());

        roleDBStore.updateRole(rangerRole, true);
    }

    @Test
    public void testDeleteRoleByRoleId() throws Exception {
        RangerRole               rangerRole         = getRangerRole();
        XXPolicyRefRoleDao       xxPolicyRefRoleDao = Mockito.mock(XXPolicyRefRoleDao.class);
        XXRoleRefRoleDao         xxRoleRefRoleDao   = Mockito.mock(XXRoleRefRoleDao.class);
        XXSecurityZoneRefRoleDao xxSzRefRoleDao     = Mockito.mock(XXSecurityZoneRefRoleDao.class);
        XXRole                   xxRole             = getTestRole();

        Mockito.when(roleService.read(roleId)).thenReturn(rangerRole);
        Mockito.when(daoMgr.getXXPolicyRefRole()).thenReturn(xxPolicyRefRoleDao);
        Mockito.when(daoMgr.getXXSecurityZoneRefRole()).thenReturn(xxSzRefRoleDao);
        Mockito.when(xxPolicyRefRoleDao.findRoleRefPolicyCount(rangerRole.getName())).thenReturn(0L);
        Mockito.when(xxSzRefRoleDao.findRoleRefZoneCount(rangerRole.getName())).thenReturn(0L);
        Mockito.when(daoMgr.getXXRoleRefRole()).thenReturn(xxRoleRefRoleDao);
        Mockito.when(xxRoleRefRoleDao.findRoleRefRoleCount(rangerRole.getName())).thenReturn(0L);
        Mockito.when(roleService.read(xxRole.getId())).thenReturn(rangerRole);
        Mockito.doNothing().when(transactionSynchronizationAdapter).executeOnTransactionCommit(Mockito.any());
        Mockito.when(roleRefUpdater.cleanupRefTables(Mockito.any())).thenReturn(true);
        Mockito.doNothing().when(svcStore).updateServiceAuditConfig(Mockito.anyString(), Mockito.any());
        Mockito.when(roleService.delete(Mockito.any())).thenReturn(true);
        Mockito.doNothing().when(roleService).createTransactionLog( Mockito.any(),  Mockito.any(), Mockito.anyInt());

        roleDBStore.deleteRole(rangerRole.getId());
    }

    @Test
    public void testDeleteRoleByValidRoleNameWhenRoleIsAssociatedWithOneOrMorePolices() throws Exception {
        XXRole             xxRole             = getTestRole();
        XXRoleDao          xxRoleDao          = Mockito.mock(XXRoleDao.class);
        XXPolicyRefRoleDao xxPolicyRefRoleDao = Mockito.mock(XXPolicyRefRoleDao.class);

        Mockito.when(xxRoleDao.findByRoleName(roleName)).thenReturn(xxRole);
        Mockito.when(daoMgr.getXXPolicyRefRole()).thenReturn(xxPolicyRefRoleDao);
        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxPolicyRefRoleDao.findRoleRefPolicyCount(roleName)).thenReturn(1L);
        thrown.expect(Exception.class);

        roleDBStore.deleteRole(roleName);
    }

    @Test
    public void testDeleteRoleByValidRoleNameWhenRoleIsAssociatedWithOneOrMoreRoles() throws Exception {
        XXRole             xxRole             = getTestRole();
        XXRoleDao          xxRoleDao          = Mockito.mock(XXRoleDao.class);
        XXPolicyRefRoleDao xxPolicyRefRoleDao = Mockito.mock(XXPolicyRefRoleDao.class);
        XXRoleRefRoleDao   xxRoleRefRoleDao   = Mockito.mock(XXRoleRefRoleDao.class);

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleName(roleName)).thenReturn(xxRole);
        Mockito.when(daoMgr.getXXPolicyRefRole()).thenReturn(xxPolicyRefRoleDao);
        Mockito.when(xxPolicyRefRoleDao.findRoleRefPolicyCount(roleName)).thenReturn(0L);
        Mockito.when(daoMgr.getXXRoleRefRole()).thenReturn(xxRoleRefRoleDao);
        Mockito.when(xxRoleRefRoleDao.findRoleRefRoleCount(roleName)).thenReturn(1L);
        thrown.expect(Exception.class);

        roleDBStore.deleteRole(roleName);
    }

    @Test
    public void testDeleteRoleByValidRoleNameWhenRoleIsAssociatedWithOneOrMoreSecurityZones() throws Exception {
        XXRole                   xxRole             = getTestRole();
        XXRoleDao                xxRoleDao          = Mockito.mock(XXRoleDao.class);
        XXPolicyRefRoleDao       xxPolicyRefRoleDao = Mockito.mock(XXPolicyRefRoleDao.class);
        XXRoleRefRoleDao         xxRoleRefRoleDao   = Mockito.mock(XXRoleRefRoleDao.class);
        XXSecurityZoneRefRoleDao xxSzRefRoleDao     = Mockito.mock(XXSecurityZoneRefRoleDao.class);

        Mockito.when(daoMgr.getXXRole()).thenReturn(xxRoleDao);
        Mockito.when(xxRoleDao.findByRoleName(roleName)).thenReturn(xxRole);
        Mockito.when(daoMgr.getXXPolicyRefRole()).thenReturn(xxPolicyRefRoleDao);
        Mockito.when(xxPolicyRefRoleDao.findRoleRefPolicyCount(roleName)).thenReturn(0L);
        Mockito.when(daoMgr.getXXRoleRefRole()).thenReturn(xxRoleRefRoleDao);
        Mockito.when(xxRoleRefRoleDao.findRoleRefRoleCount(roleName)).thenReturn(0L);
        Mockito.when(daoMgr.getXXSecurityZoneRefRole()).thenReturn(xxSzRefRoleDao);
        Mockito.when(xxSzRefRoleDao.findRoleRefZoneCount(roleName)).thenReturn(1L);
        thrown.expect(Exception.class);

        roleDBStore.deleteRole(roleName);
    }

    private XXRole getTestRole() {
        return new XXRole() {{
            setId(TestRoleDBStore.roleId);
            setCreateTime(new Date());
            setName(TestRoleDBStore.roleName);
            setDescription(TestRoleDBStore.roleName);
        }};
    }

    private VXPortalUser getUserProfile() {
        return new VXPortalUser() {{
            setEmailAddress("test@test.com");
            setFirstName("user12");
            setLastName("test12");
            setLoginId(TestRoleDBStore.userLoginID);
            setPassword("Usertest123");
            setUserSource(1);
            setPublicScreenName("testuser");
            setId(TestRoleDBStore.userId);
        }};
    }

    private RangerRole getRangerRole(){
        String           name       = "test-role";
        String           name2      = "admin";
        RoleMember       rm1        = new RoleMember(name, true);
        RoleMember       rm2        = new RoleMember(name2, true);
        List<RoleMember> usersList  = Arrays.asList(rm1,rm2);

        return new RangerRole(name, name, null, usersList, null) {{
            setCreatedByUser(name);
            setId(TestRoleDBStore.roleId);
        }};
    }

    private XXService getXXService() {
        return new XXService() {{
            setAddedByUserId(TestRoleDBStore.id);
            setCreateTime(new Date());
            setDescription("Hdfs service");
            setGuid("serviceguid");
            setId(TestRoleDBStore.id);
            setIsEnabled(true);
            setName("Hdfs");
            setPolicyUpdateTime(new Date());
            setPolicyVersion(1L);
            setType(1L);
            setUpdatedByUserId(TestRoleDBStore.id);
            setUpdateTime(new Date());
        }};
    }
}
