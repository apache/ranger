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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAssetDao;
import org.apache.ranger.db.XXResourceDao;
import org.apache.ranger.db.XXUserDao;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXResource;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.service.XAssetService;
import org.apache.ranger.service.XAuditMapService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XPolicyService;
import org.apache.ranger.service.XResourceService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXAuditMap;
import org.apache.ranger.view.VXAuditMapList;
import org.apache.ranger.view.VXPermMap;
import org.apache.ranger.view.VXPermMapList;
import org.apache.ranger.view.VXResource;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXUser;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestAssetMgr {
	
	private Long id = 1L;
	private static Long  hiveAssetId, knoxAssetId, hbaseAssetId, stormAssetId ;
	
	@InjectMocks
	AssetMgr assetMgr = new AssetMgr();
	
	@Mock
	RESTErrorUtil restErrorUtil;
	
	@Mock
	XAssetService xAssetService;
	
	@Mock
	JSONUtil jsonUtil;
	
	@Mock
	StringUtil stringUtil;
	
	@Mock
	RangerDaoManager rangerDaoManager;
	
	@Mock
	XResourceService xResourceService;
	
	@Mock
	XUserService xUserService;
	
	@Mock
	XPermMapService xPermMapService;
	
	@Mock
	XAuditMapService xAuditMapService;
	
	@Mock
	RangerBizUtil xaBizUtil;
	
	@Mock
	XPolicyService xPolicyService;
	
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
	public void setup(){
		RangerSecurityContext context = new RangerSecurityContext();
		context.setUserSession(new UserSessionBase());
		RangerContextHolder.setSecurityContext(context);		
		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
		currentUserSession.setUserAdmin(true);
	}
	
	@Test
	public void testCreateXAssetForNoUserSession(){
		RangerSecurityContext context = new RangerSecurityContext();
		context.setUserSession(new UserSessionBase());
		RangerContextHolder.setSecurityContext(context);		
		XXPortalUser portalUser = new XXPortalUser();
		portalUser.setId(id);
		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
		currentUserSession.setXXPortalUser(portalUser);
		currentUserSession.setUserAdmin(false);
		WebApplicationException webApplicationException = new WebApplicationException();
		
		Mockito.when(restErrorUtil.createRESTException("Sorry, you don't have permission to perform the operation",MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY)).thenThrow(webApplicationException);
		
		thrown.expect(WebApplicationException.class);
		String config = "{\"username\":\"admin\",\"password\":\"admin\",\"jdbc.driverClassName\":\"jdbcdrivernamefieldvalue\",\"jdbc.url\":\"jdbcurlfieldvalue\",\"commonNameForCertificate\":\"commonnameforcertification\"}";
		VXAsset vXAsset = createVXAsset("Hdfs",1,config);
		assetMgr.createXAsset(vXAsset);		
		Mockito.verify(restErrorUtil).createRESTException("Sorry, you don't have permission to perform the operation",MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
	}
	
	@Test	
	public void testCreateXAssetForHdfs(){
		setup();
		String config = "{\"username\":\"admin\",\"password\":\"admin\",\"fs.default.name\":\"defaultnamevalue\",\"hadoop.security.authorization\":\"authvalue\",\"hadoop.security.authentication\":\"authenticationvalue\",\"hadoop.security.auth_to_local\":\"localvalue\",\"dfs.datanode.kerberos.principal\":\"principalvalue\",\"dfs.namenode.kerberos.principal\":\"namenodeprincipalvalue\",\"dfs.secondary.namenode.kerberos.principal\":\"secprincipalvalue\",\"commonNameForCertificate\":\"certificatevalue\"}";
		VXAsset vXAsset = createVXAsset("Hdfs",1,config);
		
		String userName = "Test";
		XXUser xxUser = new XXUser();
		Mockito.when(xAssetService.getConfigWithEncryptedPassword(config,false)).thenReturn(config);
		Mockito.when(xAssetService.createResource(vXAsset)).thenReturn(vXAsset);
		Map<String,String> mapValue = new HashMap<String, String>();
		mapValue.put("username", userName);
		Mockito.when(jsonUtil.jsonToMap(config)).thenReturn(mapValue);
		Mockito.when(stringUtil.getValidUserName(mapValue.get("username"))).thenReturn(userName);
		VXResource vXResource = new VXResource();
		vXResource.setPermMapList(new ArrayList<VXPermMap>());
		vXResource.setAuditList(new ArrayList<VXAuditMap>());
		Mockito.when(xResourceService.createResource((VXResource)Mockito.anyObject())).thenReturn(vXResource);
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		Mockito.when(rangerDaoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.findByUserName(userName)).thenReturn(xxUser);
		VXUser vxUser = new VXUser(); 
		Mockito.when(xUserService.populateViewBean(xxUser)).thenReturn(vxUser);
		Mockito.when(xPermMapService.createResource((VXPermMap)Mockito.anyObject())).thenReturn(null);
		VXAuditMap vXAuditMap = new VXAuditMap();
		Mockito.when(xAuditMapService.createResource((VXAuditMap)Mockito.anyObject())).thenReturn(vXAuditMap);
		Mockito.when(xResourceService.readResource(vXResource.getId())).thenReturn(vXResource);
		
		VXAsset vXAssetChk = assetMgr.createXAsset(vXAsset);		
		Assert.assertNotNull(vXAssetChk);
		Assert.assertEquals(vXAsset.getName(), vXAssetChk.getName());	
		
		Mockito.when(xAssetService.readResource(vXAssetChk.getId())).thenReturn(vXAsset);
		
		VXAsset vXAssetDB = assetMgr.getXAsset(vXAssetChk.getId());
		Assert.assertNotNull(vXAssetDB);
		Assert.assertEquals(vXAssetDB.getName(), vXAssetChk.getName());
		Assert.assertEquals(vXAssetDB.getAssetType(), vXAssetChk.getAssetType());
		Assert.assertEquals(vXAssetDB.getActiveStatus(), vXAssetChk.getActiveStatus());
		Assert.assertEquals(vXAssetDB.getDescription(), vXAssetChk.getDescription());
		
		String strUpd = "HdfsUpd"+getDateTimeForFileName();
		vXAssetDB.setName(strUpd);
		vXAssetDB.setDescription(strUpd);
		vXAssetDB.setActiveStatus(0);
		
		XXAsset xxAsset = new XXAsset();
		XXResourceDao xxResourceDao = Mockito.mock(XXResourceDao.class);
		XXAssetDao xxAssetDao = Mockito.mock(XXAssetDao.class);
		Mockito.when(rangerDaoManager.getXXAsset()).thenReturn(xxAssetDao);
		Mockito.when(xxAssetDao.getById(vXAsset.getId())).thenReturn(xxAsset);
		Mockito.when(xAssetService.updateResource((VXAsset)Mockito.anyObject())).thenReturn(vXAssetDB);
		Mockito.when(rangerDaoManager.getXXResource()).thenReturn(xxResourceDao);
		List<XXResource> lst = new ArrayList<XXResource>();
		Mockito.when(xxResourceDao.findByResourceNameAndAssetIdAndRecursiveFlag(Mockito.anyString(),Mockito.anyLong(), Mockito.anyInt())).thenReturn(lst );
		
		VXAsset vxAssetUpd = assetMgr.updateXAsset(vXAssetDB);
		Assert.assertEquals(vxAssetUpd.getName(), strUpd);
		Assert.assertEquals(vxAssetUpd.getDescription(), strUpd);
		Assert.assertEquals(0, vxAssetUpd.getActiveStatus());		
		Assert.assertEquals(vxAssetUpd.getDescription(), strUpd);
		hiveAssetId = vxAssetUpd.getId();
	}
	
	@Test	
	public void testCreateXAssetForHive(){
		setup();
		String config = "{\"username\":\"admin\",\"password\":\"admin\",\"jdbc.driverClassName\":\"jdbcdrivernamefieldvalue\",\"jdbc.url\":\"jdbcurlfieldvalue\",\"commonNameForCertificate\":\"commonnameforcertification\"}";
		VXAsset vXAsset = createVXAsset("Hive",3,config);
		
		String userName = "Test";
		XXUser xxUser = new XXUser();
		Mockito.when(xAssetService.getConfigWithEncryptedPassword(config,false)).thenReturn(config);
		Mockito.when(xAssetService.createResource(vXAsset)).thenReturn(vXAsset);
		Map<String,String> mapValue = new HashMap<String, String>();
		mapValue.put("username", userName);
		Mockito.when(jsonUtil.jsonToMap(config)).thenReturn(mapValue);
		Mockito.when(stringUtil.getValidUserName(mapValue.get("username"))).thenReturn(userName);
		VXResource vXResource = new VXResource();
		vXResource.setPermMapList(new ArrayList<VXPermMap>());
		vXResource.setAuditList(new ArrayList<VXAuditMap>());
		Mockito.when(xResourceService.createResource((VXResource)Mockito.anyObject())).thenReturn(vXResource);
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		Mockito.when(rangerDaoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.findByUserName(userName)).thenReturn(xxUser);
		VXUser vxUser = new VXUser(); 
		Mockito.when(xUserService.populateViewBean(xxUser)).thenReturn(vxUser);
		Mockito.when(xPermMapService.createResource((VXPermMap)Mockito.anyObject())).thenReturn(null);
		VXAuditMap vXAuditMap = new VXAuditMap();
		Mockito.when(xAuditMapService.createResource((VXAuditMap)Mockito.anyObject())).thenReturn(vXAuditMap);
		Mockito.when(xResourceService.readResource(vXResource.getId())).thenReturn(vXResource);
		
		VXAsset vXAssetChk = assetMgr.createXAsset(vXAsset);
		Assert.assertNotNull(vXAssetChk);
		Assert.assertEquals(vXAsset.getName(), vXAssetChk.getName());
		
		Mockito.when(xAssetService.readResource(vXAssetChk.getId())).thenReturn(vXAsset);
		
		VXAsset vXAssetDB = assetMgr.getXAsset(vXAssetChk.getId());
		Assert.assertNotNull(vXAssetDB);
		Assert.assertEquals(vXAssetDB.getName(), vXAssetChk.getName());
		Assert.assertEquals(vXAssetDB.getAssetType(), vXAssetChk.getAssetType());
		Assert.assertEquals(vXAssetDB.getActiveStatus(), vXAssetChk.getActiveStatus());
		Assert.assertEquals(vXAssetDB.getDescription(), vXAssetChk.getDescription());
		hiveAssetId = vXAssetChk.getId();
	}
	
	@Test
	public void testCreateXAssetForHBase(){
		setup();
		String config = "{\"username\":\"admin\",\"password\":\"admin\",\"fs.default.name\":\"asdefaultnamevalue\",\"hadoop.security.authorization\":\"authvalue\",\"hadoop.security.authentication\":\"authenticationvalue\",\"hadoop.security.auth_to_local\":\"localvalue\",\"dfs.datanode.kerberos.principal\":\"principalvalue\",\"dfs.namenode.kerberos.principal\":\"namenodeprincipalvalue\",\"dfs.secondary.namenode.kerberos.principal\":\"secprincipalvalue\",\"hbase.master.kerberos.principal\":\"principalvalue\",\"hbase.rpc.engine\":\"enginevalue\",\"hbase.rpc.protection\":\"protectionvalue\",\"hbase.security.authentication\":\"authenvalue\",\"hbase.zookeeper.property.clientPort\":\"clientportvalue\",\"hbase.zookeeper.quorum\":\"quorumvalue\",\"zookeeper.znode.parent\":\"/hbase\",\"commonNameForCertificate\":\"certivalue\"}";
		VXAsset vXAsset = createVXAsset("HBase",2,config);
		
		String userName = "Test";
		XXUser xxUser = new XXUser();
		Mockito.when(xAssetService.getConfigWithEncryptedPassword(config,false)).thenReturn(config);
		Mockito.when(xAssetService.createResource(vXAsset)).thenReturn(vXAsset);
		Map<String,String> mapValue = new HashMap<String, String>();
		mapValue.put("username", userName);
		Mockito.when(jsonUtil.jsonToMap(config)).thenReturn(mapValue);
		Mockito.when(stringUtil.getValidUserName(mapValue.get("username"))).thenReturn(userName);
		VXResource vXResource = new VXResource();
		vXResource.setPermMapList(new ArrayList<VXPermMap>());
		vXResource.setAuditList(new ArrayList<VXAuditMap>());
		Mockito.when(xResourceService.createResource((VXResource)Mockito.anyObject())).thenReturn(vXResource);
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		Mockito.when(rangerDaoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.findByUserName(userName)).thenReturn(xxUser);
		VXUser vxUser = new VXUser(); 
		Mockito.when(xUserService.populateViewBean(xxUser)).thenReturn(vxUser);
		Mockito.when(xPermMapService.createResource((VXPermMap)Mockito.anyObject())).thenReturn(null);
		VXAuditMap vXAuditMap = new VXAuditMap();
		Mockito.when(xAuditMapService.createResource((VXAuditMap)Mockito.anyObject())).thenReturn(vXAuditMap);
		Mockito.when(xResourceService.readResource(vXResource.getId())).thenReturn(vXResource);
		
		VXAsset vXAssetChk = assetMgr.createXAsset(vXAsset);
		Assert.assertNotNull(vXAssetChk);
		Assert.assertEquals(vXAsset.getName(), vXAssetChk.getName());
		
		Mockito.when(xAssetService.readResource(vXAssetChk.getId())).thenReturn(vXAsset);
		
		VXAsset vXAssetDB = assetMgr.getXAsset(vXAssetChk.getId());
		Assert.assertNotNull(vXAssetDB);
		Assert.assertEquals(vXAssetDB.getName(), vXAssetChk.getName());
		Assert.assertEquals(vXAssetDB.getAssetType(), vXAssetChk.getAssetType());
		Assert.assertEquals(vXAssetDB.getActiveStatus(), vXAssetChk.getActiveStatus());
		Assert.assertEquals(vXAssetDB.getDescription(), vXAssetChk.getDescription());
		hbaseAssetId = vXAssetChk.getId();
	}
	
	@Test
	public void testCreateXAssetForKnox(){
		setup();
		String config = "{\"username\":\"admin\",\"password\":\"admin\",\"knox.url\":\"urltest\",\"commonNameForCertificate\":\"certvalue\"}";
		VXAsset vXAsset = createVXAsset("Knox",5,config);
		
		String userName = "Test";
		XXUser xxUser = new XXUser();
		Mockito.when(xAssetService.getConfigWithEncryptedPassword(config,false)).thenReturn(config);
		Mockito.when(xAssetService.createResource(vXAsset)).thenReturn(vXAsset);
		Map<String,String> mapValue = new HashMap<String, String>();
		mapValue.put("username", userName);
		Mockito.when(jsonUtil.jsonToMap(config)).thenReturn(mapValue);
		Mockito.when(stringUtil.getValidUserName(mapValue.get("username"))).thenReturn(userName);
		VXResource vXResource = new VXResource();
		vXResource.setPermMapList(new ArrayList<VXPermMap>());
		vXResource.setAuditList(new ArrayList<VXAuditMap>());
		Mockito.when(xResourceService.createResource((VXResource)Mockito.anyObject())).thenReturn(vXResource);
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		Mockito.when(rangerDaoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.findByUserName(userName)).thenReturn(xxUser);
		VXUser vxUser = new VXUser(); 
		Mockito.when(xUserService.populateViewBean(xxUser)).thenReturn(vxUser);
		Mockito.when(xPermMapService.createResource((VXPermMap)Mockito.anyObject())).thenReturn(null);
		VXAuditMap vXAuditMap = new VXAuditMap();
		Mockito.when(xAuditMapService.createResource((VXAuditMap)Mockito.anyObject())).thenReturn(vXAuditMap);
		Mockito.when(xResourceService.readResource(vXResource.getId())).thenReturn(vXResource);
		
		VXAsset vXAssetChk = assetMgr.createXAsset(vXAsset);
		Assert.assertNotNull(vXAssetChk);
		Assert.assertEquals(vXAsset.getName(), vXAssetChk.getName());
		
		Mockito.when(xAssetService.readResource(vXAssetChk.getId())).thenReturn(vXAsset);
		
		VXAsset vXAssetDB = assetMgr.getXAsset(vXAssetChk.getId());
		Assert.assertNotNull(vXAssetDB);
		Assert.assertEquals(vXAssetDB.getName(), vXAssetChk.getName());
		Assert.assertEquals(vXAssetDB.getAssetType(), vXAssetChk.getAssetType());
		Assert.assertEquals(vXAssetDB.getActiveStatus(), vXAssetChk.getActiveStatus());
		Assert.assertEquals(vXAssetDB.getDescription(), vXAssetChk.getDescription());
		knoxAssetId = vXAssetChk.getId();
	}
	
	@Test
	public void testCreateXAssetForStorm(){
		setup();
		String config = "{\"username\":\"admin\",\"password\":\"admin\",\"nimbus.url\":\"urlvalue\",\"commonNameForCertificate\":\"certvalue\"}";
		VXAsset vXAsset = createVXAsset("Storm",6,config);
		
		String userName = "Test";
		XXUser xxUser = new XXUser();
		Mockito.when(xAssetService.getConfigWithEncryptedPassword(config,false)).thenReturn(config);
		Mockito.when(xAssetService.createResource(vXAsset)).thenReturn(vXAsset);
		Map<String,String> mapValue = new HashMap<String, String>();
		mapValue.put("username", userName);
		Mockito.when(jsonUtil.jsonToMap(config)).thenReturn(mapValue);
		Mockito.when(stringUtil.getValidUserName(mapValue.get("username"))).thenReturn(userName);
		VXResource vXResource = new VXResource();
		vXResource.setPermMapList(new ArrayList<VXPermMap>());
		vXResource.setAuditList(new ArrayList<VXAuditMap>());
		Mockito.when(xResourceService.createResource((VXResource)Mockito.anyObject())).thenReturn(vXResource);
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		Mockito.when(rangerDaoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.findByUserName(userName)).thenReturn(xxUser);
		VXUser vxUser = new VXUser(); 
		Mockito.when(xUserService.populateViewBean(xxUser)).thenReturn(vxUser);
		Mockito.when(xPermMapService.createResource((VXPermMap)Mockito.anyObject())).thenReturn(null);
		VXAuditMap vXAuditMap = new VXAuditMap();
		Mockito.when(xAuditMapService.createResource((VXAuditMap)Mockito.anyObject())).thenReturn(vXAuditMap);
		Mockito.when(xResourceService.readResource(vXResource.getId())).thenReturn(vXResource);
		
		VXAsset vXAssetChk = assetMgr.createXAsset(vXAsset);
		Assert.assertNotNull(vXAssetChk);
		Assert.assertEquals(vXAsset.getName(), vXAssetChk.getName());
		
		Mockito.when(xAssetService.readResource(vXAssetChk.getId())).thenReturn(vXAsset);
		
		VXAsset vXAssetDB = assetMgr.getXAsset(vXAssetChk.getId());
		Assert.assertNotNull(vXAssetDB);
		Assert.assertEquals(vXAssetDB.getName(), vXAssetChk.getName());
		Assert.assertEquals(vXAssetDB.getAssetType(), vXAssetChk.getAssetType());
		Assert.assertEquals(vXAssetDB.getActiveStatus(), vXAssetChk.getActiveStatus());
		Assert.assertEquals(vXAssetDB.getDescription(), vXAssetChk.getDescription());
		stormAssetId = vXAssetChk.getId();
	}
	
	@Test
	public void testXResourceCRUDForHive() throws JsonGenerationException, JsonMappingException, IOException{
		setup();
		Long assetId = hiveAssetId;
		int assetType = AppConstants.ASSET_HIVE;
		int resourceType = AppConstants.RESOURCE_COLUMN;
		VXResource vXResource = createVXResource("Hive", assetId, assetType, resourceType);
		
		vXResource.setPermMapList(new ArrayList<VXPermMap>());
		vXResource.setAuditList(new ArrayList<VXAuditMap>());
		XXAssetDao xxAssetDao = Mockito.mock(XXAssetDao.class);
		XXAsset xxAsset = new XXAsset();
				
		VXResponse vXResponse = new VXResponse();
		Mockito.when(rangerDaoManager.getXXAsset()).thenReturn(xxAssetDao);
		Mockito.when(xxAssetDao.getById(assetId)).thenReturn(xxAsset);
		Mockito.when(stringUtil.split(vXResource.getName(), ",")).thenReturn(new String[0]);
		Mockito.when(xaBizUtil.hasPermission(vXResource,AppConstants.XA_PERM_TYPE_ADMIN)).thenReturn(vXResponse);
		Mockito.when(xResourceService.createResource(vXResource)).thenReturn(vXResource);
		
		VXResource vXResourceChk = assetMgr.createXResource(vXResource);
		Assert.assertNotNull(vXResourceChk);
		Assert.assertEquals(vXResourceChk.getAssetType(), AppConstants.ASSET_HIVE);
		
		Mockito.when(xResourceService.readResource(vXResourceChk.getId())).thenReturn(vXResource);
		
		VXResource vXResourceChkDb = assetMgr.getXResource(vXResourceChk.getId());
		Assert.assertNotNull(vXResourceChkDb);
		Assert.assertEquals(vXResourceChkDb.getAssetType(), AppConstants.ASSET_HIVE);
		Assert.assertEquals(vXResourceChkDb.getResourceType(), resourceType);
		Assert.assertEquals(vXResourceChkDb.getPolicyName(), vXResourceChk.getPolicyName());
		Assert.assertEquals(vXResourceChkDb.getAssetName(), vXResourceChk.getAssetName());
		Assert.assertEquals(vXResourceChkDb.getResourceStatus(), vXResourceChk.getResourceStatus());
				
		Mockito.when(xResourceService.readResource(vXResourceChk.getId())).thenReturn(vXResource);
		Mockito.when(jsonUtil.writeJsonToFile(vXResource, vXResource.getName())).thenReturn(new File(vXResource.getName()));
		
		File fileChk = assetMgr.getXResourceFile(vXResourceChk.getId(),"json");
		Assert.assertNotNull(fileChk);
		
		String policyNameUpd = "HiveUpd_"+getDateTimeForFileName();
		vXResourceChkDb.setPolicyName(policyNameUpd);
		vXResourceChkDb.setDatabases(policyNameUpd);
		
		XXResourceDao xxResourceDao = Mockito.mock(XXResourceDao.class);
		XXResource xxResource = new XXResource();
		Mockito.when(xResourceService.updateResource(vXResourceChkDb)).thenReturn(vXResourceChkDb);
		Mockito.when(rangerDaoManager.getXXResource()).thenReturn(xxResourceDao);
		Mockito.when(xxResourceDao.getById(vXResource.getId())).thenReturn(xxResource);
		
		VXResource vXResourceUpd = assetMgr.updateXResource(vXResourceChkDb);
		Assert.assertNotNull(vXResourceUpd);
		Assert.assertEquals(vXResourceUpd.getPolicyName(), policyNameUpd);
		Assert.assertEquals(vXResourceUpd.getDatabases(), policyNameUpd);
		
		VXPermMapList vXPermMapList = new VXPermMapList();
		Mockito.when(xPermMapService.searchXPermMaps((SearchCriteria)Mockito.anyObject())).thenReturn(vXPermMapList);
		VXAuditMapList vXAuditMapsList = new VXAuditMapList(); 
		Mockito.when(xAuditMapService.searchXAuditMaps((SearchCriteria)Mockito.anyObject())).thenReturn(vXAuditMapsList);
		
		assetMgr.deleteXResource(vXResourceChkDb.getId(), true);				
	}
	
	@Test
	public void testXResourceCRUDForHbase() throws JsonGenerationException, JsonMappingException, IOException{
		setup();
		Long assetId = hbaseAssetId;
		int assetType = AppConstants.ASSET_HBASE;
		int resourceType = AppConstants.RESOURCE_TABLE;
		VXResource vXResource = createVXResource("Hbase", assetId, assetType, resourceType);
		
		vXResource.setPermMapList(new ArrayList<VXPermMap>());
		vXResource.setAuditList(new ArrayList<VXAuditMap>());
		XXAssetDao xxAssetDao = Mockito.mock(XXAssetDao.class);
		XXAsset xxAsset = new XXAsset();
		
		xxAsset.setAssetType(AppConstants.ASSET_HBASE);
		Mockito.when(xPolicyService.getResourceType(vXResource)).thenReturn(AppConstants.RESOURCE_PATH);
		Mockito.when(stringUtil.split(vXResource.getTopologies(), ",")).thenReturn(new String[0]);
		Mockito.when(xPolicyService.getResourceType(vXResource)).thenReturn(AppConstants.RESOURCE_TABLE);
		Mockito.when(stringUtil.split(vXResource.getTables(), ",")).thenReturn(new String[0]);
		Mockito.when(stringUtil.split("", ",")).thenReturn(new String[0]);
		
		VXResponse vXResponse = new VXResponse();
		Mockito.when(rangerDaoManager.getXXAsset()).thenReturn(xxAssetDao);
		Mockito.when(xxAssetDao.getById(assetId)).thenReturn(xxAsset);
		Mockito.when(stringUtil.split(vXResource.getName(), ",")).thenReturn(new String[0]);
		Mockito.when(xaBizUtil.hasPermission(vXResource,AppConstants.XA_PERM_TYPE_ADMIN)).thenReturn(vXResponse);
		Mockito.when(xResourceService.createResource(vXResource)).thenReturn(vXResource);
		
		VXResource vXResourceChk = assetMgr.createXResource(vXResource);
		Assert.assertNotNull(vXResourceChk);
		Assert.assertEquals(vXResourceChk.getAssetType(), AppConstants.ASSET_HBASE);
		
		Mockito.when(xResourceService.readResource(vXResourceChk.getId())).thenReturn(vXResource);
		
		VXResource vXResourceChkDb = assetMgr.getXResource(vXResourceChk.getId());
		Assert.assertNotNull(vXResourceChkDb);
		Assert.assertEquals(vXResourceChkDb.getAssetType(), AppConstants.ASSET_HBASE);
		Assert.assertEquals(vXResourceChkDb.getResourceType(), resourceType);
		Assert.assertEquals(vXResourceChkDb.getPolicyName(), vXResourceChk.getPolicyName());
		Assert.assertEquals(vXResourceChkDb.getAssetName(), vXResourceChk.getAssetName());
		Assert.assertEquals(vXResourceChkDb.getResourceStatus(), vXResourceChk.getResourceStatus());
		
		Mockito.when(xResourceService.readResource(vXResourceChk.getId())).thenReturn(vXResource);
		Mockito.when(jsonUtil.writeJsonToFile(vXResource, vXResource.getName())).thenReturn(new File(vXResource.getName()));
		
		File fileChk = assetMgr.getXResourceFile(vXResourceChk.getId(),"json");
		Assert.assertNotNull(fileChk);
		
		String policyNameUpd = "HbaseUpd_"+getDateTimeForFileName();
		vXResourceChkDb.setPolicyName(policyNameUpd);
		vXResourceChkDb.setDatabases(policyNameUpd);
		
		XXResourceDao xxResourceDao = Mockito.mock(XXResourceDao.class);
		XXResource xxResource = new XXResource();
		Mockito.when(xResourceService.updateResource(vXResourceChkDb)).thenReturn(vXResourceChkDb);
		Mockito.when(rangerDaoManager.getXXResource()).thenReturn(xxResourceDao);
		Mockito.when(xxResourceDao.getById(vXResource.getId())).thenReturn(xxResource);
		
		VXResource vXResourceUpd = assetMgr.updateXResource(vXResourceChkDb);
		Assert.assertNotNull(vXResourceUpd);
		Assert.assertEquals(vXResourceUpd.getPolicyName(), policyNameUpd);
		Assert.assertEquals(vXResourceUpd.getDatabases(), policyNameUpd);
		
		VXPermMapList vXPermMapList = new VXPermMapList();
		Mockito.when(xPermMapService.searchXPermMaps((SearchCriteria)Mockito.anyObject())).thenReturn(vXPermMapList);
		VXAuditMapList vXAuditMapsList = new VXAuditMapList(); 
		Mockito.when(xAuditMapService.searchXAuditMaps((SearchCriteria)Mockito.anyObject())).thenReturn(vXAuditMapsList);
		
		assetMgr.deleteXResource(vXResourceChkDb.getId(), true);
	}
	
	@Test
	public void testXResourceCRUDForKnox() throws JsonGenerationException, JsonMappingException, IOException{
		setup();
		Long assetId = knoxAssetId;
		int assetType = AppConstants.ASSET_KNOX;
		int resourceType = AppConstants.RESOURCE_SERVICE_NAME;
		VXResource vXResource = createVXResource("Knox", assetId, assetType, resourceType);
		
		vXResource.setPermMapList(new ArrayList<VXPermMap>());
		vXResource.setAuditList(new ArrayList<VXAuditMap>());
		XXAssetDao xxAssetDao = Mockito.mock(XXAssetDao.class);
		XXAsset xxAsset = new XXAsset();
		
		xxAsset.setAssetType(AppConstants.ASSET_KNOX);
		Mockito.when(xPolicyService.getResourceType(vXResource)).thenReturn(AppConstants.RESOURCE_PATH);
		Mockito.when(stringUtil.split(vXResource.getTopologies(), ",")).thenReturn(new String[0]);
		Mockito.when(xPolicyService.getResourceType(vXResource)).thenReturn(AppConstants.RESOURCE_TOPOLOGY);
		Mockito.when(stringUtil.split("", ",")).thenReturn(new String[0]);
		
		VXResponse vXResponse = new VXResponse();
		Mockito.when(rangerDaoManager.getXXAsset()).thenReturn(xxAssetDao);
		Mockito.when(xxAssetDao.getById(assetId)).thenReturn(xxAsset);
		Mockito.when(stringUtil.split(vXResource.getName(), ",")).thenReturn(new String[0]);
		Mockito.when(xaBizUtil.hasPermission(vXResource,AppConstants.XA_PERM_TYPE_ADMIN)).thenReturn(vXResponse);
		Mockito.when(xResourceService.createResource(vXResource)).thenReturn(vXResource);
		
		VXResource vXResourceChk = assetMgr.createXResource(vXResource);
		Assert.assertNotNull(vXResourceChk);
		Assert.assertEquals(vXResourceChk.getAssetType(), AppConstants.ASSET_KNOX);
		
		Mockito.when(xResourceService.readResource(vXResourceChk.getId())).thenReturn(vXResource);
		
		VXResource vXResourceChkDb = assetMgr.getXResource(vXResourceChk.getId());
		Assert.assertNotNull(vXResourceChkDb);
		Assert.assertEquals(vXResourceChkDb.getAssetType(), AppConstants.ASSET_KNOX);
		Assert.assertEquals(vXResourceChkDb.getResourceType(), resourceType);
		Assert.assertEquals(vXResourceChkDb.getPolicyName(), vXResourceChk.getPolicyName());
		Assert.assertEquals(vXResourceChkDb.getAssetName(), vXResourceChk.getAssetName());
		Assert.assertEquals(vXResourceChkDb.getResourceStatus(), vXResourceChk.getResourceStatus());
		
		Mockito.when(xResourceService.readResource(vXResourceChk.getId())).thenReturn(vXResource);
		Mockito.when(jsonUtil.writeJsonToFile(vXResource, vXResource.getName())).thenReturn(new File(vXResource.getName()));
		
		File fileChk = assetMgr.getXResourceFile(vXResourceChk.getId(),"json");
		Assert.assertNotNull(fileChk);
		
		String policyNameUpd = "KnoxUpd_"+getDateTimeForFileName();
		vXResourceChkDb.setPolicyName(policyNameUpd);
		vXResourceChkDb.setDatabases(policyNameUpd);
		
		XXResourceDao xxResourceDao = Mockito.mock(XXResourceDao.class);
		XXResource xxResource = new XXResource();
		Mockito.when(xResourceService.updateResource(vXResourceChkDb)).thenReturn(vXResourceChkDb);
		Mockito.when(rangerDaoManager.getXXResource()).thenReturn(xxResourceDao);
		Mockito.when(xxResourceDao.getById(vXResource.getId())).thenReturn(xxResource);
		
		VXResource vXResourceUpd = assetMgr.updateXResource(vXResourceChkDb);
		Assert.assertNotNull(vXResourceUpd);
		Assert.assertEquals(vXResourceUpd.getPolicyName(), policyNameUpd);
		Assert.assertEquals(vXResourceUpd.getDatabases(), policyNameUpd);
		
		VXPermMapList vXPermMapList = new VXPermMapList();
		Mockito.when(xPermMapService.searchXPermMaps((SearchCriteria)Mockito.anyObject())).thenReturn(vXPermMapList);
		VXAuditMapList vXAuditMapsList = new VXAuditMapList(); 
		Mockito.when(xAuditMapService.searchXAuditMaps((SearchCriteria)Mockito.anyObject())).thenReturn(vXAuditMapsList);
		
		assetMgr.deleteXResource(vXResourceChkDb.getId(), true);		
	}
	
	@Test
	public void testXResourceCRUDForStorm() throws JsonGenerationException, JsonMappingException, IOException{
		setup();
		Long assetId = stormAssetId;
		int assetType = AppConstants.ASSET_STORM;
		int resourceType = AppConstants.RESOURCE_SERVICE_NAME;
		VXResource vXResource = createVXResource("Storm",assetId, assetType, resourceType);
		
		vXResource.setPermMapList(new ArrayList<VXPermMap>());
		vXResource.setAuditList(new ArrayList<VXAuditMap>());
		XXAssetDao xxAssetDao = Mockito.mock(XXAssetDao.class);
		XXAsset xxAsset = new XXAsset();
		
		xxAsset.setAssetType(AppConstants.ASSET_STORM);
		Mockito.when(xPolicyService.getResourceType(vXResource)).thenReturn(AppConstants.RESOURCE_PATH);
		Mockito.when(stringUtil.split(vXResource.getTopologies(), ",")).thenReturn(new String[0]);
		Mockito.when(xPolicyService.getResourceType(vXResource)).thenReturn(AppConstants.RESOURCE_TOPOLOGY);
		Mockito.when(stringUtil.split("", ",")).thenReturn(new String[0]);
		
		VXResponse vXResponse = new VXResponse();
		Mockito.when(rangerDaoManager.getXXAsset()).thenReturn(xxAssetDao);
		Mockito.when(xxAssetDao.getById(assetId)).thenReturn(xxAsset);
		Mockito.when(stringUtil.split(vXResource.getName(), ",")).thenReturn(new String[0]);
		Mockito.when(xaBizUtil.hasPermission(vXResource,AppConstants.XA_PERM_TYPE_ADMIN)).thenReturn(vXResponse);
		Mockito.when(xResourceService.createResource(vXResource)).thenReturn(vXResource);
		
		VXResource vXResourceChk = assetMgr.createXResource(vXResource);
		Assert.assertNotNull(vXResourceChk);
		Assert.assertEquals(vXResourceChk.getAssetType(), AppConstants.ASSET_STORM);
		
		Mockito.when(xResourceService.readResource(vXResourceChk.getId())).thenReturn(vXResource);
		
		VXResource vXResourceChkDb = assetMgr.getXResource(vXResourceChk.getId());
		Assert.assertNotNull(vXResourceChkDb);
		Assert.assertEquals(vXResourceChkDb.getAssetType(), AppConstants.ASSET_STORM);
		Assert.assertEquals(vXResourceChkDb.getResourceType(), resourceType);
		Assert.assertEquals(vXResourceChkDb.getPolicyName(), vXResourceChk.getPolicyName());
		Assert.assertEquals(vXResourceChkDb.getAssetName(), vXResourceChk.getAssetName());
		Assert.assertEquals(vXResourceChkDb.getResourceStatus(), vXResourceChk.getResourceStatus());
		
		Mockito.when(xResourceService.readResource(vXResourceChk.getId())).thenReturn(vXResource);
		Mockito.when(jsonUtil.writeJsonToFile(vXResource, vXResource.getName())).thenReturn(new File(vXResource.getName()));
		
		File fileChk = assetMgr.getXResourceFile(vXResourceChk.getId(),"json");
		Assert.assertNotNull(fileChk);
		
		String policyNameUpd = "StormUpd_"+getDateTimeForFileName();
		vXResourceChkDb.setPolicyName(policyNameUpd);
		vXResourceChkDb.setDatabases(policyNameUpd);
		
		XXResourceDao xxResourceDao = Mockito.mock(XXResourceDao.class);
		XXResource xxResource = new XXResource();
		Mockito.when(xResourceService.updateResource(vXResourceChkDb)).thenReturn(vXResourceChkDb);
		Mockito.when(rangerDaoManager.getXXResource()).thenReturn(xxResourceDao);
		Mockito.when(xxResourceDao.getById(vXResource.getId())).thenReturn(xxResource);
		
		VXResource vXResourceUpd = assetMgr.updateXResource(vXResourceChkDb);
		Assert.assertNotNull(vXResourceUpd);
		Assert.assertEquals(vXResourceUpd.getPolicyName(), policyNameUpd);
		Assert.assertEquals(vXResourceUpd.getDatabases(), policyNameUpd);
		
		VXPermMapList vXPermMapList = new VXPermMapList();
		Mockito.when(xPermMapService.searchXPermMaps((SearchCriteria)Mockito.anyObject())).thenReturn(vXPermMapList);
		VXAuditMapList vXAuditMapsList = new VXAuditMapList(); 
		Mockito.when(xAuditMapService.searchXAuditMaps((SearchCriteria)Mockito.anyObject())).thenReturn(vXAuditMapsList);
		
		assetMgr.deleteXResource(vXResourceChkDb.getId(), true);
	}	
	
	@Test
	public void testDeleteXAssetForHive(){
		setup();
		VXAsset vXAsset = new VXAsset();
		vXAsset.setId(hiveAssetId);
		vXAsset.setActiveStatus(RangerCommonEnums.STATUS_ENABLED);
		Mockito.when(xAssetService.readResource(hiveAssetId)).thenReturn(vXAsset);
		assetMgr.deleteXAsset(hiveAssetId, true);
		Assert.assertEquals(vXAsset.getActiveStatus(), RangerCommonEnums.STATUS_DELETED);
	} 
	
	@Test
	public void testDeleteXAssetForHbase(){
		setup();
		VXAsset vXAsset = new VXAsset();
		vXAsset.setId(hbaseAssetId);
		vXAsset.setActiveStatus(RangerCommonEnums.STATUS_ENABLED);
		Mockito.when(xAssetService.readResource(hiveAssetId)).thenReturn(vXAsset);
		assetMgr.deleteXAsset(hbaseAssetId, true);
		Assert.assertEquals(vXAsset.getActiveStatus(), RangerCommonEnums.STATUS_DELETED);
	}
	
	@Test
	public void testDeleteXAssetForKnox(){
		setup();
		VXAsset vXAsset = new VXAsset();
		vXAsset.setId(knoxAssetId);
		vXAsset.setActiveStatus(RangerCommonEnums.STATUS_ENABLED);
		Mockito.when(xAssetService.readResource(hiveAssetId)).thenReturn(vXAsset);
		assetMgr.deleteXAsset(knoxAssetId, true);
		Assert.assertEquals(vXAsset.getActiveStatus(), RangerCommonEnums.STATUS_DELETED);
	}
	
	@Test
	public void testDeleteXAssetForStorm(){
		setup();
		VXAsset vXAsset = new VXAsset();
		vXAsset.setId(stormAssetId);
		vXAsset.setActiveStatus(RangerCommonEnums.STATUS_ENABLED);
		Mockito.when(xAssetService.readResource(hiveAssetId)).thenReturn(vXAsset);
		assetMgr.deleteXAsset(stormAssetId, true);
		Assert.assertEquals(vXAsset.getActiveStatus(), RangerCommonEnums.STATUS_DELETED);
	}
	
	private VXResource createVXResource(String assetTypeName, Long assetId, int assetType, int resourceType){
		VXResource vxResource = new VXResource();
		vxResource.setAssetId(assetId);
		vxResource.setAssetType(assetType);
		vxResource.setResourceType(resourceType);
		vxResource.setName("Test"+getDateTimeForFileName());
		vxResource.setDatabases("TestDB"+getDateTimeForFileName());
		vxResource.setColumns("TestCol"+getDateTimeForFileName());
		vxResource.setTables("TestTables"+getDateTimeForFileName());
		vxResource.setUdfs("TestUDF"+getDateTimeForFileName());
		vxResource.setDescription(assetTypeName+"_"+getDateTimeForFileName());
		vxResource.setPolicyName(assetTypeName+"_"+getDateTimeForFileName());
		vxResource.setTopologies("Topo_"+getDateTimeForFileName());
		vxResource.setServices("Serv_"+getDateTimeForFileName());
		vxResource.setResourceStatus(1);
		vxResource.setOwner("Admin");
		vxResource.setUpdatedBy("Admin");
		return vxResource;
	}
			
	private String getDateTimeForFileName() {
		Date currentDate = new Date();
		SimpleDateFormat dateFormatter = new SimpleDateFormat("ddMMyyyyHHmmsss");
		return dateFormatter.format(currentDate);
	}
	
	private VXAsset createVXAsset(String assetTypeName, int assetType, String config){
		VXAsset vXAsset = new VXAsset();		
		vXAsset.setName(assetTypeName+"_"+getDateTimeForFileName());
		vXAsset.setActiveStatus(1);
		vXAsset.setAssetType(assetType);
		vXAsset.setConfig(config);	
		vXAsset.setDescription(assetTypeName+"Descr_"+getDateTimeForFileName());
		return vXAsset;
	}
}