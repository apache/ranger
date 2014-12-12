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
import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.biz.XABizUtil;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.common.XACommonEnums;
import org.apache.ranger.common.XAConstants;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.db.XADaoManager;
import org.apache.ranger.db.XXAssetDao;
import org.apache.ranger.db.XXPortalUserDao;
import org.apache.ranger.db.XXResourceDao;
import org.apache.ranger.db.XXUserDao;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXResource;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.security.context.XAContextHolder;
import org.apache.ranger.security.context.XASecurityContext;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXDataObject;
import org.apache.ranger.view.VXPortalUser;
import org.apache.ranger.view.VXResource;
import org.apache.ranger.view.VXResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestXABizUtil {
	
	private Long id = 1L;
	private String resourceName = "hadoopdev";
	
	@InjectMocks
	XABizUtil xABizUtil = new XABizUtil();
	
	@Mock
	XADaoManager daoManager;
	
	@Mock
	StringUtil stringUtil; 
	
	@Before
	public void setup(){
		XASecurityContext context = new XASecurityContext();
		context.setUserSession(new UserSessionBase());
		XAContextHolder.setSecurityContext(context);		
	}
	
	@Test
	public void testHasPermission_When_disableAccessControl(){
		VXResource vXResource = null;
		xABizUtil.enableResourceAccessControl = false;
		VXResponse resp = xABizUtil.hasPermission(vXResource, AppConstants.XA_PERM_TYPE_UNKNOWN);
		Assert.assertNotNull(resp);		
	}
	
	@Test
	public void testHasPermission_When_NoResource(){
		VXResource vXResource = null;
		VXResponse resp = xABizUtil.hasPermission(vXResource, AppConstants.XA_PERM_TYPE_UNKNOWN);
		Assert.assertNotNull(resp);
		Assert.assertEquals(VXResponse.STATUS_ERROR, resp.getStatusCode());
		Assert.assertEquals("Please provide valid policy.", resp.getMsgDesc());
	}
	
	@Test
	public void testHasPermission_emptyResourceName(){
		VXResource vXResource = new VXResource();
		XXPortalUser portalUser = new XXPortalUser();
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		XXPortalUserDao userDao = Mockito.mock(XXPortalUserDao.class);
		XXUser xxUser = new XXUser(); 
		XXAsset xxAsset = new XXAsset();
		List<XXResource> lst = new ArrayList<XXResource>();
		XXResourceDao xxResourceDao = Mockito.mock(XXResourceDao.class);
		XXAssetDao xxAssetDao = Mockito.mock(XXAssetDao.class);
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(userDao);
		Mockito.when(userDao.getById(Mockito.anyLong())).thenReturn(portalUser);
		Mockito.when(daoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.findByUserName(Mockito.anyString())).thenReturn(xxUser);	
		Mockito.when(daoManager.getXXResource()).thenReturn(xxResourceDao);
		Mockito.when(xxResourceDao.findByAssetIdAndResourceStatus(Mockito.anyLong(),Mockito.anyInt())).thenReturn(lst);
		Mockito.when(daoManager.getXXAsset()).thenReturn(xxAssetDao);
		Mockito.when(xxAssetDao.getById(Mockito.anyLong())).thenReturn(xxAsset);
		VXResponse resp = xABizUtil.hasPermission(vXResource, AppConstants.XA_PERM_TYPE_UNKNOWN);
		Mockito.verify(daoManager).getXXPortalUser();
		Mockito.verify(userDao).getById(Mockito.anyLong());
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(xxUserDao).findByUserName(Mockito.anyString());
		Assert.assertNotNull(resp);
		Assert.assertEquals(VXResponse.STATUS_ERROR, resp.getStatusCode());
		Assert.assertEquals("Permission Denied !", resp.getMsgDesc());		
	}
	
	@Test
	public void testHasPermission_isAdmin(){
		VXResource vXResource = new VXResource();
		vXResource.setName(resourceName);
		vXResource.setAssetId(id);		
		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
		currentUserSession.setUserAdmin(true);
		VXResponse resp = xABizUtil.hasPermission(vXResource, AppConstants.XA_PERM_TYPE_UNKNOWN);
		Assert.assertNotNull(resp);
		Assert.assertEquals(VXResponse.STATUS_SUCCESS, resp.getStatusCode());
	}	
	
	@Test
	public void testIsNotAdmin(){
		boolean isAdminChk = xABizUtil.isAdmin();
		Assert.assertFalse(isAdminChk);
	}
	
	@Test
	public void testIsAdmin(){
		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
		currentUserSession.setUserAdmin(true);
		boolean isAdminChk = xABizUtil.isAdmin();
		Assert.assertTrue(isAdminChk);
	}	
	
	@Test
	public void testUserSessionNull_forIsAdmin(){
		XAContextHolder.setSecurityContext(null);	
		boolean isAdminChk = xABizUtil.isAdmin();
		Assert.assertFalse(isAdminChk);
	}
	
	@Test
	public void testGetXUserId_NoUserSession(){
		XAContextHolder.setSecurityContext(null);
		Long chk = xABizUtil.getXUserId();
		Assert.assertNull(chk);
	}
	
	@Test
	public void testGetXUserId_NoUser(){
		XASecurityContext context = new XASecurityContext();
		context.setUserSession(new UserSessionBase());
		XAContextHolder.setSecurityContext(context);	
		XXPortalUser xxPortalUser = new XXPortalUser();
		XXUser xxUser = new XXUser();
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		XXPortalUserDao xxPortalUserDao = Mockito.mock(XXPortalUserDao.class);
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(xxPortalUserDao);
		Mockito.when(xxPortalUserDao.getById(Mockito.anyLong())).thenReturn(xxPortalUser);
		Mockito.when(daoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.findByUserName(Mockito.anyString())).thenReturn(xxUser);
		Long chk = xABizUtil.getXUserId();
		Mockito.verify(daoManager).getXXPortalUser();
		Mockito.verify(xxPortalUserDao).getById(Mockito.anyLong());
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(xxUserDao).findByUserName(Mockito.anyString());
		Assert.assertNull(chk);	
	}
	
	@Test
	public void testGetXUserId(){
		XXPortalUser xxPortalUser = new XXPortalUser();
		xxPortalUser.setId(id);
		XXUser xxUser = new XXUser();
		xxUser.setId(id);
		XXPortalUserDao xxPortalUserDao = Mockito.mock(XXPortalUserDao.class);
		XXUserDao xxUserDao = Mockito.mock(XXUserDao.class);
		XASecurityContext context = new XASecurityContext();
		UserSessionBase userSessionBase = new UserSessionBase();
		userSessionBase.setUserAdmin(true);
		context.setUserSession(userSessionBase);
		userSessionBase.setXXPortalUser(xxPortalUser);
		XAContextHolder.setSecurityContext(context);	
		Mockito.when(daoManager.getXXPortalUser()).thenReturn(xxPortalUserDao);
		Mockito.when(xxPortalUserDao.getById(Mockito.anyLong())).thenReturn(xxPortalUser);
		Mockito.when(daoManager.getXXUser()).thenReturn(xxUserDao);
		Mockito.when(xxUserDao.findByUserName(Mockito.anyString())).thenReturn(xxUser);		
		Long chk = xABizUtil.getXUserId();
		Mockito.verify(daoManager).getXXPortalUser();
		Mockito.verify(xxPortalUserDao).getById(Mockito.anyLong());
		Mockito.verify(daoManager).getXXUser();
		Mockito.verify(xxUserDao).findByUserName(Mockito.anyString());
		Assert.assertEquals(chk, id);		
	}
	
	@Test
	public void testReplaceMetaChars_PathEmpty(){
		String path = "";
		String pathChk = xABizUtil.replaceMetaChars(path);
		Assert.assertFalse(pathChk.contains("\\*"));
		Assert.assertFalse(pathChk.contains("\\?"));
	}
	
	@Test
	public void testReplaceMetaChars_NoMetaChars(){
		String path = "\\Demo\\Test";
		String pathChk = xABizUtil.replaceMetaChars(path);
		Assert.assertFalse(pathChk.contains("\\*"));
		Assert.assertFalse(pathChk.contains("\\?"));
	}
	
	@Test
	public void testReplaceMetaChars_PathNull(){
		String path = null;
		String pathChk = xABizUtil.replaceMetaChars(path);
		Assert.assertNull(pathChk);
	}
	
	@Test
	public void testReplaceMetaChars(){
		String path = "\\Demo\\Test\\*\\?";
		String pathChk = xABizUtil.replaceMetaChars(path);
		Assert.assertFalse(pathChk.contains("\\*"));
		Assert.assertFalse(pathChk.contains("\\?"));
	}
	
	@Test
	public void testGeneratePublicName(){		
		String firstName = "Test123456789123456789";
		String lastName = "Unit";
		String publicNameChk = xABizUtil.generatePublicName(firstName, lastName);
		Assert.assertEquals("Test12345678... U.", publicNameChk);
	}
	
	@Test
	public void testGeneratePublicName_fNameLessThanMax(){		
		String firstName = "Test";
		String lastName = "";
		String publicNameChk = xABizUtil.generatePublicName(firstName, lastName);
		Assert.assertNull(publicNameChk);
	}
	
	@Test
	public void testGeneratePublicName_withPortalUser(){
		VXPortalUser vXPortalUser = new VXPortalUser();
		vXPortalUser.setFirstName("Test");
		vXPortalUser.setLastName(null);
		String publicNameChk = xABizUtil.generatePublicName(vXPortalUser, null);
		Assert.assertNull(publicNameChk);
	}
	
	@Test
	public void testGetDisplayName_EmptyName() {
		String displayNameChk = xABizUtil.getDisplayName(null);
		Assert.assertEquals(xABizUtil.EMPTY_CONTENT_DISPLAY_NAME, displayNameChk);
	}
	
	@Test
	public void testGetDisplayName_AssetName() {
		XXAsset obj = new XXAsset();
		obj.setDescription(resourceName);
		String displayNameChk = xABizUtil.getDisplayName(obj);
		Assert.assertEquals(resourceName, displayNameChk);
	}
	
	@Test
	public void testGetDisplayName_MoreThanMaxLen() {
		XXAsset obj = new XXAsset();
		String name = resourceName;
		for(int i=0;i<16;i++){
			name = name + "_" + name + "1";
		}
		obj.setDescription(name);
		String displayNameChk = xABizUtil.getDisplayName(obj);
		Assert.assertEquals(displayNameChk.length(), 150);
	}
	
	@Test
	public void testGetDisplayNameForClassName(){
		XXAsset obj = new XXAsset();
		String displayNameChk = xABizUtil.getDisplayNameForClassName(obj);
		Assert.assertEquals("Asset",displayNameChk);
	}
	
	@Test
	public void testGetFileNameWithoutExtension(){
		File file = new File("test.txt");
		String fileNameChk = xABizUtil.getFileNameWithoutExtension(file);
		Assert.assertEquals("test",fileNameChk);
	}
	
	@Test
	public void testGetFileNameWithoutExtension_NoFile(){
		String fileNameChk = xABizUtil.getFileNameWithoutExtension(null);
		Assert.assertNull(fileNameChk);
	}
	
	@Test
	public void testGetFileNameWithoutExtension_noExt(){
		File file = new File("test");
		String fileNameChk = xABizUtil.getFileNameWithoutExtension(file);
		Assert.assertEquals("test",fileNameChk);
	}
	
	@Test
	public void testGetImageExtension_TestJPG(){
		String contentType = "img.JPG";
		String extChk = xABizUtil.getImageExtension(contentType);
		Assert.assertEquals("jpg",extChk);
	}
	
	@Test
	public void testGetImageExtension_TestJPEG(){
		String contentType = "img.JPEG";
		String extChk = xABizUtil.getImageExtension(contentType);
		Assert.assertEquals("jpg",extChk);
	}
	
	@Test
	public void testGetImageExtension_TestPNG(){
		String contentType = "img.PNG";
		String extChk = xABizUtil.getImageExtension(contentType);
		Assert.assertEquals("png",extChk);
	}
	
	@Test
	public void testGetImageExtension_NoExt(){
		String contentType = "img";
		String extChk = xABizUtil.getImageExtension(contentType);
		Assert.assertEquals("",extChk);
	}
	
	@Test
	public void testGetMimeType_ForJPG(){
		String mimeTypeChk = xABizUtil.getMimeType(XAConstants.MIME_JPEG);
		Assert.assertEquals("jpg",mimeTypeChk);		
	}
	
	@Test
	public void testGetMimeType_ForPNG(){
		String mimeTypeChk = xABizUtil.getMimeType(XAConstants.MIME_PNG);
		Assert.assertEquals("png",mimeTypeChk);		
	}
	
	@Test
	public void testGetMimeType_ForEmpty(){
		String mimeTypeChk = xABizUtil.getMimeType(1);
		Assert.assertEquals("",mimeTypeChk);
	}
	
	@Test
	public void testGetMimeTypeInt_ForUnknow(){
		int mimeTypeChk = xABizUtil.getMimeTypeInt("");
		Assert.assertEquals(XAConstants.MIME_UNKNOWN, mimeTypeChk);
	}
	
	@Test
	public void testGetMimeTypeInt_Forjpg(){
		int mimeTypeChk = xABizUtil.getMimeTypeInt("jpg");
		Assert.assertEquals(XAConstants.MIME_JPEG, mimeTypeChk);
	}
	
	@Test
	public void testGetMimeTypeInt_ForJPEG(){
		int mimeTypeChk = xABizUtil.getMimeTypeInt("JPEG");
		Assert.assertEquals(XAConstants.MIME_JPEG, mimeTypeChk);
	}
	
	@Test
	public void testGetMimeTypeInt_EndsWithJPEG(){
		int mimeTypeChk = xABizUtil.getMimeTypeInt("txt.jpeg");
		Assert.assertEquals(XAConstants.MIME_JPEG, mimeTypeChk);
	}
	
	@Test
	public void testGetMimeTypeInt_EndsWithJPG(){
		int mimeTypeChk = xABizUtil.getMimeTypeInt("txt.jpg");
		Assert.assertEquals(XAConstants.MIME_JPEG, mimeTypeChk);
	}
	
	@Test
	public void testGetMimeTypeInt_EndsWithPNG(){
		int mimeTypeChk = xABizUtil.getMimeTypeInt("txt.png");
		Assert.assertEquals(XAConstants.MIME_PNG, mimeTypeChk);
	}
	
	@Test
	public void testGetMimeTypeInt_ForPNG(){
		int mimeTypeChk = xABizUtil.getMimeTypeInt("png");
		Assert.assertEquals(XAConstants.MIME_PNG, mimeTypeChk);		
	}	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGetMObject(){
		BaseDao baseDao = Mockito.mock(BaseDao.class);
		Mockito.when(daoManager.getDaoForClassType(XACommonEnums.CLASS_TYPE_USER_PROFILE)).thenReturn(baseDao);
		Mockito.when(baseDao.getById(id)).thenReturn(new XXAsset());
		XXDBBase mObjChk = xABizUtil.getMObject(XACommonEnums.CLASS_TYPE_USER_PROFILE,id);
		Assert.assertNotNull(mObjChk);
	}
	
	@Test
	public void testGetMObject_NoObjId(){
		XXDBBase mObjChk = xABizUtil.getMObject(XACommonEnums.CLASS_TYPE_USER_PROFILE,null);
		Assert.assertNull(mObjChk);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGetMObject_VXDataObject(){
		VXAsset vXDataObject = new VXAsset();
		vXDataObject.setId(id);	
		BaseDao baseDao = Mockito.mock(BaseDao.class);
		Mockito.when(daoManager.getDaoForClassType(vXDataObject.getMyClassType())).thenReturn(baseDao);
		Mockito.when(baseDao.getById(vXDataObject.getId())).thenReturn(new XXAsset());
		XXDBBase xXDBaseChk = xABizUtil.getMObject(vXDataObject);		
		Assert.assertNotNull(xXDBaseChk);
	}
	
	@Test
	public void testGetMObject_NOVXDataObject(){
		XXDBBase xXDBaseChk = xABizUtil.getMObject(null);		
		Assert.assertNull(xXDBaseChk);
	}
	
	@Test
	public void testGetVObject_NOObjId(){
		VXDataObject objchk = xABizUtil.getVObject(XAConstants.CLASS_TYPE_USER_PROFILE, null);
		Assert.assertNull(objchk);
	}	
	
	@Test
	public void testMatchHdfsPolicy_NoResourceName(){
		boolean bnlChk = xABizUtil.matchHbasePolicy(null, null, null, id, AppConstants.XA_PERM_TYPE_UNKNOWN);
		Assert.assertFalse(bnlChk);
	}
	
	@Test
	public void testMatchHdfsPolicy_NoResourceList(){
		boolean bnlChk = xABizUtil.matchHbasePolicy(resourceName, null, null, id, AppConstants.XA_PERM_TYPE_UNKNOWN);
		Assert.assertFalse(bnlChk);
	}
	
	@Test
	public void testMatchHdfsPolicy_NoUserId(){
		VXResponse vXResponse = new VXResponse();
		List<XXResource> xResourceList = new ArrayList<XXResource>();
		XXResource xXResource = new XXResource();
		xXResource.setId(id);
		xXResource.setName(resourceName);
		xXResource.setIsRecursive(AppConstants.BOOL_TRUE);
		xXResource.setResourceStatus(AppConstants.STATUS_ENABLED);
		xResourceList.add(xXResource);
		boolean bnlChk = xABizUtil.matchHbasePolicy(resourceName, xResourceList, vXResponse, null, AppConstants.XA_PERM_TYPE_UNKNOWN);
		Assert.assertFalse(bnlChk);
	}
	
	@Test
	public void testMatchHdfsPolicy_NoPremission(){
		VXResponse vXResponse = new VXResponse();
		List<XXResource> xResourceList = new ArrayList<XXResource>();
		XXResource xXResource = new XXResource();
		xXResource.setId(id);
		xXResource.setName(resourceName);
		xXResource.setIsRecursive(AppConstants.BOOL_TRUE);
		xXResource.setResourceStatus(AppConstants.STATUS_ENABLED);
		xResourceList.add(xXResource);
		Mockito.when(stringUtil.isEmpty(resourceName)).thenReturn(true);
		Mockito.when(stringUtil.split(Mockito.anyString(), Mockito.anyString())).thenReturn(new String[0]);
		boolean bnlChk = xABizUtil.matchHbasePolicy("/*/*/*", xResourceList, vXResponse, id, AppConstants.XA_PERM_TYPE_UNKNOWN);
		Mockito.verify(stringUtil).split(Mockito.anyString(), Mockito.anyString());
		Assert.assertFalse(bnlChk);
	}
	
	@Test
	public void testMatchHivePolicy_NoResourceName(){
		boolean bnlChk = xABizUtil.matchHivePolicy(null, null, null, 0);
		Assert.assertFalse(bnlChk);
		
	}
	
	@Test
	public void testMatchHivePolicy_NoResourceList(){
		boolean bnlChk = xABizUtil.matchHivePolicy(resourceName, null, null, 0);
		Assert.assertFalse(bnlChk);
		
	}
	
	@Test
	public void testMatchHivePolicy_NoUserId(){
		List<XXResource> xResourceList = new ArrayList<XXResource>();
		XXResource xXResource = new XXResource();
		xXResource.setId(id);
		xXResource.setName(resourceName);
		xXResource.setIsRecursive(AppConstants.BOOL_TRUE);
		xXResource.setResourceStatus(AppConstants.STATUS_ENABLED);
		xResourceList.add(xXResource);
		boolean bnlChk = xABizUtil.matchHivePolicy(resourceName, xResourceList, null, 0);
		Assert.assertFalse(bnlChk);
		
	}
	
	@Test
	public void testMatchHivePolicy_NoPremission(){
		List<XXResource> xResourceList = new ArrayList<XXResource>();
		XXResource xXResource = new XXResource();
		xXResource.setId(id);
		xXResource.setName(resourceName);
		xXResource.setIsRecursive(AppConstants.BOOL_TRUE);
		xXResource.setResourceStatus(AppConstants.STATUS_ENABLED);
		xResourceList.add(xXResource);
		Mockito.when(stringUtil.split(Mockito.anyString(), Mockito.anyString())).thenReturn(new String[0]);
		boolean bnlChk = xABizUtil.matchHivePolicy("/*/*/*", xResourceList, id, 0);
		Assert.assertFalse(bnlChk);		
	}
	
	@Test
	public void testMatchHivePolicy(){
		List<XXResource> xResourceList = new ArrayList<XXResource>();
		XXResource xXResource = new XXResource();
		xXResource.setId(5L);
		xXResource.setName(resourceName);
		xXResource.setIsRecursive(AppConstants.BOOL_TRUE);
		xXResource.setResourceStatus(AppConstants.STATUS_ENABLED);
		xResourceList.add(xXResource);
		Mockito.when(stringUtil.split(Mockito.anyString(), Mockito.anyString())).thenReturn(new String[0]);
		boolean bnlChk = xABizUtil.matchHivePolicy("/*/*/*", xResourceList, id, 17);
		Mockito.verify(stringUtil).split(Mockito.anyString(), Mockito.anyString());
		Assert.assertFalse(bnlChk);		
	}		
}