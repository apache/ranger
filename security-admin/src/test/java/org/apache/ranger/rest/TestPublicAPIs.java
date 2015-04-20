/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 package org.apache.ranger.rest;

import javax.servlet.http.HttpServletRequest;

import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.service.AbstractBaseResourceService;
import org.apache.ranger.service.XAssetService;
import org.apache.ranger.service.XPolicyService;
import org.apache.ranger.service.XRepositoryService;
import org.apache.ranger.service.XResourceService;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXLong;
import org.apache.ranger.view.VXPolicy;
import org.apache.ranger.view.VXPolicyList;
import org.apache.ranger.view.VXRepository;
import org.apache.ranger.view.VXRepositoryList;
import org.apache.ranger.view.VXResource;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;


/**
 * JUnit testSuite for {@link org.apache.ranger.rest.PublicAPIs}
 * 
 */

@Ignore("tests to be reviewed")
@RunWith(MockitoJUnitRunner.class)
public class TestPublicAPIs {
	
	private Long id = 1L;
	
	@InjectMocks
	PublicAPIs publicAPIs = new PublicAPIs();

	@Mock
	RangerSearchUtil searchUtil;

	@Mock
	AssetMgr assetMgr;

	@Mock
	XAssetService xAssetService;

	@Mock
	RESTErrorUtil restErrorUtil;

	@Mock
	XRepositoryService xRepositoryService;

	@Mock
	XResourceService xResourceService;

	@Mock
	XPolicyService xPolicyService;

	@Mock
	StringUtil stringUtil;

	@Mock
	RangerDaoManager xaDaoMgr;

	@Test
	public void testGetRepository(){
		VXAsset vXAsset = new VXAsset();
		VXRepository vXRepository = new VXRepository();
		Mockito.when(assetMgr.getXAsset(id)).thenReturn(vXAsset);
		Mockito.when(xRepositoryService.mapXAToPublicObject(vXAsset)).thenReturn(vXRepository);
		VXRepository vXRepositoryChk = publicAPIs.getRepository(id);
		Mockito.verify(assetMgr).getXAsset(id);
		Mockito.verify(xRepositoryService).mapXAToPublicObject(vXAsset);
		Assert.assertEquals(vXRepositoryChk, vXRepository);
	}
	
	@Test
	public void testCreateRepository(){
		VXAsset vXAsset = new VXAsset();
		VXRepository vXRepository = new VXRepository();
		Mockito.when(xRepositoryService.mapPublicToXAObject(vXRepository)).thenReturn(vXAsset);
		Mockito.when(assetMgr.createXAsset(vXAsset)).thenReturn(vXAsset);
		Mockito.when(xRepositoryService.mapXAToPublicObject(vXAsset)).thenReturn(vXRepository);		
		VXRepository vXRepoChk = publicAPIs.createRepository(vXRepository);		
		Mockito.verify(xRepositoryService).mapXAToPublicObject(vXAsset);
		Mockito.verify(assetMgr).createXAsset(vXAsset);
		Mockito.verify(xRepositoryService).mapPublicToXAObject(vXRepository);
		Assert.assertEquals(vXRepository, vXRepoChk);
	}
	
	@Test
	public void testUpdateRepository(){
		VXAsset vXAsset = new VXAsset();
		VXRepository vXRepository = new VXRepository();
		Mockito.when(xRepositoryService.mapPublicToXAObject(vXRepository)).thenReturn(vXAsset);
		Mockito.when(assetMgr.updateXAsset(vXAsset)).thenReturn(vXAsset);
		Mockito.when(xRepositoryService.mapXAToPublicObject(vXAsset)).thenReturn(vXRepository);
		VXRepository vXRepoChk = publicAPIs.updateRepository(vXRepository, id);
		Mockito.verify(xRepositoryService).mapPublicToXAObject(vXRepository);
		Mockito.verify(assetMgr).updateXAsset(vXAsset);
		Mockito.verify(xRepositoryService).mapXAToPublicObject(vXAsset);
		Assert.assertEquals(vXRepository, vXRepoChk);
	}
	
	@Test
	public void testDeleteRepository(){
		String forceStr = "true";
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		Mockito.when(request.getParameter("force")).thenReturn(forceStr);
		Mockito.when(stringUtil.isEmpty(forceStr)).thenReturn(false);
		publicAPIs.deleteRepository(id, request);
		Mockito.verify(request).getParameter("force");
		Mockito.verify(stringUtil).isEmpty(forceStr);
	}
	
	@Test
	public void testSearchRepositories(){
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		VXRepositoryList vXRepoList = new VXRepositoryList();
		SearchCriteria searchCriteria = new SearchCriteria();
		Mockito.when(searchUtil.extractCommonCriterias(request, xAssetService.sortFields)).thenReturn(searchCriteria);
		Mockito.when(xRepositoryService.mapToVXRepositoryList(null)).thenReturn(vXRepoList);
		VXRepositoryList chk = publicAPIs.searchRepositories(request);
		Mockito.verify(searchUtil).extractCommonCriterias(request, xAssetService.sortFields);
		Mockito.verify(xRepositoryService).mapToVXRepositoryList(null);
		Assert.assertEquals(vXRepoList, chk);
	}
	
	@Test
	public void testCountRepositories(){
		VXLong vXLong = new VXLong();
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		SearchCriteria searchCriteria = new SearchCriteria();
		Mockito.when(searchUtil.extractCommonCriterias(request, xAssetService.sortFields)).thenReturn(searchCriteria);
		Mockito.when(assetMgr.getXAssetSearchCount(searchCriteria)).thenReturn(vXLong);		
		VXLong chk = publicAPIs.countRepositories(request);
		Mockito.verify(searchUtil).extractCommonCriterias(request, xAssetService.sortFields);
		Mockito.verify(assetMgr).getXAssetSearchCount(searchCriteria);
		Assert.assertEquals(vXLong, chk);
	}	
	
	@Test
	public void testGetPolicy(){
		VXResource vXResource = new VXResource();
		VXPolicy vXPolicy = new VXPolicy();
		Mockito.when(assetMgr.getXResource(id)).thenReturn(vXResource);
		Mockito.when(xPolicyService.mapXAToPublicObject(vXResource)).thenReturn(vXPolicy);
		VXPolicy chk = publicAPIs.getPolicy(id);
		Mockito.verify(assetMgr).getXResource(id);
		Mockito.verify(xPolicyService).mapXAToPublicObject(vXResource);
		Assert.assertEquals(vXPolicy, chk);
	}
	
	@Test
	public void testCreatePolicy(){
		VXPolicy vXPolicy = new VXPolicy();
		VXResource vXResource = new VXResource();
		Mockito.when(xPolicyService.mapPublicToXAObject(vXPolicy,AbstractBaseResourceService.OPERATION_CREATE_CONTEXT)).thenReturn(vXResource);
		Mockito.when(assetMgr.createXResource(vXResource)).thenReturn(vXResource);
		Mockito.when(xPolicyService.mapXAToPublicObject(vXResource)).thenReturn(vXPolicy);
		VXPolicy chk = publicAPIs.createPolicy(vXPolicy);
		Mockito.verify(xPolicyService).mapPublicToXAObject(vXPolicy,AbstractBaseResourceService.OPERATION_CREATE_CONTEXT);
		Mockito.verify(assetMgr).createXResource(vXResource);
		Mockito.verify(xPolicyService).mapXAToPublicObject(vXResource);
		Assert.assertEquals(vXPolicy, chk);
	}
	
	@Test
	public void testUpdatePolicy(){
		VXPolicy vXPolicy = new VXPolicy();
		VXResource vXResource = new VXResource();
		Mockito.when(xPolicyService.mapPublicToXAObject(vXPolicy,AbstractBaseResourceService.OPERATION_UPDATE_CONTEXT)).thenReturn(vXResource);
		Mockito.when(assetMgr.updateXResource(vXResource)).thenReturn(vXResource);
		Mockito.when(xPolicyService.mapXAToPublicObject(vXResource)).thenReturn(vXPolicy);
		VXPolicy chk = publicAPIs.updatePolicy(vXPolicy, id);
		Mockito.verify(xPolicyService).mapPublicToXAObject(vXPolicy,AbstractBaseResourceService.OPERATION_UPDATE_CONTEXT);
		Mockito.verify(assetMgr).updateXResource(vXResource);
		Mockito.verify(xPolicyService).mapXAToPublicObject(vXResource);
		Assert.assertEquals(vXPolicy, chk);		
	}
	
	@Test
	public void testDeletePolicy(){
		String forceStr = "true";
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		Mockito.when(request.getParameter("force")).thenReturn(forceStr);
		Mockito.when(stringUtil.isEmpty(forceStr)).thenReturn(false);
		publicAPIs.deletePolicy(id, request);
		Mockito.verify(request).getParameter("force");
		Mockito.verify(stringUtil).isEmpty(forceStr);
	}
	
	@Test
	public void testSearchPolicies(){
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		VXPolicyList vXRepoList = new VXPolicyList();
		SearchCriteria searchCriteria = new SearchCriteria();
		Mockito.when(searchUtil.extractCommonCriterias(request, xAssetService.sortFields)).thenReturn(searchCriteria);
		Mockito.when(xPolicyService.mapToVXPolicyList(null)).thenReturn(vXRepoList);
		VXPolicyList chk = publicAPIs.searchPolicies(request);
		Mockito.verify(searchUtil).extractCommonCriterias(request, xAssetService.sortFields);
		Mockito.verify(xPolicyService).mapToVXPolicyList(null);
		Assert.assertEquals(vXRepoList, chk);
	}
	
	@Test
	public void testCountPolicies(){
		VXLong vXLong = new VXLong();
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		SearchCriteria searchCriteria = new SearchCriteria();
		Mockito.when(searchUtil.extractCommonCriterias(request, xResourceService.sortFields)).thenReturn(searchCriteria);
		Mockito.when(assetMgr.getXResourceSearchCount(searchCriteria)).thenReturn(vXLong);
		VXLong chk = publicAPIs.countPolicies(request);
		Mockito.verify(searchUtil).extractCommonCriterias(request, xResourceService.sortFields);
		Mockito.verify(assetMgr).getXResourceSearchCount(searchCriteria);
		Assert.assertEquals(vXLong, chk);
	}
}
