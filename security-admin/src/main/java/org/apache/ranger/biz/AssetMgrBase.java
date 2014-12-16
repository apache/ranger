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

 package org.apache.ranger.biz;

import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.service.XAssetService;
import org.apache.ranger.service.XCredentialStoreService;
import org.apache.ranger.service.XPolicyExportAuditService;
import org.apache.ranger.service.XResourceService;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXAssetList;
import org.apache.ranger.view.VXCredentialStore;
import org.apache.ranger.view.VXCredentialStoreList;
import org.apache.ranger.view.VXLong;
import org.apache.ranger.view.VXPolicyExportAudit;
import org.apache.ranger.view.VXPolicyExportAuditList;
import org.apache.ranger.view.VXResource;
import org.apache.ranger.view.VXResourceList;
import org.springframework.beans.factory.annotation.Autowired;
public class AssetMgrBase {

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	XAssetService xAssetService;

	@Autowired
	XResourceService xResourceService;

	@Autowired
	XCredentialStoreService xCredentialStoreService;

	@Autowired
	XPolicyExportAuditService xPolicyExportAuditService;
	public VXAsset getXAsset(Long id){
		return (VXAsset)xAssetService.readResource(id);
	}

	public VXAsset createXAsset(VXAsset vXAsset){
		vXAsset =  (VXAsset)xAssetService.createResource(vXAsset);
		return vXAsset;
	}

	public VXAsset updateXAsset(VXAsset vXAsset) {
		vXAsset =  (VXAsset)xAssetService.updateResource(vXAsset);
		return vXAsset;
	}

	public void deleteXAsset(Long id, boolean force) {
		 if (force) {
			 xAssetService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXAssetList searchXAssets(SearchCriteria searchCriteria) {
		return xAssetService.searchXAssets(searchCriteria);
	}

	public VXLong getXAssetSearchCount(SearchCriteria searchCriteria) {
		return xAssetService.getSearchCount(searchCriteria,
				xAssetService.searchFields);
	}

	public VXResource getXResource(Long id){
		return (VXResource)xResourceService.readResource(id);
	}

	public VXResource createXResource(VXResource vXResource){
		vXResource =  (VXResource)xResourceService.createResource(vXResource);
		return vXResource;
	}

	public VXResource updateXResource(VXResource vXResource) {
		vXResource =  (VXResource)xResourceService.updateResource(vXResource);
		return vXResource;
	}

	public void deleteXResource(Long id, boolean force) {
		 if (force) {
			 xResourceService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXResourceList searchXResources(SearchCriteria searchCriteria) {
		return xResourceService.searchXResources(searchCriteria);
	}

	public VXLong getXResourceSearchCount(SearchCriteria searchCriteria) {
		return xResourceService.getSearchCount(searchCriteria,
				xResourceService.searchFields);
	}

	public VXCredentialStore getXCredentialStore(Long id){
		return (VXCredentialStore)xCredentialStoreService.readResource(id);
	}

	public VXCredentialStore createXCredentialStore(VXCredentialStore vXCredentialStore){
		vXCredentialStore =  (VXCredentialStore)xCredentialStoreService.createResource(vXCredentialStore);
		return vXCredentialStore;
	}

	public VXCredentialStore updateXCredentialStore(VXCredentialStore vXCredentialStore) {
		vXCredentialStore =  (VXCredentialStore)xCredentialStoreService.updateResource(vXCredentialStore);
		return vXCredentialStore;
	}

	public void deleteXCredentialStore(Long id, boolean force) {
		 if (force) {
			 xCredentialStoreService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXCredentialStoreList searchXCredentialStores(SearchCriteria searchCriteria) {
		return xCredentialStoreService.searchXCredentialStores(searchCriteria);
	}

	public VXLong getXCredentialStoreSearchCount(SearchCriteria searchCriteria) {
		return xCredentialStoreService.getSearchCount(searchCriteria,
				xCredentialStoreService.searchFields);
	}

	public VXPolicyExportAudit getXPolicyExportAudit(Long id){
		return (VXPolicyExportAudit)xPolicyExportAuditService.readResource(id);
	}

	public VXPolicyExportAudit createXPolicyExportAudit(VXPolicyExportAudit vXPolicyExportAudit){
		vXPolicyExportAudit =  (VXPolicyExportAudit)xPolicyExportAuditService.createResource(vXPolicyExportAudit);
		return vXPolicyExportAudit;
	}

	public VXPolicyExportAudit updateXPolicyExportAudit(VXPolicyExportAudit vXPolicyExportAudit) {
		vXPolicyExportAudit =  (VXPolicyExportAudit)xPolicyExportAuditService.updateResource(vXPolicyExportAudit);
		return vXPolicyExportAudit;
	}

	public void deleteXPolicyExportAudit(Long id, boolean force) {
		 if (force) {
			 xPolicyExportAuditService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXPolicyExportAuditList searchXPolicyExportAudits(SearchCriteria searchCriteria) {
		return xPolicyExportAuditService.searchXPolicyExportAudits(searchCriteria);
	}

	public VXLong getXPolicyExportAuditSearchCount(SearchCriteria searchCriteria) {
		return xPolicyExportAuditService.getSearchCount(searchCriteria,
				xPolicyExportAuditService.searchFields);
	}

}
