package com.xasecure.biz;
/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure
 */

import com.xasecure.common.*;
import com.xasecure.service.*;
import com.xasecure.view.*;
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
