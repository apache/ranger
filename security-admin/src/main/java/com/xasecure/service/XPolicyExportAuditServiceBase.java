package com.xasecure.service;
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

/**
 * 
 */

import java.util.ArrayList;
import java.util.List;

import com.xasecure.common.*;
import com.xasecure.entity.*;
import com.xasecure.view.*;
import com.xasecure.service.*;

public abstract class XPolicyExportAuditServiceBase<T extends XXPolicyExportAudit, V extends VXPolicyExportAudit>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XPolicyExportAudit";

	public XPolicyExportAuditServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXPolicyExportAudit mapViewToEntityBean(VXPolicyExportAudit vObj, XXPolicyExportAudit mObj, int OPERATION_CONTEXT) {
		mObj.setClientIP( vObj.getClientIP());
		mObj.setAgentId( vObj.getAgentId());
		mObj.setRequestedEpoch( vObj.getRequestedEpoch());
		mObj.setLastUpdated( vObj.getLastUpdated());
		mObj.setRepositoryName( vObj.getRepositoryName());
		mObj.setExportedJson( vObj.getExportedJson());
		mObj.setHttpRetCode( vObj.getHttpRetCode());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXPolicyExportAudit mapEntityToViewBean(VXPolicyExportAudit vObj, XXPolicyExportAudit mObj) {
		vObj.setClientIP( mObj.getClientIP());
		vObj.setAgentId( mObj.getAgentId());
		vObj.setRequestedEpoch( mObj.getRequestedEpoch());
		vObj.setLastUpdated( mObj.getLastUpdated());
		vObj.setRepositoryName( mObj.getRepositoryName());
		vObj.setExportedJson( mObj.getExportedJson());
		vObj.setHttpRetCode( mObj.getHttpRetCode());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXPolicyExportAuditList searchXPolicyExportAudits(SearchCriteria searchCriteria) {
		VXPolicyExportAuditList returnList = new VXPolicyExportAuditList();
		List<VXPolicyExportAudit> xPolicyExportAuditList = new ArrayList<VXPolicyExportAudit>();

		@SuppressWarnings("unchecked")
		List<XXPolicyExportAudit> resultList = (List<XXPolicyExportAudit>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXPolicyExportAudit gjXPolicyExportAudit : resultList) {
			@SuppressWarnings("unchecked")
			VXPolicyExportAudit vXPolicyExportAudit = populateViewBean((T)gjXPolicyExportAudit);
			xPolicyExportAuditList.add(vXPolicyExportAudit);
		}

		returnList.setVXPolicyExportAudits(xPolicyExportAuditList);
		return returnList;
	}

}
