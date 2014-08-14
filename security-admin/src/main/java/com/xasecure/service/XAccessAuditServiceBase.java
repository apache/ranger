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

public abstract class XAccessAuditServiceBase<T extends XXAccessAudit, V extends VXAccessAudit>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XAccessAudit";

	public XAccessAuditServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXAccessAudit mapViewToEntityBean(VXAccessAudit vObj, XXAccessAudit mObj, int OPERATION_CONTEXT) {
		mObj.setAuditType( vObj.getAuditType());
		mObj.setAccessResult( vObj.getAccessResult());
		mObj.setAccessType( vObj.getAccessType());
		mObj.setAclEnforcer( vObj.getAclEnforcer());
		mObj.setAgentId( vObj.getAgentId());
		mObj.setClientIP( vObj.getClientIP());
		mObj.setClientType( vObj.getClientType());
		mObj.setPolicyId( vObj.getPolicyId());
		mObj.setRepoName( vObj.getRepoName());
		mObj.setRepoType( vObj.getRepoType());
		mObj.setResultReason( vObj.getResultReason());
		mObj.setSessionId( vObj.getSessionId());
		mObj.setEventTime( vObj.getEventTime());
		mObj.setRequestUser( vObj.getRequestUser());
		mObj.setAction( vObj.getAction());
		mObj.setRequestData( vObj.getRequestData());
		mObj.setResourcePath( vObj.getResourcePath());
		mObj.setResourceType( vObj.getResourceType());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXAccessAudit mapEntityToViewBean(VXAccessAudit vObj, XXAccessAudit mObj) {
		vObj.setAuditType( mObj.getAuditType());
		vObj.setAccessResult( mObj.getAccessResult());
		vObj.setAccessType( mObj.getAccessType());
		vObj.setAclEnforcer( mObj.getAclEnforcer());
		vObj.setAgentId( mObj.getAgentId());
		vObj.setClientIP( mObj.getClientIP());
		vObj.setClientType( mObj.getClientType());
		vObj.setPolicyId( mObj.getPolicyId());
		vObj.setRepoName( mObj.getRepoName());
		vObj.setRepoType( mObj.getRepoType());
		vObj.setResultReason( mObj.getResultReason());
		vObj.setSessionId( mObj.getSessionId());
		vObj.setEventTime( mObj.getEventTime());
		vObj.setRequestUser( mObj.getRequestUser());
		vObj.setAction( mObj.getAction());
		vObj.setRequestData( mObj.getRequestData());
		vObj.setResourcePath( mObj.getResourcePath());
		vObj.setResourceType( mObj.getResourceType());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXAccessAuditList searchXAccessAudits(SearchCriteria searchCriteria) {
		VXAccessAuditList returnList = new VXAccessAuditList();
		List<VXAccessAudit> xAccessAuditList = new ArrayList<VXAccessAudit>();

		@SuppressWarnings("unchecked")
		List<XXAccessAudit> resultList = (List<XXAccessAudit>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXAccessAudit gjXAccessAudit : resultList) {
			@SuppressWarnings("unchecked")
			VXAccessAudit vXAccessAudit = populateViewBean((T)gjXAccessAudit);
			xAccessAuditList.add(vXAccessAudit);
		}

		returnList.setVXAccessAudits(xAccessAuditList);
		return returnList;
	}

}
