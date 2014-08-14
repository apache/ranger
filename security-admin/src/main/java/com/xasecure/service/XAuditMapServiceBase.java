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

public abstract class XAuditMapServiceBase<T extends XXAuditMap, V extends VXAuditMap>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XAuditMap";

	public XAuditMapServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXAuditMap mapViewToEntityBean(VXAuditMap vObj, XXAuditMap mObj, int OPERATION_CONTEXT) {
		mObj.setResourceId( vObj.getResourceId());
		mObj.setGroupId( vObj.getGroupId());
		mObj.setUserId( vObj.getUserId());
		mObj.setAuditType( vObj.getAuditType());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXAuditMap mapEntityToViewBean(VXAuditMap vObj, XXAuditMap mObj) {
		vObj.setResourceId( mObj.getResourceId());
		vObj.setGroupId( mObj.getGroupId());
		vObj.setUserId( mObj.getUserId());
		vObj.setAuditType( mObj.getAuditType());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXAuditMapList searchXAuditMaps(SearchCriteria searchCriteria) {
		VXAuditMapList returnList = new VXAuditMapList();
		List<VXAuditMap> xAuditMapList = new ArrayList<VXAuditMap>();

		@SuppressWarnings("unchecked")
		List<XXAuditMap> resultList = (List<XXAuditMap>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXAuditMap gjXAuditMap : resultList) {
			@SuppressWarnings("unchecked")
			VXAuditMap vXAuditMap = populateViewBean((T)gjXAuditMap);
			xAuditMapList.add(vXAuditMap);
		}

		returnList.setVXAuditMaps(xAuditMapList);
		return returnList;
	}

}
