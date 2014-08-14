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

public abstract class XPermMapServiceBase<T extends XXPermMap, V extends VXPermMap>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XPermMap";

	public XPermMapServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXPermMap mapViewToEntityBean(VXPermMap vObj, XXPermMap mObj, int OPERATION_CONTEXT) {
		mObj.setPermGroup( vObj.getPermGroup());
		mObj.setResourceId( vObj.getResourceId());
		mObj.setGroupId( vObj.getGroupId());
		mObj.setUserId( vObj.getUserId());
		mObj.setPermFor( vObj.getPermFor());
		mObj.setPermType( vObj.getPermType());
		mObj.setIsRecursive( vObj.getIsRecursive());
		mObj.setIsWildCard( vObj.isIsWildCard());
		mObj.setGrantOrRevoke( vObj.isGrantOrRevoke());
		mObj.setIpAddress( vObj.getIpAddress());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXPermMap mapEntityToViewBean(VXPermMap vObj, XXPermMap mObj) {
		vObj.setPermGroup( mObj.getPermGroup());
		vObj.setResourceId( mObj.getResourceId());
		vObj.setGroupId( mObj.getGroupId());
		vObj.setUserId( mObj.getUserId());
		vObj.setPermFor( mObj.getPermFor());
		vObj.setPermType( mObj.getPermType());
		vObj.setIsRecursive( mObj.getIsRecursive());
		vObj.setIsWildCard( mObj.isIsWildCard());
		vObj.setGrantOrRevoke( mObj.isGrantOrRevoke());
		vObj.setIpAddress( mObj.getIpAddress());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXPermMapList searchXPermMaps(SearchCriteria searchCriteria) {
		VXPermMapList returnList = new VXPermMapList();
		List<VXPermMap> xPermMapList = new ArrayList<VXPermMap>();

		@SuppressWarnings("unchecked")
		List<XXPermMap> resultList = (List<XXPermMap>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXPermMap gjXPermMap : resultList) {
			@SuppressWarnings("unchecked")
			VXPermMap vXPermMap = populateViewBean((T)gjXPermMap);
			xPermMapList.add(vXPermMap);
		}

		returnList.setVXPermMaps(xPermMapList);
		return returnList;
	}

}
