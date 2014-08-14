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

public abstract class XGroupServiceBase<T extends XXGroup, V extends VXGroup>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XGroup";

	public XGroupServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXGroup mapViewToEntityBean(VXGroup vObj, XXGroup mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setDescription( vObj.getDescription());
		mObj.setGroupType( vObj.getGroupType());
		mObj.setCredStoreId( vObj.getCredStoreId());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXGroup mapEntityToViewBean(VXGroup vObj, XXGroup mObj) {
		vObj.setName( mObj.getName());
		vObj.setDescription( mObj.getDescription());
		vObj.setGroupType( mObj.getGroupType());
		vObj.setCredStoreId( mObj.getCredStoreId());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXGroupList searchXGroups(SearchCriteria searchCriteria) {
		VXGroupList returnList = new VXGroupList();
		List<VXGroup> xGroupList = new ArrayList<VXGroup>();

		@SuppressWarnings("unchecked")
		List<XXGroup> resultList = (List<XXGroup>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXGroup gjXGroup : resultList) {
			@SuppressWarnings("unchecked")
			VXGroup vXGroup = populateViewBean((T)gjXGroup);
			xGroupList.add(vXGroup);
		}

		returnList.setVXGroups(xGroupList);
		return returnList;
	}

}
