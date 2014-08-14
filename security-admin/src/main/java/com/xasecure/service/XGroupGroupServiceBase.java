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

public abstract class XGroupGroupServiceBase<T extends XXGroupGroup, V extends VXGroupGroup>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XGroupGroup";

	public XGroupGroupServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXGroupGroup mapViewToEntityBean(VXGroupGroup vObj, XXGroupGroup mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setParentGroupId( vObj.getParentGroupId());
		mObj.setGroupId( vObj.getGroupId());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXGroupGroup mapEntityToViewBean(VXGroupGroup vObj, XXGroupGroup mObj) {
		vObj.setName( mObj.getName());
		vObj.setParentGroupId( mObj.getParentGroupId());
		vObj.setGroupId( mObj.getGroupId());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXGroupGroupList searchXGroupGroups(SearchCriteria searchCriteria) {
		VXGroupGroupList returnList = new VXGroupGroupList();
		List<VXGroupGroup> xGroupGroupList = new ArrayList<VXGroupGroup>();

		@SuppressWarnings("unchecked")
		List<XXGroupGroup> resultList = (List<XXGroupGroup>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXGroupGroup gjXGroupGroup : resultList) {
			@SuppressWarnings("unchecked")
			VXGroupGroup vXGroupGroup = populateViewBean((T)gjXGroupGroup);
			xGroupGroupList.add(vXGroupGroup);
		}

		returnList.setVXGroupGroups(xGroupGroupList);
		return returnList;
	}

}
