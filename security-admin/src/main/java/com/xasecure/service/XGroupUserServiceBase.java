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

public abstract class XGroupUserServiceBase<T extends XXGroupUser, V extends VXGroupUser>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XGroupUser";

	public XGroupUserServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXGroupUser mapViewToEntityBean(VXGroupUser vObj, XXGroupUser mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setParentGroupId( vObj.getParentGroupId());
		mObj.setUserId( vObj.getUserId());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXGroupUser mapEntityToViewBean(VXGroupUser vObj, XXGroupUser mObj) {
		vObj.setName( mObj.getName());
		vObj.setParentGroupId( mObj.getParentGroupId());
		vObj.setUserId( mObj.getUserId());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXGroupUserList searchXGroupUsers(SearchCriteria searchCriteria) {
		VXGroupUserList returnList = new VXGroupUserList();
		List<VXGroupUser> xGroupUserList = new ArrayList<VXGroupUser>();

		@SuppressWarnings("unchecked")
		List<XXGroupUser> resultList = (List<XXGroupUser>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXGroupUser gjXGroupUser : resultList) {
			@SuppressWarnings("unchecked")
			VXGroupUser vXGroupUser = populateViewBean((T)gjXGroupUser);
			xGroupUserList.add(vXGroupUser);
		}

		returnList.setVXGroupUsers(xGroupUserList);
		return returnList;
	}

}
