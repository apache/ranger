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

public abstract class XUserServiceBase<T extends XXUser, V extends VXUser>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XUser";

	public XUserServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXUser mapViewToEntityBean(VXUser vObj, XXUser mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setDescription( vObj.getDescription());
		mObj.setCredStoreId( vObj.getCredStoreId());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXUser mapEntityToViewBean(VXUser vObj, XXUser mObj) {
		vObj.setName( mObj.getName());
		vObj.setDescription( mObj.getDescription());
		vObj.setCredStoreId( mObj.getCredStoreId());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXUserList searchXUsers(SearchCriteria searchCriteria) {
		VXUserList returnList = new VXUserList();
		List<VXUser> xUserList = new ArrayList<VXUser>();

		@SuppressWarnings("unchecked")
		List<XXUser> resultList = (List<XXUser>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXUser gjXUser : resultList) {
			@SuppressWarnings("unchecked")
			VXUser vXUser = populateViewBean((T)gjXUser);
			xUserList.add(vXUser);
		}

		returnList.setVXUsers(xUserList);
		return returnList;
	}

}
