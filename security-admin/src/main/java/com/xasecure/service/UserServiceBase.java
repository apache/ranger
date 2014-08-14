package com.xasecure.service;
/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure.
 */

import java.util.ArrayList;
import java.util.List;

import com.xasecure.common.*;
import com.xasecure.entity.*;
import com.xasecure.view.*;
import com.xasecure.service.*;

public abstract class UserServiceBase<T extends XXPortalUser, V extends VXPortalUser>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "User";

	public UserServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXPortalUser mapViewToEntityBean(VXPortalUser vObj, XXPortalUser mObj, int OPERATION_CONTEXT) {
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXPortalUser mapEntityToViewBean(VXPortalUser vObj, XXPortalUser mObj) {
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXPortalUserList searchUsers(SearchCriteria searchCriteria) {
		VXPortalUserList returnList = new VXPortalUserList();
		List<VXPortalUser> userList = new ArrayList<VXPortalUser>();

		@SuppressWarnings("unchecked")
		List<XXPortalUser> resultList = (List<XXPortalUser>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXPortalUser gjUser : resultList) {
			@SuppressWarnings("unchecked")
			VXPortalUser vUser = populateViewBean((T)gjUser);
			userList.add(vUser);
		}

		returnList.setVXPortalUsers(userList);
		return returnList;
	}

}
