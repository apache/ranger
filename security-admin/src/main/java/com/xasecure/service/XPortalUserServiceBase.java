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

public abstract class XPortalUserServiceBase<T extends XXPortalUser, V extends VXPortalUser>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XPortalUser";

	public XPortalUserServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXPortalUser mapViewToEntityBean(VXPortalUser vObj, XXPortalUser mObj, int OPERATION_CONTEXT) {
		mObj.setFirstName( vObj.getFirstName());
		mObj.setLastName( vObj.getLastName());
		mObj.setPublicScreenName( vObj.getPublicScreenName());
		mObj.setLoginId( vObj.getLoginId());
		mObj.setPassword( vObj.getPassword());
		mObj.setEmailAddress( vObj.getEmailAddress());
		mObj.setStatus( vObj.getStatus());
		mObj.setUserSource( vObj.getUserSource());
		mObj.setNotes( vObj.getNotes());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXPortalUser mapEntityToViewBean(VXPortalUser vObj, XXPortalUser mObj) {
		vObj.setFirstName( mObj.getFirstName());
		vObj.setLastName( mObj.getLastName());
		vObj.setPublicScreenName( mObj.getPublicScreenName());
		vObj.setLoginId( mObj.getLoginId());
		vObj.setPassword( mObj.getPassword());
		vObj.setEmailAddress( mObj.getEmailAddress());
		vObj.setStatus( mObj.getStatus());
		vObj.setUserSource( mObj.getUserSource());
		vObj.setNotes( mObj.getNotes());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXPortalUserList searchXPortalUsers(SearchCriteria searchCriteria) {
		VXPortalUserList returnList = new VXPortalUserList();
		List<VXPortalUser> xPortalUserList = new ArrayList<VXPortalUser>();

		@SuppressWarnings("unchecked")
		List<XXPortalUser> resultList = (List<XXPortalUser>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXPortalUser gjXPortalUser : resultList) {
			@SuppressWarnings("unchecked")
			VXPortalUser vXPortalUser = populateViewBean((T)gjXPortalUser);
			xPortalUserList.add(vXPortalUser);
		}

		returnList.setVXPortalUsers(xPortalUserList);
		return returnList;
	}

}
