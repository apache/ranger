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

public abstract class XCredentialStoreServiceBase<T extends XXCredentialStore, V extends VXCredentialStore>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XCredentialStore";

	public XCredentialStoreServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXCredentialStore mapViewToEntityBean(VXCredentialStore vObj, XXCredentialStore mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setDescription( vObj.getDescription());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXCredentialStore mapEntityToViewBean(VXCredentialStore vObj, XXCredentialStore mObj) {
		vObj.setName( mObj.getName());
		vObj.setDescription( mObj.getDescription());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXCredentialStoreList searchXCredentialStores(SearchCriteria searchCriteria) {
		VXCredentialStoreList returnList = new VXCredentialStoreList();
		List<VXCredentialStore> xCredentialStoreList = new ArrayList<VXCredentialStore>();

		@SuppressWarnings("unchecked")
		List<XXCredentialStore> resultList = (List<XXCredentialStore>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXCredentialStore gjXCredentialStore : resultList) {
			@SuppressWarnings("unchecked")
			VXCredentialStore vXCredentialStore = populateViewBean((T)gjXCredentialStore);
			xCredentialStoreList.add(vXCredentialStore);
		}

		returnList.setVXCredentialStores(xCredentialStoreList);
		return returnList;
	}

}
