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

public abstract class XAssetServiceBase<T extends XXAsset, V extends VXAsset>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XAsset";

	public XAssetServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXAsset mapViewToEntityBean(VXAsset vObj, XXAsset mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setDescription( vObj.getDescription());
		mObj.setActiveStatus( vObj.getActiveStatus());
		mObj.setAssetType( vObj.getAssetType());
		mObj.setConfig( vObj.getConfig());
		mObj.setSupportNative( vObj.isSupportNative());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXAsset mapEntityToViewBean(VXAsset vObj, XXAsset mObj) {
		vObj.setName( mObj.getName());
		vObj.setDescription( mObj.getDescription());
		vObj.setActiveStatus( mObj.getActiveStatus());
		vObj.setAssetType( mObj.getAssetType());
		vObj.setConfig( mObj.getConfig());
		vObj.setSupportNative( mObj.isSupportNative());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXAssetList searchXAssets(SearchCriteria searchCriteria) {
		VXAssetList returnList = new VXAssetList();
		List<VXAsset> xAssetList = new ArrayList<VXAsset>();

		@SuppressWarnings("unchecked")
		List<XXAsset> resultList = (List<XXAsset>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXAsset gjXAsset : resultList) {
			@SuppressWarnings("unchecked")
			VXAsset vXAsset = populateViewBean((T)gjXAsset);
			xAssetList.add(vXAsset);
		}

		returnList.setVXAssets(xAssetList);
		return returnList;
	}

}
