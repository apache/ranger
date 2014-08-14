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

public abstract class XResourceServiceBase<T extends XXResource, V extends VXResource>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XResource";

	public XResourceServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXResource mapViewToEntityBean(VXResource vObj, XXResource mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setDescription( vObj.getDescription());
		mObj.setResourceType( vObj.getResourceType());
		mObj.setAssetId( vObj.getAssetId());
		mObj.setParentId( vObj.getParentId());
		mObj.setParentPath( vObj.getParentPath());
		mObj.setIsEncrypt( vObj.getIsEncrypt());
		mObj.setIsRecursive( vObj.getIsRecursive());
		mObj.setResourceGroup( vObj.getResourceGroup());
		mObj.setDatabases( vObj.getDatabases());
		mObj.setTables( vObj.getTables());
		mObj.setColumnFamilies( vObj.getColumnFamilies());
		mObj.setColumns( vObj.getColumns());
		mObj.setUdfs( vObj.getUdfs());
		mObj.setResourceStatus( vObj.getResourceStatus());
		mObj.setTableType( vObj.getTableType());
		mObj.setColumnType( vObj.getColumnType());
		mObj.setPolicyName( vObj.getPolicyName());
		mObj.setTopologies( vObj.getTopologies());
		mObj.setServices( vObj.getServices());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXResource mapEntityToViewBean(VXResource vObj, XXResource mObj) {
		vObj.setName( mObj.getName());
		vObj.setDescription( mObj.getDescription());
		vObj.setResourceType( mObj.getResourceType());
		vObj.setAssetId( mObj.getAssetId());
		vObj.setParentId( mObj.getParentId());
		vObj.setParentPath( mObj.getParentPath());
		vObj.setIsEncrypt( mObj.getIsEncrypt());
		vObj.setIsRecursive( mObj.getIsRecursive());
		vObj.setResourceGroup( mObj.getResourceGroup());
		vObj.setDatabases( mObj.getDatabases());
		vObj.setTables( mObj.getTables());
		vObj.setColumnFamilies( mObj.getColumnFamilies());
		vObj.setColumns( mObj.getColumns());
		vObj.setUdfs( mObj.getUdfs());
		vObj.setResourceStatus( mObj.getResourceStatus());
		vObj.setTableType( mObj.getTableType());
		vObj.setColumnType( mObj.getColumnType());
		vObj.setPolicyName( mObj.getPolicyName());
		vObj.setTopologies( mObj.getTopologies());
		vObj.setServices( mObj.getServices());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXResourceList searchXResources(SearchCriteria searchCriteria) {
		VXResourceList returnList = new VXResourceList();
		List<VXResource> xResourceList = new ArrayList<VXResource>();

		@SuppressWarnings("unchecked")
		List<XXResource> resultList = (List<XXResource>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXResource gjXResource : resultList) {
			@SuppressWarnings("unchecked")
			VXResource vXResource = populateViewBean((T)gjXResource);
			xResourceList.add(vXResource);
		}

		returnList.setVXResources(xResourceList);
		return returnList;
	}

}
