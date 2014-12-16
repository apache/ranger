/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 package org.apache.ranger.service;

/**
 * 
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXAssetList;

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
