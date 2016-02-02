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
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXGroupList;

public abstract class XGroupServiceBase<T extends XXGroup, V extends VXGroup>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XGroup";

	public XGroupServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXGroup mapViewToEntityBean(VXGroup vObj, XXGroup mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setIsVisible(vObj.getIsVisible());
		mObj.setDescription( vObj.getDescription());
		mObj.setGroupType( vObj.getGroupType());
		mObj.setCredStoreId( vObj.getCredStoreId());
		mObj.setGroupSource(vObj.getGroupSource());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXGroup mapEntityToViewBean(VXGroup vObj, XXGroup mObj) {
		vObj.setName( mObj.getName());
		vObj.setIsVisible( mObj.getIsVisible());
		vObj.setDescription( mObj.getDescription());
		vObj.setGroupType( mObj.getGroupType());
		vObj.setCredStoreId( mObj.getCredStoreId());
		vObj.setGroupSource(mObj.getGroupSource());
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
