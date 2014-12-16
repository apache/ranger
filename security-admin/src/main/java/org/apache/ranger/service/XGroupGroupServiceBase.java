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
import org.apache.ranger.entity.XXGroupGroup;
import org.apache.ranger.view.VXGroupGroup;
import org.apache.ranger.view.VXGroupGroupList;

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
