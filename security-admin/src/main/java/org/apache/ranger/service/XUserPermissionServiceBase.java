/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.entity.XXUserPermission;
import org.apache.ranger.view.VXUserPermission;
import org.apache.ranger.view.VXUserPermissionList;

public abstract class XUserPermissionServiceBase<T extends XXUserPermission, V extends VXUserPermission>
		extends AbstractBaseResourceService<T, V> {

	public static final String NAME = "XUserPermission";

	public XUserPermissionServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXUserPermission mapViewToEntityBean(VXUserPermission vObj,
			XXUserPermission mObj, int OPERATION_CONTEXT) {
		mObj.setUserId(vObj.getUserId());
		mObj.setModuleId(vObj.getModuleId());
		mObj.setIsAllowed(vObj.getIsAllowed());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXUserPermission mapEntityToViewBean(VXUserPermission vObj, XXUserPermission mObj) {
		vObj.setUserId(mObj.getUserId());
		vObj.setModuleId(mObj.getModuleId());
		vObj.setIsAllowed(mObj.getIsAllowed());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXUserPermissionList searchXUserPermission(SearchCriteria searchCriteria) {
		VXUserPermissionList returnList = new VXUserPermissionList();
		List<VXUserPermission> vXUserPermissions = new ArrayList<VXUserPermission>();

		@SuppressWarnings("unchecked")
		List<XXUserPermission> resultList = (List<XXUserPermission>) searchResources(
				searchCriteria, searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXUserPermission gjXUser : resultList) {
			@SuppressWarnings("unchecked")
			VXUserPermission vXUserPermission = populateViewBean((T) gjXUser);
			vXUserPermissions.add(vXUserPermission);
		}

		returnList.setvXModuleDef(vXUserPermissions);
		return returnList;
	}
}