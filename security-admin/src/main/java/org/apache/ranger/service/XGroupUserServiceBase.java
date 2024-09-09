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

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXGroupUser;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.view.VXGroupUser;
import org.apache.ranger.view.VXGroupUserList;

public abstract class XGroupUserServiceBase<T extends XXGroupUser, V extends VXGroupUser>
		extends AbstractAuditedResourceService<T, V> {
	public static final String NAME = "XGroupUser";

	public XGroupUserServiceBase() {
		super(AppConstants.CLASS_TYPE_XA_GROUP_USER, AppConstants.CLASS_TYPE_XA_GROUP);

		trxLogAttrs.put("name", new VTrxLogAttr("name", "Group Name"));
	}

	@Override
	protected T mapViewToEntityBean(V vObj, T mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setParentGroupId( vObj.getParentGroupId());
		mObj.setUserId( vObj.getUserId());
		return mObj;
	}

	@Override
	protected V mapEntityToViewBean(V vObj, T mObj) {
		vObj.setName( mObj.getName());
		vObj.setParentGroupId( mObj.getParentGroupId());
		vObj.setUserId( mObj.getUserId());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXGroupUserList searchXGroupUsers(SearchCriteria searchCriteria) {
		VXGroupUserList returnList = new VXGroupUserList();
		List<VXGroupUser> xGroupUserList = new ArrayList<VXGroupUser>();

		List<T> resultList = searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (T gjXGroupUser : resultList) {
			VXGroupUser vXGroupUser = populateViewBean(gjXGroupUser);
			xGroupUserList.add(vXGroupUser);
		}

		returnList.setVXGroupUsers(xGroupUserList);
		return returnList;
	}

	@Override
	public String getObjectName(V obj) {
		Long   userId = obj != null ? obj.getUserId() : null;
		XXUser user   = userId != null ? daoManager.getXXUser().getById(userId) : null;

		return user != null ? user.getName() : null;
	}

	@Override
	public String getParentObjectName(V obj, V oldObj) {
		Long    groupId = getParentObjectId(obj, oldObj);
		XXGroup group   = groupId != null ? daoManager.getXXGroup().getById(groupId) : null;

		return group != null ? group.getName() : null;
	}

	@Override
	public Long getParentObjectId(V obj, V oldObj) {
		return obj != null ? obj.getParentGroupId() : null;
	}
}
