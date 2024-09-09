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
import org.apache.ranger.entity.XXPermMap;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.view.VXPermMap;
import org.apache.ranger.view.VXPermMapList;

public abstract class XPermMapServiceBase<T extends XXPermMap, V extends VXPermMap>
		extends AbstractAuditedResourceService<T, V> {
	public static final String NAME = "XPermMap";

	public XPermMapServiceBase() {
		super(AppConstants.CLASS_TYPE_XA_PERM_MAP);

		//	trxLogAttrs.put("groupId", new VTrxLogAttr("groupId", "Group Permission", false));
		//	trxLogAttrs.put("userId", new VTrxLogAttr("userId", "User Permission", false));
		trxLogAttrs.put("permType",  new VTrxLogAttr("permType", "Permission Type", true));
		trxLogAttrs.put("ipAddress", new VTrxLogAttr("ipAddress", "IP Address"));
	}

	@Override
	protected T mapViewToEntityBean(V vObj, T mObj, int OPERATION_CONTEXT) {
		mObj.setPermGroup( vObj.getPermGroup());
		mObj.setResourceId( vObj.getResourceId());
		mObj.setGroupId( vObj.getGroupId());
		mObj.setUserId( vObj.getUserId());
		mObj.setPermFor( vObj.getPermFor());
		mObj.setPermType( vObj.getPermType());
		mObj.setIsRecursive( vObj.getIsRecursive());
		mObj.setIsWildCard( vObj.isIsWildCard());
		mObj.setGrantOrRevoke( vObj.isGrantOrRevoke());
		mObj.setIpAddress( vObj.getIpAddress());
		return mObj;
	}

	@Override
	protected V mapEntityToViewBean(V vObj, T mObj) {
		vObj.setPermGroup( mObj.getPermGroup());
		vObj.setResourceId( mObj.getResourceId());
		vObj.setGroupId( mObj.getGroupId());
		vObj.setUserId( mObj.getUserId());
		vObj.setPermFor( mObj.getPermFor());
		vObj.setPermType( mObj.getPermType());
		vObj.setIsRecursive( mObj.getIsRecursive());
		vObj.setIsWildCard( mObj.isIsWildCard());
		vObj.setGrantOrRevoke( mObj.isGrantOrRevoke());
		vObj.setIpAddress( mObj.getIpAddress());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXPermMapList searchXPermMaps(SearchCriteria searchCriteria) {
		VXPermMapList returnList = new VXPermMapList();
		List<VXPermMap> xPermMapList = new ArrayList<VXPermMap>();

		List<T> resultList = searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (T gjXPermMap : resultList) {
			VXPermMap vXPermMap = populateViewBean(gjXPermMap);
			xPermMapList.add(vXPermMap);
		}

		returnList.setVXPermMaps(xPermMapList);
		return returnList;
	}

	@Override
	public int getParentObjectType(V obj, V oldObj) {
		return obj.getGroupId() != null ? AppConstants.CLASS_TYPE_XA_GROUP : AppConstants.CLASS_TYPE_XA_USER;
	}

	@Override
	public String getParentObjectName(V obj, V oldObj) {
		String ret;

		if (obj.getGroupId() != null) {
			XXGroup xGroup = daoManager.getXXGroup().getById(obj.getGroupId());

			ret = xGroup != null ? xGroup.getName() : null;
		} else if (obj.getUserId() != null) {
			XXUser xUser = daoManager.getXXUser().getById(obj.getUserId());

			ret = xUser != null ? xUser.getName() : null;
		} else {
			ret = null;
		}

		return ret;
	}

	@Override
	public Long getParentObjectId(V obj, V oldObj) {
		return obj.getGroupId() != null ? obj.getGroupId() : obj.getUserId();
	}
}
