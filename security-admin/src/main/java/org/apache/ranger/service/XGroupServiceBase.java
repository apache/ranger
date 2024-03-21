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
import org.apache.ranger.plugin.model.GroupInfo;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXGroupList;

import javax.persistence.Query;

public abstract class XGroupServiceBase<T extends XXGroup, V extends VXGroup>
		extends AbstractAuditedResourceService<T, V> {
	public static final String NAME = "XGroup";

	public XGroupServiceBase() {
		super(AppConstants.CLASS_TYPE_XA_GROUP);

		trxLogAttrs.put("name",            new VTrxLogAttr("name", "Group Name", false, true));
		trxLogAttrs.put("description",     new VTrxLogAttr("description", "Group Description"));
		trxLogAttrs.put("otherAttributes", new VTrxLogAttr("otherAttributes", "Other Attributes"));
		trxLogAttrs.put("syncSource",      new VTrxLogAttr("syncSource", "Sync Source"));
	}

	@Override
	protected T mapViewToEntityBean(V vObj, T mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setIsVisible(vObj.getIsVisible());
		mObj.setDescription( vObj.getDescription());
		mObj.setGroupType( vObj.getGroupType());
		mObj.setCredStoreId( vObj.getCredStoreId());
		mObj.setGroupSource(vObj.getGroupSource());
		mObj.setOtherAttributes(vObj.getOtherAttributes());
		mObj.setSyncSource(vObj.getSyncSource());
		return mObj;
	}

	@Override
	protected V mapEntityToViewBean(V vObj, T mObj) {
		vObj.setName( mObj.getName());
		vObj.setIsVisible( mObj.getIsVisible());
		vObj.setDescription( mObj.getDescription());
		vObj.setGroupType( mObj.getGroupType());
		vObj.setCredStoreId( mObj.getCredStoreId());
		vObj.setGroupSource(mObj.getGroupSource());
		vObj.setOtherAttributes(mObj.getOtherAttributes());
		vObj.setSyncSource(mObj.getSyncSource());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXGroupList searchXGroups(SearchCriteria searchCriteria) {
		VXGroupList returnList   = new VXGroupList();
		List<VXGroup> xGroupList = new ArrayList<VXGroup>();
		List<T> resultList       = searchResources(searchCriteria, searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (T gjXGroup : resultList) {
			VXGroup vXGroup = populateViewBean(gjXGroup);
			xGroupList.add(vXGroup);
		}

		returnList.setVXGroups(xGroupList);
		return returnList;
	}

	/**
	 * Searches the XGroup table and gets the group ids matching the search criteria.
	 */
	public List<Long> searchXGroupsForIds(SearchCriteria searchCriteria){
		// construct the sort clause
		String sortClause = searchUtil.constructSortClause(searchCriteria, sortFields);

		// get only the column id from the table
		String q = "SELECT obj.id FROM " + className + " obj ";

		// construct the query object for retrieving the data
		Query query = createQuery(q, sortClause, searchCriteria, searchFields, false);

		return getDao().getIds(query);
	}

	public List<GroupInfo> getGroups() {
		return daoManager.getXXGroup().getAllGroupsInfo();
	}
}
