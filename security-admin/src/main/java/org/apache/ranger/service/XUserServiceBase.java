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
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.UserInfo;
import org.apache.ranger.view.VXUser;
import org.apache.ranger.view.VXUserList;

import javax.persistence.Query;

public abstract class XUserServiceBase<T extends XXUser, V extends VXUser>
		extends AbstractAuditedResourceService<T, V> {
	public static final String NAME = "XUser";

	public XUserServiceBase() {
		super(AppConstants.CLASS_TYPE_XA_USER);

		trxLogAttrs.put("name",            new VTrxLogAttr("name", "Login ID", false, true));
		trxLogAttrs.put("firstName",       new VTrxLogAttr("firstName", "First Name"));
		trxLogAttrs.put("lastName",        new VTrxLogAttr("lastName", "Last Name"));
		trxLogAttrs.put("emailAddress",    new VTrxLogAttr("emailAddress", "Email Address"));
		trxLogAttrs.put("password",        new VTrxLogAttr("password", "Password"));
		trxLogAttrs.put("userRoleList",    new VTrxLogAttr("userRoleList", "User Role"));
		trxLogAttrs.put("otherAttributes", new VTrxLogAttr("otherAttributes", "Other Attributes"));
		trxLogAttrs.put("syncSource",      new VTrxLogAttr("syncSource", "Sync Source"));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXUser mapViewToEntityBean(VXUser vObj, XXUser mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setIsVisible(vObj.getIsVisible());
		mObj.setDescription( vObj.getDescription());
		mObj.setCredStoreId( vObj.getCredStoreId());
		mObj.setOtherAttributes(vObj.getOtherAttributes());
		mObj.setSyncSource(vObj.getSyncSource());
		return mObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected VXUser mapEntityToViewBean(VXUser vObj, XXUser mObj) {
		vObj.setName( mObj.getName());
		vObj.setIsVisible(mObj.getIsVisible());
		vObj.setDescription( mObj.getDescription());
		vObj.setCredStoreId( mObj.getCredStoreId());
		vObj.setOtherAttributes(mObj.getOtherAttributes());
		vObj.setSyncSource(mObj.getSyncSource());
		return vObj;
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXUserList searchXUsers(SearchCriteria searchCriteria) {
		VXUserList returnList   = new VXUserList();
		List<VXUser> xUserList  = new ArrayList<VXUser>();

		@SuppressWarnings("unchecked")
		List<XXUser> resultList = (List<XXUser>)searchResources(searchCriteria, searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXUser gjXUser : resultList) {
			@SuppressWarnings("unchecked")
			VXUser vXUser = populateViewBean((T)gjXUser);
			xUserList.add(vXUser);
		}

		returnList.setVXUsers(xUserList);
		return returnList;
	}

	/**
	 * Searches the XUser table and gets the user ids matching the search criteria.
	 */
	public List<Long> searchXUsersForIds(SearchCriteria searchCriteria){
		// construct the sort clause
		String sortClause = searchUtil.constructSortClause(searchCriteria, sortFields);

		// get only the column id from the table
		String q = "SELECT obj.id FROM " + className + " obj ";

		// construct the query object for retrieving the data
		Query query = createQuery(q, sortClause, searchCriteria, searchFields, false);

		return getDao().getIds(query);
	}

	public List<UserInfo> getUsers() {
		return daoManager.getXXUser().getAllUsersInfo();
	}
}
