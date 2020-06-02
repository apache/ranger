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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.gson.Gson;

import com.google.gson.GsonBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.UserInfo;
import org.apache.ranger.view.VXUser;
import org.apache.ranger.view.VXUserList;

import static java.util.stream.Collectors.toMap;

public abstract class XUserServiceBase<T extends XXUser, V extends VXUser>
		extends AbstractBaseResourceService<T, V> {
	public static final String NAME = "XUser";
	private static final Gson gsonBuilder = new GsonBuilder().create();

	public XUserServiceBase() {

	}

	@SuppressWarnings("unchecked")
	@Override
	protected XXUser mapViewToEntityBean(VXUser vObj, XXUser mObj, int OPERATION_CONTEXT) {
		mObj.setName( vObj.getName());
		mObj.setIsVisible(vObj.getIsVisible());
		mObj.setDescription( vObj.getDescription());
		mObj.setCredStoreId( vObj.getCredStoreId());
		mObj.setOtherAttributes(vObj.getOtherAttributes());
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
		return vObj;
	}

	protected List<VXUser> mapEntityToViewBeans(Map<VXUser, XXUser> vxUserXXUserMap) {
		List<VXUser> vxUsers = new ArrayList<>();
		if (MapUtils.isNotEmpty(vxUserXXUserMap)) {
			for (Map.Entry<VXUser, XXUser> vxUserXXUserEntry : vxUserXXUserMap.entrySet()) {
				VXUser vObj = vxUserXXUserEntry.getKey();
				XXUser mObj = vxUserXXUserEntry.getValue();
				vObj.setName(mObj.getName());
				vObj.setIsVisible(mObj.getIsVisible());
				vObj.setDescription(mObj.getDescription());
				vObj.setCredStoreId(mObj.getCredStoreId());
				vObj.setOtherAttributes(mObj.getOtherAttributes());
				vxUsers.add(vObj);
			}
		}
		return vxUsers;
	}

	public List<VXUser> populateViewBeans(List<XXUser> resources) {
		List<VXUser> viewBeans = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(resources)) {
			Map<XXUser, VXUser> resourceViewBeanMap = new HashMap<>(resources.size());
			Map<VXUser, XXUser> viewBeanResourceMap = new HashMap<>(resources.size());
			for (XXUser resource : resources) {
				VXUser viewBean = createViewObject();
				viewBean.setCredStoreId(resource.getCredStoreId());
				viewBean.setDescription(resource.getDescription());
				viewBean.setName(resource.getName());
				viewBean.setStatus(resource.getStatus());
				resourceViewBeanMap.put(resource, viewBean);
				viewBeanResourceMap.put(viewBean, resource);
				viewBeans.add(viewBean);
			}
			populateViewBeans(resourceViewBeanMap);
			mapEntityToViewBeans(viewBeanResourceMap);
		}
		return viewBeans;
	}

	protected void populateViewBeans(Map<XXUser, VXUser> resourceViewBeanMap) {
		mapBaseAttributesToViewBeans(resourceViewBeanMap);
	}

	private void mapBaseAttributesToViewBeans(Map<XXUser, VXUser> resourceViewBeanMap) {
		List<XXPortalUser> allXPortalUsers = daoManager.getXXPortalUser().findAllXPortalUser();
		if (MapUtils.isNotEmpty(resourceViewBeanMap) && CollectionUtils.isNotEmpty(allXPortalUsers)) {
			Map<Long, XXPortalUser> idXXPortalUserMap = allXPortalUsers
					.stream()
					.collect(toMap(XXPortalUser::getId, Function.identity()));
			resourceViewBeanMap.forEach((resource, viewBean) -> {
				viewBean.setId(resource.getId());

				// TBD: Need to review this change later
				viewBean.setMObj(resource);
				viewBean.setCreateDate(resource.getCreateTime());
				viewBean.setUpdateDate(resource.getUpdateTime());

				Long ownerId = resource.getAddedByUserId();
				UserSessionBase currentUserSession = ContextUtil
						.getCurrentUserSession();

				if (currentUserSession == null) {
					return;
				}

				if (ownerId != null) {
					XXPortalUser tUser = idXXPortalUserMap.get(
							resource.getAddedByUserId());
					if (tUser != null) {
						if (tUser.getPublicScreenName() != null
								&& !tUser.getPublicScreenName().trim().isEmpty()
								&& !"null".equalsIgnoreCase(tUser.getPublicScreenName().trim())) {
							viewBean.setOwner(tUser.getPublicScreenName());
						} else {
							if (tUser.getFirstName() != null
									&& !tUser.getFirstName().trim().isEmpty()
									&& !"null".equalsIgnoreCase(tUser.getFirstName().trim())) {
								if (tUser.getLastName() != null
										&& !tUser.getLastName().trim().isEmpty()
										&& !"null".equalsIgnoreCase(tUser.getLastName().trim())) {
									viewBean.setOwner(tUser.getFirstName() + " "
											+ tUser.getLastName());
								} else {
									viewBean.setOwner(tUser.getFirstName());
								}
							} else {
								viewBean.setOwner(tUser.getLoginId());
							}
						}
					}
				}
				if (resource.getUpdatedByUserId() != null) {
					XXPortalUser tUser = idXXPortalUserMap.get(
							resource.getUpdatedByUserId());
					if (tUser != null) {
						if (tUser.getPublicScreenName() != null
								&& !tUser.getPublicScreenName().trim().isEmpty()
								&& !"null".equalsIgnoreCase(tUser.getPublicScreenName().trim())) {
							viewBean.setUpdatedBy(tUser.getPublicScreenName());
						} else {
							if (tUser.getFirstName() != null
									&& !tUser.getFirstName().trim().isEmpty()
									&& !"null".equalsIgnoreCase(tUser.getFirstName().trim())) {
								if (tUser.getLastName() != null
										&& !tUser.getLastName().trim().isEmpty()
										&& !"null".equalsIgnoreCase(tUser.getLastName().trim())) {
									viewBean.setUpdatedBy(tUser.getFirstName() + " "
											+ tUser.getLastName());
								} else {
									viewBean.setUpdatedBy(tUser.getFirstName());
								}
							} else {
								viewBean.setUpdatedBy(tUser.getLoginId());
							}
						}
					}
				}
			});
		}
	}

	/**
	 * @param searchCriteria
	 * @return
	 */
	public VXUserList searchXUsers(SearchCriteria searchCriteria) {
		VXUserList returnList = new VXUserList();
		List<VXUser> xUserList = new ArrayList<VXUser>();

		@SuppressWarnings("unchecked")
		List<XXUser> resultList = (List<XXUser>)searchResources(searchCriteria,
				searchFields, sortFields, returnList);

		// Iterate over the result list and create the return list
		for (XXUser gjXUser : resultList) {
			@SuppressWarnings("unchecked")
			VXUser vXUser = populateViewBean((T)gjXUser);
			xUserList.add(vXUser);
		}

		returnList.setVXUsers(xUserList);
		return returnList;
	}

	public List<UserInfo> getUsers() {
		List<UserInfo> returnList = new ArrayList<>();

		@SuppressWarnings("unchecked")
		List<XXUser> resultList = daoManager.getXXUser().getAll();

		// Iterate over the result list and create the return list
		for (XXUser gjXUser : resultList) {
			UserInfo userInfo = new UserInfo(gjXUser.getName(), gjXUser.getDescription(), gsonBuilder.fromJson(gjXUser.getOtherAttributes(), Map.class));
			returnList.add(userInfo);
		}

		return returnList;
	}

}
