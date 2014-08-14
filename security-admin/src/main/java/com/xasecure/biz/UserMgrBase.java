package com.xasecure.biz;
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

import com.xasecure.common.*;
import com.xasecure.service.*;
import com.xasecure.view.*;
import org.springframework.beans.factory.annotation.Autowired;
public class UserMgrBase {

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	XPortalUserService xPortalUserService;
	public VXPortalUser getXPortalUser(Long id){
		return (VXPortalUser)xPortalUserService.readResource(id);
	}

	public VXPortalUser createXPortalUser(VXPortalUser vXPortalUser){
		vXPortalUser =  (VXPortalUser)xPortalUserService.createResource(vXPortalUser);
		return vXPortalUser;
	}

	public VXPortalUser updateXPortalUser(VXPortalUser vXPortalUser) {
		vXPortalUser =  (VXPortalUser)xPortalUserService.updateResource(vXPortalUser);
		return vXPortalUser;
	}

	public void deleteXPortalUser(Long id, boolean force) {
		 if (force) {
			 xPortalUserService.deleteResource(id);
		 } else {
			 throw restErrorUtil.createRESTException(
				"serverMsg.modelMgrBaseDeleteModel",
				MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		 }
	}

	public VXPortalUserList searchXPortalUsers(SearchCriteria searchCriteria) {
		return xPortalUserService.searchXPortalUsers(searchCriteria);
	}

	public VXLong getXPortalUserSearchCount(SearchCriteria searchCriteria) {
		return xPortalUserService.getSearchCount(searchCriteria,
				xPortalUserService.searchFields);
	}

}
