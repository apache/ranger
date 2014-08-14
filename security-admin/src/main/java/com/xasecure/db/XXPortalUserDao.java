package com.xasecure.db;

/*
 * Copyright (c) 2014 XASecure.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure.
 */

import java.util.List;

import com.xasecure.entity.XXPortalUser;
import com.xasecure.common.db.*;

public class XXPortalUserDao extends BaseDao<XXPortalUser> {

	public XXPortalUserDao(XADaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXPortalUser findByLoginId(String loginId) {
		if (daoManager.getStringUtil().isEmpty(loginId)) {
			return null;
		}

		@SuppressWarnings("rawtypes")
		List resultList = getEntityManager()
				.createNamedQuery("XXPortalUser.findByLoginId")
				.setParameter("loginId", loginId.toLowerCase()).getResultList();
		if (resultList.size() != 0) {
			return (XXPortalUser) resultList.get(0);
		}
		return null;
	}

	public XXPortalUser findByEmailAddress(String emailAddress) {
		if (daoManager.getStringUtil().isEmpty(emailAddress)) {
			return null;
		}

		@SuppressWarnings("rawtypes")
		List resultList = getEntityManager()
				.createNamedQuery("XXPortalUser.findByEmailAddress")
				.setParameter("emailAddress", emailAddress.toLowerCase())
				.getResultList();
		if (resultList.size() != 0) {
			return (XXPortalUser) resultList.get(0);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public List<XXPortalUser> findByRole(String userRole) {
		return getEntityManager().createNamedQuery("XXPortalUser.findByRole")
				.setParameter("userRole", userRole.toUpperCase())
				.getResultList();
	}
    
    @SuppressWarnings("unchecked")
	public List<Object[]> getUserAddedReport(){
    	return getEntityManager()
    			.createNamedQuery("XXPortalUser.getUserAddedReport")
    			.getResultList();
    }

}
