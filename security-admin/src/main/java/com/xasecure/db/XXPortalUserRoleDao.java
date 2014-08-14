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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import com.xasecure.entity.XXPortalUserRole;
import com.xasecure.common.db.*;

public class XXPortalUserRoleDao extends BaseDao<XXPortalUserRole> {

	public XXPortalUserRoleDao(XADaoManagerBase daoManager) {
		super(daoManager);
	}

	@SuppressWarnings("unchecked")
	public List<XXPortalUserRole> findByUserId(Long userId) {
		if (userId == null) {
			return new ArrayList<XXPortalUserRole>();
		}
		return getEntityManager().createNamedQuery("XXPortalUserRole.findByUserId")
				.setParameter("userId", userId).getResultList();
	}
	
	public XXPortalUserRole findByRoleUserId(Long userId, String role) {
		if(userId == null || role == null || role.isEmpty()){
			return null;
		}
		try{
			return (XXPortalUserRole)getEntityManager().createNamedQuery("XXPortalUserRole.findByRoleUserId")
					.setParameter("userId", userId)
					.setParameter("userRole", role).getSingleResult();
		} catch(NoResultException e){
			//doNothing;
		}
		return null;
	}
}
