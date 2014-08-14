package com.xasecure.db;

/*
 * Copyright (c) 2014 XASecure
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



import com.xasecure.entity.XXGroup;

import com.xasecure.common.*;
import com.xasecure.common.db.*;
import com.xasecure.entity.*;

public class XXGroupDao extends BaseDao<XXGroup> {

	public XXGroupDao(XADaoManagerBase daoManager) {
		super(daoManager);
	}

	@SuppressWarnings("unchecked")
	public List<XXGroup> findByUserId(Long userId) {
		if (userId == null) {
			return new ArrayList<XXGroup>();
		}

		List<XXGroup> groupList = (List<XXGroup>) getEntityManager()
				.createNamedQuery("XXGroup.findByUserId")
				.setParameter("userId", userId).getResultList();

		if (groupList == null) {
			groupList = new ArrayList<XXGroup>();
		}

		return groupList;
	}

	@SuppressWarnings("unchecked")
	public XXGroup findByGroupName(String groupName) {
		if (groupName == null) {
			return null;
		}
		try {

			return (XXGroup) getEntityManager()
					.createNamedQuery("XXGroup.findByGroupName")
					.setParameter("name", groupName.toLowerCase())
					.getSingleResult();
		} catch (Exception e) {

		}
		return null;
	}

}
