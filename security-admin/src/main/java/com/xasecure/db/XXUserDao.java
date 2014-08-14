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

import javax.persistence.NoResultException;

import org.apache.log4j.Logger;

import com.xasecure.entity.XXUser;

import com.xasecure.common.db.*;

public class XXUserDao extends BaseDao<XXUser> {
	static final Logger logger = Logger.getLogger(XXResourceDao.class);

	public XXUserDao(XADaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXUser findByUserName(String name) {
		if (daoManager.getStringUtil().isEmpty(name)) {
			logger.debug("name is empty");
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXUser.findByUserName", XXUser.class)
					.setParameter("name", name.trim().toLowerCase())
					.getSingleResult();
		} catch (NoResultException e) {
			// ignore
		}
		return null;
	}
}
