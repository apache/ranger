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

import com.xasecure.entity.XXAsset;

import com.xasecure.common.XACommonEnums;
import com.xasecure.common.db.*;

public class XXAssetDao extends BaseDao<XXAsset> {
	static final Logger logger = Logger.getLogger(XXAssetDao.class);

    public XXAssetDao( XADaoManagerBase  daoManager ) {
		super(daoManager);
    }
    
    public XXAsset findByAssetName(String name){
		if (daoManager.getStringUtil().isEmpty(name)) {
			logger.debug("name is empty");
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXAsset.findByAssetName", XXAsset.class)
					.setParameter("name", name.trim())
					.setParameter("status",XACommonEnums.STATUS_DELETED)
					.getSingleResult();
		} catch (NoResultException e) {
			// ignore
		}
		return null;
    }
    
}

