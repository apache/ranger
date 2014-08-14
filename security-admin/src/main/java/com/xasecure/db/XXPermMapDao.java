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

import javax.persistence.NoResultException;

import org.apache.log4j.Logger;

import com.xasecure.entity.XXPermMap;
import com.xasecure.entity.XXResource;

import com.xasecure.common.*;
import com.xasecure.common.db.*;
import com.xasecure.entity.*;

public class XXPermMapDao extends BaseDao<XXPermMap> {
	static final Logger logger = Logger.getLogger(XXResourceDao.class);

    public XXPermMapDao( XADaoManagerBase daoManager ) {
		super(daoManager);
    }

	public List<XXPermMap> findByResourceId(Long resourceId) {
		if (resourceId != null) {
			try {
				return getEntityManager()
						.createNamedQuery("XXPermMap.findByResourceId", XXPermMap.class)
						.setParameter("resourceId", resourceId)
						.getResultList();
			} catch (NoResultException e) {
				logger.debug(e.getMessage());
			}
		} else {
			logger.debug("ResourceId not provided.");
			return new ArrayList<XXPermMap>();
		}
		return null;
	}
}

