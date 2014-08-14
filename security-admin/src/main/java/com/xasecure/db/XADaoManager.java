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


import javax.persistence.*;

import com.xasecure.common.*;
import org.apache.log4j.Logger;

import com.xasecure.common.db.BaseDao;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

@Component
public class XADaoManager extends XADaoManagerBase {
	final static Logger logger = Logger.getLogger(XADaoManager.class);

	@PersistenceContext(unitName = "defaultPU")
	private EntityManager em;

	@PersistenceContext(unitName = "loggingPU")
	private EntityManager loggingEM;

	@Autowired
	StringUtil stringUtil;

	@Override
	public EntityManager getEntityManager() {
		return em;
	}

	public EntityManager getEntityManager(String persistenceContextUnit) {
		logger.error("XADaoManager.getEntityManager(" + persistenceContextUnit + ")");
		if (persistenceContextUnit.equalsIgnoreCase("loggingPU")) {
			return loggingEM;
		}
		return getEntityManager();
	}

	
	/**
	 * @return the stringUtil
	 */
	public StringUtil getStringUtil() {
		return stringUtil;
	}

	/*
	 * (non-Javadoc)
	 */
	@Override
	public BaseDao<?> getDaoForClassType(int classType) {
		if (classType == XAConstants.CLASS_TYPE_NONE) {
			return null;
		}
		return super.getDaoForClassType(classType);
	}

}
