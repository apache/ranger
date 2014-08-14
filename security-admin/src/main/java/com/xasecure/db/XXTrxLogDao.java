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

import com.xasecure.entity.XXTrxLog;

import com.xasecure.common.db.*;

public class XXTrxLogDao extends BaseDao<XXTrxLog> {
	private static Logger logger = Logger.getLogger(XXTrxLogDao.class);
	
    public XXTrxLogDao( XADaoManagerBase daoManager ) {
		super(daoManager);
    }
    
    public List<XXTrxLog> findByTransactionId(String transactionId){
    	if(transactionId == null){
    		return null;
    	}
    	
		List<XXTrxLog> xTrxLogList = new ArrayList<XXTrxLog>();
		try {
			xTrxLogList = getEntityManager()
					.createNamedQuery("XXTrxLog.findByTrxId", XXTrxLog.class)
					.setParameter("transactionId", transactionId)
					.getResultList();
		} catch (NoResultException e) {
			logger.debug(e.getMessage());
		}
		
		return xTrxLogList;
	}
}

