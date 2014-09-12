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

 package com.xasecure.db;

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

