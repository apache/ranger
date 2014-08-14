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

import javax.persistence.NoResultException;

import com.xasecure.common.db.BaseDao;
import com.xasecure.entity.XXAuthSession;

public class XXAuthSessionDao extends BaseDao<XXAuthSession> {

    public XXAuthSessionDao( XADaoManagerBase daoManager ) {
		super(daoManager);
    }
    
    @SuppressWarnings("unchecked")
	public List<Object[]> getUserLoggedIn(){
    	return getEntityManager()
    			.createNamedQuery("XXAuthSession.getUserLoggedIn")
    			.getResultList();
    }
	
	public XXAuthSession getAuthSessionBySessionId(String sessionId){
		try{
	    	return (XXAuthSession) getEntityManager()
	    			.createNamedQuery("XXAuthSession.getAuthSessionBySessionId")
	    			.setParameter("sessionId", sessionId)
	    			.getSingleResult();
		} catch(NoResultException ignoreNoResultFound) {
			return null;
		}
	}
}

