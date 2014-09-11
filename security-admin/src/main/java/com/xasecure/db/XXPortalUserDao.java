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
