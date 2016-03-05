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

 package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;
import javax.persistence.NoResultException;
import org.apache.log4j.Logger;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXAccessAudit;

public class XXAccessAuditDao extends BaseDao<XXAccessAudit> {
	private static Logger logger = Logger.getLogger(XXAccessAuditDao.class);
    public XXAccessAuditDao( RangerDaoManagerBase daoManager ) {
		super(daoManager, "loggingPU");
    }
    public Long getMaxIdOfXXAccessAudit(){
		Long maxXXAccessAuditID=Long.valueOf(0L);
		try {
			maxXXAccessAuditID = (Long) getEntityManager()
					.createNamedQuery("XXAccessAudit.getMaxIdOfXXAccessAudit", Long.class)
					.getSingleResult();
		} catch (NoResultException e) {
			logger.debug(e.getMessage());
		}finally{
			if(maxXXAccessAuditID==null){
				maxXXAccessAuditID=Long.valueOf(0L);
			}
		}
		return maxXXAccessAuditID;
	}
	public List<XXAccessAudit> getByIdRange(long idFrom,long idTo){
		//idFrom and idTo both exclusive
		List<XXAccessAudit> xXAccessAuditList = new ArrayList<XXAccessAudit>();
		try {
			xXAccessAuditList= getEntityManager().createNamedQuery("XXAccessAudit.getByIdRange", tClass)
				.setParameter("idFrom", idFrom)
				.setParameter("idTo", idTo)
				.getResultList();
		} catch (NoResultException e) {
			logger.debug(e.getMessage());
		}
		return xXAccessAuditList;
	}
}

