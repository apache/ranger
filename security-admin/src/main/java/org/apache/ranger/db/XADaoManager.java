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



import javax.persistence.*;

import org.apache.log4j.Logger;
import org.apache.ranger.common.*;
import org.apache.ranger.common.db.BaseDao;
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
