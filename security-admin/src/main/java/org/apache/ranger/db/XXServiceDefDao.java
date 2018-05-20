/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.db;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXServiceDef;
import org.springframework.stereotype.Service;

@Service
public class XXServiceDefDao extends BaseDao<XXServiceDef> {
	/**
	 * Default Constructor
	 */
	public XXServiceDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXServiceDef findByName(String name) {
		if (name == null) {
			return null;
		}
		try {
			XXServiceDef xServiceDef = getEntityManager()
					.createNamedQuery("XXServiceDef.findByName", tClass)
					.setParameter("name", name).getSingleResult();
			return xServiceDef;
		} catch (NoResultException e) {
			return null;
		}
	}

	public Long getMaxIdOfXXServiceDef() {
		try {
			return (Long) getEntityManager().createNamedQuery("XXServiceDef.getMaxIdOfXXServiceDef").getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	public void updateSequence() {
		Long maxId = getMaxIdOfXXServiceDef();

		if(maxId == null) {
			return;
		}

		updateSequence("X_SERVICE_DEF_SEQ", maxId + 1);
	}

}