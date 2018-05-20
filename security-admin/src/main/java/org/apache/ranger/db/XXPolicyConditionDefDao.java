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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.springframework.stereotype.Service;

@Service
public class XXPolicyConditionDefDao extends BaseDao<XXPolicyConditionDef> {

	public XXPolicyConditionDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXPolicyConditionDef> findByServiceDefId(Long serviceDefId) {
		if (serviceDefId == null) {
			return new ArrayList<XXPolicyConditionDef>();
		}
		try {
			List<XXPolicyConditionDef> retList = getEntityManager()
					.createNamedQuery("XXPolicyConditionDef.findByServiceDefId", tClass)
					.setParameter("serviceDefId", serviceDefId).getResultList();
			return retList;
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyConditionDef>();
		}
	}

	public XXPolicyConditionDef findByServiceDefIdAndName(Long serviceDefId, String name) {
		if (serviceDefId == null) {
			return null;
		}
		try {
			XXPolicyConditionDef retList = getEntityManager()
					.createNamedQuery("XXPolicyConditionDef.findByServiceDefIdAndName", tClass)
					.setParameter("serviceDefId", serviceDefId)
					.setParameter("name", name).getSingleResult();
			return retList;
		} catch (NoResultException e) {
			return null;
		}
	}

	public List<XXPolicyConditionDef> findByPolicyItemId(Long polItemId) {
		if(polItemId == null) {
			return new ArrayList<XXPolicyConditionDef>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyConditionDef.findByPolicyItemId", tClass)
					.setParameter("polItemId", polItemId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyConditionDef>();
		}
	}
	
	public XXPolicyConditionDef findByPolicyItemIdAndName(Long polItemId, String name) {
		if(polItemId == null || name == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyConditionDef.findByPolicyItemIdAndName", tClass)
					.setParameter("polItemId", polItemId)
					.setParameter("name", name).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	
}
