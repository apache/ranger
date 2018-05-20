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
import org.apache.ranger.entity.XXPolicyItemCondition;
import org.springframework.stereotype.Service;

@Service
public class XXPolicyItemConditionDao extends BaseDao<XXPolicyItemCondition> {

	public XXPolicyItemConditionDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}
	
	public List<XXPolicyItemCondition> findByPolicyItemId(Long polItemId) {
		if(polItemId == null) {
			return new ArrayList<XXPolicyItemCondition>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemCondition.findByPolicyItemId", tClass)
					.setParameter("polItemId", polItemId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemCondition>();
		}
	}

	public List<XXPolicyItemCondition> findByPolicyId(Long policyId) {
		if(policyId == null) {
			return new ArrayList<XXPolicyItemCondition>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemCondition.findByPolicyId", tClass)
					.setParameter("policyId", policyId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemCondition>();
		}
	}

	public List<XXPolicyItemCondition> findByServiceId(Long serviceId) {
		if(serviceId == null) {
			return new ArrayList<XXPolicyItemCondition>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemCondition.findByServiceId", tClass)
					.setParameter("serviceId", serviceId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemCondition>();
		}
	}

	public List<XXPolicyItemCondition> findByPolicyItemAndDefId(Long polItemId,
			Long polCondDefId) {
		if(polItemId == null || polCondDefId == null) {
			return new ArrayList<XXPolicyItemCondition>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemCondition.findByPolicyItemAndDefId", tClass)
					.setParameter("polItemId", polItemId)
					.setParameter("polCondDefId", polCondDefId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemCondition>();
		}
	}

	public List<XXPolicyItemCondition> findByPolicyConditionDefId(Long polCondDefId) {
		if (polCondDefId == null) {
			return new ArrayList<XXPolicyItemCondition>();
		}
		try {
			return getEntityManager().createNamedQuery("XXPolicyItemCondition.findByPolicyConditionDefId", tClass)
					.setParameter("polCondDefId", polCondDefId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemCondition>();
		}
	}

}
