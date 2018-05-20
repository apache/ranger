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
import org.apache.ranger.entity.XXPolicy;
import org.springframework.stereotype.Service;

/**
 */

@Service
public class XXPolicyDao extends BaseDao<XXPolicy> {
	/**
	 * Default Constructor
	 */
	public XXPolicyDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXPolicy findByNameAndServiceId(String polName, Long serviceId) {
		if (polName == null || serviceId == null) {
			return null;
		}
		try {
			XXPolicy xPol = getEntityManager()
					.createNamedQuery("XXPolicy.findByNameAndServiceId", tClass)
					.setParameter("polName", polName).setParameter("serviceId", serviceId)
					.getSingleResult();
			return xPol;
		} catch (NoResultException e) {
			return null;
		}
	}

	public List<XXPolicy> findByServiceId(Long serviceId) {
		if (serviceId == null) {
			return new ArrayList<XXPolicy>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicy.findByServiceId", tClass)
					.setParameter("serviceId", serviceId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicy>();
		}
	}

	public Long getMaxIdOfXXPolicy() {
		try {
			return (Long) getEntityManager().createNamedQuery("XXPolicy.getMaxIdOfXXPolicy").getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}
	
	public List<XXPolicy> findByResourceSignatureByPolicyStatus(String serviceName, String policySignature, Boolean isPolicyEnabled) {
		if (policySignature == null || serviceName == null || isPolicyEnabled == null) {
			return new ArrayList<XXPolicy>();
		}
		try {
			return getEntityManager().createNamedQuery("XXPolicy.findByResourceSignatureByPolicyStatus", tClass)
					.setParameter("resSignature", policySignature)
					.setParameter("serviceName", serviceName)
					.setParameter("isPolicyEnabled", isPolicyEnabled)
					.getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicy>();
		}
	}

	public List<XXPolicy> findByResourceSignature(String serviceName, String policySignature) {
		if (policySignature == null || serviceName == null) {
			return new ArrayList<XXPolicy>();
		}
		try {
			return getEntityManager().createNamedQuery("XXPolicy.findByResourceSignature", tClass)
					.setParameter("resSignature", policySignature)
					.setParameter("serviceName", serviceName)
					.getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicy>();
		}
	}

	public List<XXPolicy> findByServiceDefId(Long serviceDefId) {
		if(serviceDefId == null) {
			return new ArrayList<XXPolicy>();
		}
		try {
			return getEntityManager().createNamedQuery("XXPolicy.findByServiceDefId", tClass)
					.setParameter("serviceDefId", serviceDefId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicy>();
		}
	}

	public void updateSequence() {
		Long maxId = getMaxIdOfXXPolicy();

		if(maxId == null) {
			return;
		}

		updateSequence("X_POLICY_SEQ", maxId + 1);
	}
	public List<XXPolicy> findByUserId(Long userId) {
		if(userId == null || userId.equals(Long.valueOf(0L))) {
			return new ArrayList<XXPolicy>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicy.findByUserId", tClass)
					.setParameter("userId", userId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicy>();
		}
	}
	public List<XXPolicy> findByGroupId(Long groupId) {
		if(groupId == null || groupId.equals(Long.valueOf(0L))) {
			return new ArrayList<XXPolicy>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicy.findByGroupId", tClass)
					.setParameter("groupId", groupId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicy>();
		}
	}
}