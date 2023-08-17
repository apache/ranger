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
import java.util.Collections;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.biz.RangerPolicyRetriever;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyRefRole;
import org.springframework.stereotype.Service;

@Service
public class XXPolicyRefRoleDao extends BaseDao<XXPolicyRefRole>{


    public XXPolicyRefRoleDao(RangerDaoManagerBase daoManager)  {
        super(daoManager);
    }

    public List<XXPolicyRefRole> findByPolicyId(Long policyId) {
        if(policyId == null) {
            return Collections.EMPTY_LIST;
        }
        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicyRefRole.findByPolicyId", tClass)
                    .setParameter("policyId", policyId).getResultList();
        } catch (NoResultException e) {
            return Collections.EMPTY_LIST;
        }
    }
    public List<XXPolicyRefRole> findByRoleName(String roleName) {
        if (roleName == null) {
            return Collections.EMPTY_LIST;
        }
        try {
            return getEntityManager().createNamedQuery("XXPolicyRefRole.findByRoleName", tClass)
                    .setParameter("roleName", roleName).getResultList();
        } catch (NoResultException e) {
            return Collections.EMPTY_LIST;
        }
    }

    @SuppressWarnings("unchecked")
    public List<RangerPolicyRetriever.PolicyTextNameMap> findUpdatedRoleNamesByPolicy(Long policyId) {
        List<RangerPolicyRetriever.PolicyTextNameMap> ret = new ArrayList<>();
        if (policyId != null) {
            List<Object[]> rows = (List<Object[]>) getEntityManager()
                    .createNamedQuery("XXPolicyRefRole.findUpdatedRoleNamesByPolicy")
                    .setParameter("policy", policyId)
                    .getResultList();
            if (rows != null) {
                for (Object[] row : rows) {
                    ret.add(new RangerPolicyRetriever.PolicyTextNameMap((Long)row[0], (String)row[1], (String)row[2]));
                }
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public List<RangerPolicyRetriever.PolicyTextNameMap> findUpdatedRoleNamesByService(Long serviceId) {
        List<RangerPolicyRetriever.PolicyTextNameMap> ret = new ArrayList<>();
        if (serviceId != null) {
            List<Object[]> rows = (List<Object[]>) getEntityManager()
                    .createNamedQuery("XXPolicyRefRole.findUpdatedRoleNamesByService")
                    .setParameter("service", serviceId)
                    .getResultList();
            if (rows != null) {
                for (Object[] row : rows) {
                    ret.add(new RangerPolicyRetriever.PolicyTextNameMap((Long)row[0], (String)row[1], (String)row[2]));
                }
            }
        }
        return ret;
    }

	public Long findRoleRefPolicyCount(String roleName) {
		Long ret = -1L;

		try {
			ret = getEntityManager().createNamedQuery("XXPolicyRefRole.findRoleRefPolicyCount", Long.class)
					.setParameter("roleName", roleName).getSingleResult();
		} catch (Exception e) {
		}

		return ret;
	}

	public void deleteByPolicyId(Long policyId) {
		if(policyId == null) {
			return;
		}

        // First select ids according to policyId, then delete records according to ids
        // The purpose of dividing the delete sql into these two steps is to avoid deadlocks at rr isolation level
        List<Long> ids = getEntityManager()
                .createNamedQuery("XXPolicyRefRole.findIdsByPolicyId", Long.class)
                .setParameter("policyId", policyId)
                .getResultList();

        if (CollectionUtils.isEmpty(ids)) {
            return;
        }

        batchDeleteByIds("XXPolicyRefRole.deleteByIds", ids, "ids");
	}
}

