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

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.biz.RangerPolicyRetriever;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyRefUser;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class XXPolicyRefUserDao extends BaseDao<XXPolicyRefUser> {
    public XXPolicyRefUserDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXPolicyRefUser> findByPolicyId(Long policyId) {
        if (policyId == null) {
            return Collections.emptyList();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicyRefUser.findByPolicyId", tClass)
                    .setParameter("policyId", policyId).getResultList();
        } catch (NoResultException e) {
            return Collections.emptyList();
        }
    }

    public List<XXPolicyRefUser> findByUserName(String userName) {
        if (userName == null) {
            return Collections.emptyList();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicyRefUser.findByUserName", tClass)
                    .setParameter("userName", userName).getResultList();
        } catch (NoResultException e) {
            return Collections.emptyList();
        }
    }

    public List<XXPolicyRefUser> findByUserId(String userID) {
        if (userID == null) {
            return Collections.emptyList();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicyRefUser.findByUserId", tClass)
                    .setParameter("userID", userID).getResultList();
        } catch (NoResultException e) {
            return Collections.emptyList();
        }
    }

    public List<RangerPolicyRetriever.PolicyTextNameMap> findUpdatedUserNamesByPolicy(Long policyId) {
        List<RangerPolicyRetriever.PolicyTextNameMap> ret = new ArrayList<>();

        if (policyId != null) {
            List<Object[]> rows = getEntityManager()
                    .createNamedQuery("XXPolicyRefUser.findUpdatedUserNamesByPolicy", Object[].class)
                    .setParameter("policy", policyId)
                    .getResultList();

            if (rows != null) {
                for (Object[] row : rows) {
                    ret.add(new RangerPolicyRetriever.PolicyTextNameMap((Long) row[0], (String) row[1], (String) row[2]));
                }
            }
        }

        return ret;
    }

    public List<RangerPolicyRetriever.PolicyTextNameMap> findUpdatedUserNamesByService(Long serviceId) {
        List<RangerPolicyRetriever.PolicyTextNameMap> ret = new ArrayList<>();

        if (serviceId != null) {
            List<Object[]> rows = getEntityManager()
                    .createNamedQuery("XXPolicyRefUser.findUpdatedUserNamesByService", Object[].class)
                    .setParameter("service", serviceId)
                    .getResultList();

            if (rows != null) {
                for (Object[] row : rows) {
                    ret.add(new RangerPolicyRetriever.PolicyTextNameMap((Long) row[0], (String) row[1], (String) row[2]));
                }
            }
        }

        return ret;
    }

    public void deleteByPolicyId(Long policyId) {
        if (policyId == null) {
            return;
        }

        // First select ids according to policyId, then delete records according to ids
        // The purpose of dividing the delete sql into these two steps is to avoid deadlocks at rr isolation level
        List<Long> ids = getEntityManager()
                .createNamedQuery("XXPolicyRefUser.findIdsByPolicyId", Long.class)
                .setParameter("policyId", policyId)
                .getResultList();

        if (CollectionUtils.isEmpty(ids)) {
            return;
        }

        batchDeleteByIds("XXPolicyRefUser.deleteByIds", ids, "ids");
    }
}
