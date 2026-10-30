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
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyRefConstraint;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.List;

@Service
public class XXPolicyRefConstraintDao extends BaseDao<XXPolicyRefConstraint> {
    public XXPolicyRefConstraintDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXPolicyRefConstraint> findByPolicyId(Long policyId) {
        if (policyId == null) {
            return Collections.emptyList();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicyRefConstraint.findByPolicyId", tClass)
                    .setParameter("policyId", policyId)
                    .getResultList();
        } catch (NoResultException e) {
            return Collections.emptyList();
        }
    }

    /**
     * IDs of policies of the given service-def that use the given constraint
     */
    public List<Long> findPolicyIdsByServiceDefIdAndConstraintName(Long serviceDefId, String constraintName) {
        if (serviceDefId == null || constraintName == null) {
            return Collections.emptyList();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicyRefConstraint.findPolicyIdsByServiceDefIdAndConstraintName", Long.class)
                    .setParameter("serviceDefId", serviceDefId)
                    .setParameter("constraintName", constraintName)
                    .getResultList();
        } catch (NoResultException e) {
            return Collections.emptyList();
        }
    }

    public void deleteByPolicyId(Long policyId) {
        if (policyId == null) {
            return;
        }

        // First select ids according to policyId, then delete records according to ids
        // The purpose of dividing the delete sql into these two steps is to avoid deadlocks at rr isolation level
        List<Long> ids = getEntityManager()
                .createNamedQuery("XXPolicyRefConstraint.findIdsByPolicyId", Long.class)
                .setParameter("policyId", policyId)
                .getResultList();

        if (CollectionUtils.isEmpty(ids)) {
            return;
        }

        batchDeleteByIds("XXPolicyRefConstraint.deleteByIds", ids, "ids");
    }
}
