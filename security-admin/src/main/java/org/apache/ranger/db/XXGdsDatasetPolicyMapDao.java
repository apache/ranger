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

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXGdsDatasetPolicyMap;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.List;

@Service
public class XXGdsDatasetPolicyMapDao extends BaseDao<XXGdsDatasetPolicyMap> {
    public XXGdsDatasetPolicyMapDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public XXGdsDatasetPolicyMap getDatasetPolicyMap(Long datasetId, Long policyId) {
        XXGdsDatasetPolicyMap ret = null;

        if (datasetId != null && policyId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDatasetPolicyMap.getDatasetPolicyMap", tClass)
                        .setParameter("datasetId", datasetId)
                        .setParameter("policyId", policyId)
                        .getSingleResult();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }

    public List<XXGdsDatasetPolicyMap> getDatasetPolicyMaps(Long datasetId) {
        List<XXGdsDatasetPolicyMap> ret = Collections.emptyList();

        if (datasetId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDatasetPolicyMap.getDatasetPolicyMaps", tClass)
                        .setParameter("datasetId", datasetId)
                        .getResultList();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }

    public List<Long> getDatasetPolicyIds(Long datasetId) {
        List<Long> ret = Collections.emptyList();

        if (datasetId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDatasetPolicyMap.getDatasetPolicyIds", Long.class)
                        .setParameter("datasetId", datasetId)
                        .getResultList();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }

    public Long getDatasetIdForPolicy(Long policyId) {
        Long ret = null;

        if (policyId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDatasetPolicyMap.getDatasetIdForPolicy", Long.class)
                        .setParameter("policyId", policyId)
                        .getSingleResult();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }
}
