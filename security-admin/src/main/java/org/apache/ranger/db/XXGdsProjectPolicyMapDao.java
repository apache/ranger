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
import org.apache.ranger.entity.XXGdsProjectPolicyMap;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.List;

@Service
public class XXGdsProjectPolicyMapDao extends BaseDao<XXGdsProjectPolicyMap> {
    public XXGdsProjectPolicyMapDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public XXGdsProjectPolicyMap getProjectPolicyMap(Long projectId, Long policyId) {
        XXGdsProjectPolicyMap ret = null;

        if (projectId != null && policyId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsProjectPolicyMap.getProjectPolicyMap", tClass)
                        .setParameter("projectId", projectId)
                        .setParameter("policyId", policyId)
                        .getSingleResult();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }

    public List<XXGdsProjectPolicyMap> getProjectPolicyMaps(Long projectId) {
        List<XXGdsProjectPolicyMap> ret = Collections.emptyList();

        if (projectId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsProjectPolicyMap.getProjectPolicyMaps", tClass)
                        .setParameter("projectId", projectId)
                        .getResultList();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }

    public List<Long> getProjectPolicyIds(Long projectId) {
        List<Long> ret = Collections.emptyList();

        if (projectId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsProjectPolicyMap.getProjectPolicyIds", Long.class)
                        .setParameter("projectId", projectId)
                        .getResultList();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }

    public Long getProjectIdForPolicy(Long policyId) {
        Long ret = null;

        if (policyId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsProjectPolicyMap.getProjectIdForPolicy", Long.class)
                        .setParameter("policyId", policyId)
                        .getSingleResult();
            } catch (NoResultException e) {
                // ignore
            }
        }

        return ret;
    }
}
