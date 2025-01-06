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

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyResource;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.List;

@Service
public class XXPolicyResourceDao extends BaseDao<XXPolicyResource> {
    public XXPolicyResourceDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXPolicyResource> findByPolicyId(Long policyId) {
        if (policyId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicyResource.findByPolicyId", tClass)
                    .setParameter("policyId", policyId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXPolicyResource> findByServiceId(Long serviceId) {
        if (serviceId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicyResource.findByServiceId", tClass)
                    .setParameter("serviceId", serviceId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXPolicyResource> findByResDefId(Long resDefId) {
        if (resDefId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXPolicyResource.findByResDefId", tClass)
                    .setParameter("resDefId", resDefId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public void deleteByPolicyId(Long policyId) {
        if (policyId == null) {
            return;
        }
        getEntityManager()
                .createNamedQuery("XXPolicyResource.deleteByPolicyId", tClass)
                .setParameter("policyId", policyId).executeUpdate();
    }
}
