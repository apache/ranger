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
import org.apache.ranger.entity.XXResourceDef;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.List;

@Service
public class XXResourceDefDao extends BaseDao<XXResourceDef> {
    public XXResourceDefDao(RangerDaoManagerBase daoMgr) {
        super(daoMgr);
    }

    public XXResourceDef findByNameAndServiceDefId(String name, Long defId) {
        if (name == null || defId == null) {
            return null;
        }

        try {
            return getEntityManager().createNamedQuery("XXResourceDef.findByNameAndDefId", tClass)
                    .setParameter("name", name).setParameter("defId", defId)
                    .getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public List<XXResourceDef> findByServiceDefId(Long serviceDefId) {
        if (serviceDefId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXResourceDef.findByServiceDefId", tClass)
                    .setParameter("serviceDefId", serviceDefId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXResourceDef> findByPolicyId(Long policyId) {
        if (policyId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXResourceDef.findByPolicyId", tClass)
                    .setParameter("policyId", policyId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public XXResourceDef findByNameAndPolicyId(String name, Long policyId) {
        if (policyId == null || name == null) {
            return null;
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXResourceDef.findByNameAndPolicyId", tClass)
                    .setParameter("policyId", policyId)
                    .setParameter("name", name).getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public List<XXResourceDef> findByParentResId(Long parentId) {
        if (parentId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXResourceDef.findByParentResId", tClass)
                    .setParameter("parentId", parentId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }
}
