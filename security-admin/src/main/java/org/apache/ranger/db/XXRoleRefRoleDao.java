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
import org.apache.ranger.entity.XXRoleRefRole;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class XXRoleRefRoleDao extends BaseDao<XXRoleRefRole> {
    public XXRoleRefRoleDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXRoleRefRole> findByRoleId(Long roleId) {
        if (roleId == null) {
            return Collections.emptyList();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXRoleRefRole.findByRoleId", tClass)
                    .setParameter("roleId", roleId).getResultList();
        } catch (NoResultException e) {
            return Collections.emptyList();
        }
    }

    public List<Long> findIdsByRoleId(Long roleId) {
        List<Long> ret = Collections.emptyList();

        if (roleId != null) {
            try {
                ret = getEntityManager()
                        .createNamedQuery("XXRoleRefRole.findIdsByRoleId", Long.class)
                        .setParameter("roleId", roleId).getResultList();
            } catch (NoResultException e) {
                ret = Collections.emptyList();
            }
        }

        return ret;
    }

    public List<XXRoleRefRole> findBySubRoleId(Long subRoleId) {
        if (subRoleId == null) {
            return Collections.emptyList();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXRoleRefRole.findBySubRoleId", tClass)
                    .setParameter("subRoleId", subRoleId).getResultList();
        } catch (NoResultException e) {
            return Collections.emptyList();
        }
    }

    public List<XXRoleRefRole> findBySubRoleName(String subRoleName) {
        if (subRoleName == null) {
            return Collections.emptyList();
        }

        try {
            return getEntityManager().createNamedQuery("XXRoleRefRole.findBySubRoleName", tClass)
                    .setParameter("subRoleName", subRoleName).getResultList();
        } catch (NoResultException e) {
            return Collections.emptyList();
        }
    }

    public Long findRoleRefRoleCount(String subRoleName) {
        Long ret = -1L;

        try {
            ret = getEntityManager().createNamedQuery("XXRoleRefRole.findRoleRefRoleCount", Long.class)
                    .setParameter("subRoleName", subRoleName).getSingleResult();
        } catch (Exception e) {
            // ignore
        }

        return ret;
    }

    public Set<Long> getContainingRoles(Long subRoleId) {
        Set<Long>           ret;
        List<XXRoleRefRole> roles = findBySubRoleId(subRoleId);

        if (CollectionUtils.isNotEmpty(roles)) {
            ret = new HashSet<>();

            for (XXRoleRefRole role : roles) {
                ret.add(role.getRoleId());
            }
        } else {
            ret = Collections.emptySet();
        }

        return ret;
    }

    public void deleteRoleRefRoleByIds(List<Long> ids) {
        if (CollectionUtils.isNotEmpty(ids)) {
            batchDeleteByIds("XXRoleRefRole.deleteRoleRefRoleByIds", ids, "ids");
        }
    }
}
