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
import org.apache.ranger.entity.XXAccessTypeDefGrants;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class XXAccessTypeDefGrantsDao extends BaseDao<XXAccessTypeDefGrants> {
    public XXAccessTypeDefGrantsDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<String> findImpliedGrantsByATDId(Long atdId) {
        if (atdId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXAccessTypeDefGrants.findImpliedGrantsByATDId", String.class)
                    .setParameter("atdId", atdId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public Map<String, List<String>> findImpliedGrantsByServiceDefId(Long serviceDefId) {
        final Map<String, List<String>> ret = new HashMap<>();

        if (serviceDefId != null) {
            List<Object[]> rows = getEntityManager()
                    .createNamedQuery("XXAccessTypeDefGrants.findByServiceDefId", Object[].class)
                    .setParameter("serviceDefId", serviceDefId)
                    .getResultList();

            if (rows != null) {
                for (Object[] row : rows) {
                    String       accessType    = (String) row[0];
                    String       impliedGrant  = (String) row[1];
                    List<String> impliedGrants = ret.computeIfAbsent(accessType, k -> new ArrayList<>());

                    impliedGrants.add(impliedGrant);
                }
            }
        }

        return ret;
    }

    public XXAccessTypeDefGrants findByNameAndATDId(Long atdId, String name) {
        if (atdId == null || name == null) {
            return null;
        }

        try {
            return getEntityManager().createNamedQuery("XXAccessTypeDefGrants.findByNameAndATDId", tClass)
                    .setParameter("atdId", atdId).setParameter("name", name).getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public List<XXAccessTypeDefGrants> findByATDId(Long atdId) {
        if (atdId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXAccessTypeDefGrants.findByATDId", tClass)
                    .setParameter("atdId", atdId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }
}
