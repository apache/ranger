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

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class XXPolicyConditionDefDao extends BaseDao<XXPolicyConditionDef> {
    private static final Logger logger = LoggerFactory.getLogger(XXPolicyConditionDefDao.class);

    public XXPolicyConditionDefDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXPolicyConditionDef> findByServiceDefId(Long serviceDefId) {
        if (serviceDefId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicyConditionDef.findByServiceDefId", tClass)
                    .setParameter("serviceDefId", serviceDefId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public XXPolicyConditionDef findByServiceDefIdAndName(Long serviceDefId, String name) {
        if (serviceDefId == null) {
            return null;
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXPolicyConditionDef.findByServiceDefIdAndName", tClass)
                    .setParameter("serviceDefId", serviceDefId)
                    .setParameter("name", name).getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public Map<String, Long> findConditionDefIdsByServiceDefIdAndNames(Long serviceDefId, Set<String> names) {
        if (serviceDefId != null && CollectionUtils.isNotEmpty(names)) {
            try {
                Collection<Object[]> result = getEntityManager()
                        .createNamedQuery("XXPolicyConditionDef.findConditionDefIdsByServiceDefIdAndNames", Object[].class)
                        .setParameter("serviceDefId", serviceDefId)
                        .setParameter("names", names)
                        .getResultList();

                return result.stream().collect(Collectors.toMap(object -> (String) object[1], object -> (Long) object[0], (a, b) -> a));
            } catch (NoResultException e) {
                logger.debug(e.getMessage());
            }
        }

        return Collections.emptyMap();
    }
}
