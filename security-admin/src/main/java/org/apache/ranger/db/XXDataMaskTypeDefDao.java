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
import org.apache.ranger.entity.XXDataMaskTypeDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class XXDataMaskTypeDefDao extends BaseDao<XXDataMaskTypeDef> {
    private static final Logger logger = LoggerFactory.getLogger(XXDataMaskTypeDefDao.class);

    public XXDataMaskTypeDefDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXDataMaskTypeDef> findByServiceDefId(Long serviceDefId) {
        if (serviceDefId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXDataMaskTypeDef.findByServiceDefId", tClass)
                    .setParameter("serviceDefId", serviceDefId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public XXDataMaskTypeDef findByNameAndServiceId(String name, Long serviceId) {
        if (name == null || serviceId == null) {
            return null;
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXDataMaskTypeDef.findByNameAndServiceId", tClass)
                    .setParameter("name", name).setParameter("serviceId", serviceId)
                    .getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public List<String> getNamesByServiceName(String serviceName) {
        List<String> ret = null;

        if (serviceName != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXDataMaskTypeDef.getNamesByServiceName", String.class)
                        .setParameter("serviceName", serviceName).getResultList();
            } catch (NoResultException excp) {
                // ignore
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public Map<String, XXDataMaskTypeDef> findByNamesAndServiceId(Set<String> names, Long serviceId) {
        Map<String, XXDataMaskTypeDef> ret = Collections.emptyMap();
        if (CollectionUtils.isEmpty(names) || serviceId == null) {
            return ret;
        }
        try {
            List<XXDataMaskTypeDef> result = getEntityManager()
                    .createNamedQuery("XXDataMaskTypeDef.findByNamesAndServiceId", tClass)
                    .setParameter("names", names)
                    .setParameter("serviceId", serviceId)
                    .getResultList();

            if (CollectionUtils.isEmpty(result)) {
                return ret;
            }
            return result.stream()
                    .collect(Collectors.toMap(XXDataMaskTypeDef::getName, java.util.function.Function.identity(), (a, b) -> a));
        } catch (Exception e) {
            logger.error("Error retrieving data mask type definitions for serviceId={} and names={}", serviceId, names);
        }

        return ret;
    }
}
