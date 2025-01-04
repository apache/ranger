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
import org.apache.ranger.entity.XXAccessTypeDef;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class XXAccessTypeDefDao extends BaseDao<XXAccessTypeDef> {
    public XXAccessTypeDefDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXAccessTypeDef> findByServiceDefId(Long serviceDefId) {
        if (serviceDefId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXAccessTypeDef.findByServiceDefId", tClass)
                    .setParameter("serviceDefId", serviceDefId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public XXAccessTypeDef findByNameAndServiceId(String name, Long serviceId) {
        if (name == null || serviceId == null) {
            return null;
        }

        try {
            return getEntityManager()
                    .createNamedQuery("XXAccessTypeDef.findByNameAndServiceId", tClass)
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
                ret = getEntityManager().createNamedQuery("XXAccessTypeDef.getNamesByServiceName", String.class)
                        .setParameter("serviceName", serviceName).getResultList();
            } catch (NoResultException excp) {
                // ignore
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }
}
