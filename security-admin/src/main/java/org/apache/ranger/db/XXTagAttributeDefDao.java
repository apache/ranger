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
import org.apache.ranger.entity.XXTagAttributeDef;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.List;

@Service
public class XXTagAttributeDefDao extends BaseDao<XXTagAttributeDef> {
    public XXTagAttributeDefDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXTagAttributeDef> findByTagDefId(Long tagDefId) {
        if (tagDefId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXTagAttributeDef.findByTagDefId", tClass)
                    .setParameter("tagDefId", tagDefId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXTagAttributeDef> findByServiceId(Long serviceId) {
        if (serviceId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXTagAttributeDef.findByServiceId", tClass)
                    .setParameter("serviceId", serviceId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }

    public List<XXTagAttributeDef> findByResourceId(Long resourceId) {
        if (resourceId == null) {
            return new ArrayList<>();
        }

        try {
            return getEntityManager().createNamedQuery("XXTagAttributeDef.findByResourceId", tClass)
                    .setParameter("resourceId", resourceId).getResultList();
        } catch (NoResultException e) {
            return new ArrayList<>();
        }
    }
}
