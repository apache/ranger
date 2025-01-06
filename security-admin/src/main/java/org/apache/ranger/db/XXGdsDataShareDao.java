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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXGdsDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsObjectACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class XXGdsDataShareDao extends BaseDao<XXGdsDataShare> {
    private static final Logger LOG = LoggerFactory.getLogger(XXGdsDataShareDao.class);

    public XXGdsDataShareDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public XXGdsDataShare findByGuid(String guid) {
        XXGdsDataShare ret = null;

        if (StringUtils.isNotBlank(guid)) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShare.findByGuid", tClass)
                        .setParameter("guid", guid).getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("findByGuid({}): ", guid, e);
            }
        }

        return ret;
    }

    public XXGdsDataShare findByName(String name) {
        XXGdsDataShare ret = null;

        if (StringUtils.isNotBlank(name)) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShare.findByName", tClass)
                        .setParameter("name", name).getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("findByName({}): ", name, e);
            }
        }

        return ret;
    }

    public List<XXGdsDataShare> findByServiceId(Long serviceId) {
        List<XXGdsDataShare> ret = null;

        if (serviceId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShare.findByServiceId", tClass)
                        .setParameter("serviceId", serviceId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByServiceId({}): ", serviceId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public List<XXGdsDataShare> findByZoneId(Long zoneId) {
        List<XXGdsDataShare> ret = null;

        if (zoneId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShare.findByZoneId", tClass)
                        .setParameter("zoneId", zoneId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByZoneId({}): ", zoneId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public List<XXGdsDataShare> findByServiceIdAndZoneId(Long serviceId, Long zoneId) {
        List<XXGdsDataShare> ret = null;

        if (serviceId != null && zoneId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShare.findByServiceIdAndZoneId", tClass)
                        .setParameter("serviceId", serviceId)
                        .setParameter("zoneId", zoneId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByServiceIdAndZoneId({}, {}): ", serviceId, zoneId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public List<XXGdsDataShare> findByDatasetId(Long datasetId) {
        List<XXGdsDataShare> ret = null;

        if (datasetId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShare.findByDatasetId", tClass)
                        .setParameter("datasetId", datasetId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByDatasetId({}): ", datasetId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public List<Long> findServiceIdsForDataShareId(Long dataShareId) {
        List<Long> ret = null;

        if (dataShareId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShare.findServiceIds", Long.class)
                        .setParameter("dataShareId", dataShareId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findServiceIdsForDataShareId({}): ", dataShareId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public Map<Long, RangerGdsObjectACL> getDataShareIdsAndACLs() {
        Map<Long, RangerGdsObjectACL> ret = new HashMap<>();

        try {
            List<Object[]> rows = getEntityManager().createNamedQuery("XXGdsDataShare.getDataShareIdsAndACLs", Object[].class).getResultList();

            if (rows != null) {
                for (Object[] row : rows) {
                    Long               id  = (Long) row[0];
                    RangerGdsObjectACL acl = JsonUtils.jsonToObject((String) row[1], RangerGdsObjectACL.class);

                    if (acl != null) {
                        ret.put(id, acl);
                    }
                }
            }
        } catch (NoResultException e) {
            LOG.debug("getDataShareIdsAndACLs()", e);
        }

        return ret;
    }
}
