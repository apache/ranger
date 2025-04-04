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
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXGdsSharedResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.List;

@Service
public class XXGdsSharedResourceDao extends BaseDao<XXGdsSharedResource> {
    private static final Logger LOG = LoggerFactory.getLogger(XXGdsSharedResourceDao.class);

    public XXGdsSharedResourceDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public XXGdsSharedResource findByGuid(String guid) {
        XXGdsSharedResource ret = null;

        if (StringUtils.isNotBlank(guid)) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsSharedResource.findByGuid", tClass)
                        .setParameter("guid", guid).getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("findByGuid({}): ", guid, e);
            }
        }

        return ret;
    }

    public XXGdsSharedResource findByName(String name) {
        XXGdsSharedResource ret = null;

        if (StringUtils.isNotBlank(name)) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsSharedResource.findByName", tClass)
                        .setParameter("name", name).getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("findByName({}): ", name, e);
            }
        }

        return ret;
    }

    public List<XXGdsSharedResource> findByServiceId(Long serviceId) {
        List<XXGdsSharedResource> ret = null;

        if (serviceId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsSharedResource.findByServiceId", tClass)
                        .setParameter("serviceId", serviceId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByServiceId({}): ", serviceId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public List<XXGdsSharedResource> findByServiceIdAndZoneId(Long serviceId, Long zoneId) {
        List<XXGdsSharedResource> ret = null;

        if (serviceId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsSharedResource.findByServiceIdAndZoneId", tClass)
                        .setParameter("serviceId", serviceId)
                        .setParameter("zoneId", zoneId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByServiceIdAndZoneId({}): ", serviceId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public List<XXGdsSharedResource> findByDatasetId(Long datasetId) {
        List<XXGdsSharedResource> ret = null;

        if (datasetId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsSharedResource.findByDatasetId", tClass)
                        .setParameter("datasetId", datasetId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByDatasetId({}): ", datasetId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public Long getIdByDataShareIdAndName(Long dataShareId, String name) {
        Long ret = null;

        if (dataShareId != null && name != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsSharedResource.getIdByDataShareIdAndName", Long.class)
                        .setParameter("dataShareId", dataShareId)
                        .setParameter("name", name).getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("getIdByDataShareIdAndName({}, {}): ", dataShareId, name, e);
            }
        }

        return ret;
    }

    public Long getIdByDataShareIdAndResourceSignature(Long dataShareId, String resourceSignature) {
        Long ret = null;

        if (dataShareId != null && resourceSignature != null) {
            try {
                ret = getEntityManager()
                        .createNamedQuery("XXGdsSharedResource.getIdByDataShareIdAndResourceSignature", Long.class)
                        .setParameter("dataShareId", dataShareId).setParameter("resourceSignature", resourceSignature)
                        .getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("getIdByDataShareIdAndName({}, {}): ", dataShareId, resourceSignature, e);
            }
        }

        return ret;
    }
}
