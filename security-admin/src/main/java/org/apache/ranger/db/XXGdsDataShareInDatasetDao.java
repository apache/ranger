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
import org.apache.ranger.entity.XXGdsDataShareInDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Service
public class XXGdsDataShareInDatasetDao extends BaseDao<XXGdsDataShareInDataset> {
    private static final Logger LOG = LoggerFactory.getLogger(XXGdsDataShareInDatasetDao.class);

    public XXGdsDataShareInDatasetDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public XXGdsDataShareInDataset findByGuid(String guid) {
        XXGdsDataShareInDataset ret = null;

        if (StringUtils.isNotBlank(guid)) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShareInDataset.findByGuid", tClass)
                        .setParameter("guid", guid).getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("findByGuid({}): ", guid, e);
            }
        }

        return ret;
    }

    public XXGdsDataShareInDataset findByDataShareIdAndDatasetId(Long dataShareId, Long datasetId) {
        XXGdsDataShareInDataset ret = null;

        if (dataShareId != null && datasetId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShareInDataset.findByDataShareIdAndDatasetId", tClass)
                        .setParameter("dataShareId", dataShareId)
                        .setParameter("datasetId", datasetId).getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("findByDataShareIdAndDatasetId({}): ", dataShareId, e);
            }
        }

        return ret;
    }

    public List<XXGdsDataShareInDataset> findByDataShareId(Long dataShareId) {
        List<XXGdsDataShareInDataset> ret = null;

        if (dataShareId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShareInDataset.findByDataShareId", tClass)
                        .setParameter("dataShareId", dataShareId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByDataShareId({}): ", dataShareId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public List<XXGdsDataShareInDataset> findByDatasetId(Long datasetId) {
        List<XXGdsDataShareInDataset> ret = null;

        if (datasetId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShareInDataset.findByDatasetId", tClass)
                        .setParameter("datasetId", datasetId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByDatasetId({}): ", datasetId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public Map<Short, Long> getDataSharesInDatasetCountByStatus(Long datasetId) {
        Map<Short, Long> ret = Collections.emptyMap();

        if (datasetId != null) {
            try {
                List<Object[]> rows = getEntityManager().createNamedQuery("XXGdsDataShareInDataset.getDataSharesInDatasetCountByStatus", Object[].class)
                        .setParameter("datasetId", datasetId).getResultList();
                if (rows != null) {
                    ret = new HashMap<>();

                    for (Object[] row : rows) {
                        if (Objects.nonNull(row) && Objects.nonNull(row[0]) && Objects.nonNull(row[1]) && (!row[0].toString().isEmpty())) {
                            ret.put((Short) row[0], (Long) row[1]);
                        }
                    }
                }
            } catch (NoResultException e) {
                LOG.debug("getDataSharesInDatasetCountByStatus({}): ", datasetId, e);
            }
        }

        return ret;
    }

    public List<Long> findDataShareIdsInStatuses(Long datasetId, Set<Integer> statuses) {
        List<Long> ret = null;

        if (datasetId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDataShareInDataset.findDataShareIdsInStatuses", Long.class)
                        .setParameter("datasetId", datasetId)
                        .setParameter("statuses", statuses)
                        .getResultList();
            } catch (NoResultException e) {
                LOG.debug("XXGdsDataShareInDataset({}, {}): ", datasetId, statuses, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }
}
