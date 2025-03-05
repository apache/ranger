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
import org.apache.ranger.entity.XXGdsDatasetInProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Collections;
import java.util.List;

@Service
public class XXGdsDatasetInProjectDao extends BaseDao<XXGdsDatasetInProject> {
    private static final Logger LOG = LoggerFactory.getLogger(XXGdsDatasetInProjectDao.class);

    public XXGdsDatasetInProjectDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public XXGdsDatasetInProject findByGuid(String guid) {
        XXGdsDatasetInProject ret = null;

        if (StringUtils.isNotBlank(guid)) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDatasetInProject.findByGuid", tClass)
                        .setParameter("guid", guid).getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("findByGuid({}): ", guid, e);
            }
        }

        return ret;
    }

    public XXGdsDatasetInProject findByDatasetIdAndProjectId(Long datasetId, Long projectId) {
        XXGdsDatasetInProject ret = null;

        if (datasetId != null && projectId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDatasetInProject.findByDatasetIdAndProjectId", tClass)
                        .setParameter("datasetId", datasetId)
                        .setParameter("projectId", projectId)
                        .getSingleResult();
            } catch (NoResultException e) {
                LOG.debug("findByDatasetIdAndProjectId({}): ", datasetId, e);
            }
        }

        return ret;
    }

    public List<XXGdsDatasetInProject> findByDatasetId(Long datasetId) {
        List<XXGdsDatasetInProject> ret = null;

        if (datasetId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDatasetInProject.findByDatasetId", tClass)
                        .setParameter("datasetId", datasetId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByDatasetId({}): ", datasetId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }

    public List<XXGdsDatasetInProject> findByProjectId(Long projectId) {
        List<XXGdsDatasetInProject> ret = null;

        if (projectId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXGdsDatasetInProject.findByProjectId", tClass)
                        .setParameter("projectId", projectId).getResultList();
            } catch (NoResultException e) {
                LOG.debug("findByProjectId({}): ", projectId, e);
            }
        }

        return ret != null ? ret : Collections.emptyList();
    }
}
