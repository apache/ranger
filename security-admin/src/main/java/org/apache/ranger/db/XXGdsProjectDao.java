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
import org.apache.ranger.entity.XXGdsProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;
import java.util.Collections;
import java.util.List;


@Service
public class XXGdsProjectDao extends BaseDao<XXGdsProject> {
	private static final Logger LOG = LoggerFactory.getLogger(XXGdsProjectDao.class);

	public XXGdsProjectDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXGdsProject findByGuid(String guid) {
		XXGdsProject ret = null;

		if (StringUtils.isNotBlank(guid)) {
			try {
				ret = getEntityManager().createNamedQuery("XXGdsProject.findByGuid", tClass)
						                .setParameter("guid", guid).getSingleResult();
			} catch (NoResultException e) {
				LOG.debug("findByGuid({}): ", guid, e);
			}
		}

		return ret;
	}

	public XXGdsProject findByName(String name) {
		XXGdsProject ret = null;

		if (StringUtils.isNotBlank(name)) {
			try {
				ret = getEntityManager().createNamedQuery("XXGdsProject.findByName", tClass)
						                .setParameter("name", name).getSingleResult();
			} catch (NoResultException e) {
				LOG.debug("findByName({}): ", name, e);
			}
		}

		return ret;
	}

	public List<XXGdsProject> findByDatasetId(Long datasetId) {
		List<XXGdsProject> ret = null;

		if (datasetId != null) {
			try {
				ret = getEntityManager().createNamedQuery("XXGdsProject.findByDatasetId", tClass)
						                .setParameter("datasetId", datasetId).getResultList();
			} catch (NoResultException e) {
				LOG.debug("findByDatasetId({}): ", datasetId, e);
			}
		}

		return ret != null ? ret : Collections.emptyList();
	}

	public List<Long> findServiceIdsForProject(Long projectId) {
		List<Long> ret = null;

		if (projectId != null) {
			try {
				ret = getEntityManager().createNamedQuery("XXGdsProject.findServiceIds", Long.class)
				                        .setParameter("projectId", projectId).getResultList();
			} catch (NoResultException e) {
				LOG.debug("findServiceIdsForProject({}): ", projectId, e);
			}
		}

		return ret != null ? ret : Collections.emptyList();
	}
}
