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

import javax.persistence.NoResultException;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXServiceDef;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Objects;

@Service
public class XXServiceDefDao extends BaseDao<XXServiceDef> {
	/**
	 * Default Constructor
	 */
	public XXServiceDefDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXServiceDef findByName(String name) {
		if (name == null) {
			return null;
		}
		try {
			XXServiceDef xServiceDef = getEntityManager()
					.createNamedQuery("XXServiceDef.findByName", tClass)
					.setParameter("name", name).getSingleResult();
			return xServiceDef;
		} catch (NoResultException e) {
			return null;
		}
	}

	public XXServiceDef findByDisplayName(String displayName) {
		if (Objects.isNull(displayName)) {
			return null;
		}
		try {
			XXServiceDef xServiceDef = getEntityManager()
					.createNamedQuery("XXServiceDef.findByDisplayName", tClass)
					.setParameter("displayName", displayName).getSingleResult();
			return xServiceDef;
		} catch (NoResultException e) {
			return null;
		}
	}


	public Long getMaxIdOfXXServiceDef() {
		try {
			return (Long) getEntityManager().createNamedQuery("XXServiceDef.getMaxIdOfXXServiceDef").getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	public void updateSequence() {
		Long maxId = getMaxIdOfXXServiceDef();

		if(maxId == null) {
			return;
		}

		updateSequence("X_SERVICE_DEF_SEQ", maxId + 1);
	}

        public String findServiceDefTypeByServiceName(String serviceName) {
                String serviceType = null;
                if (StringUtils.isNotBlank(serviceName)) {
                        try {
                                serviceType = getEntityManager()
                                                .createNamedQuery("XXServiceDef.findServiceDefNameByServiceName", String.class)
                                                .setParameter("name", serviceName).getSingleResult();
                        } catch (NoResultException e) {
                                return null;
                        }
                }
                return serviceType;
        }

	public String findServiceDefTypeByServiceId(Long serviceId) {
		String serviceType = null;
		try {
			serviceType = getEntityManager()
					.createNamedQuery("XXServiceDef.findServiceDefNameByServiceId", String.class)
					.setParameter("id", serviceId).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
		return serviceType;
	}

	/**
	 * Fetch and return names of service type/ def which are using context enrichers.
	 * @return {@link java.util.List list} of {@link java.lang.String strings} representing service type/ def
	 */
	public List<String> findAllHavingEnrichers() {
		return getEntityManager().createNamedQuery("XXServiceDef.getNameByHasEnricher", String.class).getResultList();
	}

	/**
	 * Fetch and return count of services for each service type/ def
	 * @return {@link java.util.Map map} representing service type/ def as key and their respective service count as value
	 */
	public Map<String, Long> getServiceCount() {
		Map<String, Long> ret = Collections.emptyMap();
		List<Object[]> rows = (List<Object[]>) getEntityManager().createNamedQuery("XXServiceDef.getServiceCount").getResultList();
		if (rows != null) {
			ret = new HashMap<>();
			for (Object[] row : rows) {
				if (Objects.nonNull(row) && Objects.nonNull(row[0]) && Objects.nonNull(row[1]) && (!row[0].toString().isEmpty())) {
					// since group by query will not return empty count field, no need to check
					ret.put((String) row[0], (Long) row[1]);
				}
			}
		}
		return ret;
	}

	/**
	 * Fetch and return count of policies for each service type/ def
	 * @return {@link java.util.Map map} representing service type/ def as key and their respective policy count as value
	 */
	public Map<String, Long> getPolicyCountByType(int policyType) {
		Map<String, Long> ret = Collections.emptyMap();
		List<Object[]> rows = (List<Object[]>) getEntityManager().createNamedQuery("XXServiceDef.getPolicyCountByType")
				.setParameter("policyType", policyType)
				.getResultList();
		if (rows != null) {
			ret = new HashMap<>();
			for (Object[] row : rows) {
				if (Objects.nonNull(row) && Objects.nonNull(row[0]) && Objects.nonNull(row[1]) && (!row[0].toString().isEmpty())) {
					// since group by query will not return empty count field, no need to check
					ret.put((String) row[0], (Long) row[1]);
				}
			}
		}
		return ret;
	}

	/**
	 * Fetch and return count of deny items (conditions) for each service type/ def
	 * @return {@link java.util.Map map} representing service type/ def as key and their respective deny items (conditions) count as value
	 */
	public Map<String, Long> getPolicyCountByDenyItems() {
		Map<String, Long> ret = Collections.emptyMap();
		List<Object[]> rows = (List<Object[]>) getEntityManager().createNamedQuery("XXServiceDef.getPolicyCountByDenyItems").getResultList();
		if (rows != null) {
			ret = new HashMap<>();
			for (Object[] row : rows) {
				if (Objects.nonNull(row) && Objects.nonNull(row[0]) && Objects.nonNull(row[1]) && (!row[0].toString().isEmpty())) {
					// since group by query will not return empty count field, no need to check
					ret.put((String) row[0], (Long) row[1]);
				}
			}
		}
		return ret;
	}
}