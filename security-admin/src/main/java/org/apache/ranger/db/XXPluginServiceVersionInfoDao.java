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

import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPluginServiceVersionInfo;

/**
 */

public class XXPluginServiceVersionInfoDao extends BaseDao<XXPluginServiceVersionInfo> {
	/**
	 * Default Constructor
	 */
	public XXPluginServiceVersionInfoDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXPluginServiceVersionInfo find(String serviceName, String hostName, String appType, Integer entityType) {
		if (serviceName == null || hostName == null || appType == null || entityType == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPluginServiceVersionInfo.find", tClass)
					.setParameter("serviceName", serviceName)
					.setParameter("hostName", hostName)
					.setParameter("appType", appType)
					.setParameter("entityType", entityType)
					.getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}
	public List<XXPluginServiceVersionInfo> findByServiceName(String serviceName) {
		if (serviceName == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPluginServiceVersionInfo.findByServiceName", tClass)
					.setParameter("serviceName", serviceName).getResultList();
		} catch (NoResultException e) {
			return null;
		}
	}

	public List<XXPluginServiceVersionInfo> findByServiceId(Long serviceId) {
		if (serviceId == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPluginServiceVersionInfo.findByServiceId", tClass)
					.setParameter("serviceId", serviceId).getResultList();
		} catch (NoResultException e) {
			return null;
		}
	}

	public List<XXPluginServiceVersionInfo> findByServiceAndHostName(String serviceName, String hostName) {
		if (serviceName == null || hostName == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPluginServiceVersionInfo.findByServiceAndHostName", tClass)
					.setParameter("serviceName", serviceName)
					.setParameter("hostName", hostName)
					.getResultList();
		} catch (NoResultException e) {
			return null;
		}
	}

}
