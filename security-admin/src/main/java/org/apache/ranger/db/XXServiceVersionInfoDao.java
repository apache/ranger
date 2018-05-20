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

import java.util.Date;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXServiceVersionInfo;
import org.springframework.stereotype.Service;

/**
 */
@Service
public class XXServiceVersionInfoDao extends BaseDao<XXServiceVersionInfo> {
	/**
	 * Default Constructor
	 */
	public XXServiceVersionInfoDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXServiceVersionInfo findByServiceName(String serviceName) {
		if (serviceName == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXServiceVersionInfo.findByServiceName", tClass)
					.setParameter("serviceName", serviceName).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	public XXServiceVersionInfo findByServiceId(Long serviceId) {
		if (serviceId == null) {
			return null;
		}
		try {
			return getEntityManager().createNamedQuery("XXServiceVersionInfo.findByServiceId", tClass)
					.setParameter("serviceId", serviceId).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public List<Object[]> getAllWithServiceNames(){
		return getEntityManager()
				.createNamedQuery("XXServiceVersionInfo.getAllWithServiceNames")
				.getResultList();
	}

	public void updateServiceVersionInfoForServiceResourceUpdate(Long resourceId, Date updateTime) {
		if (resourceId == null) {
			return;
		}

		try {
			List<XXServiceVersionInfo> serviceVersionInfos = getEntityManager().createNamedQuery("XXServiceVersionInfo.findByServiceResourceId", tClass).setParameter("resourceId", resourceId).getResultList();

			updateTagVersionAndTagUpdateTime(serviceVersionInfos, updateTime);
		} catch (NoResultException e) {
			return;
		}
	}

	public void updateServiceVersionInfoForTagUpdate(Long tagId, Date updateTime) {
		if (tagId == null) {
			return;
		}

		try {
			List<XXServiceVersionInfo> serviceVersionInfos = getEntityManager().createNamedQuery("XXServiceVersionInfo.findByTagId", tClass).setParameter("tagId", tagId).getResultList();

			updateTagVersionAndTagUpdateTime(serviceVersionInfos, updateTime);
		} catch (NoResultException e) {
			return;
		}
	}

	public void updateServiceVersionInfoForTagDefUpdate(Long tagDefId, Date updateTime) {
		if (tagDefId == null) {
			return;
		}

		try {
			List<XXServiceVersionInfo> serviceVersionInfos = getEntityManager().createNamedQuery("XXServiceVersionInfo.findByTagDefId", tClass).setParameter("tagDefId", tagDefId).getResultList();

			updateTagVersionAndTagUpdateTime(serviceVersionInfos, updateTime);
		} catch (NoResultException e) {
			return;
		}
	}

	private void updateTagVersionAndTagUpdateTime(List<XXServiceVersionInfo> serviceVersionInfos, Date updateTime) {
		if(CollectionUtils.isEmpty(serviceVersionInfos)) {
			return;
		}

		if(updateTime == null) {
			updateTime = new Date();
		}

		for(XXServiceVersionInfo serviceVersionInfo : serviceVersionInfos) {
			Long currentTagVersion = serviceVersionInfo.getTagVersion();

			if(currentTagVersion == null) {
				currentTagVersion = Long.valueOf(0);
			}

			serviceVersionInfo.setTagVersion(currentTagVersion + 1);
			serviceVersionInfo.setTagUpdateTime(updateTime);
		}
	}
}
