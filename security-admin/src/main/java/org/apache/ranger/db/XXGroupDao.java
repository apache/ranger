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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.persistence.NoResultException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXGroup;
import org.springframework.stereotype.Service;

@Service
public class XXGroupDao extends BaseDao<XXGroup> {
	public XXGroupDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	@SuppressWarnings("unchecked")
	public List<XXGroup> findByUserId(Long userId) {
		if (userId == null) {
			return new ArrayList<XXGroup>();
		}

		List<XXGroup> groupList = (List<XXGroup>) getEntityManager()
				.createNamedQuery("XXGroup.findByUserId")
				.setParameter("userId", userId).getResultList();

		if (groupList == null) {
			groupList = new ArrayList<XXGroup>();
		}

		return groupList;
	}

	public XXGroup findByGroupName(String groupName) {
		if (groupName == null) {
			return null;
		}
		try {

			return (XXGroup) getEntityManager()
					.createNamedQuery("XXGroup.findByGroupName")
					.setParameter("name", groupName)
					.getSingleResult();
		} catch (Exception e) {

		}
		return null;
	}

	public Map<String, Long> getIdsByGroupNames(Collection<String> groupNames) {
		Map<String, Long> ret = Collections.emptyMap();
		if (!CollectionUtils.isEmpty(groupNames)) {
			try {
				Collection<Object[]> result = getEntityManager()
						.createNamedQuery("XXGroup.getIdsByGroupNames", Object[].class)
						.setParameter("names", groupNames)
						.getResultList();
				ret = result.stream().collect(
						Collectors.toMap(
								object -> (String)(object[1]),
								object -> (Long)(object[0])));
			} catch (NoResultException e) {
				// ignore
			}
		}
		return ret;
	}
}
