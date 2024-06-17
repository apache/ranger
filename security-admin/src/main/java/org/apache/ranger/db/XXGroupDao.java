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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.plugin.model.GroupInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import static org.apache.ranger.plugin.util.RangerCommonConstants.SCRIPT_FIELD__IS_INTERNAL;
import static org.apache.ranger.plugin.util.RangerCommonConstants.SCRIPT_FIELD__SYNC_SOURCE;

@Service
public class XXGroupDao extends BaseDao<XXGroup> {
	private static final Logger logger = LoggerFactory.getLogger(XXGroupDao.class);

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

	public Map<Long, String> getAllGroupIdNames() {
		Map<Long, String> groups = new HashMap<Long, String>();
		try {
			List<Object[]> rows = (List<Object[]>) getEntityManager().createNamedQuery("XXGroup.getAllGroupIdNames").getResultList();
			if (rows != null) {
				for (Object[] row : rows) {
					groups.put((Long)row[0], (String)row[1]);
				}
			}
		} catch (Exception ex) {
		}
		return groups;
	}


	public List<GroupInfo> getAllGroupsInfo() {
		List<GroupInfo> ret = new ArrayList<>();

		try {
			List<Object[]> rows = getEntityManager().createNamedQuery("XXGroup.getAllGroupsInfo", Object[].class).getResultList();

			if (rows != null) {
				for (Object[] row : rows) {

					ret.add(toGroupInfo(row));
				}
			}
		} catch (NoResultException excp) {
			if (logger.isDebugEnabled()) {
				logger.debug(excp.getMessage());
			}
		}

		return ret;
	}

	private GroupInfo toGroupInfo(Object[] row) {
		String              name        = (String) row[0];
		String              description = (String) row[1];
		String              attributes  = (String) row[2];
		String              syncSource  = (String) row[3];
		Number              groupSource = (Number) row[4];
		Boolean             isInternal  = groupSource != null && groupSource.equals(RangerCommonEnums.GROUP_INTERNAL);
		Map<String, String> attrMap     = null;

		if (StringUtils.isNotBlank(attributes)) {
			try {
                attrMap = JsonUtils.jsonToMapStringString(attributes);
			} catch (Exception excp) {
				// ignore
			}
		}

		if (attrMap == null) {
			attrMap = new HashMap<>();
		}

		if (StringUtils.isNotBlank(syncSource)) {
			attrMap.put(SCRIPT_FIELD__SYNC_SOURCE, syncSource);
		}

		attrMap.put(SCRIPT_FIELD__IS_INTERNAL, isInternal.toString());

		return new GroupInfo(name, description, attrMap);
	}
}
