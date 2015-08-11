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
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXTaggedResourceValue;
import org.apache.ranger.entity.XXTaggedResourceValueMap;

public class XXTaggedResourceValueMapDao extends BaseDao<XXTaggedResourceValueMap> {

	public XXTaggedResourceValueMapDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public List<XXTaggedResourceValueMap> findByResValueId(Long resValueId) {
		if (resValueId == null) {
			return new ArrayList<XXTaggedResourceValueMap>();
		}
		try {
			return getEntityManager().createNamedQuery("XXTaggedResourceValueMap.findByResValueId", tClass)
					.setParameter("resValueId", resValueId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXTaggedResourceValueMap>();
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<String> findValuesByResValueId(Long resValueId) {
		if (resValueId == null) {
			return new ArrayList<String>();
		}
		try {
			return getEntityManager().createNamedQuery("XXTaggedResourceValueMap.findValuesByResValueId")
					.setParameter("resValueId", resValueId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<String>();
		}
	}

}
