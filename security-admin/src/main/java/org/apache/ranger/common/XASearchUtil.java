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

 package org.apache.ranger.common;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.log4j.Logger;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.db.XADaoManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class XASearchUtil extends SearchUtil {
	final static Logger logger = Logger.getLogger(XASearchUtil.class);
	/*
	@Override
	public Query createSearchQuery(EntityManager em, String queryStr, String sortClause,
			SearchCriteria searchCriteria, List<SearchField> searchFields,
			int objectClassType, boolean hasAttributes, boolean isCountQuery){

		// [1] Build where clause
		StringBuilder queryClause = buildWhereClause(searchCriteria,
				searchFields);

		// [2] Add domain-object-security clause if needed
		// if (objectClassType != -1
		// && !ContextUtil.getCurrentUserSession().isUserAdmin()) {
		// addDomainObjectSecuirtyClause(queryClause, hasAttributes);
		// }

		// [2] Add order by clause
		addOrderByClause(queryClause, sortClause);

		// [3] Create Query Object
		Query query = em.createQuery(
					queryStr + queryClause);

		// [4] Resolve query parameters with values
		resolveQueryParams(query, searchCriteria, searchFields);

		// [5] Resolve domain-object-security parameters
		// if (objectClassType != -1 &&
		// !securityHandler.hasModeratorPermission()) {
		// resolveDomainObjectSecuirtyParams(query, objectClassType);
		// }

		if (!isCountQuery) {
			query.setFirstResult(searchCriteria.getStartIndex());
			updateQueryPageSize(query, searchCriteria);
		}

		return query;
	}
	*/
}
