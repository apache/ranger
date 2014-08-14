package com.xasecure.common;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.xasecure.db.XADaoManager;
import com.xasecure.common.SearchCriteria;
import com.xasecure.common.SearchField;
import com.xasecure.common.SearchUtil;

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
