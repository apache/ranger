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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.stereotype.Component;

@Component
public class RangerSearchUtil extends SearchUtil {
	final static Logger logger = Logger.getLogger(RangerSearchUtil.class);

	public Query createSearchQuery(EntityManager em, String queryStr, String sortClause,
			SearchFilter searchCriteria, List<SearchField> searchFields,
			boolean isCountQuery) {
		return createSearchQuery(em, queryStr, sortClause, searchCriteria, searchFields, -1, false, isCountQuery);
	}
	
	public Query createSearchQuery(EntityManager em, String queryStr, String sortClause,
			SearchFilter searchCriteria, List<SearchField> searchFields,
			int objectClassType, boolean hasAttributes, boolean isCountQuery) {

		StringBuilder queryClause = buildWhereClause(searchCriteria, searchFields);

		super.addOrderByClause(queryClause, sortClause);

		Query query = em.createQuery(queryStr + queryClause);

		resolveQueryParams(query, searchCriteria, searchFields);

		if (!isCountQuery) {
			query.setFirstResult(searchCriteria.getStartIndex());
			updateQueryPageSize(query, searchCriteria);
		}

		return query;
	}
	
	private StringBuilder buildWhereClause(SearchFilter searchCriteria, List<SearchField> searchFields) {
		return buildWhereClause(searchCriteria, searchFields, false, false);
	}

	private StringBuilder buildWhereClause(SearchFilter searchCriteria,
			List<SearchField> searchFields, boolean isNativeQuery,
			boolean excludeWhereKeyword) {

		StringBuilder whereClause = new StringBuilder(excludeWhereKeyword ? "" : "WHERE 1 = 1 ");

		List<String> joinTableList = new ArrayList<String>();

		for (SearchField searchField : searchFields) {
			int startWhereLen = whereClause.length();

			if (searchField.getFieldName() == null && searchField.getCustomCondition() == null) { 
				continue;
			}

			if (searchField.getDataType() == SearchField.DATA_TYPE.INTEGER) {
				Integer paramVal = restErrorUtil.parseInt(searchCriteria.getParam(searchField.getClientFieldName()),
						"Invalid value for " + searchField.getClientFieldName(),
						MessageEnums.INVALID_INPUT_DATA, null, searchField.getClientFieldName());
				
				Number intFieldValue = paramVal != null ? (Number) paramVal : null;
				if (intFieldValue != null) {
					if (searchField.getCustomCondition() == null) {
						whereClause.append(" and ")
								.append(searchField.getFieldName())
								.append("=:")
								.append(searchField.getClientFieldName());
					} else {
						whereClause.append(" and ").append(searchField.getCustomCondition());
					}
				}
			} else if (searchField.getDataType() == SearchField.DATA_TYPE.STRING) {
				String strFieldValue = searchCriteria.getParam(searchField.getClientFieldName());
				if (strFieldValue != null) {
					if (searchField.getCustomCondition() == null) {
						whereClause.append(" and ").append("LOWER(").append(searchField.getFieldName()).append(")");
						if (searchField.getSearchType() == SearchField.SEARCH_TYPE.FULL) {
							whereClause.append("= :").append(searchField.getClientFieldName());
						} else {
							whereClause.append("like :").append(searchField.getClientFieldName());
						}
					} else {
						whereClause.append(" and ").append(searchField.getCustomCondition());
					}
				}
			} else if (searchField.getDataType() == SearchField.DATA_TYPE.BOOLEAN) {
				Boolean boolFieldValue = restErrorUtil.parseBoolean(searchCriteria.getParam(searchField.getClientFieldName()),
						"Invalid value for " + searchField.getClientFieldName(),
						MessageEnums.INVALID_INPUT_DATA, null, searchField.getClientFieldName());
				
				if (boolFieldValue != null) {
					if (searchField.getCustomCondition() == null) {
						whereClause.append(" and ")
								.append(searchField.getFieldName())
								.append("=:")
								.append(searchField.getClientFieldName());
					} else {
						whereClause.append(" and ").append(searchField.getCustomCondition());
					}
				}
			} else if (searchField.getDataType() == SearchField.DATA_TYPE.DATE) {
				Date fieldValue = restErrorUtil.parseDate(searchCriteria.getParam(searchField.getClientFieldName()), 
						"Invalid value for " + searchField.getClientFieldName(), MessageEnums.INVALID_INPUT_DATA, 
						null, searchField.getClientFieldName(), null);
				if (fieldValue != null) {
					if (searchField.getCustomCondition() == null) {
						whereClause.append(" and ").append(searchField.getFieldName());
						if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.LESS_THAN)) {
							whereClause.append("< :");
						} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.LESS_EQUAL_THAN)) {
							whereClause.append("<= :");
						} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.GREATER_THAN)) {
							whereClause.append("> :");
						} else if (searchField.getSearchType().equals(SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN)) {
							whereClause.append(">= :");
						}
						whereClause.append(searchField.getClientFieldName());
					} else {
						whereClause.append(" and ").append(searchField.getCustomCondition());
					}
				}
			}

			if (whereClause.length() > startWhereLen && searchField.getJoinTables() != null) {
				for (String table : searchField.getJoinTables()) {
					if (!joinTableList.contains(table)) {
						joinTableList.add(table);
					}
				}
				whereClause.append(" and (").append(searchField.getJoinCriteria()).append(")");
			}
		}
		for (String joinTable : joinTableList) {
			whereClause.insert(0, ", " + joinTable + " ");
		}
		
		return whereClause;
	}
	
	protected void resolveQueryParams(Query query, SearchFilter searchCriteria, List<SearchField> searchFields) {

		for (SearchField searchField : searchFields) {

			if (searchField.getDataType() == SearchField.DATA_TYPE.INTEGER) {
				Integer paramVal = restErrorUtil.parseInt(searchCriteria.getParam(searchField.getClientFieldName()),
						"Invalid value for " + searchField.getClientFieldName(),
						MessageEnums.INVALID_INPUT_DATA, null, searchField.getClientFieldName());
				
				Number intFieldValue = paramVal != null ? (Number) paramVal : null;
				if (intFieldValue != null) {
					query.setParameter(searchField.getClientFieldName(), intFieldValue);
				}
			} else if (searchField.getDataType() == SearchField.DATA_TYPE.STRING) {
				String strFieldValue = searchCriteria.getParam(searchField.getClientFieldName());
				if (strFieldValue != null) {
					if (searchField.getSearchType() == SearchField.SEARCH_TYPE.FULL) {
						query.setParameter(searchField.getClientFieldName(), strFieldValue.trim().toLowerCase());
					} else {
						query.setParameter(searchField.getClientFieldName(), "%" + strFieldValue.trim().toLowerCase() + "%");
					}
				}
			} else if (searchField.getDataType() == SearchField.DATA_TYPE.BOOLEAN) {
				Boolean boolFieldValue = restErrorUtil.parseBoolean(searchCriteria.getParam(searchField.getClientFieldName()),
						"Invalid value for " + searchField.getClientFieldName(),
						MessageEnums.INVALID_INPUT_DATA, null, searchField.getClientFieldName());
				
				if (boolFieldValue != null) {
					query.setParameter(searchField.getClientFieldName(), boolFieldValue);
				}
			} else if (searchField.getDataType() == SearchField.DATA_TYPE.DATE) {
				Date fieldValue = restErrorUtil.parseDate(searchCriteria.getParam(searchField.getClientFieldName()), 
						"Invalid value for " + searchField.getClientFieldName(), MessageEnums.INVALID_INPUT_DATA, 
						null, searchField.getClientFieldName(), null);
				if (fieldValue != null) {
					query.setParameter(searchField.getClientFieldName(), fieldValue);
				}
			}
		}
	}
	
	public void updateQueryPageSize(Query query, SearchFilter searchCriteria) {
		int pageSize = super.validatePageSize(searchCriteria.getMaxRows());
		query.setMaxResults(pageSize);

		query.setHint("eclipselink.jdbc.max-rows", "" + pageSize);
	}
	
	public String constructSortClause(SearchFilter searchCriteria, List<SortField> sortFields) {
		String sortBy = searchCriteria.getSortBy();
		String querySortBy = null;
		
		if (!stringUtil.isEmpty(sortBy)) {
			sortBy = sortBy.trim();
			for (SortField sortField : sortFields) {
				if (sortBy.equalsIgnoreCase(sortField.getParamName())) {
					querySortBy = sortField.getFieldName();
					// Override the sortBy using the normalized value
					searchCriteria.setSortBy(sortField.getParamName());
					break;
				}
			}
		}

		if (querySortBy == null) {
			for (SortField sortField : sortFields) {
				if (sortField.isDefault()) {
					querySortBy = sortField.getFieldName();
					// Override the sortBy using the default value
					searchCriteria.setSortBy(sortField.getParamName());
					searchCriteria.setSortType(sortField.getDefaultOrder().name());
					break;
				}
			}
		}

		if (querySortBy != null) {
			String sortType = searchCriteria.getSortType();
			String querySortType = "asc";
			if (sortType != null) {
				if (sortType.equalsIgnoreCase("asc") || sortType.equalsIgnoreCase("desc")) {
					querySortType = sortType;
				} else {
					logger.error("Invalid sortType. sortType=" + sortType);
				}
			}
			
			if(querySortType!=null){
				searchCriteria.setSortType(querySortType.toLowerCase());
			}
			String sortClause = " ORDER BY " + querySortBy + " " + querySortType;

			return sortClause;
		}
		return null;
	}
	
}
