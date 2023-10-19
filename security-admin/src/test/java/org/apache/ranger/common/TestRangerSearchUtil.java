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
package org.apache.ranger.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TestRangerSearchUtil {
	private final RangerSearchUtil  searchUtil   = new RangerSearchUtil();
	private final List<SearchField> searchFields = new ArrayList<>();

	private static final String SEARCH_PARAM_ID                = "id";
	private static final String SEARCH_PARAM_NAME              = "name";
	private static final String SEARCH_PARAM_NAME_CONTAINS     = "nameContains";
	private static final String SEARCH_PARAM_IS_ENABLED        = "isEnabled";
	private static final String SEARCH_PARAM_CREATED_TIME      = "createdTime";
	private static final String SEARCH_PARAM_CREATED_TIME_FROM = "createdTimeFrom";
	private static final String SEARCH_PARAM_CREATED_TIME_TO   = "createdTimeTo";
	private static final String SEARCH_PARAM_EXCLUDE_ID        = "excludeId";
	private static final String SEARCH_PARAM_EXCLUDE_NAME      = "excludeName";

	private static final String WHERE_PREFIX     = "WHERE 1 = 1 ";
	private static final String WHERE_PREFIX_AND = WHERE_PREFIX + " and ";

	public TestRangerSearchUtil() {
		searchFields.add(new SearchField(SEARCH_PARAM_ID,                "obj.id",          SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SEARCH_PARAM_NAME,              "obj.name",        SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SEARCH_PARAM_IS_ENABLED,        "obj.isEnabled",   SearchField.DATA_TYPE.BOOLEAN, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SEARCH_PARAM_CREATED_TIME,      "obj.createdTime", SearchField.DATA_TYPE.DATE,    SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SEARCH_PARAM_NAME_CONTAINS,     "obj.name",        SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.PARTIAL));
		searchFields.add(new SearchField(SEARCH_PARAM_CREATED_TIME_FROM, "obj.createdTime", SearchField.DATA_TYPE.DATE,    SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN));
		searchFields.add(new SearchField(SEARCH_PARAM_CREATED_TIME_TO,   "obj.createdTime", SearchField.DATA_TYPE.DATE,    SearchField.SEARCH_TYPE.LESS_THAN));
		searchFields.add(new SearchField(SEARCH_PARAM_EXCLUDE_ID,        "obj.id",          SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.NOT_EQUALS));
		searchFields.add(new SearchField(SEARCH_PARAM_EXCLUDE_NAME,      "obj.name",        SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.NOT_EQUALS));
	}

	@Test
	public void testEmptyCriteria() {
		SearchCriteria criteria    = new SearchCriteria();
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX, whereClause);
	}

	@Test
	public void testIntEquals() {
		SearchCriteria criteria    = new SearchCriteria(SEARCH_PARAM_ID, 1);
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX_AND + "obj.id = :id", whereClause);
	}

	@Test
	public void testStringEquals() {
		SearchCriteria criteria    = new SearchCriteria(SEARCH_PARAM_NAME, "test-name");
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX_AND + "LOWER(obj.name) = :name", whereClause);
	}

	@Test
	public void testBooleanEquals() {
		SearchCriteria criteria    = new SearchCriteria(SEARCH_PARAM_IS_ENABLED, false);
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX_AND + "obj.isEnabled = :isEnabled", whereClause);
	}

	@Test
	public void testDateEquals() {
		SearchCriteria criteria    = new SearchCriteria(SEARCH_PARAM_CREATED_TIME, new Date());
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX_AND + "obj.createdTime = :createdTime", whereClause);
	}

	@Test
	public void testStringContains() {
		SearchCriteria criteria    = new SearchCriteria(SEARCH_PARAM_NAME_CONTAINS, "test-name");
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX_AND + "LOWER(obj.name) like :nameContains", whereClause);
	}

	@Test
	public void testDateFrom() {
		SearchCriteria criteria    = new SearchCriteria(SEARCH_PARAM_CREATED_TIME_FROM, new Date());
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX_AND + "obj.createdTime >= :createdTimeFrom", whereClause);
	}

	@Test
	public void testDateTo() {
		SearchCriteria criteria    = new SearchCriteria(SEARCH_PARAM_CREATED_TIME_TO, new Date());
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX_AND + "obj.createdTime < :createdTimeTo", whereClause);
	}

	@Test
	public void testIntNotEquals() {
		SearchCriteria criteria    = new SearchCriteria(SEARCH_PARAM_EXCLUDE_ID, 1);
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX_AND + "obj.id != :excludeId", whereClause);
	}

	@Test
	public void testStringNotEquals() {
		SearchCriteria criteria    = new SearchCriteria(SEARCH_PARAM_EXCLUDE_NAME, "test-name");
		String         whereClause = searchUtil.buildWhereClause(criteria, searchFields).toString();

		Assert.assertEquals(WHERE_PREFIX_AND + "LOWER(obj.name) != :excludeName", whereClause);
	}
}