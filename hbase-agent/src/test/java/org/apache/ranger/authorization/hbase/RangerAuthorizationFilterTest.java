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

package org.apache.ranger.authorization.hbase;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.junit.Test;

import com.google.common.collect.Sets;

@SuppressWarnings("deprecation")
public class RangerAuthorizationFilterTest {

	@Test
	public void testFilterKeyValueCell_happyPath() throws IOException {

		// null/empty column collection in cache for a family implies family level access  
		Map<String, Set<String>> cache = new HashMap<String, Set<String>>();
		cache.put("family1", Collections.<String> emptySet());
		RangerAuthorizationFilter filter = new RangerAuthorizationFilter(cache);

		Cell aCell = mock(Cell.class);
		when(aCell.getFamily()).thenReturn("family1".getBytes());
		when(aCell.getQualifier()).thenReturn("column1".getBytes());
		assertEquals(ReturnCode.INCLUDE, filter.filterKeyValue(aCell));

		when(aCell.getQualifier()).thenReturn("column2".getBytes());
		assertEquals(ReturnCode.INCLUDE, filter.filterKeyValue(aCell));
		
		// null empty column collection in REQUEST implies family level access
		when(aCell.getQualifier()).thenReturn(null);
		assertEquals(ReturnCode.INCLUDE, filter.filterKeyValue(aCell));
		
		// specific columns in cache should be allowed only if there is a match of family and column
		cache.clear();
		cache.put("family1", Sets.newHashSet("column11", "column12"));
		cache.put("family2", Sets.newHashSet("column21", "column22"));
		filter = new RangerAuthorizationFilter(cache);

		when(aCell.getFamily()).thenReturn("family1".getBytes());
		when(aCell.getQualifier()).thenReturn("column11".getBytes());
		assertEquals(ReturnCode.INCLUDE, filter.filterKeyValue(aCell));
		when(aCell.getQualifier()).thenReturn("column12".getBytes());
		assertEquals(ReturnCode.INCLUDE, filter.filterKeyValue(aCell));
		when(aCell.getQualifier()).thenReturn("column13".getBytes());
		assertEquals(ReturnCode.NEXT_COL, filter.filterKeyValue(aCell));

		when(aCell.getFamily()).thenReturn("family2".getBytes());
		when(aCell.getQualifier()).thenReturn("column22".getBytes());
		assertEquals(ReturnCode.INCLUDE, filter.filterKeyValue(aCell));

		when(aCell.getFamily()).thenReturn("family3".getBytes());
		when(aCell.getQualifier()).thenReturn("column11".getBytes());
		assertEquals(ReturnCode.NEXT_COL, filter.filterKeyValue(aCell));
		
		// asking for family level access when one doesn't exist (colum collection for a family is not null/empty then it should get denied
		when(aCell.getFamily()).thenReturn("family1".getBytes());
		when(aCell.getQualifier()).thenReturn(null);
		assertEquals(ReturnCode.NEXT_COL, filter.filterKeyValue(aCell));
	}

	@Test
	public void testFilterKeyValueCell_firewalling() throws IOException {
		// null cache will deny everything.
		RangerAuthorizationFilter filter = new RangerAuthorizationFilter(null);
		Cell aCell = mock(Cell.class);
		when(aCell.getFamily()).thenReturn("family1".getBytes());
		when(aCell.getQualifier()).thenReturn("column1".getBytes());
		assertEquals(ReturnCode.NEXT_COL, filter.filterKeyValue(aCell));

		// non-null but empty cache should do the same
		Map<String, Set<String>> cache = new HashMap<String, Set<String>>();
		filter = new RangerAuthorizationFilter(cache);
		assertEquals(ReturnCode.NEXT_COL, filter.filterKeyValue(aCell));

		// Null family in request would get denied, too
		when(aCell.getFamily()).thenReturn(null);
		when(aCell.getQualifier()).thenReturn("column1".getBytes());
		assertEquals(ReturnCode.NEXT_COL, filter.filterKeyValue(aCell));
	}
}
