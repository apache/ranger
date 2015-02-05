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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class RangerAuthorizationFilter extends FilterBase {

	private static final Log LOG = LogFactory.getLog(RangerAuthorizationFilter.class.getName());
	final Map<String, Set<String>> _cache;

	public RangerAuthorizationFilter(Map<String, Set<String>> cache) {
		_cache = cache;
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public ReturnCode filterKeyValue(Cell kv) throws IOException {
		
		if (_cache == null || _cache.isEmpty()) {
			LOG.debug("filterKeyValue: if cache is null or empty then there is no hope for any access. Denied!");
			return ReturnCode.NEXT_COL;
		}
		
		byte[] familyBytes = kv.getFamily();
		if (familyBytes == null || familyBytes.length == 0) {
			LOG.debug("filterKeyValue: null/empty families in request! Denied!");
			return ReturnCode.NEXT_COL;
		}
		String family = Bytes.toString(familyBytes);
		if (LOG.isDebugEnabled()) {
			LOG.debug("filterKeyValue: Evaluating family[" + family + "]");
		}
		
		if (!_cache.containsKey(family)) {
			LOG.debug("filterKeyValue: Cache map did not contain the family, i.e. nothing in family has access! Denied!");
			return ReturnCode.NEXT_COL;
		}
		Set<String> columns = _cache.get(family);
		
		if (CollectionUtils.isEmpty(columns)) {
			LOG.debug("filterKeyValue: empty/null column set in cache for family implies family level access. No need to bother with column level.  Allowed!");
			return ReturnCode.INCLUDE;
		}		
		byte[] columnBytes = kv.getQualifier();
		if (columnBytes == null || columnBytes.length == 0) {
			LOG.debug("filterKeyValue: empty/null column set in request implies family level access, which isn't available.  Denied!");
			return ReturnCode.NEXT_COL;
		}
		String column = Bytes.toString(columnBytes);
		if (LOG.isDebugEnabled()) {
			LOG.debug("filterKeyValue: Evaluating column[" + column + "]");
		}
		if (columns.contains(column)) {
			LOG.debug("filterKeyValue: cache contains Column in column-family's collection.  Access allowed!");
			return ReturnCode.INCLUDE;
		} else {
			LOG.debug("filterKeyValue: cache missing Column in column-family's collection.  Access denied!");
			return ReturnCode.NEXT_COL;
		}
	}
}
