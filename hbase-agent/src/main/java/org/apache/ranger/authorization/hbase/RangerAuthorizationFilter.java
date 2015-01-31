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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class RangerAuthorizationFilter extends FilterBase {

	final Map<String, Set<String>> _cache;

	public RangerAuthorizationFilter(Map<String, Set<String>> cache) {
		_cache = cache;
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public ReturnCode filterKeyValue(Cell kv) throws IOException {
		// if our cache is null or empty then there is no hope for any access
		if (_cache == null || _cache.isEmpty()) {
			return ReturnCode.NEXT_COL;
		}
		// null/empty families are denied
		byte[] familyBytes = kv.getFamily();
		if (familyBytes == null || familyBytes.length == 0) {
			return ReturnCode.NEXT_COL;
		}
		String family = Bytes.toString(familyBytes);
		// null/empty columns are also denied
		byte[] columnBytes = kv.getQualifier();
		if (columnBytes == null || columnBytes.length == 0) {
			return ReturnCode.NEXT_COL;
		}
		String column = Bytes.toString(columnBytes);
		// allow if cache contains the family/column in it
		Set<String> columns = _cache.get(family);
		if (columns == null || columns.isEmpty()) {
			// column family with a null/empty set of columns means all columns within that family are allowed
			return ReturnCode.INCLUDE;
		}
		else if (columns.contains(column)) {
			return ReturnCode.INCLUDE;
		} else {
			return ReturnCode.NEXT_COL;
		}
	}
}
