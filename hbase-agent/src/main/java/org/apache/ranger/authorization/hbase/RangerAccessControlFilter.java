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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.TablePermission;

public class RangerAccessControlFilter extends FilterBase {

	private byte[] table = null;
	private User user = null;

	public RangerAccessControlFilter(User ugi, byte[] tableName) {
		table = tableName;
		user = ugi;
	}
	

	@SuppressWarnings("deprecation")
	@Override
	public ReturnCode filterKeyValue(Cell kv) throws IOException {
		HBaseAccessController accessController = HBaseAccessControllerFactory.getInstance();
		if (accessController.isAccessAllowed(user, table, kv.getFamily(), kv.getQualifier(), TablePermission.Action.READ)) {
			return ReturnCode.INCLUDE;
		} else {
			return ReturnCode.NEXT_COL;
		}
	}

}
