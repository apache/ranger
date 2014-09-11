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

 /**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *

 /**
  *
  *	@version: 1.0.004
  *
  */

package com.xasecure.authorization.hbase;

import java.util.List;

import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;

public interface HBaseAccessController {
	public boolean isAccessAllowed(User user, Action accessAction) ;
	public boolean isAccessAllowed(User user, byte[] tableName, Action accessAction) ;
	public boolean isAccessAllowed(User user, byte[] tableName, byte[] columnFamily, byte[] qualifier, Action accessAction) ;
	public boolean isEncrypted(byte[] tableName, byte[] columnFamily, byte[] qualifier) ;
	public boolean isAudited(byte[] tableName) ;
	public boolean isTableHasEncryptedColumn(byte[] tableName) ;
	public List<UserPermission>  getUserPermissions(User user) ;
	public List<UserPermission>  getUserPermissions(User user, byte[] tableName) ;



}
