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

package com.xasecure.pdp.hbase;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;

import com.xasecure.pdp.constants.XaSecureConstants;

public class HBaseAuthRules {
	private String tableName ;
	private String columnGroupName; 
	private String columnName ;
	private String accessType ;
	private String group ;
	private String user ;
	private String fullyQualifiedColumnName ;
	
	private static final Log LOG = LogFactory.getLog(HBaseAuthRules.class) ;
		
	public HBaseAuthRules(String tableName, String columnGroupName, String columnName, String accessType, String user, String group) {
		this.tableName = tableName;
		this.columnGroupName = columnGroupName;
		this.columnName = columnName;
		if (accessType != null) {
			this.accessType = accessType.toLowerCase() ;
		}
		this.user = user ;
		this.group = group;
		this.fullyQualifiedColumnName = tableName + "/" + columnGroupName + "/" + columnName ;
	}
	
	public String getTableName() {
		return tableName;
	}
	public String getColumnGroupName() {
		return columnGroupName;
	}
	public String getColumnName() {
		return columnName;
	}
	public String getAccessType() {
		return accessType;
	}
	public String getGroup() {
		return group;
	}
	
	public String getUser() {
		return user;
	}

	@Override
	public String toString() {
		return "table: " + tableName + ", columnGroup:" + columnGroupName + ", columnName: " + columnName + ", accessType: " + accessType + ", user:" + user + ", group: " + group ;
	}
	
	public boolean isMatched(String FQColName) {
		return FQColName.equals(fullyQualifiedColumnName) || FilenameUtils.wildcardMatch(FQColName,fullyQualifiedColumnName) ;
	}

	public boolean isGlobalRule() {
		return ("*".equals(tableName) && "*".equals(columnGroupName) && "*".equals(columnName)) ;
	}

	public boolean isTableRule() {
		return ( ("*".equals(columnGroupName) && "*".equals(columnName)) || ("admin".equals(accessType)  || "control".equals(accessType)) )  ;
	}

	public boolean isTableNameMatched(String tableNameStr) {
		boolean ret =  (tableNameStr == null) || (tableNameStr.equals(tableName)) || FilenameUtils.wildcardMatch(tableNameStr,tableName) ;
		if (LOG.isDebugEnabled()) {
			LOG.debug("TableMatched returns (" + tableNameStr + ", rule:" + tableName + ") returns: " + ret );
		}
		return  ret ;
	}
	
	public UserPermission getUserPermission(User aUser) {
		
		if (user == null) {
			return null  ;
		}
		
		Permission.Action action = null ;
		
		try {
			action = Permission.Action.valueOf(accessType.toUpperCase()) ;
		} catch (Throwable e) {
			return null ;
		}
		
		if (XaSecureConstants.PUBLIC_ACCESS_ROLE.equals(group)) {
			return new UserPermission("public".getBytes(), TableName.valueOf (  tableName )   , columnGroupName.getBytes(), columnName.getBytes(), action) ;
		}

		if (user != null) {
			if (aUser.getShortName().equals(user)) {
				return new UserPermission(("user:(" + aUser.getShortName() + ")").getBytes(), TableName.valueOf( tableName )  , columnGroupName.getBytes(), columnName.getBytes(), action) ;
			}
		}
		
		if (group != null) {
			for (String ugroups : aUser.getGroupNames()) {
				if (ugroups.equals(group)) {
					return new UserPermission(("group:(" + ugroups + ")").getBytes(), TableName.valueOf( tableName ) , columnGroupName.getBytes(), columnName.getBytes(), action) ;
				}
			}
		}
		
		return null;
	}

}
