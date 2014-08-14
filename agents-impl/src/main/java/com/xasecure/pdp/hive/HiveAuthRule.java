/**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *
 *                                                                        *
 * Copyright (c) 2013 XASecure, Inc.  All rights reserved.                *
 *                                                                        *
 *************************************************************************/

 /**
  *
  *	@version: 1.0.004
  *
  */

package com.xasecure.pdp.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

import com.xasecure.authorization.hive.constants.XaSecureHiveConstants;
import com.xasecure.authorization.utils.StringUtil;


public class HiveAuthRule {
	
	private static final Log LOG = LogFactory.getLog(HiveAuthRule.class) ;

	public static final String WILDCARD_OBJECT = ".*" ;
	
	private String databaseName;     
	private String tableName;  
	private String columnName;
	private String accessType;
	private String group;
	private String user;
	private boolean tableRule      = false;
	private boolean allGranted     = false;
	private boolean udf            = false;
	private boolean tableExcluded  = false;
	private boolean columnExcluded = false;
	private boolean audited        = false;
	private boolean encrypted      = false;

	public HiveAuthRule(String dbName, String tableName, String colName, String permission, String user, String group) {
		this(false, dbName,tableName,colName,permission,user,group, false, false) ;
	}
	
	public HiveAuthRule(boolean udfInd,  String dbName, String tableName, String colName, String permission, String user, String group, boolean tableExclusionFlag, boolean columnExclusionFlag) {
		this.udf            = udfInd ;
		this.databaseName   = StringUtil.toLower(dbName);
		this.tableName      = StringUtil.toLower(tableName);
		this.columnName     = StringUtil.toLower(colName);
		this.accessType     = permission ;
		this.user           = user;
		this.group          = group ;
		this.tableExcluded  = tableExclusionFlag ;
		this.columnExcluded = columnExclusionFlag ;

		this.allGranted = StringUtil.equalsIgnoreCase(Privilege.ALL.toString(), accessType);

		tableRule = StringUtil.isEmpty(columnName) || WILDCARD_OBJECT.matches(columnName) ;
	}
	
	@Override
	public String toString() {
		return "db:" + databaseName + ", table: " + tableName + ", columnName: " + columnName + ", accessType: " + accessType + ",user: " + user +  ", group: " + group + ",isTable:" + tableRule + ",audited:"  + audited + ",encrypted:" + encrypted ;
	}

	public boolean isMatched(String user, String[] groups, String accessType) {
		String dbName  = null;
		String tblName = null;
		String colName = null;

		return isMatched(dbName, tblName, colName, user, groups, accessType) ;
	}

	public boolean isMatched(String dbName, String user, String[] groups, String accessType) {
		String tblName = null;
		String colName = null;

		return isMatched(dbName, tblName, colName, user, groups, accessType) ;
	}
	
	public boolean isMatched(String dbName, String tblName, String user, String[] groups, String accessType) {
		String colName = null;

		return isMatched(dbName, tblName, colName, user, groups, accessType) ;
	}

	public boolean isMatched(String dbName, String tblName, String colName,  String user, String[] groups, String accessType) {
		boolean ret = isMatched(dbName, tblName, colName);

		if(ret) {
			// does accessType match?
			ret = this.isAllGranted() || StringUtil.equals(accessType, this.accessType) || StringUtil.equalsIgnoreCase(accessType, "USE");

			if(ret) {
				// does user/group match?
				ret = StringUtil.equals(user, this.user) ||
				      StringUtil.equals(XaSecureHiveConstants.PUBLIC_ACCESS_ROLE, this.group) ||
				      StringUtil.contains(groups, this.group);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("isMatched(db=" + dbName + ", table=" + tblName + ", col=" + colName + ", user=" + user + ", groups=" + StringUtil.toString(groups) + ", accessType=" + accessType + ") => rule[" + this.databaseName + ":" +  this.tableName + ":" + this.columnName + ":" + this.user + ":" + this.group + ":" + this.accessType + "] returns [" + ret + "]");
		}

		return ret ;
	}

	public boolean isMatched(String dbName, String tblName, String colName) {
		boolean ret = isTableMatch(dbName, tblName);

		if (ret) {
	 		colName = StringUtil.toLower(colName);

	 		if (colName != null) {
				ret = colName.matches(this.columnName);

				if (columnExcluded) {
					ret = (! ret) ;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("isMatched(db=" + dbName + ", table=" + tblName + ", col=" + colName + ") => rule[" + this.databaseName + ":" +  this.tableName + ":" + this.columnName + "] returns [" + ret + "]");
		}

		return ret ;
	}

	public boolean isTableMatch(String dbName, String tblName) {
		boolean ret = isDBMatch(dbName);

		if(ret) {
			tblName = StringUtil.toLower(tblName);

			if(tblName != null) {
				ret = tblName.matches(this.tableName);

				if(tableExcluded) {
					ret = !ret;
				}
			}
		}
		
		return ret;
	}

	public boolean isDBMatch(String dbName) {
		boolean ret = false;
		
		dbName = StringUtil.toLower(dbName);
		
		ret = dbName == null || dbName.matches(this.databaseName);
		
		return ret;
	}

	public String getDbName() {
		return databaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getAccessType() {
		return accessType;
	}
	
	public String getUser() {
		return user;
	}

	public String getGroup() {
		return group;
	}

	public boolean isTableRule() {
		return tableRule;
	}

	public boolean isAllGranted() {
		return allGranted ;
	}

	public boolean isUdf() {
		return udf;
	}

	public boolean isAudited() {
		return audited;
	}

	public void setAudited(boolean audited) {
		this.audited = audited;
	}

	public boolean isEncrypted() {
		return encrypted;
	}

	public void setEncrypted(boolean encrypted) {
		this.encrypted = encrypted;
	}
}
