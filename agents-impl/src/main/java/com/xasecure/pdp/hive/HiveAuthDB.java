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

package com.xasecure.pdp.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

import com.xasecure.authorization.hive.XaHiveObjectAccessInfo;
import com.xasecure.authorization.hive.XaHiveObjectAccessInfo.HiveAccessType;
import com.xasecure.authorization.hive.XaHiveObjectAccessInfo.HiveObjectType;
import com.xasecure.authorization.utils.StringUtil;

public class HiveAuthDB {

	private static final Log LOG = LogFactory.getLog(HiveAuthDB.class);

	private ArrayList<HiveAuthRule> allRuleList = null;
	private ArrayList<HiveAuthRule> tblRuleList = null;
	private ArrayList<HiveAuthRule> colRuleList = null;

	public HiveAuthDB() {
		this(null) ;
	}


	public HiveAuthDB(ArrayList<HiveAuthRule> aRuleList) {
		
		if (aRuleList == null) {
			aRuleList = new ArrayList<HiveAuthRule>() ;
		}
		
		LOG.info("Number of Rules in the PolicyContainer: " +  ((aRuleList == null) ? 0 : aRuleList.size()) ) ; 
		
		allRuleList = new ArrayList<HiveAuthRule>() ;
		colRuleList = new  ArrayList<HiveAuthRule>();
		tblRuleList = new  ArrayList<HiveAuthRule>() ;
		
		allRuleList = aRuleList ;
		
		for (HiveAuthRule rule : aRuleList) {
			if (rule.isTableRule()) {
				this.tblRuleList.add(rule);
			} else {
				this.colRuleList.add(rule);
			}
		}
		
	}

	public boolean isAccessAllowed(UserGroupInformation ugi, XaHiveObjectAccessInfo objAccessInfo) {
		boolean ret = false;

		if(objAccessInfo.getAccessType() == HiveAccessType.NONE || objAccessInfo.getObjectType() == HiveObjectType.NONE) {
			return true;
		}
		
		String accessType = objAccessInfo.getAccessType().name();

		switch(objAccessInfo.getObjectType()) {
			case DATABASE:
				ret = isAccessAllowed(ugi, accessType, objAccessInfo.getDatabase());
			break;

			case TABLE:
			case INDEX:
			case PARTITION:
				ret = isAccessAllowed(ugi, accessType, objAccessInfo.getDatabase(), objAccessInfo.getTable());
			break;

			case VIEW:
				ret = isAccessAllowed(ugi, accessType, objAccessInfo.getDatabase(), objAccessInfo.getView());
			break;

			case COLUMN:
			{
				String deniedColumn = findDeniedColumn(ugi, accessType, objAccessInfo.getDatabase(), objAccessInfo.getTable(), objAccessInfo.getColumns());
				
				ret = StringUtil.isEmpty(deniedColumn);
				
				if(! ret) {
					objAccessInfo.setDeinedObjectName(XaHiveObjectAccessInfo.getObjectName(objAccessInfo.getDatabase(), objAccessInfo.getTable(), deniedColumn));
				}
			}
			break;

			case FUNCTION:
				ret = isUDFAccessAllowed(ugi, accessType, objAccessInfo.getDatabase(), objAccessInfo.getFunction());
			break;

			case URI:
				// Handled in XaSecureHiveAuthorizer
			break;

			case NONE:
			break;
		}

		return ret;
	}

	public boolean isAudited(XaHiveObjectAccessInfo objAccessInfo) {
		boolean ret = false;

		if(   objAccessInfo.getAccessType() == HiveAccessType.NONE
           || objAccessInfo.getObjectType() == HiveObjectType.NONE
           || objAccessInfo.getObjectType() == HiveObjectType.URI
           ) {
			return false;
		}
		
		String       database = null;
		String       table    = null;
		List<String> columns  = null;
		boolean      isUDF    = false;
		
		switch(objAccessInfo.getObjectType()) {
			case DATABASE:
				database = objAccessInfo.getDatabase();
			break;

			case TABLE:
			case INDEX:
			case PARTITION:
				database = objAccessInfo.getDatabase();
				table    = objAccessInfo.getTable();
			break;

			case VIEW:
				database = objAccessInfo.getDatabase();
				table    = objAccessInfo.getView();
			break;

			case COLUMN:
				database = objAccessInfo.getDatabase();
				table    = objAccessInfo.getTable();
				columns  = objAccessInfo.getColumns();
			break;

			case FUNCTION:
				database = objAccessInfo.getDatabase();
				table    = objAccessInfo.getFunction();
				isUDF    = true;
			break;

			case NONE:
			case URI:
			break;
		}
		
		if(StringUtil.isEmpty(columns)) {
			for (HiveAuthRule rule : allRuleList) {
				if(isUDF != rule.isUdf()) {
					continue;
				}

				if (rule.isTableMatch(database, table)) {
					ret = rule.isAudited() ;

					if (ret) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("isAudited(database=" + database + ", table=" + table + ", columns=" + StringUtil.toString(columns) + ") => [" + ret + "] as matched for rule: " + rule);
						}

						break ;
					}
				}
			}
		} else {
			// is audit enabled for any one column being accessed?
			for(String colName : columns) {
				for (HiveAuthRule rule : allRuleList) {
					if(isUDF != rule.isUdf()) {
						continue;
					}

					ret = rule.isMatched(database, table, colName) && rule.isAudited();

					if (ret) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("isAudited(database=" + database + ", table=" + table + ", columns=" + StringUtil.toString(columns) + ") => [" + ret + "] as matched for rule: " + rule);
						}

						break ;
					}
				}
				
				if(ret) {
					break;
				}
			}
		}

		return ret ;
	}

	private boolean isAccessAllowed(UserGroupInformation ugi, String accessType, String database) {
		boolean ret = false;

		for (HiveAuthRule rule : allRuleList) {
			if(rule.isUdf()) {
				continue;
			}

			ret = rule.isMatched(database, ugi.getShortUserName(), ugi.getGroupNames(), accessType);

			if(ret) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("isAccessAllowed(user=" + ugi.getShortUserName() + ", groups=" + StringUtil.toString(ugi.getGroupNames()) + ", accessType=" + accessType + ", database=" + database + ") => [" + ret + "] as matched for rule: " + rule);
				}

				break;
			}
		}

		return ret;
	}

	private boolean isAccessAllowed(UserGroupInformation ugi, String accessType, String database, String tableOrView) {
		boolean ret = false;

		for (HiveAuthRule rule : tblRuleList) {
			if(rule.isUdf()) {
				continue;
			}

			ret = rule.isMatched(database, tableOrView, ugi.getShortUserName(), ugi.getGroupNames(), accessType);

			if(ret) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("isAccessAllowed(user=" + ugi.getShortUserName() + ", groups=" + StringUtil.toString(ugi.getGroupNames()) + ", accessType=" + accessType + ", database=" + database + ", tableOrView=" + tableOrView + ") => [" + ret + "] as matched for rule: " + rule);
				}

				break;
			}
		}

		return ret;
	}

	private String findDeniedColumn(UserGroupInformation ugi, String accessType, String database, String tableOrView, List<String> columns) {
		String deinedColumn = null;

		boolean isAllowed = isAccessAllowed(ugi, accessType, database, tableOrView); // check if access is allowed at the table level

		if(!isAllowed && !StringUtil.isEmpty(columns)) {
			for(String column : columns) {
				for (HiveAuthRule rule : colRuleList) {
					isAllowed = rule.isMatched(database, tableOrView, column, ugi.getShortUserName(), ugi.getGroupNames(), accessType);

					if(isAllowed) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("isAccessAllowed(user=" + ugi.getShortUserName() + ", groups=" + StringUtil.toString(ugi.getGroupNames()) + ", accessType=" + accessType + ", database=" + database + ", tableOrView=" + tableOrView + ", column=" + column + ") => [" + isAllowed + "] as matched for rule: " + rule);
						}

						break;
					}
				}
				
				if(!isAllowed) {
					deinedColumn = column;

					if (LOG.isDebugEnabled()) {
						LOG.debug("isAccessAllowed(user=" + ugi.getShortUserName() + ", groups=" + StringUtil.toString(ugi.getGroupNames()) + ", accessType=" + accessType + ", database=" + database + ", tableOrView=" + tableOrView + ", column=" + column + ") => [" + isAllowed + "]");
					}
					break;
				}
			}
		}

		return deinedColumn;
	}

	private boolean isUDFAccessAllowed(UserGroupInformation ugi, String accessType, String database, String udfName) {
		boolean ret = false;

		for (HiveAuthRule rule : tblRuleList) {
			if(! rule.isUdf()) {
				continue;
			}

			ret = rule.isMatched(database, udfName, ugi.getShortUserName(), ugi.getGroupNames(), accessType);

			if(ret) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("isAccessAllowed(user=" + ugi.getShortUserName() + ", groups=" + StringUtil.toString(ugi.getGroupNames()) + ", accessType=" + accessType + ", database=" + database + ", udfName=" + udfName + ") => [" + ret + "] as matched for rule: " + rule);
				}

				break;
			}
		}

		return ret;
	}
}
