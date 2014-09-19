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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import com.xasecure.authorization.hbase.HBaseAccessController;
import com.xasecure.pdp.constants.XaSecureConstants;

public class HBaseAuthDB implements HBaseAccessController {
	
	private static final long MAX_CACHE_AUDIT_ENTRIES = 1000L ;
	private static final long MAX_CACHE_ENCRYPT_ENTRIES = 1000L ;
	
	private static final Log LOG = LogFactory.getLog(HBaseAuthDB.class) ;
	
	private ArrayList<HBaseAuthRules> ruleList = null;
	private ArrayList<HBaseAuthRules> globalList = null;
	private ArrayList<HBaseAuthRules> tableList = null;

	private ArrayList<String> 	auditList = null ;
	private HashMap<byte[],Boolean> cachedAuditTable = new HashMap<byte[],Boolean>() ;
	
	private ArrayList<String>	encryptList = null ;
	
	private HashSet<String>     encryptTableList = null ;
	private HashMap<byte[],Boolean> cachedEncryptedTable = new HashMap<byte[],Boolean>() ;


	public HBaseAuthDB(ArrayList<HBaseAuthRules> ruleList, ArrayList<String> auditList, ArrayList<String> encryptList) {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("+Creating HBaseAuthDB is creating with ruleList [" + (ruleList == null ? 0 : ruleList.size()) + "]" );
		}
		
		this.auditList = auditList;
		this.encryptList = encryptList;
		

		this.ruleList = new ArrayList<HBaseAuthRules>() ;
		this.globalList = new ArrayList<HBaseAuthRules>() ;
		this.tableList = new ArrayList<HBaseAuthRules>() ;
		
		for(HBaseAuthRules rule : ruleList ) {
			if (rule.isGlobalRule()) {
				this.globalList.add(rule) ;
				if (LOG.isDebugEnabled()) {
					LOG.debug("RULE:[" + rule + "] is being added as GLOBAL Policy");
				}
			}
			else if (rule.isTableRule()) {
				this.tableList.add(rule) ;
				if (LOG.isDebugEnabled()) {
					LOG.debug("RULE:[" + rule + "] is being added as Table Policy");
				}
			}
			else {
				this.ruleList.add(rule) ;
				if (LOG.isDebugEnabled()) {
					LOG.debug("RULE:[" + rule + "] is being added as non-global, non-table Policy");
				}
			}
		}
		
		this.encryptTableList = new HashSet<String>() ;

		if (encryptList != null && encryptList.size() > 0) {
			for(String encryptKey : encryptList) {
				String[] objKeys = encryptKey.split("/") ;
				String tableName = objKeys[0] ;
				if (! encryptTableList.contains(tableName)) {
					encryptTableList.add(tableName) ;
					if (LOG.isDebugEnabled()) {
						LOG.debug("EncryptionList:[" + tableName + "] is being added encrypted table.");
					}
				}
			}
		}
		

	}
	
	
	public boolean isAccessAllowed(User user, Action accessAction) {
		

		String access = accessAction.toString().toLowerCase() ;

		if (user == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("rulecheck(GLOBAL," + access + ") => [FALSE] as user passed for check was null.");
			}
			return false ;
		}
		
		
		String username = user.getShortName() ;
		
		String[] groups = user.getGroupNames() ;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Init of Global access Verification - [" + access + "] for user [" + username + "], groups: [" + Arrays.toString(groups) + "]");
		}

		for (HBaseAuthRules rule : globalList) {
			
			if (rule.getAccessType().equals(access)) {
				
				String authorizedUser = rule.getUser() ;
				String authorizedGroup = rule.getGroup();
				
				if (authorizedGroup != null) {
					if (XaSecureConstants.PUBLIC_ACCESS_ROLE.equals(authorizedGroup)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("rulecheck(GLOBAL," + access + "," + username + "," + StringUtils.arrayToString(groups)  + ") => [TRUE] as matched for rule: " + rule);
						}
						return true ;
					}

					for (String group : groups) {
						if (group.equals(authorizedGroup)) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("rulecheck(GLOBAL," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [TRUE] as matched for rule: " + rule);
							}
							return true;
						}
					}
				}

				if (authorizedUser != null) {
					if (username.equals(authorizedUser)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("rulecheck(GLOBAL," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [TRUE] as matched for rule: " + rule);
						}
						return true;
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("rulecheck(GLOBAL," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [FALSE] as it did not match any rules.");
		}

		return false;
	}

	public boolean isAccessAllowed(User user, byte[] tableName, Action accessAction) {
		
		
		if ( isAccessAllowed(user,accessAction)) {							// Check Global Action
			return true ;
		}

		String tableNameStr = Bytes.toString(tableName) ;
		
		String access = accessAction.toString().toLowerCase() ;

		if (user == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("rulecheck(" + tableNameStr + "," + access + ") => [FALSE] as user passed for check was null.");
			}
			return false ;
		}
		
		String username = user.getShortName() ;
		
		String[] groups = user.getGroupNames() ;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Init of Table access Verification - [" + access + "] for user [" + username + "], groups: [" + Arrays.toString(groups) + "], tableName: [" + tableNameStr + "]");
		}
		
		for (HBaseAuthRules rule : tableList) {
			
			if (rule.isTableNameMatched(tableNameStr)) {
				if (rule.getAccessType().equals(access)) {
					
					String authorizedUser = rule.getUser() ;
					
					String authorizedGroup = rule.getGroup();
					
					if (authorizedGroup != null) {
						if (XaSecureConstants.PUBLIC_ACCESS_ROLE.equals(authorizedGroup)) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("rulecheck(" + tableNameStr + "," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [TRUE] as matched for rule: " + rule);
							}
							return true ;
						}
						
						for (String group : groups) {
							if (group.equals(authorizedGroup)) {
								if (LOG.isDebugEnabled()) {
									LOG.debug("rulecheck(" + tableNameStr + "," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [TRUE] as matched for rule: " + rule);
								}
								return true;
							}
						}
					}
					if (authorizedUser != null && username.equals(authorizedUser)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("rulecheck(" + tableNameStr + "," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [TRUE] as matched for rule: " + rule);
						}
						return true;
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("rulecheck(" + tableNameStr + "," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [FALSE] as it did not match any rules.");
		}

		return false;
	}

	
	
	
	
	
	public boolean isAccessAllowed(User user, byte[] tableName, byte[] columnFamily, byte[] qualifier, Action accessAction) {
		
		String FQColName = getFullyQualifiedColumnName(tableName, columnFamily, qualifier) ; 
		
		String access = accessAction.toString().toLowerCase() ;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("isAccessAllowed on HBaseAuthDB: for FQColName [" + FQColName + "]");
		}

		
		if (user == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("rulecheck(" + FQColName + "," + access  + ") => [FALSE] as as user passed for check was null.");
			}
			return false ;
		}
		
		
		if (isAccessAllowed(user, accessAction)) {		// Check Global Action
			return true ;
		}
		
		if (isAccessAllowed(user,tableName, accessAction)) {		// Check Table Action
			return true;
		}
		
		
		String username = user.getShortName() ;
		
		String[] groups = user.getGroupNames() ;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Init of Table access Verification - [" + access + "] for user [" + username + "], groups: [" + Arrays.toString(groups) + "], FQColumnFamily: [" + FQColName +  "]");
		}
		
		for (HBaseAuthRules rule : ruleList) {
			
			if (rule.isMatched(FQColName)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Rule [" + rule + "] matched [" + FQColName + "]");
				}
				if (rule.getAccessType().equals(access)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Access [" + rule.getAccessType() + "] matched [" + access + "]");
					}
					String authorizedUser = rule.getUser() ;
					
					String authorizedGroup = rule.getGroup();
					
					if (authorizedGroup != null) {
						if (XaSecureConstants.PUBLIC_ACCESS_ROLE.equals(authorizedGroup)) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("rulecheck(" + FQColName + "," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [TRUE] as matched for rule: " + rule);
							}
							return true ;
						}
						for (String group : groups) {
							if (group.equals(authorizedGroup)) {
								if (LOG.isDebugEnabled()) {
									LOG.debug("rulecheck(" + FQColName + "," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [TRUE] as matched for rule: " + rule);
								}
								return true;
							}
						}
					}
					
					if (authorizedUser != null) {
						if (username.equals(authorizedUser)) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("rulecheck(" + FQColName + "," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [TRUE] as matched for rule: " + rule);
							}
							return true;
						}
					}
				}
				else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Access [" + rule.getAccessType() + "] DID NOT match [" + access + "]");
					}
				}
			}
			else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Rule [" + rule + "] not matched [" + FQColName + "]");
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("rulecheck(" + FQColName + "," + access + "," + username + "," + StringUtils.arrayToString(groups) + ") => [FALSE] as it did not match any rules.");
		}

		return false;
		
	}
	
	public boolean isEncrypted(byte[] tableName, byte[] columnFamily, byte[] qualifier) {
		String colName = getFullyQualifiedColumnName(tableName, columnFamily, qualifier) ;
		for(String encryptable : encryptList) {
			if (FilenameUtils.wildcardMatch(colName,encryptable)) {
				return true ;
			}
		}
		return false;
	}
	
	public boolean isAudited(byte[] tableName) {
		Boolean ret = cachedAuditTable.get(tableName) ;
		if (ret == null) {
			ret = isAuditedFromTableList(tableName) ;
			synchronized(cachedAuditTable) {
				if (cachedAuditTable.size() > MAX_CACHE_AUDIT_ENTRIES) {
					cachedAuditTable.clear();
				}
				cachedAuditTable.put(tableName,ret) ;
			}
		}
		return ret.booleanValue();
	}
	
	private boolean isAuditedFromTableList(byte[] tableName) {
		boolean ret = false ;
		String tableNameStr = Bytes.toString(tableName) ;
		for(String auditable : auditList) {
			if (FilenameUtils.wildcardMatch(tableNameStr,auditable)) {
				ret = true ;
				break ;
			}
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("isAudited(" + tableNameStr + "):" + ret) ;
		}
		
		return ret;
	}

	
	public boolean isTableHasEncryptedColumn(byte[] tableName) {
		Boolean ret =  cachedEncryptedTable.get(tableName) ;
		if (ret == null) {
			ret = isTableHasEncryptedColumnFromTableList(tableName) ;
			synchronized(cachedEncryptedTable) {
				if (cachedEncryptedTable.size() > MAX_CACHE_ENCRYPT_ENTRIES) {
					cachedEncryptedTable.clear();
				}
				cachedEncryptedTable.put(tableName, ret) ;
			}
 		}
		return ret.booleanValue() ;
	}
	
	
	private boolean isTableHasEncryptedColumnFromTableList(byte[] tableName)
	{
		boolean ret = false ;
		
		String tableNameStr = Bytes.toString(tableName) ;

		for(String encryptTable : encryptTableList) {
			ret = FilenameUtils.wildcardMatch(tableNameStr, encryptTable) ;
			if (ret) {
				break ;
			}
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("isTableHasEncryptedColumn(" + tableNameStr + "):" + ret);
		}
		
		return ret ;
	}

	
	
	public static String getFullyQualifiedColumnName(byte[] tableName, byte[] columnFamily, byte[] qualifier) {
		StringBuilder sb = new StringBuilder() ;
		
		sb.append(((tableName != null && tableName.length > 0) ? Bytes.toString(tableName) : "*"))
		  .append("/")
		  .append(((columnFamily != null && columnFamily.length > 0) ? Bytes.toString(columnFamily) : "*"))
		  .append("/")
		  .append(((qualifier != null && qualifier.length > 0) ? Bytes.toString(qualifier) : "*")) ;

		return sb.toString() ;
	}
	
	public List<UserPermission>  getUserPermissions(User user) {
		List<UserPermission> ret = new ArrayList<UserPermission>() ;
		
		if (user != null) {
			ArrayList<ArrayList<HBaseAuthRules>> allList = new ArrayList<ArrayList<HBaseAuthRules>>();
			allList.add(globalList) ;
			allList.add(tableList) ;
			allList.add(ruleList) ;
			for(ArrayList<HBaseAuthRules> rList : allList) {
				for(HBaseAuthRules rule : rList) {
					UserPermission perm = rule.getUserPermission(user) ;
					if (perm != null) {
						ret.add(perm) ;
					}
				}
			}
		}
		
		return ret ;
	}

	public List<UserPermission>  getUserPermissions(User user, byte[] tableName) {
		
		String tableNameStr = Bytes.toString(tableName) ;
		
		List<UserPermission> ret = new ArrayList<UserPermission>() ;
		
		if (user != null) {
			ArrayList<ArrayList<HBaseAuthRules>> allList = new ArrayList<ArrayList<HBaseAuthRules>>();
			allList.add(globalList) ;
			allList.add(tableList) ;
			allList.add(ruleList) ;
			for(ArrayList<HBaseAuthRules> rList : allList) {
				for(HBaseAuthRules rule : rList) {
					if (rule.isTableNameMatched(tableNameStr)) {
						UserPermission perm = rule.getUserPermission(user) ;
						if (perm != null) {
							ret.add(perm) ;
						}
					}
				}
			}
		}
		
		return ret ;
	}



}
