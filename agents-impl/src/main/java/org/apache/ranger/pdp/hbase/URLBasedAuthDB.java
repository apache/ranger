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

package org.apache.ranger.pdp.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hbase.HBaseAccessController;
import org.apache.ranger.pdp.config.PolicyChangeListener;
import org.apache.ranger.pdp.config.PolicyRefresher;
import org.apache.ranger.pdp.constants.RangerConstants;
import org.apache.ranger.pdp.model.Policy;
import org.apache.ranger.pdp.model.PolicyContainer;
import org.apache.ranger.pdp.model.RolePermission;

public class URLBasedAuthDB implements HBaseAccessController, PolicyChangeListener {

	private static final Log LOG = LogFactory.getLog(URLBasedAuthDB.class);

	private HBaseAuthDB authDB = null;
	
	private static URLBasedAuthDB me = null ;
	
	private PolicyRefresher refresher = null ;
	
	public static URLBasedAuthDB getInstance() {
		if (me == null) {
			synchronized(URLBasedAuthDB.class) {
				URLBasedAuthDB temp = me ;
				if (temp == null) {
					me = new URLBasedAuthDB() ;
					me.init() ;
				}
			}
		}
		return me ;
	}

	
	private URLBasedAuthDB() {
		String url 			 = RangerConfiguration.getInstance().get(RangerConstants.RANGER_HBASE_POLICYMGR_URL_PROP);
		long  refreshInMilli = RangerConfiguration.getInstance().getLong(
				RangerConstants.RANGER_HBASE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP,
				RangerConstants.RANGER_HBASE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT);
		
		String lastStoredFileName = RangerConfiguration.getInstance().get(RangerConstants.RANGER_HBASE_LAST_SAVED_POLICY_FILE_PROP) ;

		String sslConfigFileName = RangerConfiguration.getInstance().get(RangerConstants.RANGER_HBASE_POLICYMGR_SSL_CONFIG_FILE_PROP) ;
		refresher = new PolicyRefresher(url, refreshInMilli,sslConfigFileName,lastStoredFileName) ;

		String saveAsFileName = RangerConfiguration.getInstance().get(RangerConstants.RANGER_HBASE_POLICYMGR_URL_SAVE_FILE_PROP) ;
		if (saveAsFileName != null) {
			refresher.setSaveAsFileName(saveAsFileName) ;
		}
		
		if (lastStoredFileName != null) {
			refresher.setLastStoredFileName(lastStoredFileName);
		}	
	}
	
	private void init() {
		refresher.setPolicyChangeListener(this);
	}
	
	public boolean isAccessAllowed(User user, Action accessAction) {
		if (authDB != null) {
			return authDB.isAccessAllowed(user, accessAction);
		} else {
			return false;
		}
	}

	public boolean isAccessAllowed(User user, byte[] tableName, Action accessAction) {
		if (authDB != null) {
			return authDB.isAccessAllowed(user, tableName, accessAction);
		} else {
			return false;
		}
	}


	public boolean isAccessAllowed(User user, byte[] tableName, byte[] columnFamily, byte[] qualifier, Action accessAction) {
		if (authDB != null) {
			return authDB.isAccessAllowed(user, tableName, columnFamily, qualifier, accessAction);
		} else {
			return false;
		}
	}

	public boolean isEncrypted(byte[] tableName, byte[] columnFamily, byte[] qualifier) {
		if (authDB != null) {
			return authDB.isEncrypted(tableName, columnFamily, qualifier);
		} else {
			return false;
		}
	}
	
	public boolean isTableHasEncryptedColumn(byte[] tableName) {
		if (authDB != null) {
			return authDB.isTableHasEncryptedColumn(tableName);
		} else {
			return false;
		}
	}


	public boolean isAudited(byte[] tableName) {
		if (authDB != null) {
			return authDB.isAudited(tableName);
		} else {
			return false;
		}
	}
	
	public List<UserPermission> getUserPermissions(User aUser) {
		if (authDB != null) {
			return authDB.getUserPermissions(aUser) ;
		} else {
			return null;
		}
	}

	public List<UserPermission> getUserPermissions(User aUser, byte[] aTableName) {
		if (authDB != null) {
			return authDB.getUserPermissions(aUser, aTableName) ;
		} else {
			return null;
		}
	}

	@Override
	public void OnPolicyChange(PolicyContainer aPolicyContainer) {
		
		if (aPolicyContainer == null) {
			return ;
		}

		ArrayList<HBaseAuthRules> ruleListTemp = new ArrayList<HBaseAuthRules>();
		
		HBaseAuthRules globalRule = new HBaseAuthRules(".META.", "*", "*", "read", null, RangerConstants.PUBLIC_ACCESS_ROLE) ;
		ruleListTemp.add(globalRule) ;
		globalRule = new HBaseAuthRules("-ROOT-", "*", "*", "read", null, RangerConstants.PUBLIC_ACCESS_ROLE) ;
		ruleListTemp.add(globalRule) ;

		ArrayList<String> auditListTemp = new ArrayList<String>();

		ArrayList<String> encryptList = new ArrayList<String>();
		
		for(Policy acl : aPolicyContainer.getAcl()) {
			
			if (! acl.isEnabled()) {
				LOG.debug("Diabled acl found [" + acl + "]. Skipping this acl ...") ;
				continue ;
			}
			
			for(String table : acl.getTableList()) {
				for(String colfamily : acl.getColumnFamilyList()) {
					for(String col : acl.getColumnList()) {
						if (table == null || table.isEmpty()) {
							table = "*" ;
						}
						if (colfamily == null || colfamily.isEmpty()) {
							colfamily = "*" ;
						}
						if (col == null || col.isEmpty()) {
							col = "*" ;
						}
						
						if (acl.getAuditInd() == 1) {
							if (!auditListTemp.contains(table)) {
								LOG.debug("Adding [" + table + "] to audit list");
								auditListTemp.add(table);
							}
						}

						if (acl.getEncryptInd() == 1) {
							String fqn = table + "/" + colfamily + "/" + col ;
							if (!encryptList.contains(fqn)) {
								LOG.debug("Adding [" + fqn + "] to encrypt list");
								encryptList.add(fqn);
							}
						}
						
						for(RolePermission rp : acl.getPermissions()) {
							for (String accessLevel : rp.getAccess() ) {
								if (rp.getGroups() != null && rp.getGroups().size() > 0) {
									for (String group : rp.getGroups()) {
										HBaseAuthRules rule = new HBaseAuthRules(table, colfamily, col, accessLevel, null, group);
										LOG.debug("Adding (group) rule: [" + rule + "]") ;
										ruleListTemp.add(rule);
									}
								}
								if (rp.getUsers() != null && rp.getUsers().size() > 0) {
									for (String user : rp.getUsers()) {
										HBaseAuthRules rule = new HBaseAuthRules(table, colfamily, col, accessLevel, user, null);
										LOG.debug("Adding (user) rule: [" + rule + "]") ;
										ruleListTemp.add(rule);
									}
								}
							}
						}
					}
				}
			}
		}
		HBaseAuthDB authDBTemp = new HBaseAuthDB(ruleListTemp, auditListTemp, encryptList);
		authDB = authDBTemp;
	}

}
