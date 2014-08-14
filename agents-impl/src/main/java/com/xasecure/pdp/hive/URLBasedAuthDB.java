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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.pdp.config.PolicyChangeListener;
import com.xasecure.pdp.config.PolicyRefresher;
import com.xasecure.pdp.constants.XaSecureConstants;
import com.xasecure.pdp.model.Policy;
import com.xasecure.pdp.model.PolicyContainer;
import com.xasecure.pdp.model.RolePermission;

public class URLBasedAuthDB extends HiveAuthorizationProviderBase implements PolicyChangeListener {
	
	private static final Log LOG = LogFactory.getLog(URLBasedAuthDB.class) ;
		
	private static URLBasedAuthDB me = null ;
	
	private PolicyContainer policyContainer = null ;
	
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
		String url 			 = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_HIVE_POLICYMGR_URL_PROP);
		long  refreshInMilli = XaSecureConfiguration.getInstance().getLong(
				XaSecureConstants.XASECURE_HIVE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP ,
				XaSecureConstants.XASECURE_HIVE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT);
		
		String lastStoredFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_HIVE_LAST_SAVED_POLICY_FILE_PROP) ;
		
		String sslConfigFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_HIVE_POLICYMGR_SSL_CONFIG_FILE_PROP) ;
		refresher = new PolicyRefresher(url, refreshInMilli,sslConfigFileName,lastStoredFileName) ;
		
		String saveAsFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_HIVE_POLICYMGR_URL_SAVE_FILE_PROP) ;
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
	
	public PolicyContainer getPolicyContainer() {
		return policyContainer;
	}

	@Override
	public void OnPolicyChange(PolicyContainer policyContainer) {

		LOG.debug("OnPolicyChange() has been called with new PolicyContainer .....") ;
		
		try {
			
			ArrayList<HiveAuthRule> ruleListTemp = new ArrayList<HiveAuthRule>();
				
			this.policyContainer = policyContainer;
	
			if (LOG.isDebugEnabled()) {
				LOG.debug("Number of acl found (before isEnabled check): " +  ( policyContainer.getAcl() == null ? 0 :  policyContainer.getAcl().size() ) );
			}
			
			for(Policy acl : policyContainer.getAcl()) {
				
				if (! acl.isEnabled()) {
					LOG.debug("Diabled acl found [" + acl + "]. Skipping this acl ...") ;
					continue ;
				}
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("Number of database found in acl [" + acl + "] " +  ( acl.getDatabaseList() == null ? 0 :  acl.getDatabaseList().size() ) );
					LOG.debug("Number of Tables found in acl [" + acl + "] " +  ( acl.getTableList() == null ? 0 :  acl.getTableList().size() ) );
					LOG.debug("Number of Columns found in acl [" + acl + "] " +  ( acl.getColumnList()== null ? 0 :  acl.getColumnList().size() ) );
				}

				boolean isUDF = false ;
				
				List<String> dbList = new ArrayList<String>() ;
				String dbs = replaceFileBasedRegEx(acl.getDatabases()) ;
				dbList.add(getRegExFormatted(dbs)) ;
				
				List<String> tableList = new ArrayList<String>() ;
				String udfs   = acl.getUdfs() ;
				if (udfs != null) {
					isUDF = true ;
					dbList.clear(); 
					dbList.add(HiveAuthRule.WILDCARD_OBJECT) ;
					tableList.clear(); 
					udfs  = replaceFileBasedRegEx(udfs) ;
					tableList.add(getRegExFormatted(udfs)) ;
				}
				else {
					String tables = replaceFileBasedRegEx(acl.getTables()) ;
					tableList.add(getRegExFormatted(tables)) ;
				}
				
				List<String> columnList = new ArrayList<String>() ;
				String columns = replaceFileBasedRegEx(acl.getColumns()) ;
				columnList.add(getRegExFormatted(columns)) ;


				boolean isAudited = (acl.getAuditInd() == 1) ;
				
				boolean isEncrypted = (acl.getEncryptInd() == 1) ;

				for(String db : dbList)  {
					
					for(String table : tableList) {
						
						for(String col : columnList) {
							
							for(RolePermission rp : acl.getPermissions()) {
								for (String accessLevel : rp.getAccess() ) {
									for (String group : rp.getGroups()) {
										HiveAuthRule rule = new HiveAuthRule(isUDF, db, table, col, accessLevel.toLowerCase(), null, group, acl.isTableSelectionExcluded(), acl.isColumnSelectionExcluded());
										rule.setAudited(isAudited);
										rule.setEncrypted(isEncrypted);
										LOG.debug("Adding rule [" + rule + "] to the authdb.");
										ruleListTemp.add(rule);
									}
									for (String user : rp.getUsers()) {
										HiveAuthRule rule = new HiveAuthRule(isUDF, db, table, col, accessLevel.toLowerCase(), user, null,acl.isTableSelectionExcluded(), acl.isColumnSelectionExcluded());
										rule.setAudited(isAudited);
										rule.setEncrypted(isEncrypted);
										LOG.debug("Adding rule [" + rule + "] to the authdb.");
										ruleListTemp.add(rule);
									}
								}
							}
							
							
						}
					}
				}
			}
			HiveAuthDB authDBTemp = new HiveAuthDB(ruleListTemp);
			authDB = authDBTemp;
		}
		catch(Throwable t) {
			LOG.error("OnPolicyChange has failed with an exception", t);
		}
	}
	
	public static String getRegExFormatted(String userEnteredStr) {
		
		if (userEnteredStr == null || userEnteredStr.trim().length() == 0) {
			return HiveAuthRule.WILDCARD_OBJECT ;
		}

		StringBuilder sb = new StringBuilder() ;

		for(String s : userEnteredStr.split(",")) {
			if (sb.length() == 0) {
				sb.append("(") ;
			}
			else {
				sb.append("|") ;
			}
			sb.append(s.trim()) ;
		}
		
		if (sb.length() > 0) {
			sb.append(")") ;
		}
		
		return sb.toString() ;
	}
	
	
	public static String replaceFileBasedRegEx(String userEnteredStr) {
		if (userEnteredStr != null) {
			userEnteredStr = userEnteredStr.replaceAll("\\.", "\\.")
												.replaceAll("\\?", "\\.") 
												.replaceAll("\\*", ".*") ;
		}
		return userEnteredStr ;
	}
	

}
