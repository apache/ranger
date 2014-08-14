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

package com.xasecure.pdp.hdfs;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.xasecure.authorization.hadoop.HDFSAccessVerifier;
import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.pdp.config.PolicyChangeListener;
import com.xasecure.pdp.config.PolicyRefresher;
import com.xasecure.pdp.constants.XaSecureConstants;
import com.xasecure.pdp.model.Policy;
import com.xasecure.pdp.model.PolicyContainer;
import com.xasecure.pdp.model.ResourcePath;
import com.xasecure.pdp.model.RolePermission;

public class URLBasedAuthDB implements HDFSAccessVerifier, PolicyChangeListener {

	private static final Log LOG = LogFactory.getLog(URLBasedAuthDB.class) ;

	private static URLBasedAuthDB me = null;
	
	private PolicyRefresher refresher = null ;
	
	private PolicyContainer policyContainer = null;
	
	private HashMap<String,Boolean> cachedAuditFlag = new HashMap<String,Boolean>() ;	// needs to be cleaned when ruleList changes
	
	private static final long MAX_NO_OF_AUDIT_CACHE_ENTRIES = 1000L ;


	public static URLBasedAuthDB getInstance() {
		if (me == null) {
			synchronized (URLBasedAuthDB.class) {
				URLBasedAuthDB temp = me;
				if (temp == null) {
					me = new URLBasedAuthDB();
					me.init() ;
				}
			}
		}
		return me;
	}

	private URLBasedAuthDB() {
		String url 			 = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_HDFS_POLICYMGR_URL_PROP);
		long  refreshInMilli = XaSecureConfiguration.getInstance().getLong(
				XaSecureConstants.XASECURE_HDFS_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP ,
				XaSecureConstants.XASECURE_HDFS_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT);
		String sslConfigFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_HDFS_POLICYMGR_SSL_CONFIG_FILE_PROP) ;
		
		String lastStoredFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_HDFS_LAST_SAVED_POLICY_FILE_PROP) ;
		
		refresher = new PolicyRefresher(url, refreshInMilli,sslConfigFileName,lastStoredFileName) ;
	
		String saveAsFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_HDFS_POLICYMGR_URL_SAVE_FILE_PROP) ;
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
	
	@Override
	public void OnPolicyChange(PolicyContainer aPolicyContainer) {
		setPolicyContainer(aPolicyContainer);
	}


	@Override
	public boolean isAccessGranted(String aPathName, String pathOwnerName, String access, String username, Set<String> groups) {

		PolicyContainer pc = getPolicyContainer() ;
		
		if (pc == null) {
			return false ;
		}
		
		for(Policy acl :  pc.getAcl()) {
			
			if (! acl.isEnabled()) {
				LOG.debug("Diabled acl found [" + acl + "]. Skipping this acl ...") ;
				continue ;
			}

			for(ResourcePath resource : acl.getResourceList()) {
				
				String path = resource.getPath() ;
				
				boolean rulePathMatched = false ;
				
				if (acl.getRecursiveInd() == 1) {
					if (resource.isWildcardPath()) {
						rulePathMatched = isRecursiveWildCardMatch(aPathName, path) ;
					}
					else {
						rulePathMatched = aPathName.startsWith(path) ;
					}
				}
				else {
					if (resource.isWildcardPath()) {
						rulePathMatched = FilenameUtils.wildcardMatch(aPathName, path) ;
					}
					else {
						rulePathMatched = aPathName.equals(path) ;
					}
				}
				
				
				if (rulePathMatched) {
					for (RolePermission rp : acl.getPermissions()) {
						if (rp.getAccess().contains(access)) {
							if ( rp.getUsers().contains(username) ) {
								return true ;
							}
							for(String ug : groups) {
								if ( rp.getGroups().contains(ug)) {
									return true ;
								}
							}
							if (rp.getGroups().contains(XaSecureConstants.PUBLIC_ACCESS_ROLE)) {
								return true ;
							}
						}
					}
				}
			
			}
		}
		
		return false ;

	}
	
	public static boolean isRecursiveWildCardMatch(String pathToCheck, String wildcardPath) {
		if (pathToCheck != null) {
			StringBuilder sb = new StringBuilder() ;
			for(String p :  pathToCheck.split(File.separator) ) {
				sb.append(p) ;
				boolean matchFound = FilenameUtils.wildcardMatch(sb.toString(), wildcardPath) ;
				if (matchFound) {
					return true ;
				}
				sb.append(File.separator) ;
			}
			sb = null;
		}
		return false;
	}

	public PolicyContainer getPolicyContainer() {
		return policyContainer;
	}

	private synchronized void setPolicyContainer(PolicyContainer aPolicyContainer) {
		
		for(Policy p : aPolicyContainer.getAcl()) {
			for(RolePermission rp : p.getPermissions()) {
				List<String> rpaccess = rp.getAccess() ;
				if (rpaccess != null && rpaccess.size() > 0) {
					List<String> temp = new ArrayList<String>() ;
					for(String s : rpaccess) {
						temp.add(s.toLowerCase()) ;
					}
					rp.setAccess(temp);
				}
			}
		}
		
		this.policyContainer = aPolicyContainer ;
		this.cachedAuditFlag.clear(); 
	}
	


	public UserPermission printPermissionInfo(UserGroupInformation ugi) {
		return printPermissionInfo(ugi, null) ;
	}

	public UserPermission printPermissionInfo(UserGroupInformation ugi, String aPathName) {
		
		String username = ugi.getShortUserName() ;
		
		String[] groups = ugi.getGroupNames() ;
		
		UserPermission up = new UserPermission(username,groups, aPathName) ;
		
		PolicyContainer pc = getPolicyContainer() ;
		
		if (pc != null) {
		
			for(Policy acl :  pc.getAcl()) {
	
				for(ResourcePath resource : acl.getResourceList()) {
					
					String path = resource.getPath() ;
					
					boolean rulePathMatched = false ;
					
					if (acl.getRecursiveInd() == 1) {
						if (resource.isWildcardPath()) {
							rulePathMatched = isRecursiveWildCardMatch(aPathName, path) ;
						}
						else {
							rulePathMatched = aPathName.startsWith(path) ;
						}
					}
					else {
						if (resource.isWildcardPath()) {
							rulePathMatched = FilenameUtils.wildcardMatch(aPathName, path) ;
						}
						else {
							rulePathMatched = aPathName.equals(path) ;
						}
					}
					
					
					if (rulePathMatched) {
						for (RolePermission rp : acl.getPermissions()) {
							boolean isAccessGranted = false ;
							if (! isAccessGranted ) {
								if ( rp.getUsers().contains(username) ) {
									up.add(resource, acl.getRecursiveInd(), username, null,  rp.getAccess());
									isAccessGranted = true ;
								}
							}
							if ( ! isAccessGranted ) { 
								for(String ug : groups) {
									if ( rp.getGroups().contains(ug)) {
										up.add(resource, acl.getRecursiveInd(), null, ug,  rp.getAccess());
									}
								}
							}
							if (! isAccessGranted ) {
								if (rp.getGroups().contains(XaSecureConstants.PUBLIC_ACCESS_ROLE)) {
									up.add(resource, acl.getRecursiveInd(), null, XaSecureConstants.PUBLIC_ACCESS_ROLE,  rp.getAccess());
								}
							}
						}
					}
				}
			}
		}
		
		return up ;
	}
	
	
	class UserPermission {

		private String userName ;
		private String groups ;
		private String pathName ;
		private HashMap<String,HashSet<String>> userPermissionMap = new HashMap<String,HashSet<String>>() ;

		public UserPermission(String userName, String[] groupList, String pathName) {
			this.userName = userName ;
			this.pathName = pathName ;
			StringBuilder sb = new StringBuilder() ;
			boolean first = true ;
			TreeSet<String> gl = new TreeSet<String>() ;
			for(String g : groupList) {
				gl.add(g) ;
			}
			for(String group : gl) {
				if (first) {
					first = false ;
				}
				else {
					sb.append(",") ;
				}
				sb.append(group) ;
			}
			this.groups = sb.toString()  ;
		}
		
		
		public void add(ResourcePath resource, int recursiveInd, String userName, String groupName, List<String> accessList) {
			
			String path = resource.getPath() ;
			
			if (recursiveInd == 1) {
				if (path.endsWith("/")) {
					path = path + "**" ;
				}
				else {
					path = path + "/" + "**" ;
				}
			}
			
			HashSet<String> permMap = userPermissionMap.get(path) ;
			
			if (permMap == null) {
				permMap = new HashSet<String>() ;
				userPermissionMap.put(path,permMap) ;
			}
			
			for(String access : accessList) {
				if (! permMap.contains(access)) {
					permMap.add(access) ;
				}
			}
			
		}
		
		public void printUserInfo() {
			System.out.println("# USER INFORMATION") ;
			System.out.println("USER:   " + userName ) ;
			System.out.println("GROUPS: " + groups ) ;
		}
		
		public void print() {
			if (pathName != null) {
				System.out.println("# PERMISSION INFORMATION FOR PATH [" + pathName + "]" + (userPermissionMap.size() == 0 ? " - NO RULES FOUND" : "")) ;
			}
			else {
				System.out.println("# PERMISSION INFORMATION" + (userPermissionMap.size() == 0 ? " - NO RULES FOUND" : "")) ;
			}
			

			if (userPermissionMap.size() > 0) {
				TreeSet<String> pathSet = new TreeSet<String>() ;
				pathSet.addAll(userPermissionMap.keySet()) ;
				StringBuilder sb = new StringBuilder();
				for(String path : pathSet) {
					sb.setLength(0) ;
					sb.append(String.format("%-50s", path)).append("|") ;
					TreeSet<String> permSet = new TreeSet<String>() ;
					permSet.addAll(userPermissionMap.get(path)) ;
					boolean first = true ;
					for(String perm: permSet) {
						if (! first) {
							sb.append(",") ;
						}
						else {
							first = false ;
						}
						sb.append(perm) ;
					}
					System.out.println(sb.toString()) ;
				}
			}
			
		}
	}
	
	
	@Override
	public boolean isAuditLogEnabled(String aPathName) {
		boolean ret = false ;
		
		HashMap<String,Boolean> tempCachedAuditFlag = cachedAuditFlag ;
		
		Boolean auditResult = (tempCachedAuditFlag == null ? null : tempCachedAuditFlag.get(aPathName)) ;
		
		if (auditResult != null) {
			ret =  auditResult ;
		}
		else {
			ret = isAuditLogEnabledByACL(aPathName) ;
			if (tempCachedAuditFlag != null) {
				// tempCachedAuditFlag.put(aPathName,Boolean.valueOf(ret)) ;
				synchronized(tempCachedAuditFlag) {
					if (tempCachedAuditFlag.size() > MAX_NO_OF_AUDIT_CACHE_ENTRIES) {
						tempCachedAuditFlag.clear(); 
					}
					tempCachedAuditFlag.put(aPathName,Boolean.valueOf(ret)) ;
				}
			}
		}
		
		return ret ;
		
	}

	
	public boolean isAuditLogEnabledByACL(String aPathName) {
		
		boolean ret = false ;
		
		PolicyContainer pc = getPolicyContainer() ;
		
		if (pc == null) {
			return false ;
		}
		
		for(Policy acl :  pc.getAcl()) {

			for(ResourcePath resource : acl.getResourceList()) {
				
				String path = resource.getPath() ;
				
				boolean rulePathMatched = false ;
				
				if (acl.getRecursiveInd() == 1) {
					if (resource.isWildcardPath()) {
						rulePathMatched = isRecursiveWildCardMatch(aPathName, path) ;
					}
					else {
						rulePathMatched = aPathName.startsWith(path) ;
					}
				}
				else {
					if (resource.isWildcardPath()) {
						rulePathMatched = FilenameUtils.wildcardMatch(aPathName, path) ;
					}
					else {
						rulePathMatched = aPathName.equals(path) ;
					}
				}
				
				
				if (rulePathMatched) {
					ret = ( acl.getAuditInd() == 1)  ;
					break ;
				}
			}
		}

		return ret ;
	}
	
	public static void main(String[] args) throws Throwable {
		LogManager.getLogger(URLBasedAuthDB.class).setLevel(Level.ERROR);
		URLBasedAuthDB authDB = URLBasedAuthDB.getInstance() ;
		UserPermission up = null; 
		if (args.length == 0) {
			up = authDB.printPermissionInfo(UserGroupInformation.getCurrentUser());
			up.printUserInfo() ;
			up.print();
		}
		else {
			up = authDB.printPermissionInfo(UserGroupInformation.getCurrentUser());
			up.printUserInfo() ;
			for(String path : args) {
				up = authDB.printPermissionInfo(UserGroupInformation.getCurrentUser(), path);
				up.print();
				System.out.println();
			}
		}
		System.exit(0);
	}

}
