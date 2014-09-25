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

package org.apache.hadoop.hdfs.server.namenode;

import static com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants.*;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;

import com.xasecure.audit.model.EnumRepositoryType;
import com.xasecure.audit.model.HdfsAuditEvent;
import com.xasecure.audit.provider.AuditProviderFactory;
import com.xasecure.authorization.hadoop.HDFSAccessVerifier;
import com.xasecure.authorization.hadoop.HDFSAccessVerifierFactory;
import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;
import com.xasecure.authorization.hadoop.exceptions.XaSecureAccessControlException;


public class XaSecureFSPermissionChecker {

	private static Map<FsAction, String[]> access2ActionListMapper = null ;

	private static HDFSAccessVerifier authorizer = null ;
	
	private static final String XaSecureModuleName  	= XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_XASECURE_MODULE_ACL_NAME_PROP , XaSecureHadoopConstants.DEFAULT_XASECURE_MODULE_ACL_NAME) ;
	private static final String HadoopModuleName    	= XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_HADOOP_MODULE_ACL_NAME_PROP , XaSecureHadoopConstants.DEFAULT_HADOOP_MODULE_ACL_NAME) ;
	private static final boolean addHadoopAuth 			= XaSecureConfiguration.getInstance().getBoolean(XaSecureHadoopConstants.XASECURE_ADD_HDFS_PERMISSION_PROP, XaSecureHadoopConstants.XASECURE_ADD_HDFS_PERMISSION_DEFAULT) ;
	private static final String excludeUserList 		= XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_HDFS_EXCLUDE_LIST_PROP, XaSecureHadoopConstants.AUDITLOG_EMPTY_STRING) ;
	private static final String repositoryName          = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_REPOSITORY_NAME_PROP);
	private static final boolean isAuditEnabled         = XaSecureConfiguration.getInstance().getBoolean(XaSecureHadoopConstants.AUDITLOG_IS_ENABLED_PROP, true);

	private static final Log LOG = LogFactory.getLog(XaSecureFSPermissionChecker.class);

	private static HashSet<String> excludeUsers = null ;
	
	private static ThreadLocal<LogEventInfo> currentValidatedLogEvent = new ThreadLocal<LogEventInfo>() ;
	

	static {
		access2ActionListMapper = new HashMap<FsAction, String[]>();
		access2ActionListMapper.put(FsAction.NONE, new String[] {});
		access2ActionListMapper.put(FsAction.ALL, new String[] { READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE });
		access2ActionListMapper.put(FsAction.READ, new String[] { READ_ACCCESS_TYPE });
		access2ActionListMapper.put(FsAction.READ_WRITE, new String[] { READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE });
		access2ActionListMapper.put(FsAction.READ_EXECUTE, new String[] { READ_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE });
		access2ActionListMapper.put(FsAction.WRITE, new String[] { WRITE_ACCCESS_TYPE });
		access2ActionListMapper.put(FsAction.WRITE_EXECUTE, new String[] { WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE });
		access2ActionListMapper.put(FsAction.EXECUTE, new String[] { EXECUTE_ACCCESS_TYPE });
		
		if (excludeUserList != null && excludeUserList.trim().length() > 0) {
			excludeUsers = new HashSet<String>() ;
			for(String excludeUser : excludeUserList.trim().split(",")) {
				excludeUser = excludeUser.trim() ;
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding exclude user [" + excludeUser + "]");
				}
				excludeUsers.add(excludeUser) ;
 			}
		}

		XaSecureConfiguration.getInstance().initAudit(AuditProviderFactory.ApplicationType.Hdfs);		
	}

	public static boolean check(UserGroupInformation ugi, INode inode, FsAction access) throws XaSecureAccessControlException {

		if (inode == null) {
			return false;
		}

		String user = ugi.getShortUserName();

		Set<String> groups = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(ugi.getGroupNames())));
		
		String pathOwnerName = inode.getUserName() ;

		boolean accessGranted =  AuthorizeAccessForUser(inode.getFullPathName(), pathOwnerName, access, user, groups);
		
		if (!accessGranted &&  !addHadoopAuth ) {
			String inodeInfo = (inode.isDirectory() ? "directory" : "file") +  "="  + "\"" + inode.getFullPathName() + "\""  ;
		    throw new XaSecureAccessControlException("Permission denied: principal{user=" + user + ",groups: " + groups + "}, access=" + access + ", " + inodeInfo ) ; 
		}
		
		return accessGranted ;

	}

	public static boolean AuthorizeAccessForUser(String aPathName, String aPathOwnerName, FsAction access, String user, Set<String> groups) throws XaSecureAccessControlException {
		boolean accessGranted = false;
		try {
			if (XaSecureHadoopConstants.HDFS_ROOT_FOLDER_PATH_ALT.equals(aPathName)) {
				aPathName = XaSecureHadoopConstants.HDFS_ROOT_FOLDER_PATH;
			}
			
			String[] accessTypes = access2ActionListMapper.get(access);

			if ((accessTypes == null) || (accessTypes.length == 0)) {
				accessGranted = false;
			} else {
				
				if (authorizer == null) {
					synchronized(XaSecureFSPermissionChecker.class) {
						HDFSAccessVerifier temp = authorizer ;
						if (temp == null) {
							try {
								authorizer = HDFSAccessVerifierFactory.getInstance();
							}
							catch(Throwable t) {
								LOG.error("Unable to create Authorizer", t);
							}
						}
					}
				}
				
				if (authorizer != null) {
					for (String accessType : accessTypes) {
						accessGranted = authorizer.isAccessGranted(aPathName, aPathOwnerName, accessType, user, groups);
						if (!accessGranted) {
							break;
						}
					}
				}
			}

		} finally {
			logEvent(XaSecureModuleName, user, aPathName, access, accessGranted);
		}
		return accessGranted;
	}
	
	
	public static void logHadoopEvent(UserGroupInformation ugi, INode inode, FsAction access, boolean accessGranted) {
		String path = (inode == null) ? XaSecureHadoopConstants.AUDITLOG_EMPTY_STRING : inode.getFullPathName() ;
		String username = (ugi == null) ? XaSecureHadoopConstants.AUDITLOG_EMPTY_STRING : ugi.getShortUserName() ;
		logEvent(HadoopModuleName, username, path,  access, accessGranted);
	}
	
	

	
	
	private static void logEvent(String moduleName,  String username, String path, FsAction access, boolean accessGranted) {
		LogEventInfo e = null;

		if(isAuditEnabled) {
		    e = new LogEventInfo(moduleName,  username, path, access, accessGranted) ;
		}

		currentValidatedLogEvent.set(e);
	}
	
	
	public static void checkPermissionPre(String pathToBeValidated) {
		// TODO: save the path in a thread-local
	}
	
	public static void checkPermissionPost(String pathToBeValidated) {
		writeLog(pathToBeValidated);
	}

	public static void writeLog(String pathValidated) {
		
		LogEventInfo e = currentValidatedLogEvent.get();
		
		if (e == null) {
			return ;
		}
		
		String username = e.getUserName() ;
		
		boolean skipLog = (username != null && excludeUsers != null && excludeUsers.contains(username)) ;
		
		if (skipLog) {
			return ;
		}

		String requestedPath = e.getPath() ;
		
		if (requestedPath == null) {
			requestedPath = XaSecureHadoopConstants.AUDITLOG_EMPTY_STRING ;
		}

		if (! authorizer.isAuditLogEnabled(requestedPath)) {
			return ;
		}
		
		
		String accessType = ( (e.getAccess() == null) ? XaSecureHadoopConstants.AUDITLOG_EMPTY_STRING : e.getAccess().toString() ) ;
		
		HdfsAuditEvent auditEvent = new HdfsAuditEvent();

		auditEvent.setUser(username);
		auditEvent.setResourcePath(requestedPath);
		auditEvent.setResourceType("HDFSPath") ;
		auditEvent.setAccessType(accessType);
		auditEvent.setAccessResult((short)(e.isAccessGranted() ? 1 : 0));
		auditEvent.setClientIP(getRemoteIp());
		auditEvent.setEventTime(getUTCDate());
		auditEvent.setAclEnforcer(e.getModuleName());
		auditEvent.setRepositoryType(EnumRepositoryType.HDFS);
		auditEvent.setRepositoryName(repositoryName);
		auditEvent.setResultReason(pathValidated);

		/*
		 * Review following audit fields for appropriate values
		 *
		auditEvent.setAgentId();
		auditEvent.setPolicyId();
		auditEvent.setSessionId();
		auditEvent.setClientType();
		 *
		 */

		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Audit log of auditEvent: [" + auditEvent.toString() + "] - START.");
			}
			AuditProviderFactory.getAuditProvider().log(auditEvent);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Audit log of auditEvent: [" + auditEvent.toString() + "] - END.");
			}
		}
		catch(Throwable t) {
			LOG.error("ERROR during audit log of auditEvent: [" + auditEvent.toString() + "]", t);
		}
	}
	
	
	private static String getRemoteIp() {
		String ret = null ;
		InetAddress ip = Server.getRemoteIp() ;
		if (ip != null) {
			ret = ip.toString() ;
		}
		else {
			ret = "" ;
		}
		return ret ;
	}
	
	
	public static Date getUTCDate() {
		Calendar local=Calendar.getInstance();
	    int offset = local.getTimeZone().getOffset(local.getTimeInMillis());
	    GregorianCalendar utc = new GregorianCalendar(TimeZone.getTimeZone("GMT+0"));
	    utc.setTimeInMillis(local.getTimeInMillis());
	    utc.add(Calendar.MILLISECOND, -offset);
	    return utc.getTime();
	}

}

class LogEventInfo {
	String moduleName ;
	String userName ;
	String path ;
	FsAction access ;
	boolean accessGranted ;
	
	LogEventInfo(String moduleName,  String username, String path, FsAction access, boolean accessGranted) {
		this.moduleName = moduleName ;
		this.userName = username ;
		this.path = path ;
		this.access = access ;
		this.accessGranted = accessGranted;
	}

	public String getModuleName() {
		return moduleName;
	}

	public String getUserName() {
		return userName;
	}

	public String getPath() {
		return path;
	}

	public FsAction getAccess() {
		return access;
	}

	public boolean isAccessGranted() {
		return accessGranted;
	}
	
	
	
}
