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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.*;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.hadoop.exceptions.RangerAccessControlException;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import com.google.common.collect.Sets;


public class RangerFSPermissionChecker {
	private static final Log LOG = LogFactory.getLog(RangerFSPermissionChecker.class);

	private static Map<FsAction, Set<String>> access2ActionListMapper = null ;

	static {
		access2ActionListMapper = new HashMap<FsAction, Set<String>>();

		access2ActionListMapper.put(FsAction.NONE,          new HashSet<String>());
		access2ActionListMapper.put(FsAction.ALL,           Sets.newHashSet(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.READ,          Sets.newHashSet(READ_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.READ_WRITE,    Sets.newHashSet(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.READ_EXECUTE,  Sets.newHashSet(READ_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.WRITE,         Sets.newHashSet(WRITE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.WRITE_EXECUTE, Sets.newHashSet(WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.EXECUTE,       Sets.newHashSet(EXECUTE_ACCCESS_TYPE));
	}

	private static volatile RangerHdfsPlugin           rangerPlugin        = null;
	private static ThreadLocal<RangerHdfsAuditHandler> currentAuditHandler = new ThreadLocal<RangerHdfsAuditHandler>();

	public static boolean check(UserGroupInformation ugi, INode inode, FsAction access) throws RangerAccessControlException {
		if (ugi == null || inode == null || access == null) {
			return false;
		}

		return check(ugi.getShortUserName(), Sets.newHashSet(ugi.getGroupNames()), inode, access);
	}

	public static boolean check(String user, Set<String> groups, INode inode, FsAction access) throws RangerAccessControlException {
		if (user == null || inode == null || access == null) {
			return false;
		}

		String path      = inode.getFullPathName();
		String pathOwner = inode.getUserName();

		boolean accessGranted =  AuthorizeAccessForUser(path, pathOwner, access, user, groups);

		if (!accessGranted &&  !RangerHdfsPlugin.isHadoopAuthEnabled()) {
			String inodeInfo = (inode.isDirectory() ? "directory" : "file") +  "="  + "\"" + path + "\""  ;
		    throw new RangerAccessControlException("Permission denied: principal{user=" + user + ",groups: " + groups + "}, access=" + access + ", " + inodeInfo ) ; 
		}

		return accessGranted ;
	}

	public static boolean AuthorizeAccessForUser(String aPathName, String aPathOwnerName, FsAction access, String user, Set<String> groups) throws RangerAccessControlException {
		boolean accessGranted = false;

		if(aPathName != null && aPathOwnerName != null && access != null && user != null && groups != null) {
			if (RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH_ALT.equals(aPathName)) {
				aPathName = RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH;
			}

			RangerHdfsPlugin plugin = rangerPlugin;

			if (plugin == null) {
				synchronized(RangerFSPermissionChecker.class) {
					plugin = rangerPlugin ;

					if (plugin == null) {
						try {
							plugin = new RangerHdfsPlugin();
							plugin.init();

							rangerPlugin = plugin;
						}
						catch(Throwable t) {
							LOG.error("Unable to create Authorizer", t);
						}
					}
				}
			}

			if (rangerPlugin != null) {
				Set<String> accessTypes = access2ActionListMapper.get(access);

				boolean isAllowed = true;
				for(String accessType : accessTypes) {
					RangerHdfsAccessRequest request = new RangerHdfsAccessRequest(aPathName, aPathOwnerName, access, accessType, user, groups);

					RangerAccessResult result = rangerPlugin.isAccessAllowed(request, getCurrentAuditHandler());

					isAllowed = result != null && result.getIsAllowed();
					
					if(!isAllowed) {
						break;
					}
				}

				accessGranted = isAllowed;
			}
		}

		return accessGranted;
	}

	public static void checkPermissionPre(INodesInPath inodesInPath) {
		String pathToBeValidated = getPath(inodesInPath);

		checkPermissionPre(pathToBeValidated);
	}

	public static void checkPermissionPost(INodesInPath inodesInPath) {
		String pathToBeValidated = getPath(inodesInPath);

		checkPermissionPost(pathToBeValidated);
	}

	public static void checkPermissionPre(String pathToBeValidated) {
		RangerHdfsAuditHandler auditHandler = new RangerHdfsAuditHandler(pathToBeValidated);
		
		currentAuditHandler.set(auditHandler);
	}

	public static void checkPermissionPost(String pathToBeValidated) {
		RangerHdfsAuditHandler auditHandler = getCurrentAuditHandler();

		if(auditHandler != null) {
			auditHandler.flushAudit();
		}

		currentAuditHandler.set(null);
	}

	public static void logHadoopEvent(INode inode, boolean accessGranted) {
		if(inode == null) {
			return;
		}

		RangerHdfsAuditHandler auditHandler = getCurrentAuditHandler();

		if(auditHandler != null) {
			auditHandler.logHadoopEvent(inode.getFullPathName(), accessGranted);
		}
	}

	private static RangerHdfsAuditHandler getCurrentAuditHandler() {
		return currentAuditHandler.get();
	}

	private static String getPath(INodesInPath inodesInPath) {
		int   length = inodesInPath.length();
		INode last   = length > 0 ? inodesInPath.getLastINode() : null;

		return last == null ? org.apache.hadoop.fs.Path.SEPARATOR : last.getFullPathName();
	}
}

class RangerHdfsPlugin extends RangerBasePlugin {
	private static boolean hadoopAuthEnabled = RangerHadoopConstants.RANGER_ADD_HDFS_PERMISSION_DEFAULT;

	public RangerHdfsPlugin() {
		super("hdfs", "hdfs");
	}
	
	public void init() {
		super.init();
		
		RangerHdfsPlugin.hadoopAuthEnabled = RangerConfiguration.getInstance().getBoolean(RangerHadoopConstants.RANGER_ADD_HDFS_PERMISSION_PROP, RangerHadoopConstants.RANGER_ADD_HDFS_PERMISSION_DEFAULT);
	}

	public static boolean isHadoopAuthEnabled() {
		return RangerHdfsPlugin.hadoopAuthEnabled;
	}
}

class RangerHdfsResource extends RangerAccessResourceImpl {
	private static final String KEY_PATH = "path";


	public RangerHdfsResource(String path, String owner) {
		super.setValue(KEY_PATH, path);
		super.setOwnerUser(owner);
	}
}

class RangerHdfsAccessRequest extends RangerAccessRequestImpl {
	public RangerHdfsAccessRequest(String path, String pathOwner, FsAction access, String accessType, String user, Set<String> groups) {
		super.setResource(new RangerHdfsResource(path, pathOwner));
		super.setAccessType(accessType);
		super.setUser(user);
		super.setUserGroups(groups);
		super.setAccessTime(StringUtil.getUTCDate());
		super.setClientIPAddress(getRemoteIp());
		super.setAction(access.toString());
	}
	
	private static String getRemoteIp() {
		String ret = null ;
		InetAddress ip = Server.getRemoteIp() ;
		if (ip != null) {
			ret = ip.getHostAddress();
		}
		return ret ;
	}
}

class RangerHdfsAuditHandler extends RangerDefaultAuditHandler {
	private static final Log LOG = LogFactory.getLog(RangerHdfsAuditHandler.class);

	private String          pathToBeValidated = null;
	private boolean         isAuditEnabled    = false;
	private AuthzAuditEvent auditEvent        = null;

	private static final String    RangerModuleName = RangerConfiguration.getInstance().get(RangerHadoopConstants.AUDITLOG_RANGER_MODULE_ACL_NAME_PROP , RangerHadoopConstants.DEFAULT_RANGER_MODULE_ACL_NAME) ;
	private static final String    HadoopModuleName = RangerConfiguration.getInstance().get(RangerHadoopConstants.AUDITLOG_HADOOP_MODULE_ACL_NAME_PROP , RangerHadoopConstants.DEFAULT_HADOOP_MODULE_ACL_NAME) ;
	private static final String    excludeUserList  = RangerConfiguration.getInstance().get(RangerHadoopConstants.AUDITLOG_HDFS_EXCLUDE_LIST_PROP, RangerHadoopConstants.AUDITLOG_EMPTY_STRING) ;
	private static HashSet<String> excludeUsers     = null ;

	static {
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
	}

	public RangerHdfsAuditHandler(String pathToBeValidated) {
		this.pathToBeValidated = pathToBeValidated;

		auditEvent = new AuthzAuditEvent();
	}

	@Override
	public void logAudit(RangerAccessResult result) {
		if(! isAuditEnabled && result.getIsAudited()) {
			isAuditEnabled = true;
		}

		RangerAccessRequest request      = result.getAccessRequest();
		RangerServiceDef    serviceDef   = result.getServiceDef();
		RangerAccessResource      resource     = request.getResource();
		String              resourceType = resource != null ? resource.getLeafName(serviceDef) : null;
		String              resourcePath = resource != null ? resource.getAsString(serviceDef) : null;

		auditEvent.setUser(request.getUser());
		auditEvent.setResourcePath(pathToBeValidated);
		auditEvent.setResourceType(resourceType) ;
		auditEvent.setAccessType(request.getAction());
		auditEvent.setAccessResult((short)(result.getIsAllowed() ? 1 : 0));
		auditEvent.setClientIP(request.getClientIPAddress());
		auditEvent.setEventTime(request.getAccessTime());
		auditEvent.setAclEnforcer(RangerModuleName);
		auditEvent.setPolicyId(result.getPolicyId());
		auditEvent.setRepositoryType(result.getServiceType());
		auditEvent.setRepositoryName(result.getServiceName());
		auditEvent.setResultReason(resourcePath);
	}

	public void logHadoopEvent(String path, boolean accessGranted) {
		auditEvent.setResultReason(path);
		auditEvent.setAccessResult((short) (accessGranted ? 1 : 0));
		auditEvent.setAclEnforcer(HadoopModuleName);
		auditEvent.setPolicyId(0);
	}

	public void flushAudit() {
		String username = auditEvent.getUser();

		boolean skipLog = (username != null && excludeUsers != null && excludeUsers.contains(username)) ;
		
		if (skipLog) {
			return ;
		}

		if(isAuditEnabled) {
			super.logAuthzAudit(auditEvent);
		}
	}
}
