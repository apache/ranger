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
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerResource;
import org.apache.ranger.plugin.service.RangerBasePlugin;


public class RangerFSPermissionChecker {
	private static final Log LOG = LogFactory.getLog(RangerFSPermissionChecker.class);

	private static final boolean addHadoopAuth 	  = RangerConfiguration.getInstance().getBoolean(RangerHadoopConstants.RANGER_ADD_HDFS_PERMISSION_PROP, RangerHadoopConstants.RANGER_ADD_HDFS_PERMISSION_DEFAULT) ;


	private static RangerHdfsPlugin                    rangerPlugin        = null;
	private static ThreadLocal<RangerHdfsAuditHandler> currentAuditHandler = new ThreadLocal<RangerHdfsAuditHandler>();


	public static boolean check(UserGroupInformation ugi, INode inode, FsAction access) throws RangerAccessControlException {
		if (ugi == null || inode == null || access == null) {
			return false;
		}

		String      path      = inode.getFullPathName();
		String      pathOwner = inode.getUserName();
		String      user      = ugi.getShortUserName();
		Set<String> groups    = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(ugi.getGroupNames())));

		boolean accessGranted =  AuthorizeAccessForUser(path, pathOwner, access, user, groups);

		if (!accessGranted &&  !addHadoopAuth ) {
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

			if (rangerPlugin == null) {
				synchronized(RangerFSPermissionChecker.class) {
					RangerHdfsPlugin temp = rangerPlugin ;
					if (temp == null) {
						try {
							temp = new RangerHdfsPlugin();
							temp.init();

							rangerPlugin = temp;
						}
						catch(Throwable t) {
							LOG.error("Unable to create Authorizer", t);
						}
					}
				}
			}

			if (rangerPlugin != null && rangerPlugin.getPolicyEngine() != null) {
				RangerHdfsAccessRequest request = new RangerHdfsAccessRequest(aPathName, aPathOwnerName, access, user, groups);

				RangerAccessResult result = rangerPlugin.getPolicyEngine().isAccessAllowed(request, getCurrentAuditHandler());

				accessGranted = result.getResult() == RangerAccessResult.Result.ALLOWED;
			}
		}

		return accessGranted;
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
}

class RangerHdfsPlugin extends RangerBasePlugin {
	public RangerHdfsPlugin() {
		super("hdfs");
	}
	
	public void init() {
		RangerPolicyEngine policyEngine = new RangerPolicyEngineImpl();
		
		super.init(policyEngine);
	}
}

class RangerHdfsResource implements RangerResource {
	private String path  = null;
	private String owner = null;

	public RangerHdfsResource(String path, String owner) {
		this.path  = path;
		this.owner = owner;
	}

	@Override
	public String getOwnerUser() {
		return owner;
	}

	@Override
	public boolean exists(String name) {
		return StringUtils.equalsIgnoreCase(name, "path");
	}

	@Override
	public String getValue(String name) {
		if(StringUtils.equalsIgnoreCase(name, "path")) {
			return path;
		}

		return null;
	}
}

class RangerHdfsAccessRequest extends RangerAccessRequestImpl {
	private static Map<FsAction, Set<String>> access2ActionListMapper = null ;

	static {
		access2ActionListMapper = new HashMap<FsAction, Set<String>>();

		access2ActionListMapper.put(FsAction.NONE,          new HashSet<String>());
		access2ActionListMapper.put(FsAction.ALL,           new HashSet<String>(Arrays.asList(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE)));
		access2ActionListMapper.put(FsAction.READ,          new HashSet<String>(Arrays.asList(READ_ACCCESS_TYPE)));
		access2ActionListMapper.put(FsAction.READ_WRITE,    new HashSet<String>(Arrays.asList(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE)));
		access2ActionListMapper.put(FsAction.READ_EXECUTE,  new HashSet<String>(Arrays.asList(READ_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE)));
		access2ActionListMapper.put(FsAction.WRITE,         new HashSet<String>(Arrays.asList(WRITE_ACCCESS_TYPE)));
		access2ActionListMapper.put(FsAction.WRITE_EXECUTE, new HashSet<String>(Arrays.asList(WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE)));
		access2ActionListMapper.put(FsAction.EXECUTE,       new HashSet<String>(Arrays.asList(EXECUTE_ACCCESS_TYPE)));
	}

	public RangerHdfsAccessRequest(String path, String pathOwner, FsAction access, String user, Set<String> groups) {
		super.setResource(new RangerHdfsResource(path, pathOwner));
		super.setAccessTypes(access2ActionListMapper.get(access));
		super.setUser(user);
		super.setUserGroups(groups);
		super.setAccessTime(getUTCDate());
		super.setClientIPAddress(getRemoteIp());
		super.setAction(access.toString());
	}

	private static Date getUTCDate() {
		Calendar local=Calendar.getInstance();
	    int offset = local.getTimeZone().getOffset(local.getTimeInMillis());
	    GregorianCalendar utc = new GregorianCalendar(TimeZone.getTimeZone("GMT+0"));
	    utc.setTimeInMillis(local.getTimeInMillis());
	    utc.add(Calendar.MILLISECOND, -offset);
	    return utc.getTime();
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

		RangerConfiguration.getInstance().initAudit("hdfs");	
	}

	public RangerHdfsAuditHandler(String pathToBeValidated) {
		this.pathToBeValidated = pathToBeValidated;

		auditEvent = new AuthzAuditEvent();
	}

	@Override
	public void logAudit(RangerAccessResult result) {
		if(! isAuditEnabled) {
			for(Map.Entry<String, RangerAccessResult.ResultDetail> e : result.getAccessTypeResults().entrySet()) {
				RangerAccessResult.ResultDetail resDetail = e.getValue();

				if(resDetail.isAudited()) {
					isAuditEnabled = true;

					break;
				}
			}
		}

		RangerAccessRequest request      = result.getAccessRequest();
		RangerServiceDef    serviceDef   = result.getServiceDef();
		int                 serviceType  = (serviceDef != null && serviceDef.getId() != null) ? serviceDef.getId().intValue() : -1;
		String              serviceName  = result.getServiceName();
		String              resourceType = getResourceName(request.getResource(), serviceDef);
		String              resourcePath = getResourceValueAsString(request.getResource(), serviceDef);
		Long                policyId     = (result.getAccessTypeResults() != null && !result.getAccessTypeResults().isEmpty())
														? result.getAccessTypeResults().values().iterator().next().getPolicyId() : null;

		auditEvent.setUser(request.getUser());
		auditEvent.setResourcePath(pathToBeValidated);
		auditEvent.setResourceType(resourceType) ;
		auditEvent.setAccessType(request.getAction());
		auditEvent.setAccessResult((short)(result.getResult() == RangerAccessResult.Result.ALLOWED ? 1 : 0));
		auditEvent.setClientIP(request.getClientIPAddress());
		auditEvent.setEventTime(request.getAccessTime());
		auditEvent.setAclEnforcer(RangerModuleName);
		auditEvent.setPolicyId(policyId != null ? policyId.longValue() : -1);
		auditEvent.setRepositoryType(serviceType);
		auditEvent.setRepositoryName(serviceName);
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
