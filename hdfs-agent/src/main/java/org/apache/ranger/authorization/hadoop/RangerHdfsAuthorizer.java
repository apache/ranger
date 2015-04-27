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

package org.apache.ranger.authorization.hadoop;

import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.EXECUTE_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.READ_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.WRITE_ACCCESS_TYPE;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
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
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import com.google.common.collect.Sets;

public class RangerHdfsAuthorizer extends INodeAttributeProvider {
	private static final Log LOG = LogFactory.getLog(RangerHdfsAuthorizer.class);

	private RangerHdfsPlugin           rangerPlugin            = null;
	private Map<FsAction, Set<String>> access2ActionListMapper = new HashMap<FsAction, Set<String>>();

	public RangerHdfsAuthorizer() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuthorizer.RangerHdfsAuthorizer()");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuthorizer.RangerHdfsAuthorizer()");
		}
	}

	public void start() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuthorizer.start()");
		}

		RangerHdfsPlugin plugin = new RangerHdfsPlugin();
		plugin.init();

		access2ActionListMapper.put(FsAction.NONE,          new HashSet<String>());
		access2ActionListMapper.put(FsAction.ALL,           Sets.newHashSet(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.READ,          Sets.newHashSet(READ_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.READ_WRITE,    Sets.newHashSet(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.READ_EXECUTE,  Sets.newHashSet(READ_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.WRITE,         Sets.newHashSet(WRITE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.WRITE_EXECUTE, Sets.newHashSet(WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE));
		access2ActionListMapper.put(FsAction.EXECUTE,       Sets.newHashSet(EXECUTE_ACCCESS_TYPE));

		rangerPlugin = plugin;

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuthorizer.start()");
		}
	}

	public void stop() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuthorizer.stop()");
		}

		RangerHdfsPlugin plugin = rangerPlugin;
		rangerPlugin = null;

		if(plugin != null) {
			plugin.cleanup();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuthorizer.stop()");
		}
	}

	@Override
	public INodeAttributes getAttributes(String fullPath, INodeAttributes inode) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuthorizer.getAttributes(" + fullPath + ")");
		}

		INodeAttributes ret = inode; // return default attributes

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuthorizer.getAttributes(" + fullPath + "): " + ret);
		}

		return ret;
	}

	@Override
	public INodeAttributes getAttributes(String[] pathElements, INodeAttributes inode) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuthorizer.getAttributes(pathElementsCount=" + (pathElements == null ? 0 : pathElements.length) + ")");
		}

		INodeAttributes ret = inode; // return default attributes

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuthorizer.getAttributes(pathElementsCount=" + (pathElements == null ? 0 : pathElements.length) + "): " + ret);
		}

		return ret;
	}

	@Override
	public AccessControlEnforcer getExternalAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuthorizer.getExternalAccessControlEnforcer()");
		}

		RangerAccessControlEnforcer rangerAce = new RangerAccessControlEnforcer(defaultEnforcer);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuthorizer.getExternalAccessControlEnforcer()");
		}

		return rangerAce;
	}


	class RangerAccessControlEnforcer implements AccessControlEnforcer {
		private INodeAttributeProvider.AccessControlEnforcer defaultEnforcer = null;

		public RangerAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.RangerAccessControlEnforcer()");
			}

			this.defaultEnforcer = defaultEnforcer;

			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerAccessControlEnforcer.RangerAccessControlEnforcer()");
			}
		}

		@Override
		public void checkPermission(String fsOwner, String superGroup, UserGroupInformation ugi,
									INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr,
									int snapshotId, String path, int ancestorIndex, boolean doCheckOwner,
									FsAction ancestorAccess, FsAction parentAccess, FsAction access,
									FsAction subAccess, boolean ignoreEmptyDir) throws AccessControlException {
			boolean                accessGranted = false;
			RangerHdfsPlugin       plugin        = rangerPlugin;
			RangerHdfsAuditHandler auditHandler  = null;
			String                 user          = ugi != null ? ugi.getShortUserName() : null;
			Set<String>            groups        = ugi != null ? Sets.newHashSet(ugi.getGroupNames()) : null;

			if(LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.checkPermission("
						+ "fsOwner=" + fsOwner + "; superGroup=" + superGroup + ", inodesCount=" + (inodes != null ? inodes.length : 0)
						+ ", snapshotId=" + snapshotId + ", user=" + user + ", path=" + path + ", ancestorIndex=" + ancestorIndex
						+ ", doCheckOwner="+ doCheckOwner + ", ancestorAccess=" + ancestorAccess + ", parentAccess=" + parentAccess
						+ ", access=" + access + ", subAccess=" + subAccess + ", ignoreEmptyDir=" + ignoreEmptyDir + ")");
			}

			try {
				if(plugin != null && (access != null || ancestorAccess != null || parentAccess != null || subAccess != null) && !ArrayUtils.isEmpty(inodes)) {
					auditHandler = new RangerHdfsAuditHandler(path);

					if(ancestorIndex >= inodes.length) {
						ancestorIndex = inodes.length - 1;
					}

					for(; ancestorIndex >= 0 && inodes[ancestorIndex] == null; ancestorIndex--);

					accessGranted = true;

					INode ancestor = inodes.length > ancestorIndex && ancestorIndex >= 0 ? inodes[ancestorIndex] : null;
					INode parent   = inodes.length > 1 ? inodes[inodes.length - 2] : null;
					INode inode    = inodes[inodes.length - 1];

					// checkStickyBit
					if (accessGranted && parentAccess != null && parentAccess.implies(FsAction.WRITE) && parent != null && inode != null) {
						if (parent.getFsPermission() != null && parent.getFsPermission().getStickyBit()) {
						    // user should be owner of the parent or the inode
						    accessGranted = StringUtils.equals(parent.getUserName(), user) || StringUtils.equals(inode.getUserName(), user);
						}
					}

					// checkAncestorAccess
					if(accessGranted && ancestorAccess != null && ancestor != null) {
						INodeAttributes ancestorAttribs = inodeAttrs.length > ancestorIndex ? inodeAttrs[ancestorIndex] : null;

						accessGranted = isAccessAllowed(ancestor, ancestorAttribs, ancestorAccess, user, groups, fsOwner, superGroup, plugin, auditHandler);
					}

					// checkParentAccess
					if(accessGranted && parentAccess != null && parent != null) {
						INodeAttributes parentAttribs = inodeAttrs.length > 1 ? inodeAttrs[inodeAttrs.length - 2] : null;

						accessGranted = isAccessAllowed(parent, parentAttribs, parentAccess, user, groups, fsOwner, superGroup, plugin, auditHandler);
					}

					// checkINodeAccess
					if(accessGranted && access != null && inode != null) {
						INodeAttributes inodeAttribs = inodeAttrs.length > 0 ? inodeAttrs[inodeAttrs.length - 1] : null;

						accessGranted = isAccessAllowed(inode, inodeAttribs, access, user, groups, fsOwner, superGroup, plugin, auditHandler);
					}

					// checkSubAccess
					if(accessGranted && subAccess != null && inode != null && inode.isDirectory()) {
						Stack<INodeDirectory> directories = new Stack<INodeDirectory>();

						for(directories.push(inode.asDirectory()); !directories.isEmpty(); ) {
							INodeDirectory      dir   = directories.pop();
							ReadOnlyList<INode> cList = dir.getChildrenList(snapshotId);

							if (!(cList.isEmpty() && ignoreEmptyDir)) {
								INodeAttributes dirAttribs = dir.getSnapshotINode(snapshotId);

								accessGranted = isAccessAllowed(dir, dirAttribs, access, user, groups, fsOwner, superGroup, plugin, auditHandler);

								if(! accessGranted) {
									break;
								}
							}

							for(INode child : cList) {
								if (child.isDirectory()) {
									directories.push(child.asDirectory());
								}
							}
						}
					}

					// checkOwnerAccess
					if(accessGranted && doCheckOwner) {
						INodeAttributes inodeAttribs = inodeAttrs.length > 0 ? inodeAttrs[inodeAttrs.length - 1] : null;
						String          owner        = inodeAttribs != null ? inodeAttribs.getUserName() : null;

						accessGranted = StringUtils.equals(user, owner);
					}
				}

				if(! accessGranted && RangerHdfsPlugin.isHadoopAuthEnabled() && defaultEnforcer != null) {
					try {
						defaultEnforcer.checkPermission(fsOwner, superGroup, ugi, inodeAttrs, inodes,
														pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
														ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir);

						accessGranted = true;
					} finally {
						if(auditHandler != null) {
							FsAction action = access;

							if(action == null) {
								if(parentAccess != null) {
									action = parentAccess;
								} else if(ancestorAccess != null) {
									action = ancestorAccess;
								} else if(subAccess != null) {
									action = subAccess;
								} else {
									action = FsAction.NONE;
								}
							}

							auditHandler.logHadoopEvent(path, action, accessGranted);
						}
					}
				}

				if(! accessGranted) {
					throw new RangerAccessControlException("Permission denied: principal{user=" + user + ",groups: " + groups + "}, access=" + access + ", " + path) ;
				}
			} finally {
				if(auditHandler != null) {
					auditHandler.flushAudit();
				}

				if(LOG.isDebugEnabled()) {
					LOG.debug("<== RangerAccessControlEnforcer.checkPermission(" + path + ", " + access + ", user=" + user + ") : " + accessGranted);
				}
			}
		}

		private boolean isAccessAllowed(INode inode, INodeAttributes inodeAttribs, FsAction access, String user, Set<String> groups, String fsOwner, String superGroup, RangerHdfsPlugin plugin, RangerHdfsAuditHandler auditHandler) {
			boolean ret       = false;
			String  path      = inode != null ? inode.getFullPathName() : null;
			String  pathOwner = inodeAttribs != null ? inodeAttribs.getUserName() : null;

			if(pathOwner == null && inode != null) {
				pathOwner = inode.getUserName();
			}

			if (RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH_ALT.equals(path)) {
				path = RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH;
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.isAccessAllowed(" + path + ", " + access + ", " + user + ")");
			}

			Set<String> accessTypes = access2ActionListMapper.get(access);

			if(accessTypes == null) {
				LOG.warn("RangerAccessControlEnforcer.isAccessAllowed(" + path + ", " + access + ", " + user + "): no Ranger accessType found for " + access);

				accessTypes = access2ActionListMapper.get(FsAction.NONE);
			}

			for(String accessType : accessTypes) {
				RangerHdfsAccessRequest request = new RangerHdfsAccessRequest(path, pathOwner, access, accessType, user, groups);

				RangerAccessResult result = plugin.isAccessAllowed(request, auditHandler);

				if (result == null) {
					LOG.error("RangerAccessControlEnforcer: Internal error: null RangerAccessResult object received back from isAccessAllowed()!");
				} else {
					ret = result.getIsAllowed();

					if (!ret) {
						break;
					}
				}
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerAccessControlEnforcer.isAccessAllowed(" + path + ", " + access + ", " + user + "): " + ret);
			}

			return ret;
		}
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

	private boolean         isAuditEnabled = false;
	private AuthzAuditEvent auditEvent     = null;

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
		auditEvent = new AuthzAuditEvent();
		auditEvent.setResourcePath(pathToBeValidated);
	}

	@Override
	public void processResult(RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuditHandler.logAudit(" + result + ")");
		}

		if(! isAuditEnabled && result.getIsAudited()) {
			isAuditEnabled = true;
		}

		RangerAccessRequest  request      = result.getAccessRequest();
		RangerServiceDef     serviceDef   = result.getServiceDef();
		RangerAccessResource resource     = request.getResource();
		String               resourceType = resource != null ? resource.getLeafName(serviceDef) : null;
		String               resourcePath = resource != null ? resource.getAsString(serviceDef) : null;

		auditEvent.setUser(request.getUser());
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

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuditHandler.logAudit(" + result + "): " + auditEvent);
		}
	}

	public void logHadoopEvent(String path, FsAction action, boolean accessGranted) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuditHandler.logHadoopEvent(" + path + ", " + action + ", " + accessGranted + ")");
		}

		auditEvent.setResultReason(path);
		auditEvent.setAccessResult((short) (accessGranted ? 1 : 0));
		auditEvent.setAccessType(action == null ? null : action.toString());
		auditEvent.setAclEnforcer(HadoopModuleName);
		auditEvent.setPolicyId(-1);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuditHandler.logHadoopEvent(" + path + ", " + action + ", " + accessGranted + "): " + auditEvent);
		}
	}

	public void flushAudit() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuditHandler.flushAudit(" + isAuditEnabled + ", " + auditEvent + ")");
		}

		if(isAuditEnabled && !StringUtils.isEmpty(auditEvent.getAccessType())) {
			String username = auditEvent.getUser();

			boolean skipLog = (username != null && excludeUsers != null && excludeUsers.contains(username)) ;

			if (! skipLog) {
				super.logAuthzAudit(auditEvent);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuditHandler.flushAudit(" + isAuditEnabled + ", " + auditEvent + ")");
		}
	}
}

