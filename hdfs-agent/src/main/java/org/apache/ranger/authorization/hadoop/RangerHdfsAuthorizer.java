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

import static java.util.stream.Collectors.toSet;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.EXECUTE_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.READ_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.WRITE_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.READ_EXECUTE_PERM;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.WRITE_EXECUTE_PERM;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.READ_WRITE_PERM;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.ALL_PERM;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.ACCESS_TYPE_MONITOR_HEALTH;


import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.hadoop.exceptions.RangerAccessControlException;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.resourcematcher.RangerPathResourceMatcher;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.ranger.plugin.util.RangerAccessRequestUtil;

public class RangerHdfsAuthorizer extends INodeAttributeProvider {
	private static final Logger LOG                       = LoggerFactory.getLogger(RangerHdfsAuthorizer.class);
	private static final Logger PERF_HDFSAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("hdfsauth.request");

	public static final String KEY_FILENAME                             = "FILENAME";
	public static final String KEY_BASE_FILENAME                        = "BASE_FILENAME";
	public static final String DEFAULT_FILENAME_EXTENSION_SEPARATOR     = ".";
	public static final String KEY_RESOURCE_PATH                        = "path";
	public static final String RANGER_FILENAME_EXTENSION_SEPARATOR_PROP = "ranger.plugin.hdfs.filename.extension.separator";
	public static final String OPERATION_NAME_CREATE                    = "create";
	public static final String OPERATION_NAME_DELETE                    = "delete";
	public static final String OPERATION_NAME_RENAME                    = "rename";
	public static final String OPERATION_NAME_LISTSTATUS                = "listStatus";
	public static final String OPERATION_NAME_MKDIRS                    = "mkdirs";
	public static final String OPERATION_NAME_GETEZFORPATH              = "getEZForPath";

	private static final Set<String> OPTIMIZED_OPERATIONS = new HashSet<String>() {{
		add(OPERATION_NAME_CREATE);
		add(OPERATION_NAME_DELETE);
		add(OPERATION_NAME_RENAME);
		add(OPERATION_NAME_LISTSTATUS);
		add(OPERATION_NAME_MKDIRS);
		add(OPERATION_NAME_GETEZFORPATH);
	}};

	private       RangerHdfsPlugin            rangerPlugin            = null;
	private final Map<FsAction, Set<String>> access2ActionListMapper = new HashMap<FsAction, Set<String>>();
	private final Path                        addlConfigFile;
	private       boolean                    AUTHZ_OPTIMIZATION_ENABLED        = true;

	private final OptimizedAuthzContext      OPT_BYPASS_AUTHZ                  = new OptimizedAuthzContext("", FsAction.NONE, FsAction.NONE, FsAction.NONE, AuthzStatus.ALLOW);



	public RangerHdfsAuthorizer() {
		this(null);
	}

	public RangerHdfsAuthorizer(Path addlConfigFile) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuthorizer.RangerHdfsAuthorizer()");
		}

		this.addlConfigFile = addlConfigFile;

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuthorizer.RangerHdfsAuthorizer()");
		}
	}

	public void start() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuthorizer.start()");
		}

		RangerHdfsPlugin plugin = new RangerHdfsPlugin(addlConfigFile);

		plugin.init();

		if (plugin.isOptimizeSubAccessAuthEnabled()) {
			LOG.info(RangerHadoopConstants.RANGER_OPTIMIZE_SUBACCESS_AUTHORIZATION_PROP + " is enabled");
		}

		LOG.info("Legacy way of authorizing sub-access requests will " + (plugin.isUseLegacySubAccessAuthorization() ? "" : "not ") + "be used");

		access2ActionListMapper.put(FsAction.NONE,
									new TreeSet<String>());
		access2ActionListMapper.put(FsAction.ALL,
									Stream.of(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
		access2ActionListMapper.put(FsAction.READ,
									Stream.of(READ_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
		access2ActionListMapper.put(FsAction.READ_WRITE,
									Stream.of(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
		access2ActionListMapper.put(FsAction.READ_EXECUTE,
									Stream.of(READ_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
		access2ActionListMapper.put(FsAction.WRITE,
									Stream.of(WRITE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
		access2ActionListMapper.put(FsAction.WRITE_EXECUTE,
									Stream.of(WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
		access2ActionListMapper.put(FsAction.EXECUTE,
									Stream.of(EXECUTE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));

		rangerPlugin = plugin;

		AUTHZ_OPTIMIZATION_ENABLED        = plugin.getConfig().getBoolean("ranger.hdfs.authz.enable.optimization", false);

		LOG.info("AUTHZ_OPTIMIZATION_ENABLED:[" + AUTHZ_OPTIMIZATION_ENABLED + "]");


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
		return inode; // return default attributes
	}

	@Override
	public INodeAttributes getAttributes(String[] pathElements, INodeAttributes inode) {
		return inode;
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


	// for testing
	public Configuration getConfig() {
		return rangerPlugin.getConfig();
	}

	private enum AuthzStatus { ALLOW, DENY, NOT_DETERMINED }

	class RangerAccessControlEnforcer implements AccessControlEnforcer {
		private final AccessControlEnforcer              defaultEnforcer;
		private       Map<String, OptimizedAuthzContext> CACHE      = null;

		public RangerAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.RangerAccessControlEnforcer()");
			}

			this.defaultEnforcer = defaultEnforcer;

			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerAccessControlEnforcer.RangerAccessControlEnforcer()");
			}
		}

		class SubAccessData {
			final INodeDirectory    dir;
			final String            resourcePath;
			final INode[]           inodes;
			final INodeAttributes[] iNodeAttributes;

			SubAccessData(INodeDirectory dir, String resourcePath, INode[] inodes, INodeAttributes[] iNodeAttributes) {
				this.dir            = dir;
				this.resourcePath   = resourcePath;
				this.iNodeAttributes = iNodeAttributes;
				this.inodes          = inodes;
			}
		}

		@Override
		public void checkPermissionWithContext(AuthorizationContext authzContext) throws AccessControlException {
			checkRangerPermission(authzContext.getFsOwner(), authzContext.getSupergroup(),
					authzContext.getCallerUgi(), authzContext.getInodeAttrs(),
					authzContext.getInodes(), authzContext.getPathByNameArr(),
					authzContext.getSnapshotId(), authzContext.getPath(),
					authzContext.getAncestorIndex(), authzContext.isDoCheckOwner(),
					authzContext.getAncestorAccess(), authzContext.getParentAccess(),
					authzContext.getAccess(), authzContext.getSubAccess(),
					authzContext.isIgnoreEmptyDir(), authzContext.getOperationName(),
					authzContext.getCallerContext());
		}

		@Override
		public void checkPermission(String fsOwner, String superGroup, UserGroupInformation ugi,
									INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr,
									int snapshotId, String path, int ancestorIndex, boolean doCheckOwner,
									FsAction ancestorAccess, FsAction parentAccess, FsAction access,
									FsAction subAccess, boolean ignoreEmptyDir) throws AccessControlException {
			checkRangerPermission(fsOwner, superGroup, ugi, inodeAttrs, inodes, pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner, ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir, null, null);
		}

		private void checkRangerPermission(String fsOwner, String superGroup, UserGroupInformation ugi,
									INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr,
									int snapshotId, String path, int ancestorIndex, boolean doCheckOwner,
									FsAction ancestorAccess, FsAction parentAccess, FsAction access,
									FsAction subAccess, boolean ignoreEmptyDir, String operationName, CallerContext callerContext ) throws AccessControlException {
			AuthzStatus  authzStatus  = AuthzStatus.NOT_DETERMINED;
			String       resourcePath = path;
			AuthzContext context      = new AuthzContext(rangerPlugin, ugi, operationName, access == null && parentAccess == null && ancestorAccess == null && subAccess == null);

			if(LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.checkRangerPermission("
						+ "fsOwner=" + fsOwner + "; superGroup=" + superGroup + ", inodesCount=" + (inodes != null ? inodes.length : 0)
						+ ", snapshotId=" + snapshotId + ", user=" + context.user + ", provided-path=" + path + ", ancestorIndex=" + ancestorIndex
						+ ", doCheckOwner="+ doCheckOwner + ", ancestorAccess=" + ancestorAccess + ", parentAccess=" + parentAccess
						+ ", access=" + access + ", subAccess=" + subAccess + ", ignoreEmptyDir=" + ignoreEmptyDir + ", operationName=" + operationName
						+ ", callerContext=" + callerContext +")");
			}

			if (LOG.isDebugEnabled()) {
				LOG.info("operationName={}, path={}, user={}, ancestorIndex={}, ancestorAccess={}, parentAccess={}, access={}, subAccess={}", context.operationName, path, context.user, ancestorIndex, ancestorAccess, parentAccess, access, subAccess);
			}

			OptimizedAuthzContext optAuthzContext = null;
			RangerPerfTracer      perf            = null;

			if(RangerPerfTracer.isPerfTraceEnabled(PERF_HDFSAUTH_REQUEST_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_HDFSAUTH_REQUEST_LOG, "RangerHdfsAuthorizer.checkRangerPermission(provided-path=" + path + ")");
			}

			try {
				INode   ancestor            = null;
				INode   parent              = null;
				INode   inode               = null;
				String  providedPath        = path;

				boolean useDefaultAuthorizerOnly = false;
				boolean doNotGenerateAuditRecord = false;

				if (context.plugin != null && !ArrayUtils.isEmpty(inodes)) {
					int sz = inodeAttrs.length;
					if (LOG.isTraceEnabled()) {
						LOG.trace("Size of INodeAttrs array:[" + sz + "]");
						LOG.trace("Size of INodes array:[" + inodes.length + "]");
					}
					byte[][] components = new byte[sz][];

					int i = 0;
					for (; i < sz; i++) {
						if (inodeAttrs[i] != null) {
							components[i] = inodeAttrs[i].getLocalNameBytes();
						} else {
							break;
						}
					}
					if (i != sz) {
						if (LOG.isTraceEnabled()) {
							LOG.trace("Input INodeAttributes array contains null at position " + i);
							LOG.trace("Will use only first [" + i + "] components");
						}
					}

					if (sz == 1 && inodes.length == 1 && inodes[0].getParent() != null) {

						doNotGenerateAuditRecord = true;

						if (LOG.isTraceEnabled()) {
							LOG.trace("Using the only inode in the array to figure out path to resource. No audit record will be generated for this authorization request");
						}

						resourcePath = inodes[0].getFullPathName();

						if (snapshotId != Snapshot.CURRENT_STATE_ID) {

							useDefaultAuthorizerOnly = true;

							if (LOG.isTraceEnabled()) {
								LOG.trace("path:[" + resourcePath + "] is for a snapshot, id=[" + snapshotId +"], default Authorizer will be used to authorize this request");
							}
						} else {
							if (LOG.isTraceEnabled()) {
								LOG.trace("path:[" + resourcePath + "] is not for a snapshot, id=[" + snapshotId +"]. It will be used to authorize this request");
							}
						}
					} else {

						if (snapshotId != Snapshot.CURRENT_STATE_ID) {
							resourcePath = DFSUtil.byteArray2PathString(pathByNameArr);

							if (LOG.isTraceEnabled()) {
								LOG.trace("pathByNameArr array is used to figure out path to resource, resourcePath:[" + resourcePath +"]");
							}
						} else {
							resourcePath = DFSUtil.byteArray2PathString(components, 0, i);

							if (LOG.isTraceEnabled()) {
								LOG.trace("INodeAttributes array is used to figure out path to resource, resourcePath:[" + resourcePath +"]");
							}
						}
					}

					if(ancestorIndex >= inodes.length) {
						ancestorIndex = inodes.length - 1;
					}

					for(; ancestorIndex >= 0 && inodes[ancestorIndex] == null; ancestorIndex--);

					ancestor = inodes.length > ancestorIndex && ancestorIndex >= 0 ? inodes[ancestorIndex] : null;
					parent   = inodes.length > 1 ? inodes[inodes.length - 2] : null;
					inode    = inodes[inodes.length - 1]; // could be null while creating a new file

					/*
						Check if optimization is done
					 */
					optAuthzContext = (new OperationOptimizer(operationName, resourcePath, ancestorAccess, parentAccess, access, subAccess, components, inodeAttrs, ancestorIndex, ancestor, parent, inode)).optimize();

					if (optAuthzContext == OPT_BYPASS_AUTHZ) {
						authzStatus = AuthzStatus.ALLOW;

						return;
					} else if (optAuthzContext != null && optAuthzContext.authzStatus != null) {
						authzStatus = optAuthzContext.authzStatus;

						if (LOG.isDebugEnabled()) {
							LOG.debug("OperationOptimizer.optimize() returned " + authzStatus + ", operationName=" + operationName + " has been pre-computed. Returning without any access evaluation!");
						}

						if (authzStatus == AuthzStatus.ALLOW) {
							return;
						}

						final FsAction action;

						if (access != null) {
							action = access;
						} else if(parentAccess != null)  {
							action = parentAccess;
						} else if(ancestorAccess != null) {
							action = ancestorAccess;
						} else {
							action = FsAction.EXECUTE;
						}

						throw new RangerAccessControlException("Permission denied: user=" + context.user + ", access=" + action + ", inode=\"" + resourcePath + "\"");
					} else {
						authzStatus = useDefaultAuthorizerOnly ? AuthzStatus.NOT_DETERMINED : AuthzStatus.ALLOW;
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("OperationOptimizer.optimize() returned null, operationName=" + operationName + " needs to be evaluated!");
					}

					if (optAuthzContext != null) {
						access         = optAuthzContext.access;
						parentAccess   = optAuthzContext.parentAccess;
						ancestorAccess = optAuthzContext.ancestorAccess;
					}

					context.isTraverseOnlyCheck = parentAccess == null && ancestorAccess == null && access == null && subAccess == null;

					context.auditHandler = doNotGenerateAuditRecord ? null : new RangerHdfsAuditHandler(providedPath, context.isTraverseOnlyCheck, context.plugin.getHadoopModuleName(), context.plugin.getExcludedUsers(), callerContext != null ? callerContext.toString() : null);

					/* Hadoop versions prior to 2.8.0 didn't ask for authorization of parent/ancestor traversal for
					 * reading or writing a file. However, Hadoop version 2.8.0 and later ask traversal authorization for
					 * such accesses. This means 2 authorization calls are made to the authorizer for a single access:
					 *  1. traversal authorization (where access, parentAccess, ancestorAccess and subAccess are null)
					 *  2. authorization for the requested permission (such as READ for reading a file)
					 *
					 * For the first call, Ranger authorizer would:
					 * - Deny traversal if Ranger policies explicitly deny EXECUTE access on the parent or closest ancestor
					 * - Else, allow traversal
					 *
					 * There are no changes to authorization of the second call listed above.
					 *
					 * This approach would ensure that Ranger authorization will continue to work with existing policies,
					 * without requiring policy migration/update, for the changes in behaviour in Hadoop 2.8.0.
					 */
					if (authzStatus == AuthzStatus.ALLOW && context.isTraverseOnlyCheck) {
						authzStatus = traverseOnlyCheck(inode, inodeAttrs, resourcePath, components, parent, ancestor, ancestorIndex, context);
					}

					// checkStickyBit
					if (authzStatus == AuthzStatus.ALLOW && parentAccess != null && parentAccess.implies(FsAction.WRITE) && parent != null && inode != null) {
						if (parent.getFsPermission() != null && parent.getFsPermission().getStickyBit()) {
						    // user should be owner of the parent or the inode
						    authzStatus = (StringUtils.equals(parent.getUserName(), context.user) || StringUtils.equals(inode.getUserName(), context.user)) ? AuthzStatus.ALLOW : AuthzStatus.NOT_DETERMINED;
						}
					}

					// checkAncestorAccess
					if(authzStatus == AuthzStatus.ALLOW && ancestorAccess != null && ancestor != null) {
						INodeAttributes ancestorAttribs = inodeAttrs.length > ancestorIndex ? inodeAttrs[ancestorIndex] : null;
						String ancestorPath = ancestorAttribs != null ? DFSUtil.byteArray2PathString(components, 0, ancestorIndex + 1) : null;

						authzStatus = isAccessAllowed(ancestor, ancestorAttribs, ancestorPath, ancestorAccess, context);
						if (authzStatus == AuthzStatus.NOT_DETERMINED) {
							authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes,
											pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
											ancestorAccess, null, null, null, ignoreEmptyDir,
											ancestor, parent, inode, context);
						}
					}

					// checkParentAccess
					if(authzStatus == AuthzStatus.ALLOW && parentAccess != null && parent != null) {
						INodeAttributes parentAttribs = inodeAttrs.length > 1 ? inodeAttrs[inodeAttrs.length - 2] : null;
						String parentPath = parentAttribs != null ? DFSUtil.byteArray2PathString(components, 0, inodeAttrs.length - 1) : null;

						authzStatus = isAccessAllowed(parent, parentAttribs, parentPath, parentAccess, context);
						if (authzStatus == AuthzStatus.NOT_DETERMINED) {
							authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes,
											pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
											null, parentAccess, null, null, ignoreEmptyDir,
											ancestor, parent, inode, context);
						}
					}

					// checkINodeAccess
					if(authzStatus == AuthzStatus.ALLOW && access != null && inode != null) {
						INodeAttributes inodeAttribs = inodeAttrs.length > 0 ? inodeAttrs[inodeAttrs.length - 1] : null;

						authzStatus = isAccessAllowed(inode, inodeAttribs, resourcePath, access, context);
						if (authzStatus == AuthzStatus.NOT_DETERMINED) {
							authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes,
											pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
											null, null, access, null, ignoreEmptyDir,
											ancestor, parent, inode, context);
						}
					}

					// checkSubAccess
					if(authzStatus == AuthzStatus.ALLOW && subAccess != null && inode != null && inode.isDirectory()) {
						Stack<SubAccessData> directories = new Stack<>();

						for(directories.push(new SubAccessData(inode.asDirectory(), resourcePath, inodes, inodeAttrs)); !directories.isEmpty(); ) {
							SubAccessData data = directories.pop();
							ReadOnlyList<INode> cList = data.dir.getChildrenList(snapshotId);

							if (!(cList.isEmpty() && ignoreEmptyDir)) {
								INodeAttributes dirAttribs = data.dir.getSnapshotINode(snapshotId);

								authzStatus = isAccessAllowed(data.dir, dirAttribs, data.resourcePath, subAccess, context);

								INodeDirectory dirINode;
								int dirAncestorIndex;
								INodeAttributes[] dirINodeAttrs;
								INode[] dirINodes;
								INode dirAncestor;
								INode dirParent;
								byte[][] dirComponents;

								if (data.dir.equals(inode)) {
									dirINode = inode.asDirectory();
									dirINodeAttrs = inodeAttrs;
									dirINodes = inodes;
									dirAncestorIndex = ancestorIndex;
									dirAncestor = ancestor;
									dirParent = parent;
									dirComponents = pathByNameArr;
								} else {
									INodeAttributes[] curINodeAttributes;
									INode[] curINodes;

									dirINode = data.dir;
									curINodeAttributes = data.iNodeAttributes;
									curINodes = data.inodes;
									int idx;

									dirINodes = new INode[curINodes.length + 1];
									for (idx = 0; idx < curINodes.length; idx++) {
										dirINodes[idx] = curINodes[idx];
									}
									dirINodes[idx] = dirINode;

									dirINodeAttrs = new INodeAttributes[curINodeAttributes.length + 1];
									for (idx = 0; idx < curINodeAttributes.length; idx++) {
										dirINodeAttrs[idx] = curINodeAttributes[idx];
									}
									dirINodeAttrs[idx] = dirAttribs;

									for (dirAncestorIndex = dirINodes.length - 1; dirAncestorIndex >= 0 && dirINodes[dirAncestorIndex] == null; dirAncestorIndex--)
										;

									dirAncestor = dirINodes.length > dirAncestorIndex && dirAncestorIndex >= 0 ? dirINodes[dirAncestorIndex] : null;
									dirParent = dirINodes.length > 1 ? dirINodes[dirINodes.length - 2] : null;

									dirComponents = dirINode.getPathComponents();
								}

								if (authzStatus == AuthzStatus.NOT_DETERMINED && !rangerPlugin.isUseLegacySubAccessAuthorization()) {
									if (LOG.isDebugEnabled()) {
										if (data.dir.equals(inode)) {
											LOG.debug("Top level directory being processed for default authorizer call, [" + data.resourcePath + "]");
										} else {
											LOG.debug("Sub directory being processed for default authorizer call, [" + data.resourcePath + "]");
										}
										LOG.debug("Calling default authorizer for hierarchy/subaccess with the following parameters");
										LOG.debug("fsOwner=" + fsOwner + "; superGroup=" + superGroup + ", inodesCount=" + (dirINodes != null ? dirINodes.length : 0)
												+ ", snapshotId=" + snapshotId + ", user=" + (ugi != null ? ugi.getShortUserName() : null) + ", provided-path=" + data.resourcePath + ", ancestorIndex=" + dirAncestorIndex
												+ ", doCheckOwner=" + doCheckOwner + ", ancestorAccess=null" + ", parentAccess=null"
												+ ", access=null" + ", subAccess=null" + ", ignoreEmptyDir=" + ignoreEmptyDir + ", operationName=" + operationName
												+ ", callerContext=null");
									}
									authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, dirINodeAttrs, dirINodes,
											dirComponents, snapshotId, data.resourcePath, dirAncestorIndex, doCheckOwner,
											null, null, null, null, ignoreEmptyDir,
											dirAncestor, dirParent, dirINode, context);
									if (LOG.isDebugEnabled()) {
										LOG.debug("Default authorizer call returned : [" + authzStatus + "]");
									}
								}
								if (authzStatus != AuthzStatus.ALLOW) {
									break;
								}

								AuthzStatus subDirAuthStatus = AuthzStatus.NOT_DETERMINED;

								boolean optimizeSubAccessAuthEnabled = rangerPlugin.isOptimizeSubAccessAuthEnabled();

								if (optimizeSubAccessAuthEnabled) {
									subDirAuthStatus = isAccessAllowedForHierarchy(data.dir, dirAttribs, data.resourcePath, subAccess, context);
								}

								if (subDirAuthStatus != AuthzStatus.ALLOW) {
									for(INode child : cList) {
										if (child.isDirectory()) {
											if (data.resourcePath.endsWith(Path.SEPARATOR)) {
												directories.push(new SubAccessData(child.asDirectory(), data.resourcePath + child.getLocalName(), dirINodes, dirINodeAttrs));
											} else {
												directories.push(new SubAccessData(child.asDirectory(), data.resourcePath + Path.SEPARATOR_CHAR + child.getLocalName(), dirINodes, dirINodeAttrs));
											}
										}
									}
								}
							}
						}
						if (authzStatus == AuthzStatus.NOT_DETERMINED) {

							authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes,
											pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
											null, null, null, subAccess, ignoreEmptyDir,
											ancestor, parent, inode, context);

						}
					}

					// checkOwnerAccess
					if(authzStatus == AuthzStatus.ALLOW && doCheckOwner) {
						INodeAttributes inodeAttribs = inodeAttrs.length > 0 ? inodeAttrs[inodeAttrs.length - 1] : null;
						String          owner        = inodeAttribs != null ? inodeAttribs.getUserName() : null;

						authzStatus = StringUtils.equals(context.user, owner) ? AuthzStatus.ALLOW : AuthzStatus.NOT_DETERMINED;
					}
				}

				if (authzStatus == AuthzStatus.NOT_DETERMINED) {
					authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes,
									pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
									ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir,
									ancestor, parent, inode, context);
				}

				if(authzStatus != AuthzStatus.ALLOW) {
					FsAction action = access;

					if(action == null) {
						if(parentAccess != null)  {
							action = parentAccess;
						} else if(ancestorAccess != null) {
							action = ancestorAccess;
						} else {
							action = FsAction.EXECUTE;
						}
					}

					throw new RangerAccessControlException("Permission denied: user=" + context.user + ", access=" + action + ", inode=\"" + resourcePath + "\"");
				}
			} finally {
				if (context.auditHandler != null) {
					context.auditHandler.flushAudit();
				}

				if (optAuthzContext != null && optAuthzContext != OPT_BYPASS_AUTHZ) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Updating OptimizedAuthzContext:[" + optAuthzContext + "] with authzStatus=" + authzStatus.name() + "]");
					}
					optAuthzContext.authzStatus = authzStatus;
				}

				RangerPerfTracer.log(perf);

				if(LOG.isDebugEnabled()) {
					LOG.debug("<== RangerAccessControlEnforcer.checkRangerPermission(" + resourcePath + ", " + access + ", user=" + context.user + ") : " + authzStatus);
				}
			}
		}

		/*
		    Check if parent or ancestor of the file being accessed is denied EXECUTE permission. If not, assume that Ranger-acls
		    allowed EXECUTE access. Do not audit this authorization check if resource is a file unless access is explicitly denied
		 */
		private AuthzStatus traverseOnlyCheck(INode inode, INodeAttributes[] inodeAttrs, String path, byte[][] components, INode parent, INode ancestor, int ancestorIndex,
											  AuthzContext context) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.traverseOnlyCheck("
						+ "path=" + path + ", user=" + context.user + ", groups=" + context.userGroups + ", operationName=" + context.operationName + ")");
			}
			final AuthzStatus ret;

			INode nodeToCheck = inode;
			INodeAttributes nodeAttribs = inodeAttrs.length > 0 ? inodeAttrs[inodeAttrs.length - 1] : null;
			boolean skipAuditOnAllow = false;

			String resourcePath = path;
			if (nodeToCheck == null || nodeToCheck.isFile()) {
				skipAuditOnAllow = true;
				if (parent != null) {
					nodeToCheck = parent;
					nodeAttribs = inodeAttrs.length > 1 ? inodeAttrs[inodeAttrs.length - 2] : null;
					resourcePath = inodeAttrs.length > 0 ? DFSUtil.byteArray2PathString(components, 0, inodeAttrs.length - 1) : HDFS_ROOT_FOLDER_PATH;
				} else if (ancestor != null) {
					nodeToCheck = ancestor;
					nodeAttribs = inodeAttrs.length > ancestorIndex ? inodeAttrs[ancestorIndex] : null;
					resourcePath = nodeAttribs != null ? DFSUtil.byteArray2PathString(components, 0, ancestorIndex+1) : HDFS_ROOT_FOLDER_PATH;
				}
			}

			if (nodeToCheck != null) {
				if (resourcePath.length() > 1) {
					if (resourcePath.endsWith(HDFS_ROOT_FOLDER_PATH)) {
						resourcePath = resourcePath.substring(0, resourcePath.length()-1);
					}
				}
				ret = isAccessAllowedForTraversal(nodeToCheck, nodeAttribs, resourcePath, skipAuditOnAllow, context, context.operationName);
			} else {
				ret = AuthzStatus.ALLOW;
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerAccessControlEnforcer.traverseOnlyCheck("
						+ "path=" + path + ", resourcePath=" + resourcePath + ", user=" + context.user + ", groups=" + context.userGroups + ", operationName=" + context.operationName + ") : " + ret);
			}
			return ret;
		}

		private AuthzStatus isAccessAllowedForTraversal(INode inode, INodeAttributes inodeAttribs, String path, boolean skipAuditOnAllow, AuthzContext context, String operation) {
			final AuthzStatus ret;
			String pathOwner = inodeAttribs != null ? inodeAttribs.getUserName() : null;
			FsAction access = FsAction.EXECUTE;

			if (pathOwner == null) {
				pathOwner = inode.getUserName();
			}

			if (RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH_ALT.equals(path)) {
				path = HDFS_ROOT_FOLDER_PATH;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.isAccessAllowedForTraversal(" + path + ", " + access + ", " + context.user + ", " + skipAuditOnAllow + ", " + context.operationName + ")");
			}

			RangerHdfsAccessRequest request = new RangerHdfsAccessRequest(inode, path, pathOwner, access, EXECUTE_ACCCESS_TYPE, operation, context.user, context.userGroups);

			RangerAccessResult result = context.plugin.isAccessAllowed(request, null);

			context.saveResult(result);

			if (result != null && result.getIsAccessDetermined() && !result.getIsAllowed()) {
				ret = AuthzStatus.DENY;
			} else {
				ret = AuthzStatus.ALLOW;
			}
			if (ret == AuthzStatus.ALLOW) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("This request is for the first time allowed by Ranger policies. request:[" + request + "]");
				}
			}

			if (ret == AuthzStatus.DENY || (!skipAuditOnAllow && result != null && result.getIsAccessDetermined())) {
				if (context.auditHandler != null) {
					context.auditHandler.processResult(result);
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerAccessControlEnforcer.isAccessAllowedForTraversal(" + path + ", " + access + ", " + context.user + ", " + skipAuditOnAllow + ", " + context.operationName + "): " + ret);
			}

			return ret;
		}

		private AuthzStatus checkDefaultEnforcer(String fsOwner, String superGroup, UserGroupInformation ugi,
									INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr,
									int snapshotId, String path, int ancestorIndex, boolean doCheckOwner,
									FsAction ancestorAccess, FsAction parentAccess, FsAction access,
									FsAction subAccess, boolean ignoreEmptyDir,
                                    INode ancestor, INode parent, INode inode, AuthzContext context
												 ) throws AccessControlException {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.checkDefaultEnforcer("
						+ "fsOwner=" + fsOwner + "; superGroup=" + superGroup + ", inodesCount=" + (inodes != null ? inodes.length : 0)
						+ ", snapshotId=" + snapshotId + ", path=" + path + ", ancestorIndex=" + ancestorIndex
						+ ", doCheckOwner=" + doCheckOwner + ", ancestorAccess=" + ancestorAccess + ", parentAccess=" + parentAccess
						+ ", access=" + access + ", subAccess=" + subAccess + ", ignoreEmptyDir=" + ignoreEmptyDir
						+ ", isTraverseOnlyCheck=" + context.isTraverseOnlyCheck + ",ancestor=" + (ancestor == null ? null : ancestor.getFullPathName())
						+ ", parent=" + (parent == null ? null : parent.getFullPathName()) + ", inode=" + (inode == null ? null : inode.getFullPathName())
						+ ")");
			}

			AuthzStatus authzStatus = AuthzStatus.NOT_DETERMINED;
			if(rangerPlugin.isHadoopAuthEnabled() && defaultEnforcer != null) {

				RangerPerfTracer hadoopAuthPerf = null;

				if(RangerPerfTracer.isPerfTraceEnabled(PERF_HDFSAUTH_REQUEST_LOG)) {
					hadoopAuthPerf = RangerPerfTracer.getPerfTracer(PERF_HDFSAUTH_REQUEST_LOG, "RangerAccessControlEnforcer.checkDefaultEnforcer(path=" + path + ")");
				}

				try {
					defaultEnforcer.checkPermission(fsOwner, superGroup, ugi, inodeAttrs, inodes,
							pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
							ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir);

					authzStatus = AuthzStatus.ALLOW;
				} finally {
					if (context.auditHandler != null) {
						INode nodeChecked = inode;
						FsAction action = access;
						if (context.isTraverseOnlyCheck) {
							if (nodeChecked == null || nodeChecked.isFile()) {
								if (parent != null) {
									nodeChecked = parent;
								} else if (ancestor != null) {
									nodeChecked = ancestor;
								}
							}

							action = FsAction.EXECUTE;
						} else if (action == null || action == FsAction.NONE) {
							if (parentAccess != null && parentAccess != FsAction.NONE) {
								nodeChecked = parent;
								action = parentAccess;
							} else if (ancestorAccess != null && ancestorAccess != FsAction.NONE) {
								nodeChecked = ancestor;
								action = ancestorAccess;
							} else if (subAccess != null && subAccess != FsAction.NONE) {
								action = subAccess;
							}
						}

						String             pathChecked = nodeChecked != null ? nodeChecked.getFullPathName() : path;
						boolean            isAllowed   = authzStatus == AuthzStatus.ALLOW;
						RangerAccessResult lastResult  = context.getLastResult();

						if (lastResult != null) {
							lastResult.setIsAllowed(isAllowed);
							lastResult.setIsAccessDetermined(true);

							context.plugin.evalAuditPolicies(lastResult);

							context.auditHandler.processResult(lastResult);
						}

						context.auditHandler.logHadoopEvent(pathChecked, action, isAllowed);
					}
					RangerPerfTracer.log(hadoopAuthPerf);
				}
			}
			LOG.debug("<== RangerAccessControlEnforcer.checkDefaultEnforcer("
					+ "fsOwner=" + fsOwner + "; superGroup=" + superGroup + ", inodesCount=" + (inodes != null ? inodes.length : 0)
					+ ", snapshotId=" + snapshotId + ", path=" + path + ", ancestorIndex=" + ancestorIndex
					+ ", doCheckOwner="+ doCheckOwner + ", ancestorAccess=" + ancestorAccess + ", parentAccess=" + parentAccess
					+ ", access=" + access + ", subAccess=" + subAccess + ", ignoreEmptyDir=" + ignoreEmptyDir
					+ ", isTraverseOnlyCheck=" + context.isTraverseOnlyCheck + ",ancestor=" + (ancestor == null ? null : ancestor.getFullPathName())
					+ ", parent=" + (parent == null ? null : parent.getFullPathName()) + ", inode=" + (inode == null ? null : inode.getFullPathName())
					+ ") : " + authzStatus );

			return authzStatus;
		}

		private AuthzStatus isAccessAllowed(INode inode, INodeAttributes inodeAttribs, String path, FsAction access, AuthzContext context) {
			AuthzStatus ret       = null;
			String      pathOwner = inodeAttribs != null ? inodeAttribs.getUserName() : null;

			if(pathOwner == null && inode != null) {
				pathOwner = inode.getUserName();
			}

			if (RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH_ALT.equals(path)) {
				path = HDFS_ROOT_FOLDER_PATH;
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.isAccessAllowed(" + path + ", " + access + ", " + context.user + ")");
			}

			Set<String> accessTypes = access2ActionListMapper.get(access);

			if(accessTypes == null) {
				LOG.warn("RangerAccessControlEnforcer.isAccessAllowed(" + path + ", " + access + ", " + context.user + "): no Ranger accessType found for " + access);

				accessTypes = access2ActionListMapper.get(FsAction.NONE);
			}

			if (accessTypes.size() > 0) {
				RangerHdfsAccessRequest request = new RangerHdfsAccessRequest(inode, path, pathOwner, access, accessTypes.iterator().next(), context.operationName, context.user, context.userGroups);

				if (accessTypes.size() > 1) {
					Set<Set<String>> allAccessTypeGroups = accessTypes.stream().map(Collections::singleton).collect(toSet());

					RangerAccessRequestUtil.setAllRequestedAccessTypeGroups(request, allAccessTypeGroups);
					RangerAccessRequestUtil.setAllRequestedAccessTypes(request.getContext(), accessTypes);
					if (accessTypes.contains(EXECUTE_ACCCESS_TYPE)) {
						RangerAccessRequestUtil.setIgnoreIfNotDeniedAccessTypes(request.getContext(), access2ActionListMapper.get(FsAction.EXECUTE));
					}
				}

				RangerAccessResult result = context.plugin.isAccessAllowed(request, context.auditHandler);

				context.saveResult(result);

				if (result == null || !result.getIsAccessDetermined()) {
					ret = AuthzStatus.NOT_DETERMINED;
				} else if (!result.getIsAllowed()) { // explicit deny
					ret = AuthzStatus.DENY;
				} else { // allowed
					ret = AuthzStatus.ALLOW;
				}
				if (ret == AuthzStatus.ALLOW) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("This request is for the first time allowed by Ranger policies. request:[" + request + "]");
					}
				}
			}

			if(ret == null) {
				ret = AuthzStatus.NOT_DETERMINED;
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug("<== RangerAccessControlEnforcer.isAccessAllowed(" + path + ", " + access + ", " + context.user + "): " + ret);
			}

			return ret;
		}

		private AuthzStatus isAccessAllowedForHierarchy(INode inode, INodeAttributes inodeAttribs, String path, FsAction access, AuthzContext context) {
			AuthzStatus ret   = null;
			String  pathOwner = inodeAttribs != null ? inodeAttribs.getUserName() : null;

			if (pathOwner == null && inode != null) {
				pathOwner = inode.getUserName();
			}

			if (RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH_ALT.equals(path)) {
				path = HDFS_ROOT_FOLDER_PATH;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerAccessControlEnforcer.isAccessAllowedForHierarchy(" + path + ", " + access + ", " + context.user + ")");
			}

			if (path != null) {

				Set<String> accessTypes = access2ActionListMapper.get(access);

				if (accessTypes == null) {
					LOG.warn("RangerAccessControlEnforcer.isAccessAllowedForHierarchy(" + path + ", " + access + ", " + context.user + "): no Ranger accessType found for " + access);

					accessTypes = access2ActionListMapper.get(FsAction.NONE);
				}

				String subDirPath = path;
				if (subDirPath.charAt(subDirPath.length() - 1) != Path.SEPARATOR_CHAR) {
					subDirPath = subDirPath + Character.toString(Path.SEPARATOR_CHAR);
				}
				subDirPath = subDirPath + rangerPlugin.getRandomizedWildcardPathName();

				if (accessTypes.size() > 0) {
					RangerHdfsAccessRequest request = new RangerHdfsAccessRequest(null, subDirPath, pathOwner, access, accessTypes.iterator().next(), context.operationName, context.user, context.userGroups);

					if (accessTypes.size() > 1) {
						Set<Set<String>> allAccessTypeGroups = accessTypes.stream().map(Collections::singleton).collect(toSet());

						RangerAccessRequestUtil.setAllRequestedAccessTypeGroups(request, allAccessTypeGroups);
						RangerAccessRequestUtil.setAllRequestedAccessTypes(request.getContext(), accessTypes);
						if (accessTypes.contains(EXECUTE_ACCCESS_TYPE)) {
							RangerAccessRequestUtil.setIgnoreIfNotDeniedAccessTypes(request.getContext(), access2ActionListMapper.get(FsAction.EXECUTE));
						}
					}

					RangerAccessResult result = context.plugin.isAccessAllowed(request, null);

					context.saveResult(result);

					if (result == null || !result.getIsAccessDetermined()) {
						ret = AuthzStatus.NOT_DETERMINED;
					} else if(! result.getIsAllowed()) { // explicit deny
						ret = AuthzStatus.DENY;
					} else { // allowed
						ret = AuthzStatus.ALLOW;
					}
				}
			}

			if(ret == null) {
				ret = AuthzStatus.NOT_DETERMINED;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerAccessControlEnforcer.isAccessAllowedForHierarchy(" + path + ", " + access + ", " + context.user + "): " + ret);
			}

			return ret;
		}
		/*
			Description	: optimize() checks if the given operation is a candidate for optimizing (reducing) the number of times it is authorized
			Returns 	: null, if the operation, in its current invocation, cannot be optimized.
						: OptimizedAuthzContext with the authzStatus set to null, if the operation in its current invocation needs to be authorized. However, the next invocation
						  for the same user and the resource can be optimized based on the result of the authorization.
						: OptimizedAuthzContext with the authzStatus set to non-null, if the operation in its current invocation need not be authorized.
			Algorithm   : The algorithm is based on the specifics of each operation that is potentially optimized.
						  	1. OPERATION_NAME_COMPLETEFILE:
						  		 Skipping this authorization check may break semantic equivalence (according to HDFS team). Therefore, no optimization
						  		is attempted for this operation.
						  	2. OPERATION_NAME_DELETE:
						  		Namenode calls this twice when deleting a file. First invocation checks if the user has a EXECUTE access on the parent directory, and second invocation
						  		checks if the user has a WRITE access on the parent directory as well as ALL access on the directory tree rooted at the parent directory. First invocation
						  		can be optimized away and the second invocation is authorized with the parent directory access modified from WRITE to a WRITE_EXECUTE.
						  		Namenode calls this three times when deleting a directory. The optimization code results in eliminating one authorization check out of three.
							3. OPERATION_NAME_CREATE, OPERATION_NAME_MKDIRS:
								Namenode calls this twice when creating a new file or a directory. First invocation checks if the user has a EXECUTE access on the parent directory, and second invocation
								checks if the user has a WRITE access to the parent directory. The optimized code combines these checks into a WRITE_EXECUTE access for the first invocation,
								and optimizes away the second call.
								Namenode calls this three times when re-creating an existing file. In addition to two invocations described above, it also checks if the user has
								a WRITE access to the file itself. This extra call is not optimized.
							4. OPERATION_NAME_RENAME:
								Namenode calls this twice when renaming a file for source as well as target directories. For each directory, first invocation checks if the user has a EXECUTE access on the parent directory, and second invocation
								checks if the user has a WRITE access to the parent (or ancestor when checking target directory) . The optimized code combines these checks into a WRITE_EXECUTE access for the first invocation,
								and optimizes away the second call.
							5. 	OPERATION_NAME_LISTSTATUS, OPERATION_NAME_GETEZFORPATH:
								Namenode calls this twice when listing a directory or getting the encryption zone for the directory. First invocation checks if the user has a EXECUTE access
								on the directory, and second checks if the user has a READ_EXECUTE access on the directory. The optimized code combines these checks into a READ_EXECUTE access
								for the first invocation.
		 */


		class OperationOptimizer {
			private final String            operationName;

			private final byte[][]          components;
			private final INodeAttributes[] inodeAttrs;
			private final int               ancestorIndex;
			private final INode             ancestor;
			private final INode             parent;
			private final INode             inode;

			private       String            resourcePath;
			private       FsAction          ancestorAccess;
			private       FsAction          parentAccess;
			private       FsAction          access;
			private final FsAction          subAccess;

			OperationOptimizer(String operationName, String resourcePath, FsAction ancestorAccess, FsAction parentAccess, FsAction access, FsAction subAccess, byte[][] components, INodeAttributes[] inodeAttrs, int ancestorIndex, INode ancestor, INode parent, INode inode) {
				this.operationName = operationName;

				this.resourcePath   = resourcePath;
				this.ancestorAccess = ancestorAccess;
				this.parentAccess   = parentAccess;
				this.access         = access;
				this.subAccess      = subAccess;

				this.components     = components;
				this.inodeAttrs     = inodeAttrs;
				this.ancestorIndex  = ancestorIndex;
				this.ancestor       = ancestor;
				this.parent         = parent;
				this.inode          = inode;
			}

			OptimizedAuthzContext optimize() {
				if (!AUTHZ_OPTIMIZATION_ENABLED || !OPTIMIZED_OPERATIONS.contains(operationName)) {
					return null;
				}
				return optimizeOp(operationName);
			}

			OptimizedAuthzContext optimizeOp(String operationName) {
				switch (operationName) {
					case OPERATION_NAME_CREATE:
						return optimizeCreateOp();
					case OPERATION_NAME_DELETE:
						return optimizeDeleteOp();
					case OPERATION_NAME_RENAME:
						return optimizeRenameOp();
					case OPERATION_NAME_MKDIRS:
						return optimizeMkdirsOp();
					case OPERATION_NAME_LISTSTATUS:
						return optimizeListStatusOp();
					case OPERATION_NAME_GETEZFORPATH:
						return optimizeGetEZForPathOp();
					default:
						break;
				}
				return null;
			}

			private OptimizedAuthzContext optimizeCreateOp() {
				INode nodeToAuthorize = getINodeToAuthorize();
				if (nodeToAuthorize == null) {
					return OPT_BYPASS_AUTHZ;
				}

				if (!nodeToAuthorize.isDirectory() && access == null) {        // If not a directory, the access must be non-null as when recreating existing file
					if (LOG.isDebugEnabled()) {
						LOG.debug("nodeToCheck is not a directory and access is null for a create operation! Optimization skipped");
					}
					return null;
				}

				return getOrCreateOptimizedAuthzContext();
			}

			private OptimizedAuthzContext optimizeDeleteOp() {
				int numOfRequestedAccesses = 0;

				if (ancestorAccess != null) numOfRequestedAccesses++;
				if (parentAccess != null) numOfRequestedAccesses++;
				if (access != null) numOfRequestedAccesses++;
				if (subAccess != null) numOfRequestedAccesses++;

				if (numOfRequestedAccesses == 0) {
					return OPT_BYPASS_AUTHZ;
				} else {
					parentAccess = FsAction.WRITE_EXECUTE;
					return getOrCreateOptimizedAuthzContext();
				}
			}

			private OptimizedAuthzContext optimizeRenameOp() {
				INode nodeToAuthorize = getINodeToAuthorize();

				if (nodeToAuthorize == null) {
					return OPT_BYPASS_AUTHZ;
				}

				if (!nodeToAuthorize.isDirectory()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("nodeToCheck is not a directory for a rename operation! Optimization skipped");
					}
					return null;
				}

				return getOrCreateOptimizedAuthzContext();
			}

			private OptimizedAuthzContext optimizeMkdirsOp() {
				INode nodeToAuthorize = getINodeToAuthorize();

				if (nodeToAuthorize == null) {
					return OPT_BYPASS_AUTHZ;
				}

				if (!nodeToAuthorize.isDirectory()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("nodeToCheck is not a directory for a mkdirs operation! Optimization skipped");
					}
					return null;
				}

				return getOrCreateOptimizedAuthzContext();
			}

			private OptimizedAuthzContext optimizeListStatusOp() {
				if (inode == null || inode.isFile()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("inode is null or is a file for a listStatus/getEZForPath operation! Optimization skipped");
					}

					return null;
				} else {
					if (resourcePath.length() > 1) {
						if (resourcePath.endsWith(HDFS_ROOT_FOLDER_PATH)) {
							resourcePath = resourcePath.substring(0, resourcePath.length() - 1);
						}
					}
					access = FsAction.READ_EXECUTE;

					return getOrCreateOptimizedAuthzContext();
				}
			}

			private OptimizedAuthzContext optimizeGetEZForPathOp() {
				if (inode == null || inode.isFile()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("inode is null or is a file for a listStatus/getEZForPath operation! Optimization skipped");
					}

					return null;
				} else {
					access = FsAction.READ_EXECUTE;

					return getOrCreateOptimizedAuthzContext();
				}
			}

			private INode getINodeToAuthorize() {
				INode ret = null;

				INode nodeToAuthorize = inode;

				if (nodeToAuthorize == null || nodeToAuthorize.isFile()) {
					// Case where the authorizer is called to authorize re-creation of an existing file. This is to check if the file itself is write-able

					if (StringUtils.equals(operationName, OPERATION_NAME_CREATE) && inode != null && access != null) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Create operation with non-null access is being authorized. authorize for write access for the file!!");
						}
					} else {
						if (parent != null) {
							nodeToAuthorize      = parent;
							resourcePath         = inodeAttrs.length > 0 ? DFSUtil.byteArray2PathString(components, 0, inodeAttrs.length - 1) : HDFS_ROOT_FOLDER_PATH;
							parentAccess = FsAction.WRITE_EXECUTE;
						} else if (ancestor != null) {
							INodeAttributes nodeAttribs = inodeAttrs.length > ancestorIndex ? inodeAttrs[ancestorIndex] : null;

							nodeToAuthorize        = ancestor;
							resourcePath           = nodeAttribs != null ? DFSUtil.byteArray2PathString(components, 0, ancestorIndex + 1) : HDFS_ROOT_FOLDER_PATH;
							ancestorAccess = FsAction.WRITE_EXECUTE;
						}
						if (resourcePath.length() > 1) {
							if (resourcePath.endsWith(HDFS_ROOT_FOLDER_PATH)) {
								resourcePath = resourcePath.substring(0, resourcePath.length() - 1);
							}
						}
					}
					ret = nodeToAuthorize;
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("inode is not null and it is not a file for a create/rename/mkdirs operation! Optimization skipped");
					}

				}
				return ret;
			}

			private OptimizedAuthzContext getOrCreateOptimizedAuthzContext() {
				if (CACHE == null) {
					CACHE = new HashMap<>();
				}

				OptimizedAuthzContext opContext = CACHE.get(resourcePath);

				if (opContext == null) {
					opContext = new OptimizedAuthzContext(resourcePath, ancestorAccess, parentAccess, access, null);

					CACHE.put(resourcePath, opContext);

					if (LOG.isDebugEnabled()) {
						LOG.debug("Added OptimizedAuthzContext:[" + opContext + "] to cache");
					}
				}

				return opContext;
			}
		}
	}

	static class OptimizedAuthzContext {
		private final String      path;
		private final FsAction    ancestorAccess;
		private final FsAction    parentAccess;
		private final FsAction    access;
		private       AuthzStatus authzStatus;

		OptimizedAuthzContext(String path, FsAction ancestorAccess, FsAction parentAccess, FsAction access, AuthzStatus authzStatus) {
			this.path           = path;
			this.ancestorAccess = ancestorAccess;
			this.parentAccess   = parentAccess;
			this.access         = access;
			this.authzStatus    = authzStatus;
		}

		@Override
		public String toString() {
			return "path=" + path + ", authzStatus=" + authzStatus;
		}
	}

}


class RangerHdfsPlugin extends RangerBasePlugin {
	private static final Logger LOG = LoggerFactory.getLogger(RangerHdfsPlugin.class);

	private static String fileNameExtensionSeparator = RangerHdfsAuthorizer.DEFAULT_FILENAME_EXTENSION_SEPARATOR;

	private final boolean     hadoopAuthEnabled;
	private final boolean     optimizeSubAccessAuthEnabled;
	private final String      randomizedWildcardPathName;
	private final String      hadoopModuleName;
	private final Set<String> excludeUsers = new HashSet<>();
	private final boolean     useLegacySubAccessAuthorization;

	public RangerHdfsPlugin(Path addlConfigFile) {
		super("hdfs", "hdfs");

		RangerPluginConfig config = getConfig();

		if (addlConfigFile != null) {
			config.addResource(addlConfigFile);
		}

		String random = generateString("^&#@!%()-_+=@:;'<>`~abcdefghijklmnopqrstuvwxyz01234567890");

		RangerHdfsPlugin.fileNameExtensionSeparator = config.get(RangerHdfsAuthorizer.RANGER_FILENAME_EXTENSION_SEPARATOR_PROP, RangerHdfsAuthorizer.DEFAULT_FILENAME_EXTENSION_SEPARATOR);

		this.hadoopAuthEnabled = config.getBoolean(RangerHadoopConstants.RANGER_ADD_HDFS_PERMISSION_PROP, RangerHadoopConstants.RANGER_ADD_HDFS_PERMISSION_DEFAULT);
		config.setIsFallbackSupported(this.hadoopAuthEnabled);

		this.optimizeSubAccessAuthEnabled = config.getBoolean(RangerHadoopConstants.RANGER_OPTIMIZE_SUBACCESS_AUTHORIZATION_PROP, RangerHadoopConstants.RANGER_OPTIMIZE_SUBACCESS_AUTHORIZATION_DEFAULT);
		this.randomizedWildcardPathName   = RangerPathResourceMatcher.WILDCARD_ASTERISK + random + RangerPathResourceMatcher.WILDCARD_ASTERISK;
		this.hadoopModuleName             = config.get(RangerHadoopConstants.AUDITLOG_HADOOP_MODULE_ACL_NAME_PROP , RangerHadoopConstants.DEFAULT_HADOOP_MODULE_ACL_NAME);

		String excludeUserList = config.get(RangerHadoopConstants.AUDITLOG_HDFS_EXCLUDE_LIST_PROP, RangerHadoopConstants.AUDITLOG_EMPTY_STRING);

		this.useLegacySubAccessAuthorization = config.getBoolean(RangerHadoopConstants.RANGER_USE_LEGACY_SUBACCESS_AUTHORIZATION_PROP, RangerHadoopConstants.RANGER_USE_LEGACY_SUBACCESS_AUTHORIZATION_DEFAULT);


		if (excludeUserList != null && excludeUserList.trim().length() > 0) {
			for(String excludeUser : excludeUserList.trim().split(",")) {
				excludeUser = excludeUser.trim();

				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding exclude user [" + excludeUser + "]");
				}

				excludeUsers.add(excludeUser);
			}
		}
	}

	// Build random string of length between 56 and 112 characters
	private static String generateString(String source)
	{
		SecureRandom rng = new SecureRandom();

		byte[] bytes = new byte[1];
		rng.nextBytes(bytes);
		int length = bytes[0];
		length = length < 56 ? 56 : length;
		length = length > 112 ? 112 : length;

		char[] text = new char[length];

		for (int i = 0; i < length; i++)
		{
			text[i] = source.charAt(rng.nextInt(source.length()));
		}
		return new String(text);
	}

	public static String getFileNameExtensionSeparator() {
		return fileNameExtensionSeparator;
	}

	public boolean isHadoopAuthEnabled() {
		return hadoopAuthEnabled;
	}
	public boolean isOptimizeSubAccessAuthEnabled() {
		return optimizeSubAccessAuthEnabled;
	}
	public String getRandomizedWildcardPathName() {
		return randomizedWildcardPathName;
	}
	public String getHadoopModuleName() { return hadoopModuleName; }
	public Set<String> getExcludedUsers() { return  excludeUsers; }
	public boolean isUseLegacySubAccessAuthorization() {
		return useLegacySubAccessAuthorization;
	}
}

class RangerHdfsResource extends RangerAccessResourceImpl {

	public RangerHdfsResource(String path, String owner) {
		super.setValue(RangerHdfsAuthorizer.KEY_RESOURCE_PATH, path);
		super.setOwnerUser(owner);
	}

	@Override
	public String getAsString() {
		String ret = super.getStringifiedValue();

		if (ret == null) {
			ret = Objects.toString(super.getValue(RangerHdfsAuthorizer.KEY_RESOURCE_PATH));

			super.setStringifiedValue(ret);
		}

		return ret;
	}
}

class RangerHdfsAccessRequest extends RangerAccessRequestImpl {

	public RangerHdfsAccessRequest(INode inode, String path, String pathOwner, FsAction access, String accessType, String action, String user, Set<String> groups) {
		if (action == null && access != null) {
			action = access.toString();
		}

		super.setResource(new RangerHdfsResource(path, pathOwner));
		super.setAccessType(accessType);
		super.setUser(user);
		super.setUserGroups(groups);
		super.setAccessTime(new Date());
		super.setClientIPAddress(getRemoteIp());
		super.setAction(action);
		super.setForwardedAddresses(null);
		super.setRemoteIPAddress(getRemoteIp());

		if (inode != null) {
			buildRequestContext(inode);
		}
	}

	private static String getRemoteIp() {
		String ret = null;
		InetAddress ip = Server.getRemoteIp();
		if (ip != null) {
			ret = ip.getHostAddress();
		}
		return ret;
	}
	private void buildRequestContext(final INode inode) {
		if (inode.isFile()) {
			String fileName = inode.getLocalName();
			RangerAccessRequestUtil.setTokenInContext(getContext(), RangerHdfsAuthorizer.KEY_FILENAME, fileName);
			int lastExtensionSeparatorIndex = fileName.lastIndexOf(RangerHdfsPlugin.getFileNameExtensionSeparator());
			if (lastExtensionSeparatorIndex != -1) {
				String baseFileName = fileName.substring(0, lastExtensionSeparatorIndex);
				RangerAccessRequestUtil.setTokenInContext(getContext(), RangerHdfsAuthorizer.KEY_BASE_FILENAME, baseFileName);
			}
		}
	}
}

class AuthzContext {
	public final RangerHdfsPlugin       plugin;
	public final String                 user;
	public final Set<String>            userGroups;
	public final String                 operationName;
	public       boolean                isTraverseOnlyCheck;
	public       RangerHdfsAuditHandler auditHandler = null;
	private      RangerAccessResult     lastResult   = null;

	public AuthzContext(RangerHdfsPlugin plugin, UserGroupInformation ugi, String operationName, boolean isTraverseOnlyCheck) {
		this.plugin              = plugin;
		this.user                = ugi != null ? ugi.getShortUserName() : null;
		this.userGroups          = ugi != null ? Sets.newHashSet(ugi.getGroupNames()) : null;
		this.operationName       = operationName;
		this.isTraverseOnlyCheck = isTraverseOnlyCheck;
	}

	public void saveResult(RangerAccessResult result) {
		if (result != null) {
			this.lastResult = result;
		}
	}

	public RangerAccessResult getLastResult() {
		return lastResult;
	}
}

class RangerHdfsAuditHandler extends RangerDefaultAuditHandler {
	private static final Logger LOG = LoggerFactory.getLogger(RangerHdfsAuditHandler.class);

	private boolean         isAuditEnabled = false;
	private AuthzAuditEvent auditEvent     = null;
	private final String pathToBeValidated;
	private final boolean auditOnlyIfDenied;

	private final String      hadoopModuleName;
	private final Set<String> excludeUsers;
	private final String      callerContext;

	public RangerHdfsAuditHandler(String pathToBeValidated, boolean auditOnlyIfDenied, String hadoopModuleName, Set<String> excludedUsers, String callerContext) {
		this.pathToBeValidated = pathToBeValidated;
		this.auditOnlyIfDenied = auditOnlyIfDenied;
		this.hadoopModuleName  = hadoopModuleName;
		this.excludeUsers      = excludedUsers;
		this.callerContext     = callerContext;
	}

	@Override
	public void processResult(RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuditHandler.logAudit(" + result + ")");
		}

		if (result != null) {
			isAuditEnabled = result.getIsAudited();

			if (auditEvent == null) {
				auditEvent = super.getAuthzEvents(result);
			}

			if (auditEvent != null) {
				RangerAccessRequest request = result.getAccessRequest();
				RangerAccessResource resource = request.getResource();
				String resourcePath = resource != null ? resource.getAsString() : null;

				// Overwrite fields in original auditEvent
				auditEvent.setEventTime(request.getAccessTime() != null ? request.getAccessTime() : new Date());
				auditEvent.setAccessType(request.getAction());
				auditEvent.setResourcePath(this.pathToBeValidated);
				auditEvent.setResultReason(resourcePath);

				auditEvent.setAccessResult((short) (result.getIsAllowed() ? 1 : 0));
				auditEvent.setPolicyId(result.getPolicyId());
				auditEvent.setPolicyVersion(result.getPolicyVersion());
				setRequestData();

				auditEvent.setAction(getAccessType(request.getAccessType()));
				auditEvent.setAdditionalInfo(getAdditionalInfo(request));

				Set<String> tags = getTags(request);
				if (tags != null) {
					auditEvent.setTags(tags);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuditHandler.logAudit(" + result + "): " + auditEvent);
		}
	}

	@Override
	public 	String getAdditionalInfo(RangerAccessRequest request) {
		String        additionalInfo = super.getAdditionalInfo(request);
		Map addInfoMap = JsonUtils.jsonToMapStringString(additionalInfo);
		if(addInfoMap == null || addInfoMap.isEmpty()) {
		      addInfoMap = new HashMap<String,String>();
		}
		String accessTypes = getAccessTypesAsString(request);
		if (addInfoMap != null && accessTypes != null) {
			addInfoMap.put("accessTypes", "[" + accessTypes + "]");
		}
		return JsonUtils.mapToJson(addInfoMap);
	}

	public void logHadoopEvent(String path, FsAction action, boolean accessGranted) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuditHandler.logHadoopEvent(" + path + ", " + action + ", " + accessGranted + ")");
		}

		if(auditEvent != null) {
			auditEvent.setResultReason(path);
			auditEvent.setAccessResult((short) (accessGranted ? 1 : 0));
			auditEvent.setAclEnforcer(hadoopModuleName);
			auditEvent.setPolicyId(-1);

			String accessType = (action == null) ? null : action.toString();

			if (StringUtils.isBlank(auditEvent.getAccessType())) { // retain existing value
				auditEvent.setAccessType(accessType);
			}

			if (accessType != null) {
				auditEvent.setAction(getAccessType(accessType));
			}

			setRequestData();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuditHandler.logHadoopEvent(" + path + ", " + action + ", " + accessGranted + "): " + auditEvent);
		}
	}

	public void flushAudit() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerHdfsAuditHandler.flushAudit(" + isAuditEnabled + ", " + auditEvent + ")");
		}

		if(isAuditEnabled && auditEvent != null && !StringUtils.isEmpty(auditEvent.getAccessType())) {
			String username = auditEvent.getUser();
			String accessType = auditEvent.getAccessType();

			boolean skipLog = (username != null && excludeUsers != null && excludeUsers.contains(username))
								|| (auditOnlyIfDenied && auditEvent.getAccessResult() != 0)
								|| (ACCESS_TYPE_MONITOR_HEALTH.equals(accessType));

			if (! skipLog) {
				super.logAuthzAudit(auditEvent);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerHdfsAuditHandler.flushAudit(" + isAuditEnabled + ", " + auditEvent + ")");
		}
	}

	private String getAccessType(String accessType) {
		String ret = accessType;

		switch (accessType) {
			case READ_EXECUTE_PERM:
				ret = READ_ACCCESS_TYPE;
				break;
			case WRITE_EXECUTE_PERM:
			case READ_WRITE_PERM:
			case ALL_PERM:
				ret = WRITE_ACCCESS_TYPE;
				break;
			default:
				break;
		}
		return ret.toLowerCase();
	}

	private String getAccessTypesAsString(RangerAccessRequest request) {
		String             ret         = null;
		Set<String>        accessTypes = RangerAccessRequestUtil.getAllRequestedAccessTypes(request);

		if (CollectionUtils.isNotEmpty(accessTypes)) {
			try {
				ret = getFormattedAccessType(accessTypes);
			} catch (Throwable t) {
				LOG.error("getAccessTypesAsString(): failed to get accessTypes from context", t);
			}
		} else {
			ret = request.getAccessType();
		}
		return ret;
	}

	private String getFormattedAccessType(Set<String> accessTypes) {
		String ret = null;
		if (CollectionUtils.isNotEmpty(accessTypes)) {
			ret = String.join(", ", accessTypes);
		}
		return ret;
	}

	private void setRequestData(){
		if (StringUtils.isNotBlank(auditEvent.getAccessType()) && StringUtils.isNotBlank(callerContext)) {
			auditEvent.setRequestData(auditEvent.getAccessType() + "/" + callerContext);
		}
	}
}
