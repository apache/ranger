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

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider.AccessControlEnforcer;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.hadoop.exceptions.RangerAccessControlException;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.apache.ranger.authorization.hadoop.OperationOptimizer.OPT_BYPASS_AUTHZ;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.EXECUTE_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.READ_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.WRITE_ACCCESS_TYPE;

public class RangerAccessControlEnforcer implements AccessControlEnforcer {
    private static final Logger LOG                       = LoggerFactory.getLogger(RangerAccessControlEnforcer.class);
    private static final Logger PERF_HDFSAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("hdfsauth.request");

    private static final Map<FsAction, Set<String>> ACCESS_TO_ACTIONS;

    private final RangerHdfsPlugin      plugin;
    private final AccessControlEnforcer defaultEnforcer;

    private Map<String, OptimizedAuthzContext> pathToContextCache;

    public RangerAccessControlEnforcer(RangerHdfsPlugin plugin, AccessControlEnforcer defaultEnforcer) {
        LOG.debug("==> RangerAccessControlEnforcer.RangerAccessControlEnforcer()");

        this.plugin          = plugin;
        this.defaultEnforcer = defaultEnforcer;

        LOG.debug("<== RangerAccessControlEnforcer.RangerAccessControlEnforcer()");
    }

    public Map<String, OptimizedAuthzContext> getOrCreateCache() {
        Map<String, OptimizedAuthzContext> ret = pathToContextCache;

        if (ret == null) {
            ret = new HashMap<>();

            pathToContextCache = ret;
        }

        return ret;
    }

    @Override
    public void checkPermission(String fsOwner, String superGroup, UserGroupInformation ugi, INodeAttributes[] inodeAttrs, INode[] inodes,
            byte[][] pathByNameArr, int snapshotId, String path, int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
            FsAction parentAccess, FsAction access, FsAction subAccess, boolean ignoreEmptyDir) throws AccessControlException {
        checkRangerPermission(fsOwner, superGroup, ugi, inodeAttrs, inodes, pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner, ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir, null, null);
    }

    @Override
    public void checkPermissionWithContext(INodeAttributeProvider.AuthorizationContext authzContext) throws AccessControlException {
        checkRangerPermission(authzContext.getFsOwner(), authzContext.getSupergroup(), authzContext.getCallerUgi(), authzContext.getInodeAttrs(),
                authzContext.getInodes(), authzContext.getPathByNameArr(), authzContext.getSnapshotId(), authzContext.getPath(),
                authzContext.getAncestorIndex(), authzContext.isDoCheckOwner(), authzContext.getAncestorAccess(), authzContext.getParentAccess(),
                authzContext.getAccess(), authzContext.getSubAccess(), authzContext.isIgnoreEmptyDir(), authzContext.getOperationName(),
                authzContext.getCallerContext());
    }

    private void checkRangerPermission(String fsOwner, String superGroup, UserGroupInformation ugi, INodeAttributes[] inodeAttrs, INode[] inodes,
            byte[][] pathByNameArr, int snapshotId, String path, int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
            FsAction parentAccess, FsAction access, FsAction subAccess, boolean ignoreEmptyDir, String operationName, CallerContext callerContext) throws AccessControlException {
        AuthzStatus  authzStatus  = AuthzStatus.NOT_DETERMINED;
        String       resourcePath = path;
        AuthzContext context      = new AuthzContext(ugi, operationName, access == null && parentAccess == null && ancestorAccess == null && subAccess == null);

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAccessControlEnforcer.checkRangerPermission(fsOwner={}; superGroup={}, inodesCount={}, snapshotId={}, user={}, provided-path={}, ancestorIndex={}, doCheckOwner={}, ancestorAccess={}, parentAccess={}, access={}, subAccess={}, ignoreEmptyDir={}, operationName={}, callerContext={})",
                    fsOwner, superGroup, inodes != null ? inodes.length : 0, snapshotId, context.user, path, ancestorIndex, doCheckOwner,
                    ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir, operationName, callerContext);

            LOG.info("operationName={}, path={}, user={}, ancestorIndex={}, ancestorAccess={}, parentAccess={}, access={}, subAccess={}", context.operationName, path, context.user, ancestorIndex, ancestorAccess, parentAccess, access, subAccess);
        }

        OptimizedAuthzContext optAuthzContext = null;
        RangerPerfTracer      perf            = null;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_HDFSAUTH_REQUEST_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_HDFSAUTH_REQUEST_LOG, "RangerHdfsAuthorizer.checkRangerPermission(provided-path=" + path + ")");
        }

        try {
            INode  ancestor     = null;
            INode  parent       = null;
            INode  inode        = null;
            String providedPath = path;

            boolean useDefaultAuthorizerOnly = false;
            boolean doNotGenerateAuditRecord = false;

            if (plugin != null && !ArrayUtils.isEmpty(inodes)) {
                int sz = inodeAttrs.length;

                LOG.trace("Size of INodeAttrs array:[{}]", sz);
                LOG.trace("Size of INodes array:[{}]", inodes.length);

                byte[][] components = new byte[sz][];
                int      i          = 0;

                for (; i < sz; i++) {
                    if (inodeAttrs[i] != null) {
                        components[i] = inodeAttrs[i].getLocalNameBytes();
                    } else {
                        break;
                    }
                }

                if (i != sz) {
                    LOG.trace("Input INodeAttributes array contains null at position {}", i);
                    LOG.trace("Will use only first [{}] components", i);
                }

                if (sz == 1 && inodes.length == 1 && inodes[0].getParent() != null) {
                    LOG.trace("Using the only inode in the array to figure out path to resource. No audit record will be generated for this authorization request");

                    doNotGenerateAuditRecord = true;

                    resourcePath = inodes[0].getFullPathName();

                    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
                        useDefaultAuthorizerOnly = true;

                        LOG.trace("path:[{}] is for a snapshot, id=[{}], default Authorizer will be used to authorize this request", resourcePath, snapshotId);
                    } else {
                        LOG.trace("path:[{}] is not for a snapshot, id=[{}]. It will be used to authorize this request", resourcePath, snapshotId);
                    }
                } else {
                    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
                        resourcePath = DFSUtil.byteArray2PathString(pathByNameArr);

                        LOG.trace("pathByNameArr array is used to figure out path to resource, resourcePath:[{}]", resourcePath);
                    } else {
                        resourcePath = DFSUtil.byteArray2PathString(components, 0, i);

                        LOG.trace("INodeAttributes array is used to figure out path to resource, resourcePath:[{}]", resourcePath);
                    }
                }

                if (ancestorIndex >= inodes.length) {
                    ancestorIndex = inodes.length - 1;
                }

                for (; ancestorIndex >= 0 && inodes[ancestorIndex] == null; ancestorIndex--) {
                    // empty
                }

                ancestor = inodes.length > ancestorIndex && ancestorIndex >= 0 ? inodes[ancestorIndex] : null;
                parent   = inodes.length > 1 ? inodes[inodes.length - 2] : null;
                inode    = inodes[inodes.length - 1]; // could be null while creating a new file

                /*
                    Check if optimization is done
                 */
                if (plugin.isAuthzOptimizationEnabled() && OperationOptimizer.isOptimizableOperation(operationName)) {
                    optAuthzContext = (new OperationOptimizer(this, operationName, resourcePath, ancestorAccess, parentAccess, access, subAccess, components, inodeAttrs, ancestorIndex, ancestor, parent, inode)).optimize();
                }

                if (optAuthzContext == OPT_BYPASS_AUTHZ) {
                    authzStatus = AuthzStatus.ALLOW;

                    return;
                } else if (optAuthzContext != null && optAuthzContext.authzStatus != null) {
                    authzStatus = optAuthzContext.authzStatus;

                    LOG.debug("OperationOptimizer.optimize() returned {}, operationName={} has been pre-computed. Returning without any access evaluation!", authzStatus, operationName);

                    if (authzStatus == AuthzStatus.ALLOW) {
                        return;
                    }

                    final FsAction action;

                    if (access != null) {
                        action = access;
                    } else if (parentAccess != null) {
                        action = parentAccess;
                    } else if (ancestorAccess != null) {
                        action = ancestorAccess;
                    } else {
                        action = FsAction.EXECUTE;
                    }

                    throw new RangerAccessControlException("Permission denied: user=" + context.user + ", access=" + action + ", inode=\"" + resourcePath + "\"");
                } else {
                    authzStatus = useDefaultAuthorizerOnly ? AuthzStatus.NOT_DETERMINED : AuthzStatus.ALLOW;
                }

                if (optAuthzContext != null) {
                    access         = optAuthzContext.access;
                    parentAccess   = optAuthzContext.parentAccess;
                    ancestorAccess = optAuthzContext.ancestorAccess;
                } else {
                    LOG.debug("OperationOptimizer.optimize() returned null, operationName={} needs to be evaluated!", operationName);
                }

                context.isTraverseOnlyCheck = parentAccess == null && ancestorAccess == null && access == null && subAccess == null;
                context.auditHandler        = doNotGenerateAuditRecord ? null : new RangerHdfsAuditHandler(providedPath, context.isTraverseOnlyCheck, plugin.getHadoopModuleName(), plugin.getExcludedUsers(), callerContext != null ? callerContext.toString() : null);

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
                if (authzStatus == AuthzStatus.ALLOW && ancestorAccess != null && ancestor != null) {
                    INodeAttributes ancestorAttribs = inodeAttrs.length > ancestorIndex ? inodeAttrs[ancestorIndex] : null;
                    String          ancestorPath    = ancestorAttribs != null ? DFSUtil.byteArray2PathString(components, 0, ancestorIndex + 1) : null;

                    authzStatus = isAccessAllowed(ancestor, ancestorAttribs, ancestorPath, ancestorAccess, context);
                    if (authzStatus == AuthzStatus.NOT_DETERMINED) {
                        authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes, pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
                                ancestorAccess, null, null, null, ignoreEmptyDir, ancestor, parent, inode, context);
                    }
                }

                // checkParentAccess
                if (authzStatus == AuthzStatus.ALLOW && parentAccess != null && parent != null) {
                    INodeAttributes parentAttribs = inodeAttrs.length > 1 ? inodeAttrs[inodeAttrs.length - 2] : null;
                    String          parentPath    = parentAttribs != null ? DFSUtil.byteArray2PathString(components, 0, inodeAttrs.length - 1) : null;

                    authzStatus = isAccessAllowed(parent, parentAttribs, parentPath, parentAccess, context);
                    if (authzStatus == AuthzStatus.NOT_DETERMINED) {
                        authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes, pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
                                null, parentAccess, null, null, ignoreEmptyDir, ancestor, parent, inode, context);
                    }
                }

                // checkINodeAccess
                if (authzStatus == AuthzStatus.ALLOW && access != null && inode != null) {
                    INodeAttributes inodeAttribs = inodeAttrs.length > 0 ? inodeAttrs[inodeAttrs.length - 1] : null;

                    authzStatus = isAccessAllowed(inode, inodeAttribs, resourcePath, access, context);
                    if (authzStatus == AuthzStatus.NOT_DETERMINED) {
                        authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes, pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
                                null, null, access, null, ignoreEmptyDir, ancestor, parent, inode, context);
                    }
                }

                // checkSubAccess
                if (authzStatus == AuthzStatus.ALLOW && subAccess != null && inode != null && inode.isDirectory()) {
                    Stack<SubAccessData> directories = new Stack<>();

                    for (directories.push(new SubAccessData(inode.asDirectory(), resourcePath, inodes, inodeAttrs)); !directories.isEmpty(); ) {
                        SubAccessData       data  = directories.pop();
                        ReadOnlyList<INode> cList = data.dir.getChildrenList(snapshotId);

                        if (!(cList.isEmpty() && ignoreEmptyDir)) {
                            INodeAttributes dirAttribs = data.dir.getSnapshotINode(snapshotId);

                            authzStatus = isAccessAllowed(data.dir, dirAttribs, data.resourcePath, subAccess, context);

                            INodeDirectory    dirINode;
                            int               dirAncestorIndex;
                            INodeAttributes[] dirINodeAttrs;
                            INode[]           dirINodes;
                            INode             dirAncestor;
                            INode             dirParent;
                            byte[][]          dirComponents;

                            if (data.dir.equals(inode)) {
                                dirINode         = inode.asDirectory();
                                dirINodeAttrs    = inodeAttrs;
                                dirINodes        = inodes;
                                dirAncestorIndex = ancestorIndex;
                                dirAncestor      = ancestor;
                                dirParent        = parent;
                                dirComponents    = pathByNameArr;
                            } else {
                                INodeAttributes[] curINodeAttributes;
                                INode[]           curINodes;

                                dirINode           = data.dir;
                                curINodeAttributes = data.iNodeAttributes;
                                curINodes          = data.inodes;

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

                                for (dirAncestorIndex = dirINodes.length - 1; dirAncestorIndex >= 0 && dirINodes[dirAncestorIndex] == null; dirAncestorIndex--) {
                                    // empty
                                }

                                dirAncestor   = dirINodes.length > dirAncestorIndex && dirAncestorIndex >= 0 ? dirINodes[dirAncestorIndex] : null;
                                dirParent     = dirINodes.length > 1 ? dirINodes[dirINodes.length - 2] : null;
                                dirComponents = dirINode.getPathComponents();
                            }

                            if (authzStatus == AuthzStatus.NOT_DETERMINED && !plugin.isUseLegacySubAccessAuthorization()) {
                                if (LOG.isDebugEnabled()) {
                                    if (data.dir.equals(inode)) {
                                        LOG.debug("Top level directory being processed for default authorizer call, [{}]", data.resourcePath);
                                    } else {
                                        LOG.debug("Sub directory being processed for default authorizer call, [{}]", data.resourcePath);
                                    }

                                    LOG.debug("Calling default authorizer for hierarchy/subaccess with the following parameters");

                                    LOG.debug("fsOwner={}; superGroup={}, inodesCount={}, snapshotId={}, user={}, provided-path={}, ancestorIndex={}, doCheckOwner={}, ancestorAccess=null, parentAccess=null, access=null, subAccess=null, ignoreEmptyDir={}, operationName={}, callerContext=null",
                                            fsOwner, superGroup, dirINodes != null ? dirINodes.length : 0, snapshotId, ugi != null ? ugi.getShortUserName() : null,
                                            data.resourcePath, dirAncestorIndex, doCheckOwner, ignoreEmptyDir, operationName);
                                }
                                authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, dirINodeAttrs, dirINodes, dirComponents, snapshotId, data.resourcePath, dirAncestorIndex, doCheckOwner,
                                        null, null, null, null, ignoreEmptyDir, dirAncestor, dirParent, dirINode, context);

                                LOG.debug("Default authorizer call returned : [{}]", authzStatus);
                            }

                            if (authzStatus != AuthzStatus.ALLOW) {
                                break;
                            }

                            AuthzStatus subDirAuthStatus             = AuthzStatus.NOT_DETERMINED;
                            boolean     optimizeSubAccessAuthEnabled = plugin.isOptimizeSubAccessAuthEnabled();

                            if (optimizeSubAccessAuthEnabled) {
                                subDirAuthStatus = isAccessAllowedForHierarchy(data.dir, dirAttribs, data.resourcePath, subAccess, context);
                            }

                            if (subDirAuthStatus != AuthzStatus.ALLOW) {
                                for (INode child : cList) {
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
                        authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes, pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
                                null, null, null, subAccess, ignoreEmptyDir, ancestor, parent, inode, context);
                    }
                }

                // checkOwnerAccess
                if (authzStatus == AuthzStatus.ALLOW && doCheckOwner) {
                    INodeAttributes inodeAttribs = inodeAttrs.length > 0 ? inodeAttrs[inodeAttrs.length - 1] : null;
                    String          owner        = inodeAttribs != null ? inodeAttribs.getUserName() : null;

                    authzStatus = StringUtils.equals(context.user, owner) ? AuthzStatus.ALLOW : AuthzStatus.NOT_DETERMINED;
                }
            }

            if (authzStatus == AuthzStatus.NOT_DETERMINED) {
                authzStatus = checkDefaultEnforcer(fsOwner, superGroup, ugi, inodeAttrs, inodes, pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
                        ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir, ancestor, parent, inode, context);
            }

            if (authzStatus != AuthzStatus.ALLOW) {
                FsAction action = access;

                if (action == null) {
                    if (parentAccess != null) {
                        action = parentAccess;
                    } else if (ancestorAccess != null) {
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
                    LOG.debug("Updating OptimizedAuthzContext:[{}] with authzStatus={}]", optAuthzContext, authzStatus.name());
                }

                optAuthzContext.authzStatus = authzStatus;
            }

            RangerPerfTracer.log(perf);

            LOG.debug("<== RangerAccessControlEnforcer.checkRangerPermission({}, {}, user={}) : {}", resourcePath, access, context.user, authzStatus);
        }
    }

    /*
        Check if parent or ancestor of the file being accessed is denied EXECUTE permission. If not, assume that Ranger-acls
        allowed EXECUTE access. Do not audit this authorization check if resource is a file unless access is explicitly denied
     */
    private AuthzStatus traverseOnlyCheck(INode inode, INodeAttributes[] inodeAttrs, String path, byte[][] components, INode parent, INode ancestor, int ancestorIndex, AuthzContext context) {
        LOG.debug("==> RangerAccessControlEnforcer.traverseOnlyCheck(path={}, user={}, groups={}, operationName={})", path, context.user, context.userGroups, context.operationName);

        final AuthzStatus ret;
        INode             nodeToCheck      = inode;
        INodeAttributes   nodeAttribs      = inodeAttrs.length > 0 ? inodeAttrs[inodeAttrs.length - 1] : null;
        boolean           skipAuditOnAllow = false;
        String            resourcePath     = path;

        if (nodeToCheck == null || nodeToCheck.isFile()) {
            skipAuditOnAllow = true;

            if (parent != null) {
                nodeToCheck  = parent;
                nodeAttribs  = inodeAttrs.length > 1 ? inodeAttrs[inodeAttrs.length - 2] : null;
                resourcePath = inodeAttrs.length > 0 ? DFSUtil.byteArray2PathString(components, 0, inodeAttrs.length - 1) : HDFS_ROOT_FOLDER_PATH;
            } else if (ancestor != null) {
                nodeToCheck  = ancestor;
                nodeAttribs  = inodeAttrs.length > ancestorIndex ? inodeAttrs[ancestorIndex] : null;
                resourcePath = nodeAttribs != null ? DFSUtil.byteArray2PathString(components, 0, ancestorIndex + 1) : HDFS_ROOT_FOLDER_PATH;
            }
        }

        if (nodeToCheck != null) {
            if (resourcePath.length() > 1) {
                if (resourcePath.endsWith(HDFS_ROOT_FOLDER_PATH)) {
                    resourcePath = resourcePath.substring(0, resourcePath.length() - 1);
                }
            }

            ret = isAccessAllowedForTraversal(nodeToCheck, nodeAttribs, resourcePath, skipAuditOnAllow, context, context.operationName);
        } else {
            ret = AuthzStatus.ALLOW;
        }

        LOG.debug("<== RangerAccessControlEnforcer.traverseOnlyCheck(path={}, resourcePath={}, user={}, groups={}, operationName={}) : {}", path, resourcePath, context.user, context.userGroups, context.operationName, ret);

        return ret;
    }

    private AuthzStatus isAccessAllowedForTraversal(INode inode, INodeAttributes inodeAttribs, String path, boolean skipAuditOnAllow, AuthzContext context, String operation) {
        final AuthzStatus ret;
        String            pathOwner = inodeAttribs != null ? inodeAttribs.getUserName() : null;
        FsAction          access    = FsAction.EXECUTE;

        if (pathOwner == null) {
            pathOwner = inode.getUserName();
        }

        if (RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH_ALT.equals(path)) {
            path = HDFS_ROOT_FOLDER_PATH;
        }

        LOG.debug("==> RangerAccessControlEnforcer.isAccessAllowedForTraversal({}, {}, {}, {}, {})", path, access, context.user, skipAuditOnAllow, context.operationName);

        RangerHdfsAccessRequest request = new RangerHdfsAccessRequest(inode, path, pathOwner, access, EXECUTE_ACCCESS_TYPE, operation, context.user, context.userGroups);
        RangerAccessResult      result  = plugin.isAccessAllowed(request, null);

        context.saveResult(result);

        if (result != null && result.getIsAccessDetermined() && !result.getIsAllowed()) {
            ret = AuthzStatus.DENY;
        } else {
            ret = AuthzStatus.ALLOW;
        }

        if (ret == AuthzStatus.ALLOW) {
            LOG.debug("This request is for the first time allowed by Ranger policies. request:[{}]", request);
        }

        if (ret == AuthzStatus.DENY || (!skipAuditOnAllow && result != null && result.getIsAccessDetermined())) {
            if (context.auditHandler != null) {
                context.auditHandler.processResult(result);
            }
        }

        LOG.debug("<== RangerAccessControlEnforcer.isAccessAllowedForTraversal({}, {}, {}, {}, {}): {}", path, access, context.user, skipAuditOnAllow, context.operationName, ret);

        return ret;
    }

    private AuthzStatus checkDefaultEnforcer(String fsOwner, String superGroup, UserGroupInformation ugi, INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr,
            int snapshotId, String path, int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess, FsAction access,
            FsAction subAccess, boolean ignoreEmptyDir, INode ancestor, INode parent, INode inode, AuthzContext context
    ) throws AccessControlException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAccessControlEnforcer.checkDefaultEnforcer(fsOwner={}; superGroup={}, inodesCount={}, snapshotId={}, path={}, ancestorIndex={}, doCheckOwner={}, ancestorAccess={}, parentAccess={}, access={}, subAccess={}, ignoreEmptyDir={}, isTraverseOnlyCheck={},ancestor={}, parent={}, inode={})",
                    fsOwner, superGroup, inodes != null ? inodes.length : 0, snapshotId, path, ancestorIndex, doCheckOwner, ancestorAccess, parentAccess, access, subAccess,
                    ignoreEmptyDir, context.isTraverseOnlyCheck, ancestor == null ? null : ancestor.getFullPathName(), parent == null ? null : parent.getFullPathName(), inode == null ? null : inode.getFullPathName());
        }

        AuthzStatus authzStatus = AuthzStatus.NOT_DETERMINED;

        if (plugin.isHadoopAuthEnabled() && defaultEnforcer != null) {
            RangerPerfTracer hadoopAuthPerf = null;

            if (RangerPerfTracer.isPerfTraceEnabled(PERF_HDFSAUTH_REQUEST_LOG)) {
                hadoopAuthPerf = RangerPerfTracer.getPerfTracer(PERF_HDFSAUTH_REQUEST_LOG, "RangerAccessControlEnforcer.checkDefaultEnforcer(path=" + path + ")");
            }

            try {
                defaultEnforcer.checkPermission(fsOwner, superGroup, ugi, inodeAttrs, inodes, pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner, ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir);

                authzStatus = AuthzStatus.ALLOW;
            } finally {
                if (context.auditHandler != null) {
                    INode    nodeChecked = inode;
                    FsAction action      = access;

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
                            action      = parentAccess;
                        } else if (ancestorAccess != null && ancestorAccess != FsAction.NONE) {
                            nodeChecked = ancestor;
                            action      = ancestorAccess;
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

                        plugin.evalAuditPolicies(lastResult);

                        context.auditHandler.processResult(lastResult);
                    }

                    context.auditHandler.logHadoopEvent(pathChecked, action, isAllowed);
                }

                RangerPerfTracer.log(hadoopAuthPerf);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerAccessControlEnforcer.checkDefaultEnforcer(fsOwner={}; superGroup={}, inodesCount={}, snapshotId={}, path={}, ancestorIndex={}, doCheckOwner={}, ancestorAccess={}, parentAccess={}, access={}, subAccess={}, ignoreEmptyDir={}, isTraverseOnlyCheck={},ancestor={}, parent={}, inode={}) : {}",
                    fsOwner, superGroup, inodes != null ? inodes.length : 0, snapshotId, path, ancestorIndex, doCheckOwner, ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir, context.isTraverseOnlyCheck, ancestor == null ? null : ancestor.getFullPathName(), parent == null ? null : parent.getFullPathName(), inode == null ? null : inode.getFullPathName(), authzStatus);
        }

        return authzStatus;
    }

    private AuthzStatus isAccessAllowed(INode inode, INodeAttributes inodeAttribs, String path, FsAction access, AuthzContext context) {
        AuthzStatus ret       = null;
        String      pathOwner = inodeAttribs != null ? inodeAttribs.getUserName() : null;

        if (pathOwner == null && inode != null) {
            pathOwner = inode.getUserName();
        }

        if (RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH_ALT.equals(path)) {
            path = HDFS_ROOT_FOLDER_PATH;
        }

        LOG.debug("==> RangerAccessControlEnforcer.isAccessAllowed({}, {}, {})", path, access, context.user);

        Set<String> accessTypes = ACCESS_TO_ACTIONS.get(access);

        if (accessTypes == null) {
            LOG.warn("RangerAccessControlEnforcer.isAccessAllowed({}, {}, {}): no Ranger accessType found for {}", path, access, context.user, access);

            accessTypes = ACCESS_TO_ACTIONS.get(FsAction.NONE);
        }

        if (!accessTypes.isEmpty()) {
            RangerHdfsAccessRequest request = new RangerHdfsAccessRequest(inode, path, pathOwner, access, accessTypes.iterator().next(), context.operationName, context.user, context.userGroups);

            if (accessTypes.size() > 1) {
                Set<Set<String>> allAccessTypeGroups = accessTypes.stream().map(Collections::singleton).collect(toSet());

                RangerAccessRequestUtil.setAllRequestedAccessTypeGroups(request, allAccessTypeGroups);
                RangerAccessRequestUtil.setAllRequestedAccessTypes(request.getContext(), accessTypes);

                if (accessTypes.contains(EXECUTE_ACCCESS_TYPE)) {
                    RangerAccessRequestUtil.setIgnoreIfNotDeniedAccessTypes(request.getContext(), ACCESS_TO_ACTIONS.get(FsAction.EXECUTE));
                }
            }

            RangerAccessResult result = plugin.isAccessAllowed(request, context.auditHandler);

            context.saveResult(result);

            if (result == null || !result.getIsAccessDetermined()) {
                ret = AuthzStatus.NOT_DETERMINED;
            } else if (!result.getIsAllowed()) { // explicit deny
                ret = AuthzStatus.DENY;
            } else { // allowed
                ret = AuthzStatus.ALLOW;
            }

            if (ret == AuthzStatus.ALLOW) {
                LOG.debug("This request is for the first time allowed by Ranger policies. request:[{}]", request);
            }
        }

        if (ret == null) {
            ret = AuthzStatus.NOT_DETERMINED;
        }

        LOG.debug("<== RangerAccessControlEnforcer.isAccessAllowed({}, {}, {}): {}", path, access, context.user, ret);

        return ret;
    }

    private AuthzStatus isAccessAllowedForHierarchy(INode inode, INodeAttributes inodeAttribs, String path, FsAction access, AuthzContext context) {
        AuthzStatus ret       = null;
        String      pathOwner = inodeAttribs != null ? inodeAttribs.getUserName() : null;

        if (pathOwner == null && inode != null) {
            pathOwner = inode.getUserName();
        }

        if (RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH_ALT.equals(path)) {
            path = HDFS_ROOT_FOLDER_PATH;
        }

        LOG.debug("==> RangerAccessControlEnforcer.isAccessAllowedForHierarchy({}, {}, {})", path, access, context.user);

        if (path != null) {
            Set<String> accessTypes = ACCESS_TO_ACTIONS.get(access);

            if (accessTypes == null) {
                LOG.warn("RangerAccessControlEnforcer.isAccessAllowedForHierarchy({}, {}, {}): no Ranger accessType found for {}", path, access, context.user, access);

                accessTypes = ACCESS_TO_ACTIONS.get(FsAction.NONE);
            }

            String subDirPath = path;

            if (subDirPath.charAt(subDirPath.length() - 1) != Path.SEPARATOR_CHAR) {
                subDirPath = subDirPath + Path.SEPARATOR_CHAR;
            }

            subDirPath = subDirPath + plugin.getRandomizedWildcardPathName();

            if (!accessTypes.isEmpty()) {
                RangerHdfsAccessRequest request = new RangerHdfsAccessRequest(null, subDirPath, pathOwner, access, accessTypes.iterator().next(), context.operationName, context.user, context.userGroups);

                if (accessTypes.size() > 1) {
                    Set<Set<String>> allAccessTypeGroups = accessTypes.stream().map(Collections::singleton).collect(toSet());

                    RangerAccessRequestUtil.setAllRequestedAccessTypeGroups(request, allAccessTypeGroups);
                    RangerAccessRequestUtil.setAllRequestedAccessTypes(request.getContext(), accessTypes);

                    if (accessTypes.contains(EXECUTE_ACCCESS_TYPE)) {
                        RangerAccessRequestUtil.setIgnoreIfNotDeniedAccessTypes(request.getContext(), ACCESS_TO_ACTIONS.get(FsAction.EXECUTE));
                    }
                }

                RangerAccessResult result = plugin.isAccessAllowed(request, null);

                context.saveResult(result);

                if (result == null || !result.getIsAccessDetermined()) {
                    ret = AuthzStatus.NOT_DETERMINED;
                } else if (!result.getIsAllowed()) { // explicit deny
                    ret = AuthzStatus.DENY;
                } else { // allowed
                    ret = AuthzStatus.ALLOW;
                }
            }
        }

        if (ret == null) {
            ret = AuthzStatus.NOT_DETERMINED;
        }

        LOG.debug("<== RangerAccessControlEnforcer.isAccessAllowedForHierarchy({}, {}, {}): {}", path, access, context.user, ret);

        return ret;
    }

    public enum AuthzStatus { ALLOW, DENY, NOT_DETERMINED }

    /*
        Description    : optimize() checks if the given operation is a candidate for optimizing (reducing) the number of times it is authorized
        Returns     : null, if the operation, in its current invocation, cannot be optimized.
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
                          5. OPERATION_NAME_LISTSTATUS, OPERATION_NAME_GETEZFORPATH:
                              Namenode calls this twice when listing a directory or getting the encryption zone for the directory. First invocation checks if the user has a EXECUTE access
                              on the directory, and second checks if the user has a READ_EXECUTE access on the directory. The optimized code combines these checks into a READ_EXECUTE access
                              for the first invocation.
     */
    public static class OptimizedAuthzContext {
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

    public static class AuthzContext {
        public final String                 user;
        public final Set<String>            userGroups;
        public final String                 operationName;
        private      boolean                isTraverseOnlyCheck;
        private      RangerHdfsAuditHandler auditHandler;
        private      RangerAccessResult     lastResult;

        public AuthzContext(UserGroupInformation ugi, String operationName, boolean isTraverseOnlyCheck) {
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

    private static class SubAccessData {
        final INodeDirectory    dir;
        final String            resourcePath;
        final INode[]           inodes;
        final INodeAttributes[] iNodeAttributes;

        SubAccessData(INodeDirectory dir, String resourcePath, INode[] inodes, INodeAttributes[] iNodeAttributes) {
            this.dir             = dir;
            this.resourcePath    = resourcePath;
            this.iNodeAttributes = iNodeAttributes;
            this.inodes          = inodes;
        }
    }

    static {
        Map<FsAction, Set<String>> accessToActions = new HashMap<>();

        accessToActions.put(FsAction.NONE, new TreeSet<>());
        accessToActions.put(FsAction.ALL, Stream.of(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
        accessToActions.put(FsAction.READ, Stream.of(READ_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
        accessToActions.put(FsAction.READ_WRITE, Stream.of(READ_ACCCESS_TYPE, WRITE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
        accessToActions.put(FsAction.READ_EXECUTE, Stream.of(READ_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
        accessToActions.put(FsAction.WRITE, Stream.of(WRITE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
        accessToActions.put(FsAction.WRITE_EXECUTE, Stream.of(WRITE_ACCCESS_TYPE, EXECUTE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
        accessToActions.put(FsAction.EXECUTE, Stream.of(EXECUTE_ACCCESS_TYPE).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));

        ACCESS_TO_ACTIONS = Collections.unmodifiableMap(accessToActions);
    }
}
