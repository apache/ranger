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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.ranger.authorization.hadoop.RangerAccessControlEnforcer.OptimizedAuthzContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.HDFS_ROOT_FOLDER_PATH;

class OperationOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(OperationOptimizer.class);

    public static final String OPERATION_NAME_CREATE       = "create";
    public static final String OPERATION_NAME_DELETE       = "delete";
    public static final String OPERATION_NAME_RENAME       = "rename";
    public static final String OPERATION_NAME_LISTSTATUS   = "listStatus";
    public static final String OPERATION_NAME_MKDIRS       = "mkdirs";
    public static final String OPERATION_NAME_GETEZFORPATH = "getEZForPath";

    public static final OptimizedAuthzContext OPT_BYPASS_AUTHZ = new OptimizedAuthzContext("", FsAction.NONE, FsAction.NONE, FsAction.NONE, RangerAccessControlEnforcer.AuthzStatus.ALLOW);

    private static final Set<String> OPTIMIZED_OPERATIONS;

    private final RangerAccessControlEnforcer enforcer;
    private final String                      operationName;
    private final byte[][]                    components;
    private final INodeAttributes[]           inodeAttrs;
    private final int                         ancestorIndex;
    private final INode                       ancestor;
    private final INode                       parent;
    private final INode                       inode;
    private final FsAction                    subAccess;
    private       String                      resourcePath;
    private       FsAction                    ancestorAccess;
    private       FsAction                    parentAccess;
    private       FsAction                    access;

    OperationOptimizer(RangerAccessControlEnforcer enforcer, String operationName, String resourcePath, FsAction ancestorAccess, FsAction parentAccess, FsAction access, FsAction subAccess, byte[][] components, INodeAttributes[] inodeAttrs, int ancestorIndex, INode ancestor, INode parent, INode inode) {
        this.enforcer       = enforcer;
        this.operationName  = operationName;
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

    public static boolean isOptimizableOperation(String operationName) {
        return OPTIMIZED_OPERATIONS.contains(operationName);
    }

    OptimizedAuthzContext optimize() {
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
            LOG.debug("nodeToCheck is not a directory and access is null for a create operation! Optimization skipped");

            return null;
        }

        return getOrCreateOptimizedAuthzContext();
    }

    private OptimizedAuthzContext optimizeDeleteOp() {
        int numOfRequestedAccesses = 0;

        if (ancestorAccess != null) {
            numOfRequestedAccesses++;
        }

        if (parentAccess != null) {
            numOfRequestedAccesses++;
        }

        if (access != null) {
            numOfRequestedAccesses++;
        }

        if (subAccess != null) {
            numOfRequestedAccesses++;
        }

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
            LOG.debug("nodeToCheck is not a directory for a rename operation! Optimization skipped");

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
            LOG.debug("nodeToCheck is not a directory for a mkdirs operation! Optimization skipped");

            return null;
        }

        return getOrCreateOptimizedAuthzContext();
    }

    private OptimizedAuthzContext optimizeListStatusOp() {
        if (inode == null || inode.isFile()) {
            LOG.debug("inode is null or is a file for a listStatus/getEZForPath operation! Optimization skipped");

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
            LOG.debug("inode is null or is a file for a listStatus/getEZForPath operation! Optimization skipped");

            return null;
        } else {
            access = FsAction.READ_EXECUTE;

            return getOrCreateOptimizedAuthzContext();
        }
    }

    private INode getINodeToAuthorize() {
        INode ret             = null;
        INode nodeToAuthorize = inode;

        if (nodeToAuthorize == null || nodeToAuthorize.isFile()) {
            // Case where the authorizer is called to authorize re-creation of an existing file. This is to check if the file itself is write-able
            if (StringUtils.equals(operationName, OPERATION_NAME_CREATE) && inode != null && access != null) {
                LOG.debug("Create operation with non-null access is being authorized. authorize for write access for the file!!");
            } else {
                if (parent != null) {
                    nodeToAuthorize = parent;
                    resourcePath    = inodeAttrs.length > 0 ? DFSUtil.byteArray2PathString(components, 0, inodeAttrs.length - 1) : HDFS_ROOT_FOLDER_PATH;
                    parentAccess    = FsAction.WRITE_EXECUTE;
                } else if (ancestor != null) {
                    INodeAttributes nodeAttribs = inodeAttrs.length > ancestorIndex ? inodeAttrs[ancestorIndex] : null;

                    nodeToAuthorize = ancestor;
                    resourcePath    = nodeAttribs != null ? DFSUtil.byteArray2PathString(components, 0, ancestorIndex + 1) : HDFS_ROOT_FOLDER_PATH;
                    ancestorAccess  = FsAction.WRITE_EXECUTE;
                }

                if (resourcePath.length() > 1) {
                    if (resourcePath.endsWith(HDFS_ROOT_FOLDER_PATH)) {
                        resourcePath = resourcePath.substring(0, resourcePath.length() - 1);
                    }
                }
            }

            ret = nodeToAuthorize;
        } else {
            LOG.debug("inode is not null and it is not a file for a create/rename/mkdirs operation! Optimization skipped");
        }

        return ret;
    }

    private OptimizedAuthzContext getOrCreateOptimizedAuthzContext() {
        Map<String, OptimizedAuthzContext> pathToContextCache = enforcer.getOrCreateCache();
        OptimizedAuthzContext              opContext          = pathToContextCache.get(resourcePath);

        if (opContext == null) {
            opContext = new OptimizedAuthzContext(resourcePath, ancestorAccess, parentAccess, access, null);

            pathToContextCache.put(resourcePath, opContext);

            LOG.debug("Added OptimizedAuthzContext:[{}] to cache", opContext);
        }

        return opContext;
    }

    static {
        Set<String> optimizedOperations = new HashSet<>();

        optimizedOperations.add(OPERATION_NAME_CREATE);
        optimizedOperations.add(OPERATION_NAME_DELETE);
        optimizedOperations.add(OPERATION_NAME_RENAME);
        optimizedOperations.add(OPERATION_NAME_LISTSTATUS);
        optimizedOperations.add(OPERATION_NAME_MKDIRS);
        optimizedOperations.add(OPERATION_NAME_GETEZFORPATH);

        OPTIMIZED_OPERATIONS = Collections.unmodifiableSet(optimizedOperations);
    }
}
