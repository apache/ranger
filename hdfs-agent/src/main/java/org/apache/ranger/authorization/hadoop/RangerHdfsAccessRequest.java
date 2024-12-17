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

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.ipc.Server;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;

import java.net.InetAddress;
import java.util.Date;
import java.util.Set;

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
        String      ret = null;
        InetAddress ip  = Server.getRemoteIp();

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
