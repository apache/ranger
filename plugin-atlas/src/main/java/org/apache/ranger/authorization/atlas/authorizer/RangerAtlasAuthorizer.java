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

package org.apache.ranger.authorization.atlas.authorizer;

import java.util.Date;
import java.util.Set;

import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.apache.commons.logging.Log;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerAtlasAuthorizer implements AtlasAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAtlasAuthorizer.class);
    private static final Log PERF_ATLASAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("atlasauth.request");
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private static volatile RangerBasePlugin atlasPlugin = null;

    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAtlasPlugin.init()");
        }

        RangerBasePlugin plugin = atlasPlugin;

        if (plugin == null) {
            synchronized (RangerAtlasPlugin.class) {
                plugin = atlasPlugin;

                if (plugin == null) {
                    plugin = new RangerAtlasPlugin();
                    plugin.init();
                    plugin.setResultProcessor(new RangerDefaultAuditHandler());
                    atlasPlugin = plugin;

                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerAtlasPlugin.init()");
        }
    }

    @Override
    public boolean isAccessAllowed(AtlasAccessRequest request) throws AtlasAuthorizationException {
        boolean isAccessAllowed = true;
        if (isDebugEnabled) {
            LOG.debug("==> isAccessAllowed( " + request + " )");
        }
        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_ATLASAUTH_REQUEST_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_ATLASAUTH_REQUEST_LOG, "RangerAtlasAuthorizer.isAccessAllowed(request=" + request + ")");
        }

        String resource = request.getResource();
        String user = request.getUser();
        Set<String> userGroups = request.getUserGroups();
        String action = request.getAction().name();
        Set<AtlasResourceTypes> resourceTypes = request.getResourceTypes();
        String clientIPAddress = request.getClientIPAddress();
        String clusterName = atlasPlugin.getClusterName();

        for (AtlasResourceTypes resourceType : resourceTypes) {
            RangerAtlasAccessRequest rangerRequest =
                new RangerAtlasAccessRequest(resourceType, resource, action, user, userGroups, clientIPAddress, clusterName);
            if (isDebugEnabled) {
                LOG.debug("Creating RangerAtlasAccessRequest with values [resource : " + resource + ", user : " + user
                    + ", Groups : " + userGroups + ", action : " + action + ", resourceType : " + resourceType
                    + ", clientIP : " + clientIPAddress + ", clusterName : " + clusterName + "]");
            }
            isAccessAllowed = checkAccess(rangerRequest);
            if (!isAccessAllowed) {
                break;
            }
        }

        RangerPerfTracer.log(perf);

        if (isDebugEnabled) {
            LOG.debug("<== isAccessAllowed Returning value :: " + isAccessAllowed);
        }
        return isAccessAllowed;
    }

    private boolean checkAccess(RangerAtlasAccessRequest request) {
        boolean isAccessAllowed = false;
        RangerBasePlugin plugin = atlasPlugin;

        if (plugin != null) {
            RangerAccessResult rangerResult = plugin.isAccessAllowed(request);
            isAccessAllowed = rangerResult != null && rangerResult.getIsAllowed();
        } else {
            isAccessAllowed = false;
            LOG.warn("AtlasPlugin not initialized properly : " + plugin+"... Access blocked!!!");
        }
        return isAccessAllowed;
    }

    @Override
    public void cleanUp() {
        if (isDebugEnabled) {
            LOG.debug("==> cleanUp ");
        }
    }

    class RangerAtlasPlugin extends RangerBasePlugin {
        RangerAtlasPlugin() {
            super("atlas", "atlas");
        }
    }

}

class RangerAtlasAccessRequest extends RangerAccessRequestImpl {

    public RangerAtlasAccessRequest(AtlasResourceTypes resType, String resource, String action, String user,
        Set<String> userGroups, String clientIp, String clusterName) {
        super.setResource(new RangerAtlasResource(resType, resource));
        super.setAccessType(action);
        super.setUser(user);
        super.setUserGroups(userGroups);
        super.setAccessTime(new Date(System.currentTimeMillis()));
        super.setClientIPAddress(clientIp);
        super.setAction(action);
        super.setClusterName(clusterName);
    }

}
