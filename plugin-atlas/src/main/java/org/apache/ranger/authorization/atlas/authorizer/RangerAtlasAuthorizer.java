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


import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RangerAtlasAuthorizer implements AtlasAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAtlasAuthorizer.class);

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
    public void cleanUp() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> cleanUp ");
        }
    }

    @Override
    public boolean isAccessAllowed(AtlasAdminAccessRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isAccessAllowed(AtlasAdminAccessRequest)");
        }

        final boolean ret;

        ret = true; // TODO: evaluate Ranger policies

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isAccessAllowed(AtlasAdminAccessRequest)");
        }

        return ret;
    }

    @Override
    public boolean isAccessAllowed(AtlasEntityAccessRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isAccessAllowed(AtlasEntityAccessRequest)");
        }

        final boolean ret;

        ret = true; // TODO: evaluate Ranger policies

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isAccessAllowed(AtlasEntityAccessRequest)");
        }

        return ret;
    }

    @Override
    public boolean isAccessAllowed(AtlasTypeAccessRequest request) throws AtlasAuthorizationException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isAccessAllowed(AtlasTypeAccessRequest)");
        }

        final boolean ret;

        ret = true; // TODO: evaluate Ranger policies

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isAccessAllowed(AtlasTypeAccessRequest)");
        }

        return ret;
    }

    class RangerAtlasPlugin extends RangerBasePlugin {
        RangerAtlasPlugin() {
            super("atlas", "atlas");
        }
    }
}
