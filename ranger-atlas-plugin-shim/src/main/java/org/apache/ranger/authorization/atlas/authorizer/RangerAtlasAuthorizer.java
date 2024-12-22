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
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorize.AtlasSearchResultScrubRequest;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.authorize.AtlasTypesDefFilterRequest;
import org.apache.ranger.plugin.classloader.PluginClassLoaderActivator;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerAtlasAuthorizer implements AtlasAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAtlasAuthorizer.class);

    private static final String  RANGER_PLUGIN_TYPE                     = "atlas";
    private static final String  RANGER_ATLAS_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.atlas.authorizer.RangerAtlasAuthorizer";

    private AtlasAuthorizer         rangerAtlasAuthorizerImpl;
    private RangerPluginClassLoader pluginClassLoader;

    public RangerAtlasAuthorizer() {
        LOG.debug("==> RangerAtlasAuthorizer.RangerAtlasAuthorizer()");

        this.init0();

        LOG.debug("<== RangerAtlasAuthorizer.RangerAtlasAuthorizer()");
    }

    @Override
    public void init() {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init")) {
            rangerAtlasAuthorizerImpl.init();
        }
    }

    @Override
    public void cleanUp() {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "cleanUp")) {
            rangerAtlasAuthorizerImpl.cleanUp();
        }
    }

    @Override
    public boolean isAccessAllowed(AtlasAdminAccessRequest request) throws AtlasAuthorizationException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "isAccessAllowed:adminAccess")) {
            return rangerAtlasAuthorizerImpl.isAccessAllowed(request);
        }
    }

    @Override
    public boolean isAccessAllowed(AtlasEntityAccessRequest request) throws AtlasAuthorizationException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "isAccessAllowed:entityAccess")) {
            return rangerAtlasAuthorizerImpl.isAccessAllowed(request);
        }
    }

    @Override
    public boolean isAccessAllowed(AtlasTypeAccessRequest request) throws AtlasAuthorizationException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "isAccessAllowed:typeAccess")) {
            return rangerAtlasAuthorizerImpl.isAccessAllowed(request);
        }
    }

    @Override
    public boolean isAccessAllowed(AtlasRelationshipAccessRequest request) throws AtlasAuthorizationException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "isAccessAllowed:relationshipAccess")) {
            return rangerAtlasAuthorizerImpl.isAccessAllowed(request);
        }
    }

    @Override
    public void scrubSearchResults(AtlasSearchResultScrubRequest request) throws AtlasAuthorizationException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "scrubSearchResults")) {
            rangerAtlasAuthorizerImpl.scrubSearchResults(request);
        }
    }

    @Override
    public void filterTypesDef(AtlasTypesDefFilterRequest request) throws AtlasAuthorizationException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "filterTypesDef")) {
            rangerAtlasAuthorizerImpl.filterTypesDef(request);
        }
    }

    private void init0() {
        LOG.info("==> RangerAtlasPlugin.init0()");

        try {
            pluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<AtlasAuthorizer> cls = (Class<AtlasAuthorizer>) Class.forName(RANGER_ATLAS_AUTHORIZER_IMPL_CLASSNAME, true, pluginClassLoader);

            try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init0")) {
                rangerAtlasAuthorizerImpl = cls.newInstance();
            }
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerAtlasPlugin", e);
        }

        LOG.debug("<== RangerAtlasPlugin.init0()");
    }
}
