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
package org.apache.ranger.authorization.solr.authorizer;

import org.apache.ranger.plugin.classloader.PluginClassLoaderActivator;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.AuthorizationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class RangerSolrAuthorizer extends SearchComponent implements AuthorizationPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerSolrAuthorizer.class);

    private static final String RANGER_PLUGIN_TYPE                    = "solr";
    private static final String RANGER_SOLR_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.solr.authorizer.RangerSolrAuthorizer";

    private AuthorizationPlugin     rangerSolrAuthorizerImpl;
    private SearchComponent         rangerSearchComponentImpl;
    private RangerPluginClassLoader pluginClassLoader;

    public RangerSolrAuthorizer() {
        LOG.debug("==> RangerSolrAuthorizer.RangerSolrAuthorizer()");

        this.init0();

        LOG.debug("<== RangerSolrAuthorizer.RangerSolrAuthorizer()");
    }

    @Override
    public void close() throws IOException {
        LOG.debug("==> RangerSolrAuthorizer.close(Resource)");

        // close() to be forwarded only for authorizer instance
        boolean isAuthorizer = StringUtils.equals(super.getName(), RANGER_SOLR_AUTHORIZER_IMPL_CLASSNAME);

        if (isAuthorizer) {
            try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "close")) {
                rangerSolrAuthorizerImpl.close();
            }
        } else {
            LOG.debug("RangerSolrAuthorizer.close(): not forwarding for instance '{}'", super.getName());
        }
    }

    @Override
    public AuthorizationResponse authorize(AuthorizationContext context) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "authorize")) {
            return rangerSolrAuthorizerImpl.authorize(context);
        }
    }

    @Override
    public void init(Map<String, Object> initInfo) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init")) {
            rangerSolrAuthorizerImpl.init(initInfo);
        }
    }

    @Override
    public void prepare(ResponseBuilder responseBuilder) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "prepare")) {
            rangerSearchComponentImpl.prepare(responseBuilder);
        }
    }

    @Override
    public void process(ResponseBuilder responseBuilder) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "process")) {
            rangerSearchComponentImpl.process(responseBuilder);
        }
    }

    @Override
    public void init(NamedList args) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init")) {
            rangerSearchComponentImpl.init(args);
        }
    }

    @Override
    public String getDescription() {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "getDescription")) {
            return rangerSearchComponentImpl.getDescription();
        }
    }

    private void init0() {
        LOG.debug("==> RangerSolrAuthorizer.init0()");

        try {
            pluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<AuthorizationPlugin> cls = (Class<AuthorizationPlugin>) Class.forName(RANGER_SOLR_AUTHORIZER_IMPL_CLASSNAME, true, pluginClassLoader);

            try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init0")) {
                AuthorizationPlugin impl = cls.newInstance();

                rangerSolrAuthorizerImpl  = impl;
                rangerSearchComponentImpl = (SearchComponent) impl;
            }
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerSolrPlugin", e);
        }

        LOG.debug("<== RangerSolrAuthorizer.init0()");
    }
}
