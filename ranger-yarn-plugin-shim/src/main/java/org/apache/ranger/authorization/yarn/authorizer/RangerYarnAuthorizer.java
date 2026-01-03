
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

package org.apache.ranger.authorization.yarn.authorizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.ranger.plugin.classloader.PluginClassLoaderActivator;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RangerYarnAuthorizer extends YarnAuthorizationProvider {
    private static final Logger LOG = LoggerFactory.getLogger(RangerYarnAuthorizer.class);

    private static final String RANGER_PLUGIN_TYPE                    = "yarn";
    private static final String RANGER_YARN_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.yarn.authorizer.RangerYarnAuthorizer";

    private YarnAuthorizationProvider yarnAuthorizationProviderImpl;
    private RangerPluginClassLoader   pluginClassLoader;

    public RangerYarnAuthorizer() {
        LOG.debug("==> RangerYarnAuthorizer.RangerYarnAuthorizer()");

        this.init();

        LOG.debug("<== RangerYarnAuthorizer.RangerYarnAuthorizer()");
    }

    @Override
    public void init(Configuration conf) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init")) {
            yarnAuthorizationProviderImpl.init(conf);
        }
    }

    @Override
    public boolean checkPermission(AccessRequest accessRequest) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "checkPermission")) {
            return yarnAuthorizationProviderImpl.checkPermission(accessRequest);
        }
    }

    @Override
    public void setPermission(List<Permission> permissions, UserGroupInformation ugi) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "setPermission")) {
            yarnAuthorizationProviderImpl.setPermission(permissions, ugi);
        }
    }

    @Override
    public void setAdmins(AccessControlList acls, UserGroupInformation ugi) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "setAdmins")) {
            yarnAuthorizationProviderImpl.setAdmins(acls, ugi);
        }
    }

    @Override
    public boolean isAdmin(UserGroupInformation ugi) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "isAdmin")) {
            return yarnAuthorizationProviderImpl.isAdmin(ugi);
        }
    }

    private void init() {
        LOG.debug("==> RangerYarnAuthorizer.init()");

        try {
            pluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<YarnAuthorizationProvider> cls = (Class<YarnAuthorizationProvider>) Class.forName(RANGER_YARN_AUTHORIZER_IMPL_CLASSNAME, true, pluginClassLoader);

            try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init")) {
                yarnAuthorizationProviderImpl = cls.newInstance();
            }
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerYarnPlugin", e);
        }

        LOG.debug("<== RangerYarnAuthorizer.init()");
    }
}
