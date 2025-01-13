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

package org.apache.ranger.authorization.kms.authorizer;

import org.apache.hadoop.crypto.key.kms.server.KMS.KMSOp;
import org.apache.hadoop.crypto.key.kms.server.KMSACLsType.Type;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyACLs;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.classloader.PluginClassLoaderActivator;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerKmsAuthorizer implements Runnable, KeyACLs {
    private static final Logger LOG = LoggerFactory.getLogger(RangerKmsAuthorizer.class);

    private static final String RANGER_PLUGIN_TYPE                   = "kms";
    private static final String RANGER_KMS_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.kms.authorizer.RangerKmsAuthorizer";

    private Runnable                implRunnable;
    private KeyACLs                 implKeyACLs;
    private RangerPluginClassLoader pluginClassLoader;

    public RangerKmsAuthorizer() {
        LOG.debug("==> RangerKmsAuthorizer.RangerKmsAuthorizer()");

        this.init();

        LOG.debug("<== RangerKmsAuthorizer.RangerKmsAuthorizer()");
    }

    @Override
    public boolean hasAccessToKey(String keyName, UserGroupInformation ugi, KeyOpType opType) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "hasAccessToKey")) {
            return implKeyACLs.hasAccessToKey(keyName, ugi, opType);
        }
    }

    @Override
    public boolean isACLPresent(String aclName, KeyOpType opType) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "isACLPresent")) {
            return implKeyACLs.isACLPresent(aclName, opType);
        }
    }

    @Override
    public void startReloader() {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "startReloader")) {
            implKeyACLs.startReloader();
        }
    }

    @Override
    public void stopReloader() {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "stopReloader")) {
            implKeyACLs.stopReloader();
        }
    }

    @Override
    public boolean hasAccess(Type aclType, UserGroupInformation ugi, String clientIp) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "hasAccess")) {
            return implKeyACLs.hasAccess(aclType, ugi, clientIp);
        }
    }

    @Override
    public void assertAccess(Type aclType, UserGroupInformation ugi, KMSOp operation, String key, String clientIp) throws AccessControlException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "assertAccess")) {
            implKeyACLs.assertAccess(aclType, ugi, operation, key, clientIp);
        }
    }

    @Override
    public void run() {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "run")) {
            implRunnable.run();
        }
    }

    private void init() {
        LOG.debug("==> RangerKmsAuthorizer.init()");

        try {
            pluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            Class<?> cls = Class.forName(RANGER_KMS_AUTHORIZER_IMPL_CLASSNAME, true, pluginClassLoader);

            try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init")) {
                Object impl = cls.newInstance();

                implRunnable = (Runnable) impl;
                implKeyACLs  = (KeyACLs) impl;
            }
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerKMSPlugin", e);
        }

        LOG.debug("<== RangerKmsAuthorizer.init()");
    }
}
