/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.elasticsearch.authorizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.List;

/**
 * Shim-side delegate that loads the real {@link RangerElasticsearchAuthorizer} implementation
 * from the Elasticsearch plugin classloader. ES 7.17+ does not grant plugins
 * {@code createClassLoader}, so we avoid {@code RangerPluginClassLoader} here and rely on
 * impl jars being on the plugin classpath.
 */
public class RangerElasticsearchAuthorizerDelegate {
    private static final Logger LOG = LoggerFactory.getLogger(RangerElasticsearchAuthorizerDelegate.class);

    private static final String RANGER_ELASTICSEARCH_AUTHORIZER_IMPL_CLASSNAME =
            "org.apache.ranger.authorization.elasticsearch.authorizer.RangerElasticsearchAuthorizer";

    private RangerElasticsearchAccessControl rangerElasticsearchAccessControl;

    private final String configDir;

    public RangerElasticsearchAuthorizerDelegate(String configDir) {
        LOG.debug("==> RangerElasticsearchAuthorizerDelegate()");

        this.configDir = configDir;
        this.init();

        LOG.debug("<== RangerElasticsearchAuthorizerDelegate()");
    }

    public void init() {
        LOG.debug("==> RangerElasticsearchAuthorizerDelegate.init()");

        try {
            @SuppressWarnings("unchecked")
            Class<RangerElasticsearchAccessControl> cls = (Class<RangerElasticsearchAccessControl>) Class.forName(
                    RANGER_ELASTICSEARCH_AUTHORIZER_IMPL_CLASSNAME);

            rangerElasticsearchAccessControl = java.security.AccessController.doPrivileged(
                    (PrivilegedExceptionAction<RangerElasticsearchAccessControl>) () -> cls.getDeclaredConstructor(String.class)
                            .newInstance(configDir));
        } catch (Exception e) {
            LOG.error("Error Enabling RangerElasticsearchAuthorizer", e);
        }

        LOG.debug("<== RangerElasticsearchAuthorizerDelegate.init()");
    }

    public boolean checkPermission(String user, List<String> groups, String index, String action, String clientIPAddress) {
        LOG.debug("==> RangerElasticsearchAuthorizerDelegate.checkPermission()");

        if (rangerElasticsearchAccessControl == null) {
            LOG.warn("RangerElasticsearchAuthorizer is not initialized; denying access.");

            return false;
        }

        boolean ret;

        try {
            ret = java.security.AccessController.doPrivileged(
                    (PrivilegedExceptionAction<Boolean>) () -> rangerElasticsearchAccessControl.checkPermission(
                            user, groups, index, action, clientIPAddress));
        } catch (Exception e) {
            LOG.error("Error checking Ranger permission for user[{}] action[{}] index[{}]", user, action, index, e);

            ret = false;
        }

        LOG.debug("<== RangerElasticsearchAuthorizerDelegate.checkPermission()");

        return ret;
    }
}
