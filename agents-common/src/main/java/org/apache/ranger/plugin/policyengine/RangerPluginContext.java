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

package org.apache.ranger.plugin.policyengine;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RangerPluginContext {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPluginContext.class);

    private final RangerPluginConfig                                                         config;
    private final Map<String, Map<RangerPolicy.RangerPolicyResource, RangerResourceMatcher>> resourceMatchers = new HashMap<>();
    private final ReentrantReadWriteLock                                                     lock             = new ReentrantReadWriteLock(true); // fair lock
    private       RangerAuthContext                                                          authContext;
    private       RangerAuthContextListener                                                  authContextListener;
    private       RangerAdminClient                                                          adminClient;

    public RangerPluginContext(RangerPluginConfig config) {
        this.config = config;
    }

    public RangerPluginConfig getConfig() {
        return config;
    }

    public String getClusterName() {
        return config.getClusterName();
    }

    public String getClusterType() {
        return config.getClusterType();
    }

    public RangerAuthContext getAuthContext() {
        return authContext;
    }

    public void setAuthContext(RangerAuthContext authContext) {
        this.authContext = authContext;
    }

    public RangerResourceMatcher getResourceMatcher(String resourceDefName, RangerPolicy.RangerPolicyResource resource) {
        LOG.debug("==> getResourceMatcher(resourceDefName={}, resource={})", resourceDefName, resource);

        RangerResourceMatcher ret = null;

        try {
            lock.readLock().lock();

            Map<RangerPolicy.RangerPolicyResource, RangerResourceMatcher> matchersForResource = resourceMatchers.get(resourceDefName);

            if (matchersForResource != null) {
                ret = matchersForResource.get(resource);
            }
        } finally {
            lock.readLock().unlock();
        }

        LOG.debug("<== getResourceMatcher(resourceDefName={}, resource={}) : ret={}", resourceDefName, resource, ret);

        return ret;
    }

    public void setResourceMatcher(String resourceDefName, RangerPolicy.RangerPolicyResource resource, RangerResourceMatcher matcher) {
        LOG.debug("==> setResourceMatcher(resourceDefName={}, resource={}, matcher={})", resourceDefName, resource, matcher);

        if (config != null && config.getPolicyEngineOptions().enableResourceMatcherReuse) {
            try {
                lock.writeLock().lock();

                Map<RangerPolicy.RangerPolicyResource, RangerResourceMatcher> matchersForResource = resourceMatchers.computeIfAbsent(resourceDefName, k -> new HashMap<>());
                matchersForResource.put(resource, matcher);
            } finally {
                lock.writeLock().unlock();
            }
        }

        LOG.debug("<== setResourceMatcher(resourceDefName={}, resource={}, matcher={})", resourceDefName, resource, matcher);
    }

    public void setAuthContextListener(RangerAuthContextListener authContextListener) {
        this.authContextListener = authContextListener;
    }

    public void notifyAuthContextChanged() {
        RangerAuthContextListener authContextListener = this.authContextListener;

        if (authContextListener != null) {
            authContextListener.contextChanged();
        }
    }

    public RangerAdminClient getAdminClient() {
        return adminClient;
    }

    public void setAdminClient(RangerAdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public RangerAdminClient createAdminClient(RangerPluginConfig pluginConfig) {
        LOG.debug("==> RangerBasePlugin.createAdminClient({}, {}, {})", pluginConfig.getServiceName(), pluginConfig.getAppId(), pluginConfig.getPropertyPrefix());

        RangerAdminClient ret              = null;
        String            propertyName     = pluginConfig.getPropertyPrefix() + ".policy.source.impl";
        String            policySourceImpl = pluginConfig.get(propertyName);

        if (StringUtils.isEmpty(policySourceImpl)) {
            LOG.debug("Value for property[{}] was null or empty. Unexpected! Will use policy source of type[{}]", propertyName, RangerAdminRESTClient.class.getName());
        } else {
            LOG.debug("Value for property[{}] was [{}].", propertyName, policySourceImpl);

            try {
                @SuppressWarnings("unchecked")
                Class<RangerAdminClient> adminClass = (Class<RangerAdminClient>) Class.forName(policySourceImpl);

                ret = adminClass.newInstance();
            } catch (Exception excp) {
                LOG.error("failed to instantiate policy source of type '{}'. Will use policy source of type '{}'", policySourceImpl, RangerAdminRESTClient.class.getName(), excp);
            }
        }

        if (ret == null) {
            ret = new RangerAdminRESTClient();
        }

        ret.init(pluginConfig.getServiceName(), pluginConfig.getAppId(), pluginConfig.getPropertyPrefix(), pluginConfig);

        LOG.debug("<== RangerBasePlugin.createAdminClient({}, {}, {}): policySourceImpl={}, client={}",
                pluginConfig.getServiceName(), pluginConfig.getAppId(), pluginConfig.getPropertyPrefix(), policySourceImpl, ret);

        setAdminClient(ret);

        return ret;
    }

    void cleanResourceMatchers() {
        LOG.debug("==> cleanResourceMatchers()");

        try {
            lock.writeLock().lock();

            resourceMatchers.clear();
        } finally {
            lock.writeLock().unlock();
        }

        LOG.debug("<== cleanResourceMatchers()");
    }
}
