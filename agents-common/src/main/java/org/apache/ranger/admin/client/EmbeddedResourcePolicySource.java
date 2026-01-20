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

package org.apache.ranger.admin.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;

// this implementation loads policies, roles, tags and userstore from embedded resources at following paths:
//   policies:  {resource-path}/{serviceName}.json           or {resource-path}/{appId}_{serviceName}.json
//   roles:     {resource-path}/{serviceName}_roles.json     or {resource-path}/{appId}_{serviceName}_roles.json
//   tags:      {resource-path}/{serviceName}_tag.json       or {resource-path}/{appId}_{serviceName}_tag.json
//   userstore: {resource-path}/{serviceName}_userstore.json or {resource-path}/{appId}_{serviceName}_userstore.json
public class EmbeddedResourcePolicySource extends RangerPolicySource {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedResourcePolicySource.class);

    private String prefix;
    private String prefixWithAppId;

    private ServicePolicies policies;
    private RangerRoles     roles;
    private ServiceTags     tags;
    private RangerUserStore userStore;

    @Override
    public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config) {
        super.init(serviceName, appId, configPropertyPrefix, config);

        String directory = config.get(configPropertyPrefix + ".policy.source.embedded_resource.path");

        if (StringUtils.isBlank(directory)) {
            directory = "/";
        } else if (!directory.endsWith("/")) {
            directory += "/";
        }

        prefix          = directory + serviceName;
        prefixWithAppId = StringUtils.isBlank(appId) ? null : (directory + appId + "_" + serviceName);
    }

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        loadPolicies();

        return (lastKnownVersion == -1 || policies == null || policies.getPolicyVersion() == null || !policies.getPolicyVersion().equals(lastKnownVersion)) ? policies : null;
    }

    @Override
    public RangerRoles getRolesIfUpdated(long lastKnownVersion, long lastActivationTimeInMills) throws Exception {
        loadRoles();

        return (lastKnownVersion == -1 || roles == null || roles.getRoleVersion() == null || !roles.getRoleVersion().equals(lastKnownVersion)) ? roles : null;    }

    @Override
    public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        loadTags();

        return (lastKnownVersion == -1 || tags == null || tags.getTagVersion() == null || !tags.getTagVersion().equals(lastKnownVersion)) ? tags : null;
    }

    @Override
    public RangerUserStore getUserStoreIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        loadUserStore();

        return (lastKnownVersion == -1 || userStore == null || userStore.getUserStoreVersion() == null || !userStore.getUserStoreVersion().equals(lastKnownVersion)) ? userStore : null;
    }

    private void loadPolicies() throws Exception {
        if (policies == null) {
            InputStream input = getResourceStream(SUFFIX_POLICIES_FILE);

            policies = gson.fromJson(new InputStreamReader(input), ServicePolicies.class);
        }
    }

    private void loadRoles() throws Exception {
        if (roles == null) {
            InputStream input = getResourceStream(SUFFIX_ROLES_FILE);

            roles = gson.fromJson(new InputStreamReader(input), RangerRoles.class);
        }
    }

    private void loadUserStore() throws Exception {
        if (userStore == null) {
            InputStream input = getResourceStream(SUFFIX_USERSTORE_FILE);

            userStore = gson.fromJson(new InputStreamReader(input), RangerUserStore.class);
        }
    }

    private void loadTags() throws Exception {
        if (tags == null) {
            InputStream input = getResourceStream(SUFFIX_TAG_FILE);

            tags = gson.fromJson(new InputStreamReader(input), ServiceTags.class);
        }
    }

    private InputStream getResourceStream(String suffix) throws Exception {
        if (StringUtils.isBlank(prefix)) {
            throw new Exception(EmbeddedResourcePolicySource.class.getName() + ": not initialized");
        }

        try {
            InputStream src = getClass().getResourceAsStream(prefix + suffix);

            if (src == null && StringUtils.isNotBlank(prefixWithAppId)) {
                src = getClass().getResourceAsStream(prefixWithAppId + suffix);
            }

            if (src == null) {
                LOG.error("{}{}: resource not found", prefix, suffix);

                throw new Exception(prefix + suffix + ": resource not found");
            }

            return src;
        } catch (Exception excp) {
            LOG.error("{}{}: resource not found", prefix, suffix, excp);

            throw new Exception(prefix + suffix + ": resource not found", excp);
        }
    }
}
