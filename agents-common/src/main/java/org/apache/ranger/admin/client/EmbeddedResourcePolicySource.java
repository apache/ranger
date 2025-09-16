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

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;

// this implementation loads policies, roles, tags, userstore and gds info from embedded resources at following paths:
//   {resource-path}/{appId}_{serviceName}.json           -> policies
//   {resource-path}/{appId}_{serviceName}_roles.json     -> roles
//   {resource-path}/{appId}_{serviceName}_tag.json       -> tags
//   {resource-path}/{appId}_{serviceName}_userstore.json -> userstore
//   {resource-path}/{appId}_{serviceName}_gds.json       -> gds info
public class EmbeddedResourcePolicySource extends AbstractRangerAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedResourcePolicySource.class);

    private ServicePolicies policies;
    private RangerRoles     roles;
    private ServiceTags     tags;
    private RangerUserStore userStore;
    private ServiceGdsInfo  gdsInfo;

    private String policiesPath;
    private String rolesPath;
    private String tagsPath;
    private String userStorePath;
    private String gdsInfoPath;

    @Override
    public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config) {
        super.init(serviceName, appId, configPropertyPrefix, config);

        String directory  = config.get(configPropertyPrefix + ".policy.source.embedded_resource.path");
        String pathPrefix = (directory == null ? "" : directory) + "/" + appId + "_" + serviceName;

        if (!pathPrefix.startsWith("/")) {
            pathPrefix = "/" + pathPrefix;
        }

        this.policiesPath  = pathPrefix + ".json";
        this.rolesPath     = pathPrefix + "_roles.json";
        this.tagsPath      = pathPrefix + "_tag.json";
        this.userStorePath = pathPrefix + "_userstore.json";
        this.gdsInfoPath   = pathPrefix + "_gds.json";
    }

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) {
        loadPolicies();

        return (lastKnownVersion == -1 || policies == null || policies.getPolicyVersion() == null || !policies.getPolicyVersion().equals(lastKnownVersion)) ? policies : null;
    }

    @Override
    public RangerRoles getRolesIfUpdated(long lastKnownVersion, long lastActivationTimeInMills) {
        loadRoles();

        return (lastKnownVersion == -1 || roles == null || roles.getRoleVersion() == null || !roles.getRoleVersion().equals(lastKnownVersion)) ? roles : null;    }

    @Override
    public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) {
        loadTags();

        return (lastKnownVersion == -1 || tags == null || tags.getTagVersion() == null || !tags.getTagVersion().equals(lastKnownVersion)) ? tags : null;
    }

    @Override
    public RangerUserStore getUserStoreIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) {
        loadUserStore();

        return (lastKnownVersion == -1 || userStore == null || userStore.getUserStoreVersion() == null || !userStore.getUserStoreVersion().equals(lastKnownVersion)) ? userStore : null;
    }

    @Override
    public ServiceGdsInfo getGdsInfoIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) {
        loadGdsInfo();

        return (lastKnownVersion == -1 || gdsInfo == null || gdsInfo.getGdsVersion() == null || !gdsInfo.getGdsVersion().equals(lastKnownVersion)) ? gdsInfo : null;
    }

    private void loadPolicies() {
        if (policies == null) {
            try {
                InputStream input = getClass().getResourceAsStream(policiesPath);

                if (input != null) {
                    policies = gson.fromJson(new InputStreamReader(input), ServicePolicies.class);
                }
            } catch (Throwable t) {
                LOG.error("loadPolicies(): failed to load policies from {}", policiesPath, t);
            }
        }
    }

    private void loadRoles() {
        if (roles == null) {
            try {
                InputStream input = getClass().getResourceAsStream(rolesPath);

                if (input != null) {
                    roles = gson.fromJson(new InputStreamReader(input), RangerRoles.class);
                }
            } catch (Throwable t) {
                LOG.error("loadRoles(): failed to load roles from {}", rolesPath, t);
            }
        }
    }

    private void loadUserStore() {
        if (userStore == null) {
            try {
                InputStream input = getClass().getResourceAsStream(userStorePath);

                if (input != null) {
                    userStore = gson.fromJson(new InputStreamReader(input), RangerUserStore.class);
                }
            } catch (Throwable t) {
                LOG.error("loadUserStore(): failed to load userstore from {}", userStorePath, t);
            }
        }
    }

    private void loadTags() {
        if (tags == null) {
            try {
                InputStream input = getClass().getResourceAsStream(tagsPath);

                if (input != null) {
                    tags = gson.fromJson(new InputStreamReader(input), ServiceTags.class);
                }
            } catch (Throwable t) {
                LOG.error("loadTags(): failed to load tags from {}", tagsPath, t);
            }
        }
    }

    private void loadGdsInfo() {
        if (gdsInfo == null) {
            try {
                InputStream input = getClass().getResourceAsStream(gdsInfoPath);

                if (input != null) {
                    gdsInfo = JsonUtilsV2.readValue(new InputStreamReader(input), ServiceGdsInfo.class);
                }
            } catch (Throwable t) {
                LOG.error("loadGdsInfo(): failed to load gdsInfo from {}", gdsInfoPath, t);
            }
        }
    }
}
